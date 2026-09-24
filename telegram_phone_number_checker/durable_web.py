"""Store-backed local web application for the durable validator queue."""

from __future__ import annotations

import asyncio
import html
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

from telegram_phone_number_checker import main as upstream
from telegram_phone_number_checker import web
from telegram_phone_number_checker.queue_store import QueueStore
from telegram_phone_number_checker.validator_worker import ValidatorWorker


class TelethonGateway:
    """Short-lived sync adapter; sessions and credentials remain owned by Telethon."""

    def _call(self, operation):
        async def run():
            client = await upstream.login(
                os.getenv("API_ID"), os.getenv("API_HASH"), os.getenv("PHONE_NUMBER")
            )
            try:
                return await operation(client)
            finally:
                await client.disconnect()
        return asyncio.run(run())

    def import_phones(self, phones: list[str]) -> dict[str, dict]:
        return self._call(
            lambda client: upstream.get_names_batch(client, phones, cleanup_contacts=False)
        )

    def resolve_phone(self, phone: str) -> dict:
        return self._call(lambda client: upstream.resolve_phone(client, phone))

    def cleanup_contacts(self, user_ids: list[int]) -> None:
        self._call(lambda client: upstream.cleanup_imported_contacts(client, user_ids))


class DurableValidatorApplication:
    def __init__(self, database_path: Path, legacy_json_path: Path | None = None, gateway=None):
        self.store = QueueStore(database_path)
        self.gateway = gateway or TelethonGateway()
        self.owner = uuid4().hex
        self._worker = ValidatorWorker(self.store, self.gateway, self.owner)
        self.startup_error = ""
        self.worker_started = False
        if legacy_json_path and legacy_json_path.exists():
            try:
                self.store.migrate_json(legacy_json_path)
            except Exception as error:
                self.startup_error = (
                    "Не удалось перенести сохранённую очередь из JSON: "
                    f"{error}. Исправьте или восстановите файл и перезапустите сервис."
                )
            else:
                diagnostic = self.store.migration_diagnostic()
                if diagnostic["active"]:
                    self.startup_error = str(diagnostic["message"])
        if not self.startup_error:
            Thread(target=self._run_worker, daemon=True).start()
            self.worker_started = True

    def _run_worker(self) -> None:
        while True:
            try:
                did_work = self._worker.run_once()
                self.store.finalize_ready_jobs()
            except Exception:
                did_work = False
            time.sleep(0.2 if did_work else 1)

    def enqueue(self, raw: str) -> str:
        return self.store.create_job(raw)

    def snapshot(self, job_id: str) -> dict | None:
        job = self.store.snapshot(job_id)
        if not job:
            return None
        state = job["state"]
        return {
            **job,
            "raw": job["raw_input"],
            "complete": state in {"COMPLETED", "CANCELLED", "FAILED"},
            "started": state in {"QUEUED", "RUNNING", "WAITING_RATE_LIMIT"},
            "waiting_until": job["resume_after"],
            "error": job["last_error"],
            "rows": self.store.rows(job_id),
            "deferred_phones": [],
            "eta_seconds": None,
        }

    def render_home_page(self) -> str:
        return _render_page(error=self.startup_error, accept_jobs=not self.startup_error)

    def render_job_page(self, job_id: str) -> str:
        job = self.snapshot(job_id)
        return (
            _render_page(job=job, error=self.startup_error, accept_jobs=not self.startup_error)
            if job
            else ""
        )

    def start(self, job_id: str) -> bool:
        return not self.startup_error and self.store.start(job_id)

    def pause(self, job_id: str) -> bool:
        return self.store.pause(job_id)

    def cancel(self, job_id: str) -> bool:
        self.store.cancel(job_id)
        return True

    def handler(self):
        application = self

        class RequestHandler(BaseHTTPRequestHandler):
            def send_content(self, content: str, content_type: str = "text/html; charset=utf-8"):
                encoded = content.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

            def redirect(self, location: str):
                self.send_response(303)
                self.send_header("Location", location)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def do_GET(self):
                parsed = urlparse(self.path)
                if parsed.path == "/download.csv":
                    job_id = parse_qs(parsed.query).get("job", [""])[0]
                    job = application.snapshot(job_id)
                    if not job:
                        self.send_error(404)
                        return
                    self.send_content(web.rows_to_csv(job["rows"]), "text/csv; charset=utf-8")
                    return
                if parsed.path.startswith("/jobs/"):
                    job = application.snapshot(parsed.path.removeprefix("/jobs/"))
                    if not job:
                        self.send_error(404)
                        return
                    self.send_content(application.render_job_page(job["id"]))
                    return
                self.send_content(application.render_home_page())

            def do_POST(self):
                path = urlparse(self.path).path
                length = int(self.headers.get("Content-Length", "0"))
                values = parse_qs(self.rfile.read(length).decode("utf-8"))
                if path == "/check":
                    if application.startup_error:
                        self.send_content(application.render_home_page())
                        return
                    if values.get("queue_confirmation", [""])[0] != "yes":
                        self.send_content(_render_page(error="Нажмите «Поставить в очередь», чтобы сохранить список."))
                        return
                    self.redirect(f"/jobs/{application.enqueue(values.get('phone_numbers', [''])[0])}")
                    return
                parts = path.removeprefix("/jobs/").split("/")
                if len(parts) != 2 or not parts[0]:
                    self.send_error(404)
                    return
                job_id, action = parts
                actions = {"start": application.start, "pause": application.pause, "resume": application.start, "cancel": application.cancel}
                if action in actions and actions[action](job_id):
                    self.redirect(f"/jobs/{job_id}")
                    return
                self.send_error(404)

            def log_message(self, format: str, *args):
                return

        return RequestHandler


def create_server(application: DurableValidatorApplication, port: int) -> ThreadingHTTPServer:
    return ThreadingHTTPServer(("127.0.0.1", port), application.handler())


def _render_page(job: dict | None = None, error: str = "", accept_jobs: bool = True) -> str:
    """Render the durable-queue UI without inheriting the legacy job controls."""
    raw = job.get("raw", "") if job else ""
    job_html = ""
    results_html = ""
    refresh_html = ""

    if job:
        state = job["state"]
        processed = job["processed"]
        total = job["total"]
        message = html.escape(job.get("message", ""))
        controls = ""
        if state == "STAGED":
            controls = _action_form(job["id"], "start", "Запустить обработку")
        elif state == "PAUSED":
            controls = (
                _action_form(job["id"], "resume", "Возобновить обработку")
                + _action_form(job["id"], "cancel", "Отменить обработку", secondary=True)
            )
        elif state in {"QUEUED", "RUNNING", "WAITING_RATE_LIMIT"}:
            controls = (
                _action_form(job["id"], "pause", "Приостановить")
                + _action_form(job["id"], "cancel", "Отменить обработку", secondary=True)
            )
            refresh_html = '<meta http-equiv="refresh" content="2">'
        waiting = ""
        if job.get("waiting_until"):
            remaining = max(0, round(job["waiting_until"] - time.time()))
            waiting = (
                f"<p>Ожидание Telegram: следующая попытка через {remaining // 60} мин. "
                f"{remaining % 60} сек.</p>"
            )
        job_html = (
            f"<p>Обработано номеров: {processed} из {total}.</p>"
            f"<p>Найдено аккаунтов: {job.get('confirmed', 0)}.</p>"
            f"{waiting}<p class=\"hint\">{message}</p>{controls}"
        )
        results_html = _render_results(job["rows"], processed, total, job["id"])

    error_html = f'<p class="error">{html.escape(error)}</p>' if error else ""
    disabled = " disabled" if not accept_jobs else ""
    return f"""<!doctype html>
<html lang="ru"><head><meta charset="utf-8"><title>Telegram validator</title>
{refresh_html}
<style>body{{font:16px system-ui,sans-serif;max-width:960px;margin:40px auto;padding:0 20px;color:#18212b}}textarea{{width:100%;min-height:220px;box-sizing:border-box;font:inherit}}button,.button{{display:inline-block;background:#276ef1;color:white;padding:10px 14px;border:0;border-radius:6px;text-decoration:none;font:inherit;cursor:pointer;margin-right:8px}}.secondary{{background:#667085}}label{{display:block;margin:16px 0 8px}}.hint{{color:#586574}}.error{{color:#b42318}}table{{border-collapse:collapse;width:100%;font-size:14px;margin-top:16px}}th,td{{border:1px solid #d0d5dd;padding:8px;text-align:left;vertical-align:top;word-break:break-word}}th{{background:#f2f4f7}}.action{{display:inline}}</style>
</head><body><h1>Проверка номеров Telegram</h1>
<p class="hint">Вставьте номера по одному в строке. Перед группой укажите компанию обычной строкой или как <code>[Название](https://example.com)</code>.</p>
{error_html}<form id="queue-form" method="post" action="/check"><label for="phone_numbers">Компании и номера</label><textarea id="phone_numbers" name="phone_numbers" required placeholder="ООО Пример&#10;+79991234567&#10;+447911123456"{disabled}>{html.escape(raw)}</textarea><input id="queue-confirmation" type="hidden" name="queue_confirmation" value=""><p><button id="queue-button" type="button"{disabled}>Поставить в очередь</button></p></form>{job_html}{results_html}
<script>const form=document.getElementById('queue-form');const confirmation=document.getElementById('queue-confirmation');document.getElementById('queue-button').addEventListener('click',()=>{{confirmation.value='yes';form.requestSubmit();}});form.addEventListener('submit',(event)=>{{if(confirmation.value!=='yes')event.preventDefault();}});</script>
</body></html>"""


def _action_form(job_id: str, action: str, label: str, secondary: bool = False) -> str:
    css_class = " secondary" if secondary else ""
    return (
        f'<form class="action" method="post" action="/jobs/{html.escape(job_id, quote=True)}/{action}">'
        f'<button class="{css_class.strip()}" type="submit">{html.escape(label)}</button></form>'
    )


def _render_results(rows: list[dict[str, str]], processed: int, total: int, job_id: str) -> str:
    if not rows:
        return ""
    headers = "".join(f"<th>{html.escape(field)}</th>" for field in web.ROW_FIELDS)
    body = "".join(
        "<tr>"
        + "".join(
            f"<td>{html.escape(row.get(field, ''))}</td>"
            if field not in {"company_link", "telegram_link"} or not row.get(field)
            else f'<td><a href="{html.escape(row[field], quote=True)}" target="_blank" rel="noreferrer">{html.escape(row[field])}</a></td>'
            for field in web.ROW_FIELDS
        )
        + "</tr>"
        for row in rows
    )
    return (
        f"<p class=\"hint\">Показаны текущие строки: {processed} завершено из {total}.</p>"
        f'<p><a class="button" href="/download.csv?job={html.escape(job_id, quote=True)}">Скачать текущий CSV</a></p>'
        f"<table><thead><tr>{headers}</tr></thead><tbody>{body}</tbody></table>"
    )
