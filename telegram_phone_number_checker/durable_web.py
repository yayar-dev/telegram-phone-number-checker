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
        return self._call(lambda client: upstream.get_names_batch(client, phones))

    def resolve_phone(self, phone: str) -> dict:
        return self._call(lambda client: upstream.resolve_phone(client, phone))


class DurableValidatorApplication:
    def __init__(self, database_path: Path, legacy_json_path: Path | None = None, gateway=None):
        self.store = QueueStore(database_path)
        if legacy_json_path and legacy_json_path.exists():
            self.store.migrate_json(legacy_json_path)
        self.gateway = gateway or TelethonGateway()
        self.owner = uuid4().hex
        self._worker = ValidatorWorker(self.store, self.gateway, self.owner)
        Thread(target=self._run_worker, daemon=True).start()

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

    def start(self, job_id: str) -> bool:
        return self.store.start(job_id)

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
                    self.send_content(web.render_page(job=job))
                    return
                self.send_content(web.render_page())

            def do_POST(self):
                path = urlparse(self.path).path
                length = int(self.headers.get("Content-Length", "0"))
                values = parse_qs(self.rfile.read(length).decode("utf-8"))
                if path == "/check":
                    if values.get("queue_confirmation", [""])[0] != "yes":
                        self.send_content(web.render_page(error="Нажмите «Поставить в очередь», чтобы сохранить список."))
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
