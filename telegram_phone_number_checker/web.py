"""Small local web interface for the Telegram phone-number checker."""

import asyncio
import csv
import html
import inspect
import io
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable
from urllib.parse import parse_qs, urlparse

from telegram_phone_number_checker import main as upstream
from telegram_phone_number_checker.web_service import (
    is_valid_phone,
    map_lookup_result,
    parse_company_phone_batch,
)


ROW_FIELDS = (
    "company_name",
    "company_link",
    "phone",
    "status",
    "telegram_id",
    "username",
    "name",
    "telegram_link",
    "detail",
)
BATCH_SIZE = 10
BATCH_PAUSE_SECONDS = 1


class ConfigurationError(Exception):
    """Raised when the local Telegram credentials are not configured."""


def rows_to_csv(rows: list[dict[str, str]]) -> str:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=ROW_FIELDS)
    writer.writeheader()
    for row in rows:
        writer.writerow(
            {
                field: (
                    f"'{row.get(field, '')}"
                    if row.get(field, "").startswith(("=", "+", "-", "@"))
                    else row.get(field, "")
                )
                for field in ROW_FIELDS
            }
        )
    return output.getvalue()


async def _check_phone_batch(raw: str) -> list[dict[str, str]]:
    missing = [name for name in ("API_ID", "API_HASH", "PHONE_NUMBER") if not os.getenv(name)]
    if missing:
        raise ConfigurationError(f"Не заданы {', '.join(missing)} в .env")

    entries = parse_company_phone_batch(raw)
    valid_phones = list(
        dict.fromkeys(entry["phone"] for entry in entries if is_valid_phone(entry["phone"]))
    )
    results_by_phone: dict[str, dict] = {}

    if valid_phones:
        client = await upstream.login(
            os.getenv("API_ID"), os.getenv("API_HASH"), os.getenv("PHONE_NUMBER")
        )
        try:
            for start in range(0, len(valid_phones), BATCH_SIZE):
                phones = valid_phones[start : start + BATCH_SIZE]
                try:
                    results_by_phone.update(await upstream.get_names_batch(client, phones))
                except Exception as error:  # Telegram failures are reported per batch.
                    results_by_phone.update({phone: {"error": str(error)} for phone in phones})
                if start + BATCH_SIZE < len(valid_phones):
                    await asyncio.sleep(BATCH_PAUSE_SECONDS)
        finally:
            disconnected = client.disconnect()
            if inspect.isawaitable(disconnected):
                await disconnected

    rows: list[dict[str, str]] = []
    for entry in entries:
        phone = entry["phone"]
        if is_valid_phone(phone):
            row = map_lookup_result(phone, results_by_phone[phone])
        else:
            row = {
                "phone": phone,
                "status": "INVALID_PHONE",
                "telegram_id": "",
                "username": "",
                "name": "",
                "telegram_link": "",
                "detail": "Введите номер в международном формате, например +79991234567.",
            }
        rows.append({"company_name": entry["company_name"], "company_link": entry["company_link"], **row})
    return rows


def check_phone_batch(raw: str) -> list[dict[str, str]]:
    return asyncio.run(_check_phone_batch(raw))


def render_page(rows: list[dict[str, str]] | None = None, error: str = "") -> str:
    rows = rows or []
    error_html = f'<p class="error">{html.escape(error)}</p>' if error else ""
    result_html = ""
    if rows:
        body = "".join(
            "<tr>"
            + "".join(
                f"<td>{html.escape(row.get(field, ''))}</td>"
                if field not in {"company_link", "telegram_link"} or not row.get(field)
                else f'<td><a href="{html.escape(row[field], quote=True)}" target="_blank" rel="noreferrer">{html.escape(row[field])}</a></td>'
                for field in ROW_FIELDS
            )
            + "</tr>"
            for row in rows
        )
        headers = "".join(f"<th>{html.escape(field)}</th>" for field in ROW_FIELDS)
        result_html = (
            f"<p>Обработано номеров: {len(rows)}</p>"
            '<p><a class="button" href="/download.csv">Скачать CSV</a></p>'
            f"<table><thead><tr>{headers}</tr></thead><tbody>{body}</tbody></table>"
        )
    return f"""<!doctype html>
<html lang="ru"><head><meta charset="utf-8"><title>Telegram validator</title>
<style>body{{font:16px system-ui,sans-serif;max-width:960px;margin:40px auto;padding:0 20px;color:#18212b}}textarea{{width:100%;min-height:220px;box-sizing:border-box;font:inherit}}button,.button{{display:inline-block;background:#276ef1;color:white;padding:10px 14px;border:0;border-radius:6px;text-decoration:none;font:inherit;cursor:pointer}}label{{display:block;margin:16px 0 8px}}.hint{{color:#586574}}.error{{color:#b42318}}table{{border-collapse:collapse;width:100%;font-size:14px;margin-top:16px}}th,td{{border:1px solid #d0d5dd;padding:8px;text-align:left;vertical-align:top;word-break:break-word}}th{{background:#f2f4f7}}</style>
</head><body><h1>Проверка номеров Telegram</h1>
<p class="hint">Вставьте номера по одному в строке, в международном формате. Можно предварять группу номеров строкой <code>[Название компании](https://example.com)</code>. Результат «не найден» также может означать ограничение приватности.</p>
{error_html}<form method="post" action="/check"><label for="phone_numbers">Номера телефонов</label><textarea id="phone_numbers" name="phone_numbers" required placeholder="+79991234567&#10;+447911123456"></textarea><p><button type="submit">Проверить</button></p></form>{result_html}
</body></html>"""


class ValidatorApplication:
    def __init__(self, check_batch: Callable[[str], list[dict[str, str]]] = check_phone_batch):
        self.check_batch = check_batch
        self.last_rows: list[dict[str, str]] = []

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

            def do_GET(self):
                if urlparse(self.path).path == "/download.csv":
                    self.send_content(rows_to_csv(application.last_rows), "text/csv; charset=utf-8")
                    return
                self.send_content(render_page(application.last_rows))

            def do_POST(self):
                if urlparse(self.path).path != "/check":
                    self.send_error(404)
                    return
                length = int(self.headers.get("Content-Length", "0"))
                values = parse_qs(self.rfile.read(length).decode("utf-8"))
                phone_numbers = values.get("phone_numbers", [""])[0]
                try:
                    application.last_rows = application.check_batch(phone_numbers)
                    self.send_content(render_page(application.last_rows))
                except ConfigurationError as error:
                    self.send_content(render_page(error=str(error)))
                except EOFError:
                    self.send_content(
                        render_page(
                            error="Требуется авторизация Telegram в терминале. "
                            "Перезапустите проверку после входа в аккаунт."
                        )
                    )

            def log_message(self, format: str, *args):
                return

        return RequestHandler


def create_server(application: ValidatorApplication, host: str = "127.0.0.1", port: int = 8000):
    return ThreadingHTTPServer((host, port), application.handler())


def get_port() -> int:
    value = os.getenv("TELEGRAM_VALIDATOR_PORT", "8000")
    try:
        port = int(value)
    except ValueError as error:
        raise ValueError("TELEGRAM_VALIDATOR_PORT must be a number.") from error
    if not 1 <= port <= 65535:
        raise ValueError("TELEGRAM_VALIDATOR_PORT must be between 1 and 65535.")
    return port


def main() -> None:
    port = get_port()
    server = create_server(ValidatorApplication(), port=port)
    print(f"Откройте http://127.0.0.1:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
