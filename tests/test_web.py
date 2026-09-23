from http.client import HTTPConnection
from threading import Thread
from urllib.parse import urlencode
import asyncio

from telegram_phone_number_checker import web


def test_reads_custom_port_from_environment(monkeypatch):
    monkeypatch.setenv("TELEGRAM_VALIDATOR_PORT", "8010")
    assert web.get_port() == 8010


def test_web_uses_one_batch_lookup_for_valid_phone_numbers(monkeypatch):
    batch_calls = []

    class Client:
        async def disconnect(self):
            return None

    async def login(*args):
        return Client()

    async def get_names_batch(client, phones):
        batch_calls.append(phones)
        return {phone: {"error": "No response, the phone number is not on Telegram."} for phone in phones}

    monkeypatch.setenv("API_ID", "1")
    monkeypatch.setenv("API_HASH", "hash")
    monkeypatch.setenv("PHONE_NUMBER", "+79990000000")
    monkeypatch.setattr(web.upstream, "login", login)
    monkeypatch.setattr(web.upstream, "get_names_batch", get_names_batch, raising=False)

    rows = asyncio.run(web._check_phone_batch("+79991234567\n+79990000000"))

    assert batch_calls == [["+79991234567", "+79990000000"]]
    assert [row["status"] for row in rows] == [
        "NOT_FOUND_OR_PRIVACY_RESTRICTED",
        "NOT_FOUND_OR_PRIVACY_RESTRICTED",
    ]


def test_csv_neutralizes_spreadsheet_formula_cells():
    csv_text = web.rows_to_csv(
        [
            {
                "phone": "+79991234567",
                "status": "FOUND",
                "telegram_id": "123",
                "username": "=HYPERLINK(\"https://example.test\")",
                "name": "@formula",
                "telegram_link": "https://t.me/test_user",
                "detail": "-danger",
            }
        ]
    )
    assert "'+79991234567" in csv_text
    assert "'=HYPERLINK" in csv_text
    assert "'@formula" in csv_text
    assert "'-danger" in csv_text


def test_web_form_renders_results_and_downloads_csv():
    def check_batch(raw: str):
        assert raw == "+79991234567"
        return [
            {
                "phone": "+79991234567",
                "status": "FOUND",
                "telegram_id": "123",
                "username": "test_user",
                "name": "Тест Пользователь",
                "telegram_link": "https://t.me/test_user",
                "detail": "",
            }
        ]

    app = web.ValidatorApplication(check_batch=check_batch)
    server = web.create_server(app, host="127.0.0.1", port=0)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = HTTPConnection("127.0.0.1", server.server_port)
        body = urlencode({"phone_numbers": "+79991234567"})
        connection.request(
            "POST",
            "/check",
            body=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        response = connection.getresponse()
        page = response.read().decode()
        assert response.status == 200
        assert "FOUND" in page
        assert "https://t.me/test_user" in page

        connection.request("GET", "/download.csv")
        response = connection.getresponse()
        csv_text = response.read().decode()
        assert response.status == 200
        assert "telegram_link" in csv_text
        assert "https://t.me/test_user" in csv_text
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_web_form_shows_configuration_problem_without_calling_telegram():
    def check_batch(raw: str):
        raise web.ConfigurationError("Не заданы API_ID, API_HASH, PHONE_NUMBER в .env")

    app = web.ValidatorApplication(check_batch=check_batch)
    server = web.create_server(app, host="127.0.0.1", port=0)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = HTTPConnection("127.0.0.1", server.server_port)
        body = urlencode({"phone_numbers": "+79991234567"})
        connection.request(
            "POST",
            "/check",
            body=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        response = connection.getresponse()
        page = response.read().decode()
        assert response.status == 200
        assert "API_ID" in page
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_web_form_shows_authorization_problem_instead_of_dropping_response():
    def check_batch(raw: str):
        raise EOFError("EOF when reading a Telegram code")

    app = web.ValidatorApplication(check_batch=check_batch)
    server = web.create_server(app, host="127.0.0.1", port=0)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = HTTPConnection("127.0.0.1", server.server_port)
        body = urlencode({"phone_numbers": "+79991234567"})
        connection.request(
            "POST",
            "/check",
            body=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        response = connection.getresponse()
        page = response.read().decode()
        assert response.status == 200
        assert "Требуется авторизация Telegram" in page
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
