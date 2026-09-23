from http.client import HTTPConnection
from threading import Thread
import time
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


def test_web_keeps_company_context_for_each_phone(monkeypatch):
    class Client:
        async def disconnect(self):
            return None

    async def login(*args):
        return Client()

    async def get_names_batch(client, phones):
        return {
            phone: {"id": 123, "username": "test_user", "first_name": "Тест"}
            for phone in phones
        }

    monkeypatch.setenv("API_ID", "1")
    monkeypatch.setenv("API_HASH", "hash")
    monkeypatch.setenv("PHONE_NUMBER", "+79990000000")
    monkeypatch.setattr(web.upstream, "login", login)
    monkeypatch.setattr(web.upstream, "get_names_batch", get_names_batch, raising=False)

    rows = asyncio.run(
        web._check_phone_batch(
            "[ООО Тест](https://example.test/company/1)\n+7 (999) 123-45-67"
        )
    )

    assert rows == [
        {
            "company_name": "ООО Тест",
            "company_link": "https://example.test/company/1",
            "phone": "+79991234567",
            "status": "FOUND",
            "telegram_id": "123",
            "username": "test_user",
            "name": "Тест",
            "telegram_link": "https://t.me/test_user",
            "detail": "",
        }
    ]


def test_web_waits_and_retries_after_telegram_flood_wait(monkeypatch):
    attempts = 0
    pauses = []
    progress = []

    class Client:
        async def disconnect(self):
            return None

    class FloodWait(Exception):
        seconds = 230

    async def login(*args):
        return Client()

    async def get_names_batch(client, phones):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise FloodWait()
        return {phones[0]: {"id": 123, "username": "test_user"}}

    async def sleep(seconds):
        pauses.append(seconds)

    monkeypatch.setenv("API_ID", "1")
    monkeypatch.setenv("API_HASH", "hash")
    monkeypatch.setenv("PHONE_NUMBER", "+79990000000")
    monkeypatch.setattr(web.upstream, "login", login)
    monkeypatch.setattr(web.upstream, "get_names_batch", get_names_batch, raising=False)
    monkeypatch.setattr(web, "FloodWaitError", FloodWait, raising=False)
    monkeypatch.setattr(web.asyncio, "sleep", sleep)

    rows = asyncio.run(
        web._check_phone_batch("+79991234567", progress=lambda *args: progress.append(args))
    )

    assert attempts == 2
    assert pauses == [231]
    assert rows[0]["status"] == "FOUND"
    assert any("230" in message for _, _, message in progress)


def test_web_retries_contacts_requested_by_telegram(monkeypatch):
    attempts = 0
    pauses = []

    class Client:
        async def disconnect(self):
            return None

    async def login(*args):
        return Client()

    async def get_names_batch(client, phones):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return {phones[0]: {"retry": True, "error": "Retry later"}}
        return {phones[0]: {"id": 123, "username": "test_user"}}

    async def sleep(seconds):
        pauses.append(seconds)

    monkeypatch.setenv("API_ID", "1")
    monkeypatch.setenv("API_HASH", "hash")
    monkeypatch.setenv("PHONE_NUMBER", "+79990000000")
    monkeypatch.setattr(web.upstream, "login", login)
    monkeypatch.setattr(web.upstream, "get_names_batch", get_names_batch, raising=False)
    monkeypatch.setattr(web.asyncio, "sleep", sleep)

    rows = asyncio.run(web._check_phone_batch("+79991234567"))

    assert attempts == 2
    assert pauses == [web.RETRY_CONTACT_PAUSE_SECONDS]
    assert rows[0]["status"] == "FOUND"


def test_web_splits_eleven_phones_into_batches_of_ten(monkeypatch):
    batch_calls = []

    class Client:
        async def disconnect(self):
            return None

    async def login(*args):
        return Client()

    async def get_names_batch(client, phones):
        batch_calls.append(phones)
        return {phone: {"error": "No response"} for phone in phones}

    phones = [f"+799900000{number:02d}" for number in range(1, 12)]
    monkeypatch.setenv("API_ID", "1")
    monkeypatch.setenv("API_HASH", "hash")
    monkeypatch.setenv("PHONE_NUMBER", "+79990000000")
    monkeypatch.setattr(web.upstream, "login", login)
    monkeypatch.setattr(web.upstream, "get_names_batch", get_names_batch, raising=False)
    monkeypatch.setattr(web, "BATCH_PAUSE_SECONDS", 0)

    asyncio.run(web._check_phone_batch("\n".join(phones)))

    assert batch_calls == [phones[:10], phones[10:]]


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


def test_web_queues_job_and_keeps_browser_responsive():
    def check_batch(raw: str):
        time.sleep(0.05)
        return [
            {
                "company_name": "ООО Тест",
                "company_link": "",
                "phone": "+79991234567",
                "status": "FOUND",
                "telegram_id": "123",
                "username": "test_user",
                "name": "Тест",
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
        connection.request(
            "POST",
            "/check",
            body=urlencode({"phone_numbers": "+79991234567", "queue_confirmation": "yes"}),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        response = connection.getresponse()
        response.read()
        assert response.status == 303
        job_url = response.getheader("Location")
        assert job_url.startswith("/jobs/")

        for _ in range(20):
            connection.request("GET", job_url)
            response = connection.getresponse()
            page = response.read().decode()
            if "Готово." in page:
                break
            time.sleep(0.02)
        assert response.status == 200
        assert "ООО Тест" in page
        assert "FOUND" in page
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_web_does_not_queue_without_explicit_confirmation():
    calls = []

    def check_batch(raw: str):
        calls.append(raw)
        return []

    app = web.ValidatorApplication(check_batch=check_batch)
    server = web.create_server(app, host="127.0.0.1", port=0)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = HTTPConnection("127.0.0.1", server.server_port)
        connection.request(
            "POST",
            "/check",
            body=urlencode({"phone_numbers": "+79991234567"}),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        response = connection.getresponse()
        page = response.read().decode()
        assert response.status == 200
        assert "Нажмите «Поставить в очередь»" in page
        time.sleep(0.05)
        assert calls == []
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


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
        body = urlencode({"phone_numbers": "+79991234567", "queue_confirmation": "yes"})
        connection.request(
            "POST",
            "/check",
            body=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        response = connection.getresponse()
        response.read()
        assert response.status == 303
        job_url = response.getheader("Location")
        for _ in range(20):
            connection.request("GET", job_url)
            response = connection.getresponse()
            page = response.read().decode()
            if "Готово." in page:
                break
            time.sleep(0.02)
        assert response.status == 200
        assert "FOUND" in page
        assert "https://t.me/test_user" in page

        connection.request("GET", f"/download.csv?job={job_url.rsplit('/', 1)[1]}")
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
        body = urlencode({"phone_numbers": "+79991234567", "queue_confirmation": "yes"})
        connection.request(
            "POST",
            "/check",
            body=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        response = connection.getresponse()
        response.read()
        assert response.status == 303
        job_url = response.getheader("Location")
        for _ in range(20):
            connection.request("GET", job_url)
            response = connection.getresponse()
            page = response.read().decode()
            if "Не заданы API_ID" in page:
                break
            time.sleep(0.02)
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
        body = urlencode({"phone_numbers": "+79991234567", "queue_confirmation": "yes"})
        connection.request(
            "POST",
            "/check",
            body=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        response = connection.getresponse()
        response.read()
        assert response.status == 303
        job_url = response.getheader("Location")
        for _ in range(20):
            connection.request("GET", job_url)
            response = connection.getresponse()
            page = response.read().decode()
            if "Требуется авторизация Telegram" in page:
                break
            time.sleep(0.02)
        assert response.status == 200
        assert "Требуется авторизация Telegram" in page
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
