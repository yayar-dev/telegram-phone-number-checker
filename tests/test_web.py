from http.client import HTTPConnection
from pathlib import Path
from threading import Thread
import time
from urllib.parse import urlencode
import asyncio

import pytest

from telegram_phone_number_checker import web
from telegram_phone_number_checker.durable_web import (
    DurableValidatorApplication,
    TelegramAuthorization,
    TelethonGateway,
    create_server as create_durable_server,
)
from telegram_phone_number_checker.queue_store import QueueStore


class _BlockingGateway:
    """Keeps one claimed item active so the durable UI can be inspected."""

    def __init__(self):
        from threading import Event

        self.called = Event()
        self.release = Event()

    def import_phones(self, phones):
        self.called.set()
        self.release.wait(timeout=2)
        return {phone: {"error": "No response"} for phone in phones}

    def resolve_phone(self, phone):
        return {"error": "No response"}


class _AuthorizationClient:
    def __init__(self):
        self.code_requested = False
        self.signed_in_with = ""
        self.disconnected = False

    async def connect(self):
        return None

    async def is_user_authorized(self):
        return False

    async def send_code_request(self, phone):
        self.code_requested = phone

    async def sign_in(self, phone, code):
        self.signed_in_with = (phone, code)

    async def disconnect(self):
        self.disconnected = True


class _TwoFactorAuthorizationClient(_AuthorizationClient):
    def __init__(self):
        super().__init__()
        self.password = ""

    async def sign_in(self, phone=None, code=None, password=None):
        if password is None:
            raise web.upstream.errors.SessionPasswordNeededError(None)
        self.password = password


def test_telegram_authorization_accepts_code_from_web_memory(monkeypatch):
    monkeypatch.setenv("API_ID", "1")
    monkeypatch.setenv("API_HASH", "hash")
    monkeypatch.setenv("PHONE_NUMBER", "+79990000000")
    client = _AuthorizationClient()
    authorization = TelegramAuthorization(client_factory=lambda *args, **kwargs: client)

    assert authorization.begin("+79991112233") is True
    for _ in range(20):
        if authorization.snapshot()["state"] == "CODE_SENT":
            break
        time.sleep(0.01)
    assert authorization.snapshot()["state"] == "CODE_SENT"
    assert authorization.submit_code("12345") is True
    for _ in range(20):
        if authorization.snapshot()["state"] == "AUTHORIZED":
            break
        time.sleep(0.01)

    assert client.code_requested == "+79991112233"
    assert client.signed_in_with == ("+79991112233", "12345")
    assert client.disconnected is True
    assert authorization.snapshot()["state"] == "AUTHORIZED"


def test_telegram_authorization_accepts_two_factor_password_from_web_memory(monkeypatch):
    monkeypatch.setenv("API_ID", "1")
    monkeypatch.setenv("API_HASH", "hash")
    client = _TwoFactorAuthorizationClient()
    authorization = TelegramAuthorization(client_factory=lambda *args, **kwargs: client)

    assert authorization.begin("+79991112233") is True
    for _ in range(20):
        if authorization.snapshot()["state"] == "CODE_SENT":
            break
        time.sleep(0.01)
    assert authorization.submit_code("12345") is True
    for _ in range(20):
        if authorization.snapshot()["state"] == "PASSWORD_REQUIRED":
            break
        time.sleep(0.01)
    assert authorization.snapshot()["state"] == "PASSWORD_REQUIRED"
    assert authorization.submit_password("secret-password") is True
    for _ in range(20):
        if authorization.snapshot()["state"] == "AUTHORIZED":
            break
        time.sleep(0.01)

    assert client.password == "secret-password"
    assert authorization.snapshot()["state"] == "AUTHORIZED"


class _CodeSentAuthorization:
    def __init__(self):
        self.submitted_codes = []

    def snapshot(self):
        return {"state": "CODE_SENT", "message": "Введите код из Telegram."}

    def begin(self, phone_number=None):
        return True

    def submit_code(self, code):
        self.submitted_codes.append(code)
        return True

    def submit_password(self, password):
        return False


class _NotAuthorizedAuthorization:
    def __init__(self):
        self.phone_numbers = []

    def snapshot(self):
        return {"state": "NOT_AUTHORIZED", "message": "Подключите Telegram."}

    def begin(self, phone_number):
        self.phone_numbers.append(phone_number)
        return True

    def submit_code(self, code):
        return False

    def submit_password(self, password):
        return False


class _PasswordAuthorization:
    def __init__(self):
        self.passwords = []

    def snapshot(self):
        return {"state": "PASSWORD_REQUIRED", "message": "Введите пароль 2FA."}

    def begin(self, phone_number=None):
        return False

    def submit_code(self, code):
        return False

    def submit_password(self, password):
        self.passwords.append(password)
        return True


class _AuthorizedAuthorization:
    def snapshot(self):
        return {"state": "AUTHORIZED", "message": "Telegram подключён."}

    def begin(self, phone_number=None):
        return False

    def submit_code(self, code):
        return False

    def submit_password(self, password):
        return False


def test_durable_home_shows_telegram_phone_form_before_authorization(tmp_path):
    app = DurableValidatorApplication(
        tmp_path / "validator.db", authorization=_NotAuthorizedAuthorization()
    )

    page = app.render_home_page()

    assert 'action="/auth/begin"' in page
    assert 'name="phone_number"' in page


def test_durable_home_shows_telegram_code_form_when_authorization_waits(tmp_path):
    app = DurableValidatorApplication(
        tmp_path / "validator.db", authorization=_CodeSentAuthorization()
    )

    page = app.render_home_page()

    assert "Введите код Telegram" in page
    assert 'action="/auth/code"' in page


def test_durable_home_shows_two_factor_password_form_when_required(tmp_path):
    app = DurableValidatorApplication(
        tmp_path / "validator.db", authorization=_PasswordAuthorization()
    )

    page = app.render_home_page()

    assert "пароль 2FA" in page
    assert 'action="/auth/password"' in page


def test_durable_home_refreshes_while_telegram_verifies_otp(tmp_path):
    class _VerifyingCodeAuthorization:
        def snapshot(self):
            return {"state": "VERIFYING_CODE", "message": "Проверяем код Telegram…"}

        def begin(self, phone_number=None):
            return False

        def submit_code(self, code):
            return False

        def submit_password(self, password):
            return False

    app = DurableValidatorApplication(
        tmp_path / "validator.db", authorization=_VerifyingCodeAuthorization()
    )

    page = app.render_home_page()

    assert "Проверяем код Telegram…" in page
    assert '<meta http-equiv="refresh" content="2">' in page


def test_durable_web_submits_telegram_code_from_browser(tmp_path):
    authorization = _CodeSentAuthorization()
    app = DurableValidatorApplication(tmp_path / "validator.db", authorization=authorization)
    server = create_durable_server(app, port=0)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = HTTPConnection("127.0.0.1", server.server_port)
        connection.request(
            "POST",
            "/auth/code",
            body=urlencode({"code": "12345"}),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        response = connection.getresponse()
        response.read()

        assert response.status == 303
        assert response.getheader("Location") == "/"
        assert authorization.submitted_codes == ["12345"]
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_durable_web_submits_telegram_phone_from_browser(tmp_path):
    authorization = _NotAuthorizedAuthorization()
    app = DurableValidatorApplication(tmp_path / "validator.db", authorization=authorization)
    server = create_durable_server(app, port=0)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = HTTPConnection("127.0.0.1", server.server_port)
        connection.request(
            "POST",
            "/auth/begin",
            body=urlencode({"phone_number": "+79991112233"}),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        response = connection.getresponse()
        response.read()

        assert response.status == 303
        assert response.getheader("Location") == "/"
        assert authorization.phone_numbers == ["+79991112233"]
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_durable_web_submits_two_factor_password_from_browser(tmp_path):
    authorization = _PasswordAuthorization()
    app = DurableValidatorApplication(tmp_path / "validator.db", authorization=authorization)
    server = create_durable_server(app, port=0)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = HTTPConnection("127.0.0.1", server.server_port)
        connection.request(
            "POST",
            "/auth/password",
            body=urlencode({"password": "secret-password"}),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        response = connection.getresponse()
        response.read()

        assert response.status == 303
        assert response.getheader("Location") == "/"
        assert authorization.passwords == ["secret-password"]
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_durable_gateway_defers_contact_cleanup_to_the_durable_worker(monkeypatch):
    calls = []

    class Client:
        async def disconnect(self):
            return None

    async def login(*args):
        return Client()

    async def get_names_batch(client, phones, *, cleanup_contacts):
        calls.append(("import", phones, cleanup_contacts))
        return {phones[0]: {"id": 123, "_cleanup_contact_id": 123}}

    async def cleanup_imported_contacts(client, user_ids):
        calls.append(("cleanup", user_ids))

    monkeypatch.setenv("API_ID", "1")
    monkeypatch.setenv("API_HASH", "hash")
    monkeypatch.setenv("PHONE_NUMBER", "+79990000000")
    monkeypatch.setattr(web.upstream, "login", login)
    monkeypatch.setattr(web.upstream, "get_names_batch", get_names_batch, raising=False)
    monkeypatch.setattr(web.upstream, "cleanup_imported_contacts", cleanup_imported_contacts, raising=False)

    gateway = TelethonGateway()
    gateway.import_phones(["+79991234567"])
    gateway.cleanup_contacts([123])

    assert calls == [
        ("import", ["+79991234567"], False),
        ("cleanup", [123]),
    ]


def test_durable_job_page_shows_partial_rows_counts_and_controls_while_running(tmp_path):
    gateway = _BlockingGateway()
    app = DurableValidatorApplication(
        tmp_path / "validator.db", gateway=gateway, authorization=_AuthorizedAuthorization()
    )
    job_id = app.enqueue("+79991234567\n123")

    assert app.start(job_id) is True
    assert gateway.called.wait(timeout=2)

    page = app.render_job_page(job_id)

    assert "Обработано номеров: 1 из 2" in page
    assert "INVALID_PHONE" in page
    assert "Этап:" in page
    assert "обработано в этапе:" in page
    assert "Статусы строк" in page
    assert f'action="/jobs/{job_id}/pause"' in page
    assert f'action="/jobs/{job_id}/cancel"' in page
    assert "stop-deferred" not in page
    gateway.release.set()


def test_durable_paused_job_page_exposes_resume_and_cancel_controls(tmp_path):
    app = DurableValidatorApplication(tmp_path / "validator.db", authorization=_AuthorizedAuthorization())
    job_id = app.enqueue("+79991234567")

    assert app.start(job_id) is True
    assert app.pause(job_id) is True

    page = app.render_job_page(job_id)

    assert f'action="/jobs/{job_id}/resume"' in page
    assert f'action="/jobs/{job_id}/cancel"' in page
    assert "stop-deferred" not in page


def test_durable_application_surfaces_json_migration_failure_without_starting_worker(tmp_path):
    legacy = tmp_path / "jobs.json"
    legacy.write_text("{not json", encoding="utf-8")

    app = DurableValidatorApplication(tmp_path / "validator.db", legacy_json_path=legacy)

    assert app.startup_error
    assert app.worker_started is False
    page = app.render_home_page()
    assert "не удалось перенести" in page.lower()
    assert 'id="queue-button" type="button" disabled' in page


def test_reads_custom_port_from_environment(monkeypatch):
    monkeypatch.setenv("TELEGRAM_VALIDATOR_PORT", "8010")
    assert web.get_port() == 8010


def test_durable_web_stages_job_until_explicit_start(tmp_path):
    app = DurableValidatorApplication(tmp_path / "validator.db", authorization=_AuthorizedAuthorization())

    job_id = app.enqueue("+79991234567")

    assert app.snapshot(job_id)["started"] is False
    assert app.start(job_id) is True


def test_durable_web_keeps_job_staged_until_telegram_is_authorized(tmp_path):
    app = DurableValidatorApplication(
        tmp_path / "validator.db", authorization=_CodeSentAuthorization()
    )
    job_id = app.enqueue("+79991234567")

    assert app.start(job_id) is False
    assert app.snapshot(job_id)["started"] is False


def test_staged_job_shows_authorization_requirement_before_start(tmp_path):
    app = DurableValidatorApplication(
        tmp_path / "validator.db", authorization=_CodeSentAuthorization()
    )
    job_id = app.enqueue("+79991234567")

    page = app.render_job_page(job_id)

    assert "Авторизуйте Telegram перед запуском обработки." in page
    assert f'action="/jobs/{job_id}/start"' not in page


def test_durable_home_page_lists_a_staged_queue(tmp_path):
    app = DurableValidatorApplication(tmp_path / "validator.db")
    job_id = app.enqueue("+79991234567")

    page = app.render_home_page()

    assert f'href="/jobs/{job_id}"' in page
    assert "Сохранённые очереди" in page
    assert "STAGED" in page


def test_durable_job_shows_manual_confirmation_for_privacy_limited_row(tmp_path):
    database = tmp_path / "validator.db"
    store = QueueStore(database)
    job_id = store.create_job("+79089145093")
    store.start(job_id)
    claim = store.claim_next("worker-a")
    store.finish_claim(claim, {"error": "No response, the phone number is not on Telegram or has blocked phone lookup."})
    app = DurableValidatorApplication(database)

    page = app.render_job_page(job_id)

    assert "https://t.me/+79089145093?profile" in page
    assert f'action="/jobs/{job_id}/confirm/' in page
    assert "Подтвердить вручную" in page


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


def test_web_resolves_retry_contacts_without_reimporting_them(monkeypatch):
    batch_calls = []
    resolved_phones = []

    class Client:
        async def disconnect(self):
            return None

    async def login(*args):
        return Client()

    async def get_names_batch(client, phones):
        batch_calls.append(phones)
        return {phones[0]: {"retry": True, "error": "Retry later"}}

    async def resolve_phone(client, phone):
        resolved_phones.append(phone)
        return {"id": 123, "username": "resolved_user", "first_name": "Тест"}

    monkeypatch.setenv("API_ID", "1")
    monkeypatch.setenv("API_HASH", "hash")
    monkeypatch.setenv("PHONE_NUMBER", "+79990000000")
    monkeypatch.setattr(web.upstream, "login", login)
    monkeypatch.setattr(web.upstream, "get_names_batch", get_names_batch, raising=False)
    monkeypatch.setattr(web.upstream, "resolve_phone", resolve_phone, raising=False)
    monkeypatch.setattr(web, "BATCH_PAUSE_SECONDS", 0)

    rows = asyncio.run(web._check_phone_batch("+79991234567"))

    assert batch_calls == [["+79991234567"]]
    assert resolved_phones == ["+79991234567"]
    assert rows[0]["status"] == "FOUND"
    assert rows[0]["username"] == "resolved_user"


def test_web_retries_contacts_requested_by_telegram(monkeypatch):
    attempts = 0
    resolved_phones = []

    class Client:
        async def disconnect(self):
            return None

    async def login(*args):
        return Client()

    async def get_names_batch(client, phones):
        nonlocal attempts
        attempts += 1
        return {phones[0]: {"retry": True, "error": "Retry later"}}

    async def resolve_phone(client, phone):
        resolved_phones.append(phone)
        return {"id": 123, "username": "test_user"}

    monkeypatch.setenv("API_ID", "1")
    monkeypatch.setenv("API_HASH", "hash")
    monkeypatch.setenv("PHONE_NUMBER", "+79990000000")
    monkeypatch.setattr(web.upstream, "login", login)
    monkeypatch.setattr(web.upstream, "get_names_batch", get_names_batch, raising=False)
    monkeypatch.setattr(web.upstream, "resolve_phone", resolve_phone, raising=False)

    rows = asyncio.run(web._check_phone_batch("+79991234567"))

    assert attempts == 1
    assert resolved_phones == ["+79991234567"]
    assert rows[0]["status"] == "FOUND"


def test_web_continues_with_other_batches_while_retry_contacts_wait(monkeypatch):
    calls = []
    resolved_phones = []

    class Client:
        async def disconnect(self):
            return None

    async def login(*args):
        return Client()

    async def get_names_batch(client, phones):
        calls.append(phones)
        phone = phones[0]
        if phone == "+79990000001" and calls.count(phones) == 1:
            return {phone: {"retry": True, "error": "Retry later"}}
        return {phone: {"id": 123, "username": "test_user"}}

    async def resolve_phone(client, phone):
        resolved_phones.append(phone)
        return {"id": 123, "username": "test_user"}

    monkeypatch.setenv("API_ID", "1")
    monkeypatch.setenv("API_HASH", "hash")
    monkeypatch.setenv("PHONE_NUMBER", "+79990000000")
    monkeypatch.setattr(web.upstream, "login", login)
    monkeypatch.setattr(web.upstream, "get_names_batch", get_names_batch, raising=False)
    monkeypatch.setattr(web.upstream, "resolve_phone", resolve_phone, raising=False)
    monkeypatch.setattr(web, "BATCH_SIZE", 1)
    monkeypatch.setattr(web, "BATCH_PAUSE_SECONDS", 0)

    rows = asyncio.run(web._check_phone_batch("+79990000001\n+79990000002"))

    assert calls == [["+79990000001"], ["+79990000002"]]
    assert resolved_phones == ["+79990000001"]
    assert [row["status"] for row in rows] == ["FOUND", "FOUND"]


def test_web_resolves_multiple_deferred_batches_one_by_one(monkeypatch):
    calls = []
    pauses = []
    resolved_phones = []

    class Client:
        async def disconnect(self):
            return None

    async def login(*args):
        return Client()

    async def get_names_batch(client, phones):
        phone = phones[0]
        calls.append(phone)
        return {phone: {"retry": True, "error": "Retry later"}}

    async def resolve_phone(client, phone):
        resolved_phones.append(phone)
        return {"id": 123, "username": "test_user"}

    async def sleep(seconds):
        pauses.append(seconds)

    monkeypatch.setenv("API_ID", "1")
    monkeypatch.setenv("API_HASH", "hash")
    monkeypatch.setenv("PHONE_NUMBER", "+79990000000")
    monkeypatch.setattr(web.upstream, "login", login)
    monkeypatch.setattr(web.upstream, "get_names_batch", get_names_batch, raising=False)
    monkeypatch.setattr(web.upstream, "resolve_phone", resolve_phone, raising=False)
    monkeypatch.setattr(web, "BATCH_SIZE", 1)
    monkeypatch.setattr(web, "BATCH_PAUSE_SECONDS", 0)
    monkeypatch.setattr(web.asyncio, "sleep", sleep)

    rows = asyncio.run(web._check_phone_batch("+79990000001\n+79990000002"))

    assert calls == ["+79990000001", "+79990000002"]
    assert resolved_phones == ["+79990000001", "+79990000002"]
    assert pauses == [web.RESOLVE_PHONE_PAUSE_SECONDS]
    assert [row["status"] for row in rows] == ["FOUND", "FOUND"]


def test_web_marks_directly_resolved_absence_without_reimporting(monkeypatch):
    calls = []

    class Client:
        async def disconnect(self):
            return None

    async def login(*args):
        return Client()

    async def get_names_batch(client, phones):
        calls.append(phones)
        return {phones[0]: {"retry": True, "error": "Retry later"}}

    async def resolve_phone(client, phone):
        return {"error": "No response, the phone number is not on Telegram or has blocked phone lookup."}

    monkeypatch.setenv("API_ID", "1")
    monkeypatch.setenv("API_HASH", "hash")
    monkeypatch.setenv("PHONE_NUMBER", "+79990000000")
    monkeypatch.setattr(web.upstream, "login", login)
    monkeypatch.setattr(web.upstream, "get_names_batch", get_names_batch, raising=False)
    monkeypatch.setattr(web.upstream, "resolve_phone", resolve_phone, raising=False)
    monkeypatch.setattr(web, "BATCH_SIZE", 1)
    monkeypatch.setattr(web, "BATCH_PAUSE_SECONDS", 0)

    rows = asyncio.run(web._check_phone_batch("+79991234567"))

    assert len(calls) == 1
    assert rows[0]["status"] == "NOT_FOUND_OR_PRIVACY_RESTRICTED"


def test_web_resolves_an_entire_deferred_batch_without_scheduling_another_import(monkeypatch):
    scheduled = []

    class Client:
        async def disconnect(self):
            return None

    async def login(*args):
        return Client()

    async def get_names_batch(client, phones):
        return {phone: {"retry": True, "error": "Retry later"} for phone in phones}

    async def resolve_phone(client, phone):
        return {"id": 123, "username": "test_user"}

    async def sleep(seconds):
        return None

    monkeypatch.setenv("API_ID", "1")
    monkeypatch.setenv("API_HASH", "hash")
    monkeypatch.setenv("PHONE_NUMBER", "+79990000000")
    monkeypatch.setattr(web.upstream, "login", login)
    monkeypatch.setattr(web.upstream, "get_names_batch", get_names_batch, raising=False)
    monkeypatch.setattr(web.upstream, "resolve_phone", resolve_phone, raising=False)
    monkeypatch.setattr(web.asyncio, "sleep", sleep)

    rows = asyncio.run(
        web._check_phone_batch(
            "+79991234567\n+79990000000",
            schedule_later=lambda phones: scheduled.extend(phones),
        )
    )

    assert scheduled == []
    assert [row["status"] for row in rows] == ["FOUND", "FOUND"]


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


def test_job_state_survives_restart_and_keeps_completed_phone_results(tmp_path):
    state_path = tmp_path / "jobs.json"
    app = web.ValidatorApplication(state_path=state_path)
    job_id = app.enqueue("ООО Тест\n+79991234567\n+79990000000")
    app._update_job(
        job_id,
        started=True,
        processed=1,
        results_by_phone={"+79991234567": {"id": 123, "username": "test_user"}},
    )

    restored = web.ValidatorApplication(state_path=state_path)
    job = restored.snapshot(job_id)

    assert job["started"] is False
    assert job["results_by_phone"]["+79991234567"]["id"] == 123
    assert "перезапущен" in job["message"]


def test_deferred_csv_rows_keep_company_context():
    rows = web.deferred_rows(
        "ООО Тест\n+79991234567",
        ["+79991234567"],
    )

    assert rows[0]["company_name"] == "ООО Тест"
    assert rows[0]["status"] == "DEFERRED_NOT_CHECKED"


def test_web_queues_job_and_keeps_browser_responsive():
    calls = []

    def check_batch(raw: str):
        calls.append(raw)
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
        assert calls == []

        connection.request("GET", job_url)
        response = connection.getresponse()
        page = response.read().decode()
        assert "Запустить обработку" in page

        connection.request("POST", f"{job_url}/start")
        response = connection.getresponse()
        response.read()
        assert response.status == 303

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


def test_web_can_stop_deferred_contacts_without_dropping_them(monkeypatch):
    class Client:
        async def disconnect(self):
            return None

    async def login(*args):
        return Client()

    async def get_names_batch(client, phones):
        return {phones[0]: {"retry": True, "error": "Retry later"}}

    monkeypatch.setenv("API_ID", "1")
    monkeypatch.setenv("API_HASH", "hash")
    monkeypatch.setenv("PHONE_NUMBER", "+79990000000")
    monkeypatch.setattr(web.upstream, "login", login)
    monkeypatch.setattr(web.upstream, "get_names_batch", get_names_batch, raising=False)

    rows = asyncio.run(
        web._check_phone_batch("+79991234567", skip_deferred=lambda: True)
    )

    assert rows[0]["status"] == "DEFERRED_NOT_CHECKED"


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


def test_job_page_keeps_the_submitted_input_visible():
    page = web.render_page(
        job={
            "id": "job-1",
            "raw": 'ООО Тест\n+7 (999) 123-45-67',
            "total": 1,
            "processed": 0,
            "message": "Задача поставлена в очередь.",
            "rows": [],
            "error": "",
            "complete": False,
        }
    )

    assert 'ООО Тест\n+7 (999) 123-45-67' in page


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
        connection.request("POST", f"{job_url}/start")
        response = connection.getresponse()
        response.read()
        assert response.status == 303
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
        connection.request("POST", f"{job_url}/start")
        response = connection.getresponse()
        response.read()
        assert response.status == 303
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
        connection.request("POST", f"{job_url}/start")
        response = connection.getresponse()
        response.read()
        assert response.status == 303
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


def test_uses_durable_data_directory_from_environment(monkeypatch):
    monkeypatch.setenv("TELEGRAM_VALIDATOR_DATA_DIR", "/data")

    assert web.get_data_dir() == Path("/data")


def test_uses_configured_listen_host(monkeypatch):
    monkeypatch.setenv("TELEGRAM_VALIDATOR_HOST", "0.0.0.0")

    assert web.get_host() == "0.0.0.0"
