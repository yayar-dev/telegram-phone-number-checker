# Persistent User-Session Validator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Validate pasted phone-number batches through one persistent, browser-authorized Telegram user session, without treating privacy-limited API responses as account absence.

**Architecture:** Replace the short-lived `TelethonGateway` with a single-owner asyncio session gateway that serializes all Telegram operations through one connected Telethon user client. Preserve the SQLite queue as the authority for claims, FloodWait, retries, cleanup, and job recovery; change `retry_contacts` from a direct-lookup fallback into a persisted import retry. Add a paused-job migration and UI counts that separate confirmed, pending retry, and privacy-ambiguous results.

**Tech Stack:** Python 3.12, Telethon 1.33, SQLite, stdlib `asyncio`/`threading`/`http.server`, pytest, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-10-05-user-session-validator-design.md`

## Global Constraints

- Use the one browser-authorized Telegram **user account** stored at `/data/telegram-session`; do not add bots, additional accounts, or Telegram Desktop UI automation.
- Keep the service bound to `127.0.0.1` through Compose and keep queue state in `tg-validatorForSelf_data`.
- OTP and 2FA values remain process-memory-only; queue rows, events, CSV, and logs must not contain them.
- Preserve Telegram `FloodWait` delays exactly; performance improvements must not suppress, parallelize around, or bypass them.
- `UNRESOLVABLE_OR_PRIVACY` remains an API ambiguity, never a claim that a Telegram account is absent.
- Preserve the existing paused job until the operator explicitly chooses its migration action.

## Review Focus

- A revoked or missing Telethon session must leave the job staged/paused with a clear authorization state, never invoke terminal `input()`; cover in Task 1.
- A transport failure while the reusable client is connected must reconnect once and retain the original durable claim; cover in Task 1.
- A `FloodWaitError` during import or cleanup must retain its exact server delay after restart; cover in Task 2.
- `retry_contacts` must never call `resolvePhone` or produce a terminal privacy row before an import response; cover in Task 2.
- Migration must not overwrite rows manually confirmed by the operator or any API-confirmed rows; cover in Task 3.

---

## File Structure

- Create `telegram_phone_number_checker/session_gateway.py`: owns the single asyncio loop and persistent Telethon user client; exposes the existing synchronous gateway protocol.
- Modify `telegram_phone_number_checker/durable_web.py`: wires the session gateway into the application, closes it at shutdown, exposes migration action, and renders truthful result counts.
- Modify `telegram_phone_number_checker/validator_worker.py`: defers `retry_contacts` back to import instead of invoking `resolvePhone`; raises import batch size only after persisted stable success.
- Modify `telegram_phone_number_checker/queue_store.py`: persists import-retry state, adaptive-success counter, migration audit event, and summary counts.
- Modify `telegram_phone_number_checker/web.py`: creates and closes the durable application around the HTTP server lifetime.
- Modify `tests/test_validator_worker.py`, `tests/test_queue_store.py`, and `tests/test_web.py`; create `tests/test_session_gateway.py`.
- Modify `README.md`: describe the user-session lifecycle, the meaning of each result category, and the paused-job migration control.

### Task 1: Persistent user-session gateway

**Files:**
- Create: `telegram_phone_number_checker/session_gateway.py`
- Modify: `telegram_phone_number_checker/durable_web.py:135-158,161-195`
- Modify: `telegram_phone_number_checker/web.py:760-779`
- Create: `tests/test_session_gateway.py`
- Modify: `tests/test_web.py`

**Interfaces:**
- Consumes: `upstream.get_session_name(phone_number)`, `upstream.get_names_batch`, `upstream.resolve_phone`, `upstream.cleanup_imported_contacts`, and environment API credentials.
- Produces: `PersistentTelethonGateway.import_phones(phones: list[str]) -> dict[str, dict]`, `resolve_phone(phone: str) -> dict`, `cleanup_contacts(user_ids: list[int]) -> None`, and `close() -> None`.
- Produces: `UserSessionUnavailableError`, raised when the persisted client session is not authorized.

- [ ] **Step 1: Write failing gateway tests**

```python
def test_gateway_reuses_one_connected_client_for_two_import_calls():
    gateway = PersistentTelethonGateway(client_factory=fake_factory)
    gateway.import_phones(["+79990000001"])
    gateway.import_phones(["+79990000002"])
    assert fake_factory.created == 1
    assert fake_client.connect_calls == 1


def test_gateway_reconnects_once_after_transport_failure_without_terminal_login():
    gateway = PersistentTelethonGateway(client_factory=fake_factory)
    assert gateway.import_phones(["+79990000001"])["+79990000001"]["id"] == 1
    assert fake_factory.created == 2
```

Add a test that an unauthorized client raises `UserSessionUnavailableError` and proves `upstream.login` was not called.

- [ ] **Step 2: Run the new gateway tests to verify they fail**

Run: `/Users/jared/.codex/worktrees/local-web-validator/tg-validator/.venv/bin/python -m pytest -q tests/test_session_gateway.py`

Expected: FAIL because `PersistentTelethonGateway` does not exist.

- [ ] **Step 3: Implement `PersistentTelethonGateway`**

Implement the exact protocol above in a dedicated background asyncio loop. Each public method submits one coroutine to that loop and waits for its result; the loop owns the only live Telethon client. Create the client from `TELEGRAM_SESSION_PATH`, `API_ID`, `API_HASH`, and optional `PROXY`; call `connect()` and `is_user_authorized()` before the first request. Recreate the client once after an `OSError` or Telethon transport disconnect, then replay only that uncompleted operation. Do not call `upstream.login` or any terminal prompt. `close()` disconnects the client and stops the loop.

- [ ] **Step 4: Replace `TelethonGateway` application wiring**

Instantiate `PersistentTelethonGateway` in `DurableValidatorApplication` when no test gateway is supplied. Add `DurableValidatorApplication.close() -> None`; call it from `web.main()` after `server_close()`. Keep test injection compatible.

- [ ] **Step 5: Run targeted tests and commit**

Run: `/Users/jared/.codex/worktrees/local-web-validator/tg-validator/.venv/bin/python -m pytest -q tests/test_session_gateway.py tests/test_web.py`

Expected: PASS.

```bash
git add telegram_phone_number_checker/session_gateway.py telegram_phone_number_checker/durable_web.py telegram_phone_number_checker/web.py tests/test_session_gateway.py tests/test_web.py
git commit -m "feat: reuse authenticated Telegram user session"
```

### Task 2: Preserve import retries and adaptive throughput

**Files:**
- Modify: `telegram_phone_number_checker/queue_store.py:466-523,592-622,734-742`
- Modify: `telegram_phone_number_checker/validator_worker.py:16-17,57-90,145-158`
- Modify: `tests/test_queue_store.py`
- Modify: `tests/test_validator_worker.py`

**Interfaces:**
- Consumes: `ValidatorWorker._run_import_batch(claims)` and `QueueStore.claim_batch(owner, limit, account_key)`.
- Produces: `QueueStore.defer_import_retries(claims: list[dict]) -> None`, `QueueStore.record_import_batch_success(job_id: str) -> None`, and `QueueStore.record_import_retry(job_id: str) -> None`.
- Produces: visible nonterminal status `PENDING_RETRY`; `claim_batch` accepts it when `next_attempt_at` is due.

- [ ] **Step 1: Write failing retry-routing tests**

```python
def test_retry_contacts_return_to_import_queue_without_resolve_phone():
    worker = ValidatorWorker(store, gateway, "owner")
    worker.run_once()
    assert gateway.resolve_calls == []
    assert store.rows(job_id)[0]["status"] == "PENDING_RETRY"
    assert store.rows(job_id)[0]["method"] == "IMPORT"


def test_retry_contact_reduces_batch_then_successes_restore_batch_size():
    # 10 -> 5 -> 1 after two retry responses; five successful import batches restore 1 -> 5.
    assert store.snapshot(job_id)["import_batch_size"] == 5
```

Add a `FloodWaitError(seconds=230)` test proving the persisted `resume_after` equals `now + 230` and that no retry state bypasses it.

- [ ] **Step 2: Run retry-routing tests to verify they fail**

Run: `/Users/jared/.codex/worktrees/local-web-validator/tg-validator/.venv/bin/python -m pytest -q tests/test_validator_worker.py tests/test_queue_store.py -k 'retry or batch or flood'`

Expected: FAIL because retry contacts currently become `DIRECT_LOOKUP`.

- [ ] **Step 3: Implement persisted import retry behavior**

Add `PENDING_RETRY` to the ready/duplicate-safe SQL predicates. `defer_import_retries` keeps `method='IMPORT'`, sets `next_attempt_at` to `now + 60` seconds for the first retry, `+300` seconds for the second, and `+900` seconds for the third and later retries, clears the lease, writes one `IMPORT_RETRY` event, and reduces the job batch size through the existing 10 → 5 → 1 sequence.

Replace `ValidatorWorker._move_to_direct_lookup` with `QueueStore.defer_import_retries`; do not invoke `resolve_phone` for `retry_contacts`. Add `import_success_streak INTEGER NOT NULL DEFAULT 0` to `jobs`, reset it on retry or FloodWait, and raise a job batch size after five successful import batches: 1 → 5 and 5 → 10. `record_import_batch_success` runs only after an import batch that contained no retry contacts.

- [ ] **Step 4: Run targeted tests and commit**

Run: `/Users/jared/.codex/worktrees/local-web-validator/tg-validator/.venv/bin/python -m pytest -q tests/test_validator_worker.py tests/test_queue_store.py`

Expected: PASS.

```bash
git add telegram_phone_number_checker/queue_store.py telegram_phone_number_checker/validator_worker.py tests/test_queue_store.py tests/test_validator_worker.py
git commit -m "fix: retry Telegram contact imports safely"
```

### Task 3: Migrate privacy-ambiguous direct rows only on operator action

**Files:**
- Modify: `telegram_phone_number_checker/queue_store.py:291-348,849-924`
- Modify: `telegram_phone_number_checker/durable_web.py:237-300,380-470`
- Modify: `tests/test_queue_store.py`
- Modify: `tests/test_web.py`

**Interfaces:**
- Consumes: paused job id.
- Produces: `QueueStore.requeue_privacy_direct_rows(job_id: str) -> int` and `DurableValidatorApplication.requeue_privacy_direct_rows(job_id: str) -> int`.
- Produces: `POST /jobs/<job_id>/requeue-privacy`, valid only for a `PAUSED` job.

- [ ] **Step 1: Write failing migration tests**

```python
def test_requeue_privacy_direct_rows_preserves_manual_confirmation_and_api_history():
    moved = store.requeue_privacy_direct_rows(job_id)
    assert moved == 1
    assert migrated["status"] == "PENDING_RETRY"
    assert migrated["method"] == "IMPORT"
    assert migrated["api_status"] == "UNRESOLVABLE_OR_PRIVACY"
    assert manually_confirmed_row["status"] == "UNRESOLVABLE_OR_PRIVACY"
```

Add an HTTP test showing that a paused job renders a `requeue-privacy` action and redirects back to that job after a successful POST; a running job must return 404 and move no rows.

- [ ] **Step 2: Run migration tests to verify they fail**

Run: `/Users/jared/.codex/worktrees/local-web-validator/tg-validator/.venv/bin/python -m pytest -q tests/test_queue_store.py tests/test_web.py -k 'requeue_privacy'`

Expected: FAIL because the migration action does not exist.

- [ ] **Step 3: Implement auditable migration**

Within one SQLite transaction, require `jobs.state='PAUSED'`. Move only rows with `status='UNRESOLVABLE_OR_PRIVACY'`, `method='DIRECT_LOOKUP'`, and `manual_confirmation!='MANUAL_CHAT_CONFIRMED'` to `status='PENDING_RETRY'`, `method='IMPORT'`, and `next_attempt_at=now + 60`; keep `api_status` as `UNRESOLVABLE_OR_PRIVACY`, clear current display fields, and append one `MIGRATE_PRIVACY_DIRECT` job event with the moved count. Do not change API-confirmed rows or manually confirmed rows.

Render a button labelled `Вернуть API-неопределённые номера в очередь импорта` only for paused jobs with eligible rows. Show the moved count after redirect. Do not automatically resume the job.

- [ ] **Step 4: Run targeted tests and commit**

Run: `/Users/jared/.codex/worktrees/local-web-validator/tg-validator/.venv/bin/python -m pytest -q tests/test_queue_store.py tests/test_web.py`

Expected: PASS.

```bash
git add telegram_phone_number_checker/queue_store.py telegram_phone_number_checker/durable_web.py tests/test_queue_store.py tests/test_web.py
git commit -m "feat: requeue privacy-limited user-session checks"
```

### Task 4: Make queue results and speed claims truthful

**Files:**
- Modify: `telegram_phone_number_checker/queue_store.py:252-304`
- Modify: `telegram_phone_number_checker/durable_web.py:200-214,380-440`
- Modify: `tests/test_queue_store.py`
- Modify: `tests/test_web.py`

**Interfaces:**
- Produces: `QueueStore.result_summary(job_id: str) -> dict[str, int]` with `api_confirmed`, `pending_retry`, `privacy_ambiguous`, and `pending_import`.
- Consumes: the result summary in `DurableValidatorApplication.snapshot(job_id)` and `_render_page`.

- [ ] **Step 1: Write failing summary and UI tests**

```python
def test_result_summary_separates_api_matches_from_privacy_ambiguity():
    assert store.result_summary(job_id) == {
        "api_confirmed": 2,
        "pending_retry": 3,
        "privacy_ambiguous": 4,
        "pending_import": 5,
    }
```

Add a page test asserting `Подтверждено API: 2`, `API не определил из-за приватности: 4`, and `Повторный импорт ожидают: 3`; assert that no text equates the privacy count to absence.

- [ ] **Step 2: Run summary tests to verify they fail**

Run: `/Users/jared/.codex/worktrees/local-web-validator/tg-validator/.venv/bin/python -m pytest -q tests/test_queue_store.py tests/test_web.py -k 'result_summary or privacy_ambiguity'`

Expected: FAIL because `result_summary` is missing.

- [ ] **Step 3: Implement result summary and UI copy**

Count `FOUND` and `FOUND_NO_USERNAME` as `api_confirmed`; count `PENDING_RETRY` separately; count `UNRESOLVABLE_OR_PRIVACY` separately; count pending import rows whose method is `IMPORT`. Render these counts next to existing progress and retain the full status table/CSV. Keep ETA derived only from persisted queue state and append `без гарантии прохождения серверного лимита Telegram` whenever a rate wait or retry set is present.

- [ ] **Step 4: Run targeted tests and commit**

Run: `/Users/jared/.codex/worktrees/local-web-validator/tg-validator/.venv/bin/python -m pytest -q tests/test_queue_store.py tests/test_web.py`

Expected: PASS.

```bash
git add telegram_phone_number_checker/queue_store.py telegram_phone_number_checker/durable_web.py tests/test_queue_store.py tests/test_web.py
git commit -m "feat: clarify API and privacy validation results"
```

### Task 5: End-to-end verification and operator documentation

**Files:**
- Modify: `README.md`
- Modify: `tests/test_web.py`
- Modify: `tests/test_session_gateway.py`

**Interfaces:**
- Consumes: Tasks 1-4 runtime interfaces and Docker Compose configuration.
- Produces: documented operator flow: browser authorization, paused-job migration, resume, rate-wait interpretation, manual confirmation.

- [ ] **Step 1: Write failing lifecycle smoke test**

```python
def test_paused_job_migrates_then_resumes_through_persistent_authorized_gateway():
    # Assert no new client is created across migration, import retry, and cleanup.
    assert fake_factory.created == 1
    assert app.snapshot(job_id)["result_summary"]["api_confirmed"] == 1
```

- [ ] **Step 2: Run the lifecycle test to verify it fails**

Run: `/Users/jared/.codex/worktrees/local-web-validator/tg-validator/.venv/bin/python -m pytest -q tests/test_web.py tests/test_session_gateway.py -k lifecycle`

Expected: FAIL until Tasks 1-4 are integrated.

- [ ] **Step 3: Document the operator flow**

Document the user-account session lifecycle, explicit migration button, difference between API confirmation and privacy ambiguity, manual confirmation, and `docker compose stop`/`start`. Do not claim that a Telegram profile is absent when the API returns a privacy-limited result.

- [ ] **Step 4: Run full verification, Docker smoke, and commit**

Run:

```bash
/Users/jared/.codex/worktrees/local-web-validator/tg-validator/.venv/bin/python -m pytest -q
docker compose up -d --build
curl --silent --show-error --fail http://127.0.0.1:8010/ > /tmp/tg-validator-home.html
rg -n "Подтверждено API|Вернуть API-неопределённые" /tmp/tg-validator-home.html
```

Expected: pytest passes, Compose reports `tg-validatorForSelf` running, local HTTP returns 200, and the page contains the new labels when rendering a paused migrated job fixture.

```bash
git add README.md tests/test_web.py tests/test_session_gateway.py
git commit -m "docs: explain persistent user-session validation"
```

## Self-Review

- Spec coverage: Task 1 covers the persistent user session; Task 2 covers rate-aware imports; Task 3 covers explicit migration; Task 4 covers truthful result categories; Task 5 covers integration and operator documentation.
- Type consistency: the gateway preserves the existing worker protocol; new QueueStore and application methods are named consistently across later tasks.
- Review Focus: every listed failure mode has a named test in its owning task.
- Proportion: no task embeds implementation bodies beyond the durable scheduling values the spec requires.
