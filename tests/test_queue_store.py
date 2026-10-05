import json

from telegram_phone_number_checker.queue_store import QueueStore


class Clock:
    def __init__(self, value=1_000.0):
        self.value = value

    def __call__(self):
        return self.value


def test_store_creates_every_input_ordinal_and_restores_it(tmp_path):
    database = tmp_path / "validator.db"
    store = QueueStore(database)

    job_id = store.create_job("ООО Тест\n+79991234567\n+79991234567\n799")

    assert store.snapshot(job_id)["total"] == 3
    assert [row["phone"] for row in store.rows(job_id)] == [
        "+79991234567",
        "+79991234567",
        "799",
    ]
    assert QueueStore(database).snapshot(job_id)["total"] == 3


def test_migration_preserves_found_deferred_and_unprocessed_ordinals(tmp_path):
    source = tmp_path / "jobs.json"
    source.write_text(
        json.dumps(
            {
                "jobs": {
                    "legacy": {
                        "id": "legacy",
                        "raw": "ООО Тест\n+79991234567\n+79990000000\n+79991111111",
                        "results_by_phone": {
                            "+79991234567": {"id": 1, "username": "known"},
                        },
                        "deferred_phones": ["+79990000000"],
                        "skip_deferred": True,
                        "waiting_until": None,
                    }
                },
                "last_rows": [],
            }
        ),
        encoding="utf-8",
    )
    store = QueueStore(tmp_path / "validator.db")

    store.migrate_json(source)

    assert store.snapshot("legacy")["state"] == "STAGED"
    assert [row["status"] for row in store.rows("legacy")] == [
        "FOUND",
        "DEFERRED_NOT_CHECKED",
        "PENDING",
    ]
    store.migrate_json(source)
    assert len(store.rows("legacy")) == 3


def test_migration_keeps_the_legacy_deduplicated_row_count(tmp_path):
    source = tmp_path / "jobs.json"
    source.write_text(
        json.dumps({"jobs": {"legacy": {"id": "legacy", "raw": "+79991234567\n+79991234567"}}}),
        encoding="utf-8",
    )

    store = QueueStore(tmp_path / "validator.db")
    store.migrate_json(source)

    assert store.snapshot("legacy")["total"] == 1
    assert [row["phone"] for row in store.rows("legacy")] == ["+79991234567"]


def test_live_account_lease_excludes_second_process_and_cancel_preserves_found(tmp_path):
    store = QueueStore(tmp_path / "validator.db")
    job_id = store.create_job("+79991234567\n+79990000000")
    assert store.start(job_id) is True

    first = store.claim_next("worker-a")

    assert first["phone"] == "+79991234567"
    assert store.claim_next("worker-b") is None
    store.finish_claim(first, {"id": 1, "username": "known"})
    store.cancel(job_id)
    assert [row["status"] for row in store.rows(job_id)] == ["FOUND", "CANCELLED"]


def test_cancel_keeps_an_already_sent_claim_and_saves_its_late_result(tmp_path):
    store = QueueStore(tmp_path / "validator.db")
    job_id = store.create_job("+79991234567")
    store.start(job_id)
    claim = store.claim_next("worker-a")

    store.cancel(job_id)
    store.finish_claim(claim, {"id": 1, "username": "late"})

    assert store.snapshot(job_id)["state"] == "CANCELLED"
    assert store.rows(job_id)[0]["status"] == "FOUND"


def test_expired_in_progress_import_lease_is_unknown_and_not_replayed(tmp_path):
    clock = Clock()
    store = QueueStore(tmp_path / "validator.db", now=clock)
    job_id = store.create_job("+79991234567")
    assert store.start(job_id) is True
    first_claim = store.claim_next("worker-a")

    clock.value += 61
    recovered_claim = QueueStore(tmp_path / "validator.db", now=clock).claim_next("worker-b")

    assert recovered_claim is None
    assert store.rows(job_id)[0]["status"] == "UNKNOWN_OUTCOME_REVIEW"
    with store._connect() as connection:
        assert connection.execute("SELECT outcome FROM job_item_attempts WHERE item_id = ?", (first_claim['id'],)).fetchone()[0] == "UNKNOWN_OUTCOME"


def test_reconcile_requeues_an_expired_running_job_lease_without_item_loss(tmp_path):
    clock = Clock()
    store = QueueStore(tmp_path / "validator.db", now=clock)
    job_id = store.create_job("+79991234567\n+79990000000")
    assert store.start(job_id) is True
    store.claim_next("worker-a")

    clock.value += 61
    store.reconcile()

    snapshot = store.snapshot(job_id)
    assert snapshot["state"] == "QUEUED"
    assert snapshot["counts"]["PENDING"] == 1
    assert snapshot["counts"]["UNKNOWN_OUTCOME_REVIEW"] == 1


def test_floodwait_blocks_every_job_and_automatically_resumes_after_deadline(tmp_path):
    clock = Clock()
    database = tmp_path / "validator.db"
    store = QueueStore(database, now=clock)
    first_job = store.create_job("+79991234567")
    second_job = store.create_job("+79990000000")
    assert store.start(first_job) is True
    assert store.start(second_job) is True
    claim = store.claim_next("worker-a")

    store.defer_account_floodwait([claim], seconds=90)

    reopened = QueueStore(database, now=clock)
    assert reopened.snapshot(first_job)["state"] == "WAITING_RATE_LIMIT"
    assert reopened.snapshot(first_job)["resume_after"] == 1_090.0
    assert reopened.claim_next("worker-b") is None

    clock.value += 91
    resumed_claim = reopened.claim_next("worker-b")

    assert resumed_claim["phone"] == "+79991234567"
    assert reopened.snapshot(first_job)["state"] == "RUNNING"


def test_cleanup_floodwait_returns_cleanup_to_the_durable_queue(tmp_path):
    clock = Clock()
    store = QueueStore(tmp_path / "validator.db", now=clock)
    job_id = store.create_job("+79991234567")
    store.start(job_id)
    claim = store.claim_next("worker-a")
    store.finish_claim(claim, {"id": 123, "username": "known"})
    cleanup = store.claim_cleanup("worker-a")

    store.defer_cleanup_floodwait(cleanup, seconds=90)

    assert store.claim_cleanup("worker-b") is None
    clock.value += 91
    recovered = store.claim_cleanup("worker-b")
    assert recovered["telegram_user_ids"] == [123]


def test_renew_claims_keeps_a_slow_request_owned(tmp_path):
    clock = Clock()
    store = QueueStore(tmp_path / "validator.db", now=clock)
    job_id = store.create_job("+79991234567")
    store.start(job_id)
    claim = store.claim_next("worker-a")

    clock.value += 30
    store.renew_claims([claim])
    clock.value += 31

    assert store.claim_next("worker-b") is None


def test_malformed_json_keeps_source_and_enters_diagnostic_mode(tmp_path):
    source = tmp_path / "jobs.json"
    source.write_text("{bad json", encoding="utf-8")
    store = QueueStore(tmp_path / "validator.db")

    store.migrate_json(source)

    diagnostic = store.migration_diagnostic()
    assert source.read_text(encoding="utf-8") == "{bad json"
    assert diagnostic["active"] is True
    assert "не удалось" in diagnostic["message"].lower()


def test_manual_chat_confirmation_keeps_api_status_and_adds_phone_link(tmp_path):
    store = QueueStore(tmp_path / "validator.db")
    job_id = store.create_job("+79089145093")
    store.start(job_id)
    claim = store.claim_next("worker-a")
    store.finish_claim(claim, {"error": "No response, the phone number is not on Telegram or has blocked phone lookup."})

    before = store.rows(job_id)[0]
    assert before["api_status"] == "UNRESOLVABLE_OR_PRIVACY"
    assert before["manual_confirmation"] == "NOT_CONFIRMED"
    assert before["telegram_link"] == "https://t.me/+79089145093?profile"

    assert store.confirm_manual_chat(job_id, before["_item_id"]) is True

    after = store.rows(job_id)[0]
    assert after["status"] == "UNRESOLVABLE_OR_PRIVACY"
    assert after["api_status"] == "UNRESOLVABLE_OR_PRIVACY"
    assert after["manual_confirmation"] == "MANUAL_CHAT_CONFIRMED"


def test_import_retry_backoff_is_durable_and_remains_import(tmp_path):
    clock = Clock()
    store = QueueStore(tmp_path / "validator.db", now=clock)
    job_id = store.create_job("+79991234567\n+79991234567")
    store.start(job_id)
    for delay in (60, 300, 900, 900):
        claims = store.claim_batch("worker", 10)
        assert len(claims) == 1
        store.defer_import_retries(claims, "Telegram requested retry_contacts")
        assert [row["status"] for row in store.rows(job_id)] == ["PENDING_RETRY"] * 2
        assert store.rows(job_id)[0]["method"] == "IMPORT"
        assert store.rows(job_id)[0]["next_attempt_at"] == clock.value + delay
        reopened = QueueStore(store.path, now=clock)
        assert reopened.claim_next("worker") is None
        clock.value += delay
        store = reopened
    with store._connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM job_item_attempts WHERE outcome='IMPORT_RETRY'").fetchone()[0] == 8


def test_pending_retry_cancel_and_pause_preserve_durable_delay(tmp_path):
    clock = Clock()
    store = QueueStore(tmp_path / "validator.db", now=clock)
    job_id = store.create_job("+79991234567")
    store.start(job_id)
    store.defer_import_retries([store.claim_next("worker")], "retry_contacts")
    store.pause(job_id)
    clock.value += 100
    assert QueueStore(store.path, now=clock).claim_next("worker") is None
    assert store.rows(job_id)[0]["next_attempt_at"] == 1060
    store.cancel(job_id)
    assert store.rows(job_id)[0]["status"] == "CANCELLED"


def test_five_successful_import_batches_restore_batch_size_and_resets_streak(tmp_path):
    store = QueueStore(tmp_path / "validator.db")
    job_id = store.create_job("+79991234567")
    store.set_import_batch_size(job_id, 1)
    for expected in (5, 10):
        for _ in range(4):
            store.record_import_batch_success(job_id)
        assert store.snapshot(job_id)["import_batch_size"] == (1 if expected == 5 else 5)
        store.record_import_batch_success(job_id)
        assert store.snapshot(job_id)["import_batch_size"] == expected
        assert store.snapshot(job_id)["import_success_streak"] == 0
    store.start(job_id)
    store.record_import_batch_success(job_id)
    store.defer_import_retries([store.claim_next("worker")], "retry_contacts")
    assert store.snapshot(job_id)["import_success_streak"] == 0


def test_230_second_floodwait_is_exact_and_records_immutable_attempt(tmp_path):
    clock = Clock()
    store = QueueStore(tmp_path / "validator.db", now=clock)
    job_id = store.create_job("+79991234567")
    store.start(job_id)
    store.record_import_batch_success(job_id)
    store.defer_account_floodwait([store.claim_next("worker")], 230)
    assert store.snapshot(job_id)["resume_after"] == 1230
    assert store.snapshot(job_id)["import_success_streak"] == 0
    assert store.rows(job_id)[0]["next_attempt_at"] == 1230
    with store._connect() as connection:
        attempt = dict(connection.execute("SELECT * FROM job_item_attempts").fetchone())
        assert attempt["outcome"] == "FLOOD_WAIT"
        assert "230" in attempt["detail"]
    clock.value = 1229
    assert QueueStore(store.path, now=clock).claim_next("worker") is None
    clock.value = 1230
    assert store.claim_next("worker") is not None


def test_terminal_results_and_json_migration_append_attempt_history(tmp_path):
    store = QueueStore(tmp_path / "validator.db")
    source = tmp_path / "jobs.json"
    source.write_text(json.dumps({"jobs": {"legacy": {"raw": "+79991234567", "results_by_phone": {"+79991234567": {"id": 1}}}}}))
    store.migrate_json(source)
    store.migrate_json(source)
    job_id = store.create_job("+79991234567\n+79991234567")
    store.start(job_id)
    store.finish_claim(store.claim_next("worker"), {"id": 2})
    with store._connect() as connection:
        attempts = [dict(row) for row in connection.execute("SELECT * FROM job_item_attempts ORDER BY id")]
        assert len(attempts) == 3
        assert attempts[0]["source"] == "JSON_MIGRATION"
        assert [row["outcome"] for row in attempts] == ["FOUND_NO_USERNAME"] * 3
    store.record_item_attempt(int(store.rows(job_id)[0]["_item_id"]), "IMPORT", "REVIEW_NOTE", "kept", "OPERATOR")
    with store._connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM job_item_attempts").fetchone()[0] == 4


def test_cleanup_claim_excludes_every_non_running_job_state(tmp_path):
    store = QueueStore(tmp_path / "validator.db")
    for state in ("STAGED", "PAUSED", "WAITING_AUTHORIZATION", "WAITING_RATE_LIMIT", "CANCELLED", "COMPLETED", "FAILED"):
        job_id = store.create_job("+79991234567")
        with store._connect() as connection:
            connection.execute("UPDATE jobs SET state = ? WHERE id = ?", (state, job_id))
            connection.execute("INSERT INTO contact_cleanup(job_id, telegram_user_id, state, created_at) VALUES (?, 123, 'PENDING', 0)", (job_id,))
    assert store.claim_cleanup_batch("worker", 10) is None


def test_expired_direct_lookup_lease_is_unknown_after_restart(tmp_path):
    clock = Clock()
    store = QueueStore(tmp_path / "validator.db", now=clock)
    job_id = store.create_job("+79991234567")
    store.start(job_id)
    with store._connect() as connection:
        connection.execute("UPDATE job_items SET method = 'DIRECT_LOOKUP'")
    store.claim_next("worker-a")
    clock.value += 61
    reopened = QueueStore(store.path, now=clock)
    assert reopened.claim_next("worker-b") is None
    assert reopened.rows(job_id)[0]["status"] == "UNKNOWN_OUTCOME_REVIEW"
    assert 'неизвестен' in reopened.snapshot(job_id)['message']


def test_expired_cleanup_lease_is_unknown_and_keeps_job_unresolved(tmp_path):
    clock = Clock()
    store = QueueStore(tmp_path / "validator.db", now=clock)
    job_id = store.create_job("+79991234567")
    store.start(job_id)
    store.finish_claim(store.claim_next("worker-a"), {"id": 123})
    store.claim_cleanup_batch("worker-a", 100)
    clock.value += 61
    reopened = QueueStore(store.path, now=clock)
    assert reopened.claim_cleanup_batch("worker-b", 100) is None
    reopened.finalize_ready_jobs()
    assert reopened.snapshot(job_id)["state"] != "COMPLETED"
    with store._connect() as connection:
        assert connection.execute("SELECT state FROM contact_cleanup").fetchone()[0] == "UNKNOWN_OUTCOME_REVIEW"
        assert connection.execute("SELECT outcome FROM job_item_attempts ORDER BY id DESC").fetchone()[0] == "UNKNOWN_OUTCOME"


def test_pending_retry_requires_persisted_due_time(tmp_path):
    store = QueueStore(tmp_path / "validator.db")
    job_id = store.create_job("+79991234567")
    store.start(job_id)
    with store._connect() as connection:
        connection.execute("UPDATE job_items SET status = 'PENDING_RETRY', next_attempt_at = NULL")
    assert store.claim_next("worker") is None


def test_additive_schema_upgrade_preserves_paused_queue_without_resume(tmp_path):
    import sqlite3
    clock = Clock()
    database = tmp_path / "validator.db"
    store = QueueStore(database, now=clock)
    job_id = store.create_job("+79991234567")
    store.start(job_id)
    store.pause(job_id)
    with sqlite3.connect(database) as connection:
        connection.execute("DROP TABLE job_item_attempts")
        connection.execute("ALTER TABLE jobs DROP COLUMN import_success_streak")
        connection.execute("ALTER TABLE job_items DROP COLUMN import_retry_count")
        connection.execute("ALTER TABLE worker_state DROP COLUMN import_batches_since_cleanup")
        connection.execute("ALTER TABLE contact_cleanup DROP COLUMN item_id")
    reopened = QueueStore(database, now=clock)
    assert reopened.snapshot(job_id)['state'] == 'PAUSED'
    assert reopened.rows(job_id)[0]['status'] == 'PENDING'
    assert reopened.claim_next('worker') is None
    with reopened._connect() as connection:
        assert connection.execute('SELECT COUNT(*) FROM job_item_attempts').fetchone()[0] == 0


def test_duplicate_terminal_result_does_not_overwrite_unknown_evidence(tmp_path):
    store = QueueStore(tmp_path / "validator.db")
    job_id = store.create_job("+79991234567\n+79991234567")
    store.start(job_id)
    claim = store.claim_next('worker')
    store.defer_unknown_outcome([claim], 'IMPORT')
    store.finish_claim(claim, {'id': 123})
    assert [row['status'] for row in store.rows(job_id)] == ['UNKNOWN_OUTCOME_REVIEW'] * 2
    with store._connect() as connection:
        assert connection.execute('SELECT COUNT(*) FROM job_item_attempts').fetchone()[0] == 2


def test_only_actual_failure_counts_toward_terminal_retry_limit(tmp_path):
    clock = Clock()
    store = QueueStore(tmp_path / "validator.db", now=clock)
    job_id = store.create_job("+79991234567")
    store.start(job_id)
    for _ in range(4):
        store.defer_import_retries([store.claim_next('worker')], 'retry_contacts')
        clock.value = store.rows(job_id)[0]['next_attempt_at']
    for status, delay in [('RETRYABLE_ERROR', 30), ('RETRYABLE_ERROR', 120), ('API_ERROR_FINAL', 600)]:
        store.retry_claim(store.claim_next('worker'), RuntimeError('raw details'))
        assert store.rows(job_id)[0]['status'] == status
        clock.value += delay
    with store._connect() as connection:
        assert connection.execute('SELECT attempts FROM job_items').fetchone()[0] == 3
