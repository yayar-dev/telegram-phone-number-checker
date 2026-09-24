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


def test_claim_reclaims_an_expired_in_progress_item_lease(tmp_path):
    clock = Clock()
    store = QueueStore(tmp_path / "validator.db", now=clock)
    job_id = store.create_job("+79991234567")
    assert store.start(job_id) is True
    first_claim = store.claim_next("worker-a")

    clock.value += 61
    recovered_claim = QueueStore(tmp_path / "validator.db", now=clock).claim_next("worker-b")

    assert recovered_claim["id"] == first_claim["id"]
    assert recovered_claim["owner"] == "worker-b"
    assert QueueStore(tmp_path / "validator.db", now=clock).snapshot(job_id)["state"] == "RUNNING"


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
    assert snapshot["counts"]["PENDING"] == 2


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
