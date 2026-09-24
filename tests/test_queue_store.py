import json

from telegram_phone_number_checker.queue_store import QueueStore


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

    assert [row["status"] for row in store.rows("legacy")] == [
        "FOUND",
        "DEFERRED_NOT_CHECKED",
        "PENDING",
    ]
    store.migrate_json(source)
    assert len(store.rows("legacy")) == 3


def test_live_account_lease_excludes_second_process_and_cancel_preserves_found(tmp_path):
    store = QueueStore(tmp_path / "validator.db")
    job_id = store.create_job("+79991234567\n+79990000000")

    first = store.claim_next("worker-a")

    assert first["phone"] == "+79991234567"
    assert store.claim_next("worker-b") is None
    store.finish_claim(first, {"id": 1, "username": "known"})
    store.cancel(job_id)
    assert [row["status"] for row in store.rows(job_id)] == ["FOUND", "CANCELLED"]
