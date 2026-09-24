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
