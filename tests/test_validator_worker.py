from telegram_phone_number_checker.queue_store import QueueStore
from telegram_phone_number_checker.validator_worker import ValidatorWorker


class Gateway:
    def __init__(self):
        self.imports = []
        self.resolves = []

    def import_phones(self, phones):
        self.imports.append(phones)
        return {phones[0]: {"retry": True, "error": "Retry later"}}

    def resolve_phone(self, phone):
        self.resolves.append(phone)
        return {"id": 1, "username": "resolved"}


def test_worker_moves_retry_contact_to_direct_lookup(tmp_path):
    store = QueueStore(tmp_path / "validator.db")
    job_id = store.create_job("+79991234567")
    gateway = Gateway()
    worker = ValidatorWorker(store, gateway, "test-worker")

    assert worker.run_once() is True
    assert store.rows(job_id)[0]["status"] == "PENDING"
    assert worker.run_once() is True

    assert gateway.imports == [["+79991234567"]]
    assert gateway.resolves == ["+79991234567"]
    assert store.rows(job_id)[0]["status"] == "FOUND"
