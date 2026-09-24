from telegram_phone_number_checker.queue_store import QueueStore
from telegram_phone_number_checker.validator_worker import ValidatorWorker
from telethon.errors import FloodWaitError


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
    assert store.start(job_id) is True
    gateway = Gateway()
    worker = ValidatorWorker(store, gateway, "test-worker")

    assert worker.run_once() is True
    assert store.rows(job_id)[0]["status"] == "PENDING"
    assert worker.run_once() is True

    assert gateway.imports == [["+79991234567"]]
    assert gateway.resolves == ["+79991234567"]
    assert store.rows(job_id)[0]["status"] == "FOUND"


def test_worker_persists_temporary_gateway_failure_for_retry(tmp_path):
    class FailingGateway:
        def import_phones(self, phones):
            raise RuntimeError("network unavailable")

        def resolve_phone(self, phone):
            raise AssertionError("not called")

    store = QueueStore(tmp_path / "validator.db")
    job_id = store.create_job("+79991234567")
    store.start(job_id)

    assert ValidatorWorker(store, FailingGateway(), "test-worker").run_once() is True
    assert store.rows(job_id)[0]["status"] == "RETRYABLE_ERROR"


class BatchStore:
    """Small store double that documents the worker/store boundary."""

    def __init__(self, claims):
        self.claims = list(claims)
        self.finished = []
        self.direct = []
        self.import_sizes = []
        self.flood_waits = []
        self.cleanup_claims = []
        self.cleaned = []
        self.cleanup_retries = []

    def claim_batch(self, owner, limit):
        if not self.claims:
            return []
        job_id = self.claims[0]["job_id"]
        selected = [claim for claim in self.claims if claim["job_id"] == job_id][:limit]
        selected_ids = {claim["id"] for claim in selected}
        self.claims = [claim for claim in self.claims if claim["id"] not in selected_ids]
        return selected

    def finish_batch(self, claims, results):
        self.finished.extend((claim["id"], results[claim["phone"]]) for claim in claims)

    def move_batch_to_direct_lookup(self, claims):
        self.direct.extend(claim["id"] for claim in claims)

    def retry_claim(self, claim, error):
        raise AssertionError(f"FloodWait must not become a retry: {error}")

    def set_import_batch_size(self, job_id, size):
        self.import_sizes.append((job_id, size))

    def defer_account_floodwait(self, claims, seconds):
        self.flood_waits.append(([claim["id"] for claim in claims], seconds))

    def claim_cleanup(self, owner):
        return self.cleanup_claims.pop(0) if self.cleanup_claims else None

    def finish_cleanup(self, claim):
        self.cleaned.append(claim["id"])

    def retry_cleanup(self, claim, error):
        self.cleanup_retries.append((claim["id"], str(error)))


def _claim(item_id, phone, *, method="IMPORT", job_id="job-1"):
    return {
        "id": item_id,
        "phone": phone,
        "normalized_phone": phone,
        "method": method,
        "job_id": job_id,
        "owner": "test-worker",
        "account_key": "default",
    }


def test_worker_imports_at_most_ten_unique_phones_and_reuses_duplicate_result():
    claims = [_claim(item_id, "+79990000001") for item_id in range(1, 12)]
    store = BatchStore(claims)

    class Gateway:
        def __init__(self):
            self.imports = []

        def import_phones(self, phones):
            self.imports.append(phones)
            return {phones[0]: {"id": 1, "username": "same"}}

        def resolve_phone(self, phone):
            raise AssertionError("not called")

    gateway = Gateway()
    worker = ValidatorWorker(store, gateway, "test-worker")
    assert worker.run_once() is True
    assert worker.run_once() is True

    assert gateway.imports == [["+79990000001"]]
    assert [item_id for item_id, _ in store.finished] == list(range(1, 12))


def test_worker_does_not_reuse_a_cached_phone_result_for_another_job():
    store = BatchStore([
        _claim(1, "+79990000001", job_id="job-1"),
        _claim(2, "+79990000001", job_id="job-2"),
    ])

    class Gateway:
        def __init__(self):
            self.imports = []

        def import_phones(self, phones):
            self.imports.append(phones)
            return {phones[0]: {"id": len(self.imports)}}

        def resolve_phone(self, phone):
            raise AssertionError("not called")

    gateway = Gateway()
    worker = ValidatorWorker(store, gateway, "test-worker")
    assert worker.run_once() is True
    assert worker.run_once() is True

    assert gateway.imports == [["+79990000001"], ["+79990000001"]]


def test_worker_reduces_later_import_batches_after_retry_contacts():
    store = BatchStore([_claim(1, "+79990000001"), _claim(2, "+79990000002")])

    class Gateway:
        def import_phones(self, phones):
            return {
                phones[0]: {"retry": True, "error": "Retry later"},
                phones[1]: {"error": "No response"},
            }

        def resolve_phone(self, phone):
            raise AssertionError("not called")

    assert ValidatorWorker(store, Gateway(), "test-worker").run_once() is True

    assert store.direct == [1]
    assert store.import_sizes == [("job-1", 5)]


def test_worker_persists_exact_floodwait_without_marking_item_retryable():
    store = BatchStore([_claim(1, "+79990000001")])

    class Gateway:
        def import_phones(self, phones):
            raise FloodWaitError(None, 47)

        def resolve_phone(self, phone):
            raise AssertionError("not called")

    assert ValidatorWorker(store, Gateway(), "test-worker").run_once() is True

    assert store.flood_waits == [([1], 47)]


def test_worker_processes_durable_cleanup_separately_from_validation():
    store = BatchStore([])
    store.cleanup_claims.append({"id": 9, "telegram_user_ids": [123, 456]})

    class Gateway:
        def import_phones(self, phones):
            raise AssertionError("not called")

        def resolve_phone(self, phone):
            raise AssertionError("not called")

        def cleanup_contacts(self, user_ids):
            assert user_ids == [123, 456]

    assert ValidatorWorker(store, Gateway(), "test-worker").run_once() is True
    assert store.cleaned == [9]
