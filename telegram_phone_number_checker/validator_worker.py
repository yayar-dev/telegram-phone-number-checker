"""Single-account worker for durable validator jobs.

The worker has no Telegram state of its own: claims, rate limits and cleanup
work are persisted by the store before or after every network request.
"""

from __future__ import annotations

import time
from typing import Protocol

from telethon.errors import FloodWaitError


MAX_IMPORT_BATCH_SIZE = 10
_NEXT_IMPORT_BATCH_SIZE = {10: 5, 5: 1, 1: 1}


class TelegramGateway(Protocol):
    def import_phones(self, phones: list[str]) -> dict[str, dict]: ...
    def resolve_phone(self, phone: str) -> dict: ...
    def cleanup_contacts(self, user_ids: list[int]) -> None: ...


class ValidatorWorker:
    def __init__(self, store, gateway: TelegramGateway, owner: str, sleep=time.sleep):
        self.store = store
        self.gateway = gateway
        self.owner = owner
        self.sleep = sleep
        # Avoid a second request for a duplicate that appears in a later claimed
        # batch. The store also deduplicates durable work across restarts.
        self._results_by_phone: dict[str, dict] = {}

    def run_once(self) -> bool:
        if self._run_cleanup_once():
            return True

        claims = self._claim_batch()
        if not claims:
            return False
        if claims[0]["method"] == "DIRECT_LOOKUP":
            return self._run_direct_lookup(claims)
        return self._run_import_batch(claims)

    def _claim_batch(self) -> list[dict]:
        """Claim a store-selected import batch, or retain legacy single claims."""
        claim_batch = getattr(self.store, "claim_batch", None)
        if claim_batch:
            # The store applies the job's persisted adaptive batch size. A direct
            # lookup claim is always returned alone.
            return claim_batch(self.owner, MAX_IMPORT_BATCH_SIZE)
        claim = self.store.claim_next(self.owner)
        return [claim] if claim else []

    def _run_import_batch(self, claims: list[dict]) -> bool:
        cached, pending = self._split_cached_claims(claims)
        if cached:
            self._finish(cached, self._results_for(cached))
        if not pending:
            return True

        unique_phones = list(dict.fromkeys(claim["phone"] for claim in pending))
        try:
            results = self.gateway.import_phones(unique_phones)
        except FloodWaitError as error:
            self.store.defer_account_floodwait(claims, error.seconds)
            return True
        except Exception as error:
            for claim in pending:
                self.store.retry_claim(claim, error)
            return True

        normalized_results = {
            phone: results.get(phone, {"error": "Telegram returned no result for this phone."})
            for phone in unique_phones
        }
        retry_claims = [
            claim for claim in pending if normalized_results[claim["phone"]].get("retry")
        ]
        completed_claims = [claim for claim in pending if claim not in retry_claims]
        if completed_claims:
            completed_results = self._results_for(completed_claims, normalized_results)
            self._finish(completed_claims, completed_results)
            self._remember(completed_results)
        if retry_claims:
            self._move_to_direct_lookup(retry_claims)
            self._reduce_import_batch_size(retry_claims[0])
        return True

    def _run_direct_lookup(self, claims: list[dict]) -> bool:
        # `claim_batch` deliberately returns one direct lookup claim, keeping the
        # persisted 3.1-second account limiter in the store authoritative.
        claim = claims[0]
        cached, pending = self._split_cached_claims([claim])
        if cached:
            self._finish(cached, self._results_for(cached))
            return True
        try:
            result = self.gateway.resolve_phone(claim["phone"])
        except FloodWaitError as error:
            self.store.defer_account_floodwait([claim], error.seconds)
            return True
        except Exception as error:
            self.store.retry_claim(claim, error)
            return True
        self._finish([claim], {claim["phone"]: result})
        self._remember({claim["phone"]: result})
        return True

    def _split_cached_claims(self, claims: list[dict]) -> tuple[list[dict], list[dict]]:
        cached = [claim for claim in claims if claim["phone"] in self._results_by_phone]
        return cached, [claim for claim in claims if claim not in cached]

    def _results_for(self, claims: list[dict], source: dict[str, dict] | None = None) -> dict[str, dict]:
        source = source or self._results_by_phone
        return {claim["phone"]: source[claim["phone"]] for claim in claims}

    def _remember(self, results: dict[str, dict]) -> None:
        self._results_by_phone.update(
            {phone: result for phone, result in results.items() if not result.get("retry")}
        )

    def _finish(self, claims: list[dict], results: dict[str, dict]) -> None:
        finish_batch = getattr(self.store, "finish_batch", None)
        if finish_batch:
            finish_batch(claims, results)
            return
        for claim in claims:
            self.store.finish_claim(claim, results[claim["phone"]])

    def _move_to_direct_lookup(self, claims: list[dict]) -> None:
        move_batch = getattr(self.store, "move_batch_to_direct_lookup", None)
        if move_batch:
            move_batch(claims)
            return
        for claim in claims:
            self.store.move_claim_to_direct_lookup(claim)

    def _reduce_import_batch_size(self, claim: dict) -> None:
        current = claim.get("import_batch_size", MAX_IMPORT_BATCH_SIZE)
        next_size = _NEXT_IMPORT_BATCH_SIZE.get(current, 1)
        setter = getattr(self.store, "set_import_batch_size", None)
        if setter:
            setter(claim["job_id"], next_size)

    def _run_cleanup_once(self) -> bool:
        claim_cleanup = getattr(self.store, "claim_cleanup", None)
        if not claim_cleanup:
            return False
        claim = claim_cleanup(self.owner)
        if not claim:
            return False
        try:
            self.gateway.cleanup_contacts(claim["telegram_user_ids"])
        except FloodWaitError as error:
            self.store.defer_account_floodwait([claim], error.seconds)
        except Exception as error:
            self.store.retry_cleanup(claim, error)
        else:
            self.store.finish_cleanup(claim)
        return True
