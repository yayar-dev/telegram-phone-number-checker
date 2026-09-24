"""Single-account worker for durable validator jobs."""

from __future__ import annotations

import time
from typing import Protocol


class TelegramGateway(Protocol):
    def import_phones(self, phones: list[str]) -> dict[str, dict]: ...
    def resolve_phone(self, phone: str) -> dict: ...


class ValidatorWorker:
    def __init__(self, store, gateway: TelegramGateway, owner: str, sleep=time.sleep):
        self.store = store
        self.gateway = gateway
        self.owner = owner
        self.sleep = sleep

    def run_once(self) -> bool:
        claim = self.store.claim_next(self.owner)
        if not claim:
            return False
        if claim["method"] == "DIRECT_LOOKUP":
            self.store.finish_claim(claim, self.gateway.resolve_phone(claim["phone"]))
            return True
        results = self.gateway.import_phones([claim["phone"]])
        result = results[claim["phone"]]
        if result.get("retry"):
            self.store.move_claim_to_direct_lookup(claim)
        else:
            self.store.finish_claim(claim, result)
        return True
