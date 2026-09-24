"""Durable local storage for Telegram validation jobs."""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Callable
from uuid import uuid4

from telegram_phone_number_checker.web_service import (
    is_valid_phone,
    map_lookup_result,
    parse_company_phone_batch,
)


TERMINAL_STATUSES = {
    "FOUND",
    "FOUND_NO_USERNAME",
    "UNRESOLVABLE_OR_PRIVACY",
    "INVALID_PHONE",
    "API_ERROR_FINAL",
    "DEFERRED_NOT_CHECKED",
    "CANCELLED",
}


class QueueStore:
    """SQLite-backed queue; every public mutation is a short transaction."""

    def __init__(self, path: Path, now: Callable[[], float] = time.time):
        self.path = path
        self.now = now
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            self._create_schema(connection)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA busy_timeout=5000")
        return connection

    @staticmethod
    def _create_schema(connection: sqlite3.Connection) -> None:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS schema_meta (
                key TEXT PRIMARY KEY, value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY, raw_input TEXT NOT NULL, state TEXT NOT NULL,
                phase TEXT NOT NULL, message TEXT NOT NULL, created_at REAL NOT NULL,
                started_at REAL, updated_at REAL NOT NULL, completed_at REAL,
                total_rows INTEGER NOT NULL, paused_at REAL, resume_after REAL,
                import_batch_size INTEGER NOT NULL DEFAULT 10,
                import_retry_count INTEGER NOT NULL DEFAULT 0, last_error TEXT NOT NULL DEFAULT '',
                run_lease_owner TEXT, run_lease_expires_at REAL
            );
            CREATE TABLE IF NOT EXISTS job_items (
                id INTEGER PRIMARY KEY, job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
                ordinal INTEGER NOT NULL, company_name TEXT NOT NULL, company_link TEXT NOT NULL,
                raw_phone TEXT NOT NULL, normalized_phone TEXT NOT NULL, status TEXT NOT NULL,
                method TEXT NOT NULL, detail TEXT NOT NULL DEFAULT '', attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at REAL, lease_owner TEXT, lease_expires_at REAL,
                telegram_id TEXT NOT NULL DEFAULT '', username TEXT NOT NULL DEFAULT '',
                name TEXT NOT NULL DEFAULT '', telegram_link TEXT NOT NULL DEFAULT '',
                started_at REAL, completed_at REAL, UNIQUE(job_id, ordinal)
            );
            CREATE TABLE IF NOT EXISTS job_events (
                id INTEGER PRIMARY KEY, job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
                created_at REAL NOT NULL, kind TEXT NOT NULL, message TEXT NOT NULL,
                processed INTEGER NOT NULL, total INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS worker_state (
                account_key TEXT PRIMARY KEY, lease_owner TEXT, lease_expires_at REAL,
                next_import_at REAL NOT NULL DEFAULT 0, next_direct_lookup_at REAL NOT NULL DEFAULT 0,
                next_available_at REAL NOT NULL DEFAULT 0, updated_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS contact_cleanup (
                id INTEGER PRIMARY KEY, job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
                telegram_user_id INTEGER NOT NULL, state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at REAL, last_error TEXT NOT NULL DEFAULT '', created_at REAL NOT NULL,
                completed_at REAL, UNIQUE(job_id, telegram_user_id)
            );
            CREATE INDEX IF NOT EXISTS job_items_ready ON job_items(status, next_attempt_at);
            CREATE INDEX IF NOT EXISTS job_items_expired_lease ON job_items(lease_expires_at);
            CREATE INDEX IF NOT EXISTS cleanup_ready ON contact_cleanup(state, next_attempt_at);
            """
        )
        connection.execute(
            "INSERT OR IGNORE INTO schema_meta(key, value) VALUES ('schema_version', '1')"
        )

    def create_job(self, raw: str) -> str:
        job_id = uuid4().hex
        timestamp = self.now()
        entries = parse_company_phone_batch(raw, deduplicate=False)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """INSERT INTO jobs(id, raw_input, state, phase, message, created_at, updated_at, total_rows)
                   VALUES (?, ?, 'QUEUED', 'IMPORT', 'Задача поставлена в очередь.', ?, ?, ?)""",
                (job_id, raw, timestamp, timestamp, len(entries)),
            )
            for ordinal, entry in enumerate(entries):
                phone = entry["phone"]
                status = "PENDING" if is_valid_phone(phone) else "INVALID_PHONE"
                detail = "" if status == "PENDING" else "Введите номер в международном формате, например +79991234567."
                completed_at = None if status == "PENDING" else timestamp
                connection.execute(
                    """INSERT INTO job_items(job_id, ordinal, company_name, company_link, raw_phone,
                       normalized_phone, status, method, detail, completed_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, 'IMPORT', ?, ?)""",
                    (job_id, ordinal, entry["company_name"], entry["company_link"], phone, phone, status, detail, completed_at),
                )
            self._event(connection, job_id, "CREATED", "Задача поставлена в очередь.")
            connection.commit()
        return job_id

    def _event(self, connection: sqlite3.Connection, job_id: str, kind: str, message: str) -> None:
        counts = connection.execute(
            "SELECT COUNT(*) AS total, SUM(status IN (%s)) AS completed FROM job_items WHERE job_id = ?"
            % ",".join("?" for _ in TERMINAL_STATUSES),
            (*TERMINAL_STATUSES, job_id),
        ).fetchone()
        connection.execute(
            "INSERT INTO job_events(job_id, created_at, kind, message, processed, total) VALUES (?, ?, ?, ?, ?, ?)",
            (job_id, self.now(), kind, message, int(counts["completed"] or 0), counts["total"]),
        )

    def snapshot(self, job_id: str) -> dict | None:
        with self._connect() as connection:
            job = connection.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            if not job:
                return None
            counts = connection.execute(
                "SELECT status, COUNT(*) AS count FROM job_items WHERE job_id = ? GROUP BY status", (job_id,)
            ).fetchall()
        by_status = {row["status"]: row["count"] for row in counts}
        processed = sum(by_status.get(status, 0) for status in TERMINAL_STATUSES)
        confirmed = by_status.get("FOUND", 0) + by_status.get("FOUND_NO_USERNAME", 0)
        return {**dict(job), "total": job["total_rows"], "processed": processed, "confirmed": confirmed, "counts": by_status}

    def rows(self, job_id: str) -> list[dict[str, str]]:
        with self._connect() as connection:
            items = connection.execute(
                "SELECT * FROM job_items WHERE job_id = ? ORDER BY ordinal", (job_id,)
            ).fetchall()
        rows = []
        for item in items:
            status = item["status"]
            if status in {"FOUND", "FOUND_NO_USERNAME"}:
                rendered = {
                    "phone": item["normalized_phone"], "status": status,
                    "telegram_id": item["telegram_id"], "username": item["username"],
                    "name": item["name"], "telegram_link": item["telegram_link"], "detail": item["detail"],
                }
            else:
                rendered = {
                    "phone": item["normalized_phone"], "status": status,
                    "telegram_id": "", "username": "", "name": "", "telegram_link": "",
                    "detail": item["detail"],
                }
            rows.append({"company_name": item["company_name"], "company_link": item["company_link"], **rendered})
        return rows
