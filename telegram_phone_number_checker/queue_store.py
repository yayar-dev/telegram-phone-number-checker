"""Durable local storage for Telegram validation jobs."""

from __future__ import annotations

import json
import hashlib
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
                   VALUES (?, ?, 'STAGED', 'IMPORT', 'Задача поставлена в очередь.', ?, ?, ?)""",
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

    def migrate_json(self, source: Path) -> None:
        """Import the former JSON snapshots once, without modifying their source."""
        contents = source.read_bytes()
        checksum = hashlib.sha256(contents).hexdigest()
        state = json.loads(contents.decode("utf-8"))
        legacy_jobs = state.get("jobs")
        if not isinstance(legacy_jobs, dict):
            raise ValueError("Файл очереди не содержит jobs.")
        timestamp = self.now()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            marker = f"json_migrated:{checksum}"
            if connection.execute("SELECT 1 FROM schema_meta WHERE key = ?", (marker,)).fetchone():
                connection.commit()
                return
            for legacy_id, legacy in legacy_jobs.items():
                if connection.execute("SELECT 1 FROM jobs WHERE id = ?", (legacy_id,)).fetchone():
                    continue
                raw = str(legacy.get("raw", ""))
                entries = parse_company_phone_batch(raw, deduplicate=False)
                resume_after = legacy.get("waiting_until")
                state_name = "WAITING_RATE_LIMIT" if resume_after and resume_after > timestamp else "QUEUED"
                connection.execute(
                    """INSERT INTO jobs(id, raw_input, state, phase, message, created_at, updated_at,
                       total_rows, resume_after)
                       VALUES (?, ?, ?, 'IMPORT', 'Задача перенесена из JSON.', ?, ?, ?, ?)""",
                    (legacy_id, raw, state_name, timestamp, timestamp, len(entries), resume_after),
                )
                results = legacy.get("results_by_phone", {})
                deferred = set(legacy.get("deferred_phones", []))
                skip_deferred = bool(legacy.get("skip_deferred"))
                for ordinal, entry in enumerate(entries):
                    phone = entry["phone"]
                    result = results.get(phone, {})
                    status, method, detail = "PENDING", "IMPORT", ""
                    fields: dict[str, str] = {}
                    if not is_valid_phone(phone):
                        status, method, detail = "INVALID_PHONE", "NONE", "Введите номер в международном формате, например +79991234567."
                    elif result.get("skipped") or (skip_deferred and phone in deferred):
                        status, method, detail = "DEFERRED_NOT_CHECKED", "NONE", result.get("error", "Проверка отложена пользователем.")
                    elif result.get("id"):
                        username = result.get("username") or ""
                        status, method = ("FOUND" if username else "FOUND_NO_USERNAME"), "MIGRATED"
                        name = " ".join(filter(None, [result.get("first_name"), result.get("last_name")]))
                        fields = {
                            "telegram_id": str(result["id"]), "username": username, "name": name,
                            "telegram_link": f"https://t.me/{username}" if username else f"https://t.me/{phone}?profile",
                        }
                    elif result.get("retry") or phone in deferred:
                        method = "DIRECT_LOOKUP"
                    elif str(result.get("error", "")).startswith("No response"):
                        status, method, detail = "UNRESOLVABLE_OR_PRIVACY", "MIGRATED", result["error"]
                    elif result.get("error"):
                        status, method, detail = "RETRYABLE_ERROR", "IMPORT", result["error"]
                    completed_at = timestamp if status in TERMINAL_STATUSES else None
                    connection.execute(
                        """INSERT INTO job_items(job_id, ordinal, company_name, company_link, raw_phone,
                           normalized_phone, status, method, detail, telegram_id, username, name,
                           telegram_link, completed_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (legacy_id, ordinal, entry["company_name"], entry["company_link"], phone, phone,
                         status, method, detail, fields.get("telegram_id", ""), fields.get("username", ""),
                         fields.get("name", ""), fields.get("telegram_link", ""), completed_at),
                    )
                self._event(connection, legacy_id, "JSON_MIGRATED", "Очередь перенесена из JSON.")
            connection.execute("INSERT INTO schema_meta(key, value) VALUES (?, ?)", (marker, str(timestamp)))
            connection.commit()

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

    def claim_next(self, owner: str, account_key: str = "default") -> dict | None:
        """Claim one ready item and the account lease before a Telegram request."""
        timestamp = self.now()
        expires_at = timestamp + 60
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            worker = connection.execute(
                "SELECT * FROM worker_state WHERE account_key = ?", (account_key,)
            ).fetchone()
            if worker and (
                worker["next_available_at"] > timestamp
                or (worker["lease_owner"] and worker["lease_owner"] != owner and worker["lease_expires_at"] > timestamp)
            ):
                connection.commit()
                return None
            connection.execute(
                """INSERT INTO worker_state(account_key, lease_owner, lease_expires_at, updated_at)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(account_key) DO UPDATE SET lease_owner = excluded.lease_owner,
                   lease_expires_at = excluded.lease_expires_at, updated_at = excluded.updated_at""",
                (account_key, owner, expires_at, timestamp),
            )
            item = connection.execute(
                """SELECT i.* FROM job_items i JOIN jobs j ON j.id = i.job_id
                   WHERE i.status IN ('PENDING', 'RETRYABLE_ERROR')
                   AND (i.next_attempt_at IS NULL OR i.next_attempt_at <= ?)
                   AND j.state IN ('QUEUED', 'RUNNING')
                   ORDER BY CASE i.method WHEN 'DIRECT_LOOKUP' THEN 0 ELSE 1 END, i.id LIMIT 1""",
                (timestamp,),
            ).fetchone()
            if not item:
                connection.execute("UPDATE worker_state SET lease_owner = NULL, lease_expires_at = NULL WHERE account_key = ?", (account_key,))
                connection.commit()
                return None
            if item["method"] == "DIRECT_LOOKUP" and worker and worker["next_direct_lookup_at"] > timestamp:
                connection.execute("UPDATE worker_state SET lease_owner = NULL, lease_expires_at = NULL WHERE account_key = ?", (account_key,))
                connection.commit()
                return None
            connection.execute(
                """UPDATE job_items SET status = 'IN_PROGRESS', lease_owner = ?, lease_expires_at = ?,
                   attempts = attempts + 1, started_at = COALESCE(started_at, ?) WHERE id = ?""",
                (owner, expires_at, timestamp, item["id"]),
            )
            if item["method"] == "DIRECT_LOOKUP":
                connection.execute(
                    "UPDATE worker_state SET next_direct_lookup_at = ? WHERE account_key = ?",
                    (timestamp + 3.1, account_key),
                )
            connection.execute(
                "UPDATE jobs SET state = 'RUNNING', run_lease_owner = ?, run_lease_expires_at = ?, updated_at = ? WHERE id = ?",
                (owner, expires_at, timestamp, item["job_id"]),
            )
            connection.commit()
            return {**dict(item), "phone": item["normalized_phone"], "owner": owner, "account_key": account_key}

    def finish_claim(self, claim: dict, result: dict) -> None:
        timestamp = self.now()
        mapped = map_lookup_result(claim["phone"], result)
        status = mapped["status"]
        if status == "NOT_FOUND_OR_PRIVACY_RESTRICTED":
            status = "UNRESOLVABLE_OR_PRIVACY"
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """UPDATE job_items SET status = ?, method = ?, detail = ?, telegram_id = ?, username = ?,
                   name = ?, telegram_link = ?, lease_owner = NULL, lease_expires_at = NULL, completed_at = ?
                   WHERE id = ? AND lease_owner = ?""",
                (status, claim["method"], mapped["detail"], mapped["telegram_id"], mapped["username"],
                 mapped["name"], mapped["telegram_link"], timestamp, claim["id"], claim["owner"]),
            )
            connection.execute(
                "UPDATE worker_state SET lease_owner = NULL, lease_expires_at = NULL WHERE account_key = ? AND lease_owner = ?",
                (claim["account_key"], claim["owner"]),
            )
            self._event(connection, claim["job_id"], "RESULT", "Результат сохранён.")
            connection.commit()

    def move_claim_to_direct_lookup(self, claim: dict) -> None:
        """Persist Telegram retry_contacts as direct lookup work, never re-import it."""
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """UPDATE job_items SET status = 'PENDING', method = 'DIRECT_LOOKUP',
                   detail = 'Telegram отложил импорт; запланирована прямая проверка.',
                   lease_owner = NULL, lease_expires_at = NULL WHERE id = ? AND lease_owner = ?""",
                (claim["id"], claim["owner"]),
            )
            connection.execute(
                "UPDATE worker_state SET lease_owner = NULL, lease_expires_at = NULL WHERE account_key = ? AND lease_owner = ?",
                (claim["account_key"], claim["owner"]),
            )
            self._event(connection, claim["job_id"], "RETRY_CONTACT", "Импорт отложен; будет прямая проверка.")
            connection.commit()

    def cancel(self, job_id: str) -> None:
        timestamp = self.now()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """UPDATE job_items SET status = 'CANCELLED', detail = 'Проверка отменена пользователем.',
                   completed_at = ? WHERE job_id = ? AND status IN ('PENDING', 'RETRYABLE_ERROR')""",
                (timestamp, job_id),
            )
            connection.execute(
                "UPDATE jobs SET state = 'CANCELLED', updated_at = ? WHERE id = ?", (timestamp, job_id)
            )
            self._event(connection, job_id, "CANCELLED", "Проверка отменена пользователем.")
            connection.commit()

    def start(self, job_id: str) -> bool:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            changed = connection.execute(
                "UPDATE jobs SET state = 'QUEUED', message = 'Задача ожидает worker.', updated_at = ? WHERE id = ? AND state IN ('STAGED', 'PAUSED', 'WAITING_RATE_LIMIT')",
                (self.now(), job_id),
            ).rowcount
            if changed:
                self._event(connection, job_id, "STARTED", "Запущена обработка задачи.")
            connection.commit()
            return bool(changed)

    def pause(self, job_id: str) -> bool:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            changed = connection.execute(
                "UPDATE jobs SET state = 'PAUSED', paused_at = ?, message = 'Обработка приостановлена.', updated_at = ? WHERE id = ? AND state IN ('QUEUED', 'RUNNING', 'WAITING_RATE_LIMIT')",
                (self.now(), self.now(), job_id),
            ).rowcount
            if changed:
                self._event(connection, job_id, "PAUSED", "Обработка приостановлена.")
            connection.commit()
            return bool(changed)

    def finalize_ready_jobs(self) -> None:
        timestamp = self.now()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            for job in connection.execute("SELECT id FROM jobs WHERE state IN ('QUEUED', 'RUNNING')").fetchall():
                remaining = connection.execute(
                    "SELECT COUNT(*) FROM job_items WHERE job_id = ? AND status NOT IN (%s)"
                    % ",".join("?" for _ in TERMINAL_STATUSES),
                    (job["id"], *TERMINAL_STATUSES),
                ).fetchone()[0]
                if not remaining:
                    connection.execute(
                        "UPDATE jobs SET state = 'COMPLETED', message = 'Готово.', completed_at = ?, updated_at = ? WHERE id = ?",
                        (timestamp, timestamp, job["id"]),
                    )
                    self._event(connection, job["id"], "COMPLETED", "Задача завершена.")
            connection.commit()
