"""Persist remote candidates and acquisition receipts in the caller's catalogue.

No Asset/Replica rows or bibliographic records are written here. These tables
describe observations and workflow state; completed captures refer to identities
returned by the existing storage manager. Portable macros own transactions.
"""

from __future__ import annotations

import json
import time
from typing import Any
from uuid import NAMESPACE_URL, uuid4, uuid5

from LiuXin_alpha.ingest.web.models import WebIngestOptions, WebRunBusyError

_TABLES = {
    "remote_files": """
        remote_file_id TEXT PRIMARY KEY,
        remote_file_root_url TEXT NOT NULL,
        remote_file_url TEXT NOT NULL,
        remote_file_referrer TEXT,
        remote_file_first_seen BIGINT NOT NULL,
        remote_file_last_seen BIGINT NOT NULL,
        remote_file_latest_capture_json TEXT,
        UNIQUE(remote_file_root_url, remote_file_url)
    """,
    "web_ingest_runs": """
        web_ingest_run_id TEXT PRIMARY KEY,
        web_ingest_run_root_url TEXT NOT NULL,
        web_ingest_run_options_json TEXT NOT NULL,
        web_ingest_run_status TEXT NOT NULL,
        web_ingest_run_owner TEXT,
        web_ingest_run_lease_until BIGINT NOT NULL,
        web_ingest_run_started BIGINT NOT NULL,
        web_ingest_run_bytes_received BIGINT NOT NULL DEFAULT 0,
        web_ingest_run_discovery_complete BIGINT NOT NULL DEFAULT 0,
        web_ingest_run_report_json TEXT
    """,
    "web_ingest_items": """
        web_ingest_item_id TEXT PRIMARY KEY,
        web_ingest_item_run_id TEXT NOT NULL,
        web_ingest_item_remote_file_id TEXT NOT NULL,
        web_ingest_item_state TEXT NOT NULL,
        web_ingest_item_receipt_json TEXT NOT NULL,
        web_ingest_item_error TEXT,
        UNIQUE(web_ingest_item_run_id, web_ingest_item_remote_file_id)
    """,
}


def now_ms() -> int:
    """Use wall time for durable observations and cross-process lease expiry."""
    return int(time.time() * 1000)


class WebIngestRepository:
    """One catalogue owner for unverified locations and resumable acquisition runs."""

    def __init__(self, database: Any, *, create: bool = True) -> None:
        self.db = database
        self.macros = database.macros
        if create:
            self._prepare_schema()

    def _prepare_schema(self) -> None:
        """Add only missing workflow tables; preserve existing catalogues and data."""
        missing = set(_TABLES) - set(self.db.get_tables())
        if not missing:
            return
        with self.macros.transaction(serialized=True) as connection:
            for table in sorted(missing):
                connection.execute(
                    f"CREATE TABLE IF NOT EXISTS {table} ({_TABLES[table]})"
                )
        self.db.get_tables(force_refresh=True)
        if hasattr(self.db, "conn"):
            self.db.conn = self.db.driver.conn

    def observe(
        self, root: str, url: str, referrer: str | None = None
    ) -> dict[str, Any]:
        """Record a link observation without inventing a hash, size or acquired timestamp."""
        identity = str(uuid5(NAMESPACE_URL, f"liuxin:remote:{root}\n{url}"))
        with self.macros.transaction(serialized=True):
            rows = self.macros.get_rows(
                "remote_files", where={"remote_file_id": identity}
            )
            stamp = now_ms()
            if rows:
                values: dict[str, Any] = {"remote_file_last_seen": stamp}
                if referrer:
                    values["remote_file_referrer"] = referrer
                self.macros.update_row(
                    "remote_files", identity, values, id_column="remote_file_id"
                )
            else:
                self.macros.insert_row(
                    "remote_files",
                    {
                        "remote_file_id": identity,
                        "remote_file_root_url": root,
                        "remote_file_url": url,
                        "remote_file_referrer": referrer,
                        "remote_file_first_seen": stamp,
                        "remote_file_last_seen": stamp,
                    },
                    id_column="remote_file_id",
                )
            return dict(
                self.macros.get_rows(
                    "remote_files", where={"remote_file_id": identity}
                )[0]
            )

    def files(self, root: str) -> list[dict[str, Any]]:
        """Inspect retained candidates without crawling or creating workflow tables."""
        if "remote_files" not in self.db.get_tables():
            return []
        return [
            dict(row)
            for row in self.macros.get_rows(
                "remote_files",
                where={"remote_file_root_url": root},
                order_by=("remote_file_url",),
            )
        ]

    def new_run(self, options: WebIngestOptions) -> str:
        """Allocate a distinct capture attempt; re-crawling never rewrites earlier receipts."""
        identity = str(uuid4())
        self.macros.insert_row(
            "web_ingest_runs",
            {
                "web_ingest_run_id": identity,
                "web_ingest_run_root_url": options.root_url,
                "web_ingest_run_options_json": json.dumps(options.to_dict()),
                "web_ingest_run_status": "pending",
                "web_ingest_run_lease_until": 0,
                "web_ingest_run_started": now_ms(),
            },
            id_column="web_ingest_run_id",
        )
        return identity

    def runs(self, root: str) -> list[dict[str, Any]]:
        """Find run identities even when a worker died before returning its first report."""
        if "web_ingest_runs" not in self.db.get_tables():
            return []
        return [
            dict(row)
            for row in self.macros.get_rows(
                "web_ingest_runs",
                where={"web_ingest_run_root_url": root},
                order_by=("web_ingest_run_started", "web_ingest_run_id"),
            )
        ]

    def run(self, run_id: str) -> dict[str, Any]:
        """Return one saved run or refuse an unknown identifier."""
        if "web_ingest_runs" not in self.db.get_tables():
            raise ValueError(f"unknown web ingest run: {run_id}")
        rows = self.macros.get_rows(
            "web_ingest_runs", where={"web_ingest_run_id": run_id}
        )
        if not rows:
            raise ValueError(f"unknown web ingest run: {run_id}")
        return dict(rows[0])

    def items(self, run_id: str) -> list[dict[str, Any]]:
        """Return the run's persisted selection independently of later remote links."""
        return [
            dict(row)
            for row in self.macros.get_rows(
                "web_ingest_items", where={"web_ingest_item_run_id": run_id}
            )
        ]

    def discovery_complete(self, run_id: str, owner: str) -> None:
        """A completed selection can resume acquisition without re-contacting navigation pages."""
        with self.macros.transaction(serialized=True):
            self.require_owner(run_id, owner)
            self.macros.update_row(
                "web_ingest_runs",
                run_id,
                {
                    "web_ingest_run_discovery_complete": 1,
                },
                id_column="web_ingest_run_id",
            )

    def claim(self, run_id: str, *, lease_ms: int) -> str:
        """Serialize claims so two workers cannot publish or clean one run's staging."""
        with self.macros.transaction(serialized=True):
            run = self.run(run_id)
            if (
                run["web_ingest_run_owner"]
                and run["web_ingest_run_lease_until"] > now_ms()
            ):
                raise WebRunBusyError(
                    "this web ingest run is active; resume after its owner releases or lease expires"
                )
            owner = str(uuid4())
            self.macros.update_row(
                "web_ingest_runs",
                run_id,
                {
                    "web_ingest_run_owner": owner,
                    "web_ingest_run_lease_until": now_ms() + lease_ms,
                    "web_ingest_run_status": "running",
                },
                id_column="web_ingest_run_id",
            )
            return owner

    def heartbeat(self, run_id: str, owner: str, *, lease_ms: int) -> None:
        """Fence expired owners before extending a live lease."""
        with self.macros.transaction(serialized=True):
            self.require_owner(run_id, owner)
            self.macros.update_row(
                "web_ingest_runs",
                run_id,
                {
                    "web_ingest_run_lease_until": now_ms() + lease_ms,
                },
                id_column="web_ingest_run_id",
            )

    def require_owner(self, run_id: str, owner: str) -> None:
        """A stale worker must stop before publication, receipts or staging cleanup."""
        run = self.run(run_id)
        if (
            run["web_ingest_run_owner"] != owner
            or run["web_ingest_run_lease_until"] <= now_ms()
        ):
            raise WebRunBusyError("web ingest lease was lost; this worker must stop")

    def charge_bytes(self, run_id: str, owner: str, count: int, limit: int) -> int:
        """Count received payload even if a subsequent staging write or publication fails."""
        from LiuXin_alpha.ingest.web.models import WebIngestLimitError

        with self.macros.transaction(serialized=True):
            self.require_owner(run_id, owner)
            total = int(self.run(run_id)["web_ingest_run_bytes_received"]) + count
            self.macros.update_row(
                "web_ingest_runs",
                run_id,
                {
                    "web_ingest_run_bytes_received": total,
                },
                id_column="web_ingest_run_id",
            )
        # The body bytes have already arrived. Record even a rejected lookahead
        # byte, outside the transaction that will propagate the limit error.
        if total > limit:
            raise WebIngestLimitError(
                "web ingest reached its total download byte limit"
            )
        return total

    def item(self, run_id: str, remote_id: str) -> dict[str, Any] | None:
        """Read the latest state for one URL within this exact run."""
        rows = self.macros.get_rows(
            "web_ingest_items",
            where={
                "web_ingest_item_run_id": run_id,
                "web_ingest_item_remote_file_id": remote_id,
            },
        )
        return None if not rows else dict(rows[0])

    def save_item(
        self,
        run_id: str,
        owner: str,
        remote_id: str,
        *,
        state: str,
        receipt: dict[str, Any],
        error: str | None = None,
    ) -> None:
        """Atomically update a workflow receipt and, on success, its candidate's latest capture."""
        with self.macros.transaction(serialized=True):
            self.require_owner(run_id, owner)
            previous = self.item(run_id, remote_id)
            values = {
                "web_ingest_item_state": state,
                "web_ingest_item_receipt_json": json.dumps(receipt),
                "web_ingest_item_error": error,
            }
            if previous:
                self.macros.update_row(
                    "web_ingest_items",
                    previous["web_ingest_item_id"],
                    values,
                    id_column="web_ingest_item_id",
                )
            else:
                self.macros.insert_row(
                    "web_ingest_items",
                    {
                        **values,
                        "web_ingest_item_id": str(
                            uuid5(NAMESPACE_URL, run_id + remote_id)
                        ),
                        "web_ingest_item_run_id": run_id,
                        "web_ingest_item_remote_file_id": remote_id,
                    },
                    id_column="web_ingest_item_id",
                )
            if state == "downloaded":
                self.macros.update_row(
                    "remote_files",
                    remote_id,
                    {
                        "remote_file_latest_capture_json": json.dumps(receipt),
                    },
                    id_column="remote_file_id",
                )

    def finish(self, run_id: str, owner: str, report: dict[str, Any]) -> None:
        """Retain the attempt report and release its claim together."""
        with self.macros.transaction(serialized=True):
            self.require_owner(run_id, owner)
            self.macros.update_row(
                "web_ingest_runs",
                run_id,
                {
                    "web_ingest_run_status": report["status"],
                    "web_ingest_run_report_json": json.dumps(report),
                    "web_ingest_run_owner": None,
                    "web_ingest_run_lease_until": 0,
                },
                id_column="web_ingest_run_id",
            )
