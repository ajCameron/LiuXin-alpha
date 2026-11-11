"""Bounded history retirement after a domain owner proves delivery is complete.

This explicitly retires request keys as well as runs. It is not automatic result
retention: the caller must establish that resubmitting those keys is no longer
possible through its delivery protocol. Active work and retry ancestry survive.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable

from LiuXin_alpha.jobs.models import JobRun
from LiuXin_alpha.jobs.repository import JobRepository

_TERMINAL = ("succeeded", "failed", "cancelled", "timed_out", "aborted")


def terminal_history_page(
    repository: JobRepository,
    *,
    job_kind: str,
    finished_before_ms: int,
    before_run_id: int | None = None,
    limit: int = 128,
) -> list[JobRun]:
    """Inspect newest-first terminal history, including one optional lookahead row."""
    if type(limit) is not int or not 1 <= limit <= 501:
        raise ValueError("History page size must be between 1 and 501")
    if before_run_id is not None and (
        type(before_run_id) is not int or before_run_id < 1
    ):
        raise ValueError("History cursor must be a positive run ID")
    with repository._connection() as connection:
        records = connection.execute(
            "SELECT * FROM job_runs WHERE job_kind=? "
            "AND state IN ('succeeded','failed','cancelled','timed_out','aborted') "
            "AND finished_timestamp_ep_k <= ? "
            + ("AND job_run_id < ? " if before_run_id is not None else "")
            + "ORDER BY job_run_id DESC LIMIT ?",
            (
                job_kind,
                finished_before_ms,
                *((before_run_id,) if before_run_id is not None else ()),
                limit,
            ),
        ).fetchall()
        return [repository._run_from_row(row) for row in records]


def prune_terminal_history(
    repository: JobRepository,
    run_ids: Iterable[int],
    *,
    job_kind: str,
    finished_before_ms: int,
) -> dict[str, int]:
    """Retire approved runs and keys atomically, rechecking live ledger state.

    Remove children before parents. A retained successor or any active run on the
    same definition protects its history. Scheduled definitions are never retired.
    IDs remain monotonic and ledger identity is unchanged. SQLite reuses freed
    pages; this operation does not VACUUM or promise immediate file shrinkage.
    """
    from itertools import islice

    identities = tuple(islice(iter(run_ids), 501))
    if len(identities) > 500 or any(
        type(value) is not int or value < 1 for value in identities
    ):
        raise ValueError("History retirement requires at most 500 positive run IDs")
    counts: dict[str, int] = dict.fromkeys(
        ("removed_runs", "removed_events", "removed_requests", "removed_definitions"), 0
    )
    with repository._connection(write=True) as connection:
        for run_id in sorted(set(identities), reverse=True):
            row = connection.execute(
                "SELECT * FROM job_runs WHERE job_run_id=?", (run_id,)
            ).fetchone()
            if (
                row is None
                or row["job_kind"] != job_kind
                or row["state"] not in _TERMINAL
            ):
                continue
            if (
                row["finished_timestamp_ep_k"] is None
                or row["finished_timestamp_ep_k"] > finished_before_ms
            ):
                continue
            definition_id = row["job_definition_id"]
            if not _retirable(connection, run_id, definition_id):
                continue
            for table, key in (
                ("job_run_events", "removed_events"),
                ("job_requests", "removed_requests"),
                ("job_runs", "removed_runs"),
            ):
                counts[key] += connection.execute(
                    f"DELETE FROM {table} WHERE job_run_id=?", (run_id,)
                ).rowcount
            counts["removed_definitions"] += connection.execute(
                "DELETE FROM job_definitions WHERE job_definition_id=? "
                "AND NOT EXISTS (SELECT 1 FROM job_runs WHERE job_definition_id=?)",
                (definition_id, definition_id),
            ).rowcount
    return counts


def _retirable(connection: sqlite3.Connection, run_id: int, definition_id: int) -> bool:
    """Keep active definitions, schedules and ancestors of retained runs intact."""
    return not (
        connection.execute(
            "SELECT 1 FROM job_runs WHERE retry_of_job_run_id=? LIMIT 1", (run_id,)
        ).fetchone()
        or connection.execute(
            "SELECT 1 FROM job_runs WHERE job_definition_id=? "
            "AND state NOT IN ('succeeded','failed','cancelled','timed_out','aborted') LIMIT 1",
            (definition_id,),
        ).fetchone()
        or connection.execute(
            "SELECT 1 FROM job_definitions WHERE job_definition_id=? AND schedule_json IS NOT NULL",
            (definition_id,),
        ).fetchone()
    )
