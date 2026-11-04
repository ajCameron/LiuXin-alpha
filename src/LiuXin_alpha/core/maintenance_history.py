"""Retire maintenance job history only after proving catalogue completion."""

from __future__ import annotations

import json

from LiuXin_alpha.core.maintenance_jobs import MAINTENANCE_JOB_KIND
from LiuXin_alpha.databases.maintenance.journal import now_ms
from LiuXin_alpha.databases.maintenance.schema import migration_status
from LiuXin_alpha.jobs.history import prune_terminal_history, terminal_history_page


def prune_maintenance_history(
    runtime,
    repository,
    *,
    library_uuid,
    older_than_days=7,
    limit=128,
    before_run_id=None,
):
    """Prune one age-limited page while serializing catalogue delivery/reconfiguration.

    Only this library and journal generation qualify. Missing obligations are safe
    after journal pruning because their identities are never reused. Pending work
    protects all its attempts, including superseded generations. The jobs owner
    rechecks terminal state and retry ancestry in its own deletion transaction.
    A crash between ledgers cannot lose pending work or resurrect completed work.
    """
    if type(older_than_days) is not int or not 0 <= older_than_days <= 36500:
        raise ValueError("History age must be an integer between 0 and 36500 days")
    if type(limit) is not int or not 1 <= limit <= 500:
        raise ValueError("History page size must be between 1 and 500")
    cutoff = now_ms() - older_than_days * 86400000
    with runtime.unit() as connection, runtime._internal():
        if not runtime._refresh(connection):
            raise RuntimeError("Install maintenance before retiring its job history")
        journal_uuid = migration_status(connection, runtime.schema)["journal_uuid"]
        page = terminal_history_page(
            repository,
            job_kind=MAINTENANCE_JOB_KIND,
            finished_before_ms=cutoff,
            before_run_id=before_run_id,
            limit=limit + 1,
        )
        selected = page[:limit]
        approved = []
        for run in selected:
            identity = _obligation_identity(
                run.payload_json, library_uuid, journal_uuid
            )
            if identity is None:
                continue
            obligation = connection.execute(
                f"SELECT completed_ms FROM {runtime.table('maintenance_obligations')} WHERE obligation_id=?",
                (identity,),
            ).fetchone()
            if obligation is None or obligation[0] is not None:
                approved.append(run.job_run_id)
        result = prune_terminal_history(
            repository,
            approved,
            job_kind=MAINTENANCE_JOB_KIND,
            finished_before_ms=cutoff,
        )
        return {
            **result,
            "scanned": len(selected),
            "retained_runs": len(selected) - result["removed_runs"],
            "older_than_days": older_than_days,
            "has_more": len(page) > limit,
            "next_before_run_id": selected[-1].job_run_id
            if len(page) > limit
            else None,
        }


def _obligation_identity(payload_json, library_uuid, journal_uuid):
    try:
        payload = json.loads(payload_json)
    except (TypeError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    identity = payload.get("obligation_id")
    if (
        payload.get("library_uuid") != library_uuid
        or payload.get("journal_uuid") != journal_uuid
        or type(identity) is not int
        or identity < 1
    ):
        return None
    return identity
