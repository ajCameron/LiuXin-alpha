"""Bridge committed catalogue obligations to the existing jobs claim owner."""

from __future__ import annotations

import json

from LiuXin_alpha.databases.maintenance.journal import now_ms, rows
from LiuXin_alpha.jobs.handler_api import JobHandlerAPI
from LiuXin_alpha.jobs.models import JobDefinition, JobRunState

MAINTENANCE_JOB_KIND = "core.maintenance.delivery.v1"


class MaintenanceJobHandler(JobHandlerAPI):
    """Reopen the target library and deliver with a live jobs claim fence."""

    job_kind = MAINTENANCE_JOB_KIND

    def __init__(self, *, library_uuid, open_library):
        """Retain a library identity and a factory that owns each worker graph."""
        self.library_uuid = library_uuid
        self.open_library = open_library

    def validate_payload(self, payload_json):
        """Require a bounded captured delivery for the configured library."""
        payload = json.loads(payload_json)
        if payload.get("library_uuid") != self.library_uuid:
            raise ValueError("Maintenance belongs to a different library")
        if not isinstance(payload.get("plugin_id"), str) or not payload["plugin_id"]:
            raise ValueError("Maintenance requires a plugin identity")
        if (
            not isinstance(payload.get("plugin_version"), str)
            or not payload["plugin_version"]
        ):
            raise ValueError("Maintenance requires a captured plugin version")
        if (
            type(payload.get("obligation_id")) is not int
            or payload["obligation_id"] < 1
        ):
            raise ValueError("Maintenance requires a positive delivery identity")
        if (
            not isinstance(payload.get("journal_uuid"), str)
            or not payload["journal_uuid"]
        ):
            raise ValueError("Maintenance requires a catalogue journal identity")

    def run(self, *, payload_json, run_context):
        """Apply effects and catalogue acknowledgement together, before jobs receipt."""
        self.validate_payload(payload_json)
        payload = json.loads(payload_json)
        with self.open_library() as services:
            database = services.library.db
            if str(database.uuid) != self.library_uuid:
                raise ValueError(
                    "Library identity changed before maintenance execution"
                )
            from LiuXin_alpha.catalog.maintenance import catalog_maintenance

            runtime = catalog_maintenance(database)
            if runtime.status(limit=1).get("journal_uuid") != payload["journal_uuid"]:
                raise ValueError("Catalogue journal was replaced before delivery")
            return runtime.deliver(
                payload["plugin_id"],
                [payload["obligation_id"]],
                check_claim=run_context.raise_if_cancel_requested,
                expected_version=payload["plugin_version"],
                job_run_id=run_context.job_run_id,
                job_ledger_uuid=run_context.repository.ledger_uuid,
            )


def publish_obligations(
    runtime, repository, *, library_uuid: str, limit: int = 128
) -> dict:
    """Publish a bounded page with stable keys and reconcile interrupted handoffs.

    Jobs may be committed before the catalogue stores their IDs. Resubmission
    uses the same key and captured payload. Recovery retries are safe because all
    handler effects and their completion receipt share one catalogue transaction.
    """
    if type(limit) is not int or not 1 <= limit <= 500:
        raise ValueError("Maintenance publication limit must be between 1 and 500")
    state = runtime.status(limit=1)
    if not state["installed"]:
        raise RuntimeError("Install maintenance before publishing obligations")
    repository.recover_expired_runs()
    published = []
    with runtime.unit() as connection, runtime._internal():
        runtime._refresh(connection)
        eligible, values = [], []
        for name, declaration in runtime._declarations.items():
            if declaration.mode.value != "lazy" or not runtime.ready(
                name, declaration.version
            ):
                continue
            conditions = ["o.plugin_id=?"]
            values.append(name)
            for dependency in declaration.dependencies:
                conditions.append(
                    f"NOT EXISTS (SELECT 1 FROM {runtime.table('maintenance_obligations')} d "
                    "WHERE d.plugin_id=? AND d.completed_ms IS NULL AND d.change_id<=o.change_id)"
                )
                values.append(dependency)
            eligible.append("(" + " AND ".join(conditions) + ")")
        if not eligible:
            return {"published": 0, "run_ids": []}
        candidates = rows(
            connection,
            f"SELECT o.* FROM {runtime.table('maintenance_obligations')} o "
            f"JOIN {runtime.table('maintenance_plugins')} p ON p.plugin_id=o.plugin_id "
            "WHERE o.mode='lazy' AND o.completed_ms IS NULL AND p.enabled=1 "
            "AND o.available_ms <= ? AND o.attempts <= p.retry_limit AND ("
            + " OR ".join(eligible)
            + ") ORDER BY p.priority DESC, o.obligation_id LIMIT ?",
            (now_ms(), *values, limit),
        )
        captured_runs = repository.get_runs(
            row["job_run_id"]
            for row in candidates
            if row["job_ledger_uuid"] == repository.ledger_uuid
            and row["job_run_id"] is not None
        )
        for row in candidates:
            name = row["plugin_id"]
            if not runtime.ready(name, row["plugin_version"]):
                continue
            declaration = runtime._declarations[name]
            if any(
                connection.execute(
                    f"SELECT 1 FROM {runtime.table('maintenance_obligations')} WHERE plugin_id=? "
                    "AND completed_ms IS NULL AND change_id <= ? LIMIT 1",
                    (dependency, row["change_id"]),
                ).fetchone()
                for dependency in declaration.dependencies
            ):
                continue
            payload = json.dumps(
                {
                    "library_uuid": library_uuid,
                    "journal_uuid": state["journal_uuid"],
                    "plugin_id": name,
                    "plugin_version": row["plugin_version"],
                    "obligation_id": row["obligation_id"],
                },
                sort_keys=True,
            )
            key = f"maintenance:{state['journal_uuid']}:{row['obligation_id']}:{row['retry_generation']}"
            if (
                row["job_ledger_uuid"] == repository.ledger_uuid
                and row["job_run_id"] is not None
            ):
                run = captured_runs[row["job_run_id"]]
            else:
                run = repository.find_run_by_request(key)
                if run is not None and (
                    run.job_kind != MAINTENANCE_JOB_KIND
                    or json.loads(run.payload_json) != json.loads(payload)
                ):
                    raise ValueError(
                        "Maintenance handoff has different captured inputs"
                    )
                run = run or repository.submit_job(
                    JobDefinition(
                        job_kind=MAINTENANCE_JOB_KIND,
                        job_name=f"Maintenance: {name}",
                        payload_json=payload,
                        priority=declaration.priority,
                    ),
                    request_key=key,
                )
            if row["job_ledger_uuid"] != repository.ledger_uuid:
                # Run IDs are local to a ledger. Persist the new association even
                # if failure reconciliation below defers this obligation.
                connection.execute(
                    f"UPDATE {runtime.table('maintenance_obligations')} "
                    "SET job_ledger_uuid=?, job_run_id=?, last_failure_run_id=NULL WHERE obligation_id=?",
                    (repository.ledger_uuid, run.job_run_id, row["obligation_id"]),
                ).close()
                row["last_failure_run_id"] = None
            if run.state in {
                JobRunState.FAILED,
                JobRunState.ABORTED,
                JobRunState.TIMED_OUT,
            }:
                # An expired process might not have recorded a catalogue failure.
                if row["last_failure_run_id"] != run.job_run_id:
                    delay = min(
                        86400.0,
                        declaration.retry_backoff_s * 2 ** min(row["attempts"], 20),
                    )
                    delay_ms = int(delay * 1000)
                    connection.execute(
                        f"UPDATE {runtime.table('maintenance_obligations')} SET attempts=attempts+1, last_error=?, available_ms=?, last_failure_run_id=? "
                        "WHERE obligation_id=?",
                        (
                            run.error_text or run.state.value,
                            # Match immediate runtime retries after clock steps.
                            now_ms() + delay_ms if delay_ms else 0,
                            run.job_run_id,
                            row["obligation_id"],
                        ),
                    ).close()
                    continue
                run = repository.retry_run(
                    run.job_run_id,
                    request_key=f"{key}:retry:{run.job_run_id}",
                    acknowledge_possible_effects=True,
                    reuse_successor=True,
                )
            if run.state is JobRunState.CANCELLED:
                # Cancellation is an operator pause, never an automatic requeue.
                connection.execute(
                    f"UPDATE {runtime.table('maintenance_obligations')} SET last_error=?, attempts=?, available_ms=? WHERE obligation_id=?",
                    (
                        "Job cancelled; explicit maintenance retry required",
                        declaration.retry_limit + 1,
                        now_ms(),
                        row["obligation_id"],
                    ),
                ).close()
            connection.execute(
                f"UPDATE {runtime.table('maintenance_obligations')} SET job_ledger_uuid=?, job_run_id=? WHERE obligation_id=?",
                (repository.ledger_uuid, run.job_run_id, row["obligation_id"]),
            ).close()
            published.append(run.job_run_id)
    return {"published": len(published), "run_ids": published}
