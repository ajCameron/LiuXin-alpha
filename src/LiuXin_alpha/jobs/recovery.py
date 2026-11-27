"""Resume interrupted work only under its registered handler's recovery contract."""

from __future__ import annotations

from LiuXin_alpha.jobs.execution_guard import execution_guard, resource_guard
from LiuXin_alpha.jobs.handler_api import JobHandlerRegistry
from LiuXin_alpha.jobs.repository import JobRepository


def resume_interrupted(
    repository: JobRepository,
    handlers: JobHandlerRegistry,
    *,
    after_run_id: int = 0,
    limit: int = 32,
) -> tuple[list[int], int]:
    """Inspect one bounded page; never infer executor death from an expired lease.

    A handler must opt into both execution exclusion and replay safety. The OS
    guard proves a previous cooperating executor has left its effect region.
    The domain policy still decides whether repeating its captured intent is safe.
    Uncertain, cancelled or repeatedly interrupted work remains held.
    """
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError("Recovery page size must be between 1 and 100")
    with repository._connection() as connection:
        rows = connection.execute(
            "SELECT * FROM job_runs r WHERE state='aborted' AND job_run_id>? "
            "AND (recovery_state IS NULL OR recovery_state='waiting_executor') "
            "AND NOT EXISTS (SELECT 1 FROM job_runs c WHERE c.retry_of_job_run_id=r.job_run_id) "
            "ORDER BY job_run_id LIMIT ?",
            (after_run_id, limit),
        ).fetchall()
    resumed = []
    for row in rows:
        run = repository._run_from_row(row)
        assert run.job_run_id is not None  # Persisted rows always have a primary key.
        if run.cancel_requested or run.attempt_number > 3 or run.payload_json is None:
            repository.record_recovery_decision(run.job_run_id, "held")
            continue
        try:
            handler = handlers.get(run.job_kind)
        except KeyError:
            repository.record_recovery_decision(run.job_run_id, "held")
            continue
        if not handler.guarded_execution:
            repository.record_recovery_decision(run.job_run_id, "held")
            continue
        try:
            with (
                execution_guard(repository, run.job_run_id),
                resource_guard(repository, handler.execution_key(run.payload_json)),
            ):
                if not handler.can_resume(run.payload_json):
                    repository.record_recovery_decision(run.job_run_id, "held")
                    continue
                successor = repository.retry_run(
                    run.job_run_id,
                    request_key=f"recovery:{run.job_run_id}",
                    acknowledge_possible_effects=True,
                    reuse_successor=True,
                    automatic=True,
                )
                assert successor.job_run_id is not None
                resumed.append(successor.job_run_id)
                repository.record_recovery_decision(run.job_run_id, "resumed")
        except BlockingIOError, PermissionError:
            repository.record_recovery_decision(run.job_run_id, "waiting_executor")
            continue
        except Exception as error:
            repository.record_recovery_decision(run.job_run_id, "held")
            # A damaged checkpoint holds this action, not every later action in
            # the page. Do not include payloads or credentials in diagnostics.
            from LiuXin_alpha.utils.logging import default_log

            default_log.error(
                "Recovery proof failed for job %s (%s); held",
                run.job_run_id,
                type(error).__name__,
            )
    return resumed, int(rows[-1]["job_run_id"]) if rows else 0
