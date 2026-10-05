"""
Provide test existing drive backup job handler utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test existing drive backup job handler through a consuming regression::

        python -m pytest -q tests/jobs/test_existing_drive_backup_job_handler.py
"""
from __future__ import annotations

from LiuXin_alpha.jobs.api import JobDefinition

import json

from pathlib import Path

from LiuXin_alpha.jobs.handlers.existing_drive_squashfs_backup import (
    ExistingDriveSquashfsBackupJobHandler,
    ExistingDriveSquashfsBackupJobPayload,
)
from LiuXin_alpha.jobs.handler_api import JobRunContext
from LiuXin_alpha.jobs.repository import JobRepository
from LiuXin_alpha.jobs.models import now_ep_k
from LiuXin_alpha.storage.backup.prototype_pipeline import PrototypeRunResult, IndexedStoreRun, PackExecutionRun


class _FakePrototype:
    """
    Provide the fakeprototype contract for validated ebook processing.

    Example:
        Exercise  FakePrototype through a consuming regression::

            python -m pytest -q tests/jobs/test_existing_drive_backup_job_handler.py
    """
    last_init_kwargs = None
    last_run_args = None

    def __init__(self, **kwargs):
        """
        Initialize and validate the fakeprototype state.

        Example:
            Exercise  FakePrototype.  init   through a consuming regression::

                python -m pytest -q tests/jobs/test_existing_drive_backup_job_handler.py


        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; validated state is stored on the receiving object.
        """
        type(self).last_init_kwargs = kwargs

    def run(self, input_paths):
        """
        Execute the configured conversion stage and return its primary result.

        Example:
            Exercise  FakePrototype.run through a consuming regression::

                python -m pytest -q tests/jobs/test_existing_drive_backup_job_handler.py


        :param input_paths: Value supplied for input paths under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        type(self).last_run_args = tuple(str(x) for x in input_paths)
        return PrototypeRunResult(
            database_path=str(_FakePrototype.last_init_kwargs["database_path"]),
            indexed_stores=(IndexedStoreRun("/tmp/source", 1, "store", "/tmp/source", 10, 8, 8, 0, 0),),
            executed_packs=(PackExecutionRun(1, "pack-0001", "/tmp/out.sqsh", 8, 1024, 2, 8),),
        )


def test_existing_drive_backup_handler_round_trip_payload_and_run(monkeypatch, tmp_path) -> None:
    """
    Perform the test existing drive backup handler round trip payload and run operation under explicit file-format and conversion rules.

    Example:
        Exercise test existing drive backup handler round trip payload and run through a consuming regression::

            python -m pytest -q tests/jobs/test_existing_drive_backup_job_handler.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    payload = ExistingDriveSquashfsBackupJobPayload(
        input_paths=(str(tmp_path / "input_a"), str(tmp_path / "input_b")),
        database_path=str(tmp_path / "library.sqlite"),
        output_dir=str(tmp_path / "packs"),
        target_pack_size_bytes=1024,
        max_files_per_pack=50,
        ebook_extensions=("epub", "pdf"),
    )
    handler = ExistingDriveSquashfsBackupJobHandler()
    handler.validate_payload(payload.to_json())

    repo = JobRepository(tmp_path / "jobs.sqlite")
    definition = repo.create_definition(
        JobDefinition(
            job_kind=handler.job_kind,
            job_name="Backup job",
            payload_json=payload.to_json(),
        )
    )
    queued = repo.enqueue_run(job_definition_id=int(definition.job_definition_id))
    context = JobRunContext(
        repository=repo,
        job_definition_id=int(definition.job_definition_id),
        job_run_id=int(queued.job_run_id),
        worker_id="worker-test",
        started_timestamp_ep_k=now_ep_k(),
    )

    monkeypatch.setattr("LiuXin_alpha.jobs.handlers.existing_drive_squashfs_backup.ExistingDriveSquashfsPrototype", _FakePrototype)
    result = handler.run(payload_json=payload.to_json(), run_context=context)

    assert result["total_indexed_stores"] == 1
    assert result["total_executed_packs"] == 1
    assert _FakePrototype.last_run_args == payload.input_paths
    assert Path(_FakePrototype.last_init_kwargs["output_dir"]).name == "packs"
