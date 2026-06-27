from __future__ import annotations

from pathlib import Path

from tests.storage._mini_db import build_mini_db

from LiuXin_alpha.storage.backup import BackupWorkflowRepository
from LiuXin_alpha.storage.api.backup_api import (
    BackupSourceKind,
    BackupSourceResult,
    BackupSourceSpec,
    BackupWorkflowKind,
    BackupWorkflowResumeState,
    BackupWorkflowSpec,
    BackupWorkflowStatus,
    BackupWorkflowStepKind,
)


def test_backup_workflow_repository_roundtrips_spec_and_resume_state(tmp_path: Path) -> None:
    db_path = tmp_path / "backup_workflow_repo.sqlite"
    db = build_mini_db(db_path)
    try:
        repo = BackupWorkflowRepository(db)
        spec = BackupWorkflowSpec(
            workflow_name="nightly-squashfs",
            workflow_kind=BackupWorkflowKind.SQUASHFS_PACK,
            output_url=str(tmp_path / "nightly.sqsh"),
            sources=(
                BackupSourceSpec(
                    source_kind=BackupSourceKind.LOCAL_PATH,
                    source_identifier=str(tmp_path / "src1.txt"),
                    archive_path="src1.txt",
                    expected_size=3,
                ),
                BackupSourceSpec(
                    source_kind=BackupSourceKind.STORE_LOCATION,
                    source_identifier="file:///store/book.epub",
                    archive_path="book.epub",
                    expected_hash="abc123",
                ),
            ),
            verify_after_build=True,
            cleanup_staging_after_success=False,
            staging_root=str(tmp_path / "stage"),
            options=(("compression", "zstd"), ("deterministic", "1")),
        )

        workflow_row = repo.save_workflow_spec(spec, destination_store_id=None, status=BackupWorkflowStatus.RUNNING)
        workflow_id = int(workflow_row["backup_workflow_id"])

        state = BackupWorkflowResumeState(
            spec=spec,
            status=BackupWorkflowStatus.RUNNING,
            next_source_index=1,
            staged_source_count=1,
            source_results=(
                BackupSourceResult(
                    source_index=0,
                    source_identifier=str(tmp_path / "src1.txt"),
                    archive_path="src1.txt",
                    staged_location_url="file:///stage/src1.txt",
                    ok=True,
                    error=None,
                ),
            ),
            completed_steps=(BackupWorkflowStepKind.STAGE_SOURCES,),
            output_artifact_url=None,
            last_error=None,
        )
        repo.save_resume_state(workflow_id, state)
        output_row = repo.record_output(
            workflow_id,
            output_url=str(tmp_path / "nightly.sqsh"),
            output_store_id=None,
            verified_ok=True,
        )

        loaded_spec = repo.load_workflow_spec(workflow_id)
        loaded_state = repo.load_resume_state(workflow_id)

        assert loaded_spec == spec
        assert loaded_state == state
        assert output_row["backup_workflow_output_url"] == str(tmp_path / "nightly.sqsh")
        assert int(output_row["backup_workflow_output_verified_ok"]) == 1

    finally:
        db.conn.close()
