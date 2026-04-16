from __future__ import annotations

from pathlib import Path

from LiuXin_alpha.storage import SquashfsBackupWorkflow
from LiuXin_alpha.storage.api import BackupWorkflowStatus, BackupWorkflowStepKind
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive import OnDiskUnmanagedStorageBackend


def _fake_mksquashfs(self, *, force: bool, quiet: bool) -> None:
    self.archive_path.parent.mkdir(parents=True, exist_ok=True)
    self.archive_path.write_bytes(b"fake squashfs archive")


def test_squashfs_backup_workflow_local_paths_resume(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(
        "LiuXin_alpha.storage.store_backend_plugins.squashfs_build.squashfs_build_storage_backend.SquashfsBuildStorageBackend._run_mksquashfs",
        _fake_mksquashfs,
    )
    source_a = tmp_path / "source_a.txt"
    source_b = tmp_path / "source_b.txt"
    source_a.write_text("alpha", encoding="utf-8")
    source_b.write_text("beta", encoding="utf-8")

    archive_path = tmp_path / "bundle.squashfs"
    staging_root = tmp_path / "staging"

    workflow = SquashfsBackupWorkflow(
        str(archive_path),
        workflow_name="nightly_backup",
        staging_root=str(staging_root),
        verify_after_build=True,
    )
    workflow.designate_local_path(str(source_a), archive_path="docs/source_a.txt")
    workflow.designate_local_path(str(source_b), archive_path="docs/source_b.txt")

    state = workflow.run_next()
    assert state.status is BackupWorkflowStatus.RUNNING
    assert state.next_source_index == 1
    assert len(state.source_results) == 1
    assert (staging_root / "docs" / "source_a.txt").read_text(encoding="utf-8") == "alpha"

    resumed = SquashfsBackupWorkflow.from_resume_state(state)
    resumed.run_to_completion()
    final_state = resumed.progress()

    assert final_state.status is BackupWorkflowStatus.COMPLETE
    assert final_state.output_artifact_url == str(archive_path)
    assert archive_path.exists()
    assert BackupWorkflowStepKind.STAGE_SOURCES in final_state.completed_steps
    assert BackupWorkflowStepKind.SEAL_ARTIFACT in final_state.completed_steps
    assert BackupWorkflowStepKind.VERIFY_ARTIFACT in final_state.completed_steps


def test_squashfs_backup_workflow_designate_location(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(
        "LiuXin_alpha.storage.store_backend_plugins.squashfs_build.squashfs_build_storage_backend.SquashfsBuildStorageBackend._run_mksquashfs",
        _fake_mksquashfs,
    )
    source_root = tmp_path / "source_root"
    source_root.mkdir()
    source_file = source_root / "nested" / "thing.txt"
    source_file.parent.mkdir(parents=True)
    source_file.write_text("hello from location", encoding="utf-8")

    source_store = OnDiskUnmanagedStorageBackend(url=str(source_root))
    source_location = source_store.locate("nested/thing.txt")

    archive_path = tmp_path / "location_bundle.squashfs"
    staging_root = tmp_path / "location_staging"

    workflow = SquashfsBackupWorkflow(
        str(archive_path),
        staging_root=str(staging_root),
        location_loader=source_store.locate,
    )
    workflow.designate_location(source_location)
    result = workflow.run_to_completion()

    assert result.status is BackupWorkflowStatus.COMPLETE
    assert (staging_root / "nested" / "thing.txt").read_text(encoding="utf-8") == "hello from location"
    assert archive_path.exists()
