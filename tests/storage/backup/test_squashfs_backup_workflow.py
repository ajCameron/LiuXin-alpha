from __future__ import annotations

from pathlib import Path

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.backup import SquashfsBackupWorkflow
from LiuXin_alpha.storage.store_manager import StorageManager
from LiuXin_alpha.storage.stores import FilesystemStore


def _fake_mksquashfs(self, output: Path, *, quiet: bool) -> None:
    del self, quiet
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(b"fake-squashfs-archive")


def _accept_fake_candidate(self, candidate: Path, manifest: object) -> None:
    """Keep workflow tests independent of SquashFS tools.

    Hostile-candidate validation is exercised by the backend tests; these tests
    substitute both sides of the external builder boundary.
    """

    del self
    assert candidate.read_bytes() == b"fake-squashfs-archive"
    assert manifest


def _install_fake_builder(monkeypatch) -> None:
    backend = (
        "LiuXin_alpha.storage.store_backend_plugins.squashfs_build."
        "squashfs_build_storage_backend.SquashfsBuildStorageBackend"
    )
    monkeypatch.setattr(f"{backend}._run_mksquashfs", _fake_mksquashfs)
    monkeypatch.setattr(f"{backend}._validate_candidate", _accept_fake_candidate)


def test_local_sources_checkpoint_resume_and_complete(monkeypatch, tmp_path: Path) -> None:
    _install_fake_builder(monkeypatch)
    first = tmp_path / "first.txt"
    second = tmp_path / "second.txt"
    first.write_bytes(b"alpha")
    second.write_bytes(b"beta")
    output = tmp_path / "bundle.sqsh"
    staging = tmp_path / "staging"

    workflow = SquashfsBackupWorkflow(
        str(output),
        workflow_name="nightly",
        staging_target=str(staging),
        verify_after_build=False,
    )
    workflow.designate_local_path(str(first), archive_path="docs/first.txt")
    workflow.designate_local_path(str(second), archive_path="docs/second.txt")

    checkpoint = workflow.run_next()
    assert checkpoint.status is api.WorkflowStatus.RUNNING
    assert checkpoint.next_source_index == 1
    assert (staging / "docs/first.txt").read_bytes() == b"alpha"

    resumed = SquashfsBackupWorkflow.from_checkpoint(checkpoint)
    result = resumed.run_to_completion()

    assert result.successful
    assert result.output_artifact_reference == str(output)
    assert output.read_bytes() == b"fake-squashfs-archive"
    assert len(result.source_reports) == 2
    assert api.BackupWorkflowStepKind.STAGE_SOURCES in result.completed_steps
    assert api.BackupWorkflowStepKind.SEAL_ARTIFACT in result.completed_steps


def test_store_location_source_streams_through_manager(monkeypatch, tmp_path: Path) -> None:
    _install_fake_builder(monkeypatch)
    source_store = FilesystemStore(tmp_path / "source")
    manager = StorageManager(stores=[source_store], startup_on_add=True)
    source = source_store.store_bytes(b"managed-source", location="objects/source")

    workflow = SquashfsBackupWorkflow(
        str(tmp_path / "managed.sqsh"),
        staging_target=str(tmp_path / "managed-staging"),
        verify_after_build=False,
        storage_manager=manager,
    )
    declaration = workflow.designate_location(
        source.location,
        archive_path="books/source.epub",
    )
    result = workflow.run_to_completion()

    assert declaration.source_identifier == source.location
    assert result.successful
    assert (tmp_path / "managed-staging/books/source.epub").read_bytes() == b"managed-source"


def test_source_snapshot_change_fails_without_publishing_archive(tmp_path: Path) -> None:
    source = tmp_path / "changing.bin"
    source.write_bytes(b"first")
    output = tmp_path / "must-not-exist.sqsh"
    workflow = SquashfsBackupWorkflow(
        str(output),
        staging_target=str(tmp_path / "failed-staging"),
        verify_after_build=False,
    )
    workflow.designate_local_path(str(source), archive_path="changing.bin")
    source.write_bytes(b"changed-size")

    checkpoint = workflow.run_next()

    assert checkpoint.status is api.WorkflowStatus.FAILED
    assert "expected" in (checkpoint.last_error or "")
    assert not output.exists()
    assert not (tmp_path / "failed-staging/changing.bin").exists()


def test_location_output_is_committed_through_manager(monkeypatch, tmp_path: Path) -> None:
    _install_fake_builder(monkeypatch)
    destination = FilesystemStore(tmp_path / "destination")
    manager = StorageManager(stores=[destination], startup_on_add=True)
    target = destination.locate("packs/nightly.sqsh")
    source = tmp_path / "source.epub"
    source.write_bytes(b"ebook")

    workflow = SquashfsBackupWorkflow(
        target,
        staging_target=str(tmp_path / "location-output-staging"),
        verify_after_build=False,
        storage_manager=manager,
    )
    workflow.designate_local_path(str(source), archive_path="source.epub")
    result = workflow.run_to_completion()

    assert result.output_artifact_reference == target
    assert manager.read_bytes(target) == b"fake-squashfs-archive"


def test_location_outputs_use_workflow_specific_local_artifacts(tmp_path: Path) -> None:
    destination = FilesystemStore(tmp_path / "destination")
    first = SquashfsBackupWorkflow(
        destination.locate("packs/first.sqsh"),
        staging_target=str(tmp_path / "first-staging"),
    )
    second = SquashfsBackupWorkflow(
        destination.locate("packs/second.sqsh"),
        staging_target=str(tmp_path / "second-staging"),
    )

    assert first._builder.archive_path != second._builder.archive_path


def test_existing_output_without_sealed_checkpoint_is_never_adopted(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _install_fake_builder(monkeypatch)
    output = tmp_path / "preexisting.sqsh"
    output.write_bytes(b"not-created-by-this-workflow")
    source = tmp_path / "source.epub"
    source.write_bytes(b"ebook")
    workflow = SquashfsBackupWorkflow(
        str(output),
        staging_target=str(tmp_path / "staging"),
        verify_after_build=False,
    )
    workflow.designate_local_path(str(source))

    result = workflow.run_to_completion()

    assert result.status is api.WorkflowStatus.FAILED
    assert "without a checkpoint" in (result.last_error or "")
    assert output.read_bytes() == b"not-created-by-this-workflow"
