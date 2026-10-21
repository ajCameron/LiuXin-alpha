"""Exercise real staging/cleanup while substituting only external archive boundaries."""

from __future__ import annotations

import dataclasses
from pathlib import Path
from types import SimpleNamespace

import pytest

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.backup import BackupWorkflowRepository, SquashfsBackupWorkflow
from LiuXin_alpha.storage.durable_manager import StorageManager
from LiuXin_alpha.storage.stores import FilesystemStore
from tests.storage._mini_db import build_mini_db
from tests.storage.backup.test_squashfs_backup_workflow import _install_fake_builder

READER = (
    "LiuXin_alpha.storage.backup.squashfs_backup_workflow."
    "SquashfsReadOnlyStorageBackend"
)
BUILDER = (
    "LiuXin_alpha.storage.backup.squashfs_backup_workflow.SquashfsBuildStorageBackend"
)


@pytest.fixture
def backup(monkeypatch, tmp_path):
    """Prepare a real caller-owned parent, source and run with cleanup enabled."""
    _install_fake_builder(monkeypatch)
    parent = tmp_path / "caller-staging"
    parent.mkdir()
    (parent / "keep.txt").write_bytes(b"caller data")
    source = tmp_path / "source.epub"
    source.write_bytes(b"book bytes")
    workflow = SquashfsBackupWorkflow(
        str(tmp_path / "backup.sqsh"),
        staging_target=str(parent),
        verify_after_build=False,
        cleanup_staging_after_success=True,
    )
    workflow.designate_local_path(str(source), archive_path="book.epub")
    return workflow, parent


@pytest.mark.parametrize("cleanup", [False, True])
def test_success_preserves_parent_and_other_runs(backup, tmp_path, cleanup):
    """Only this run's private scratch is eligible for automatic cleanup."""
    workflow, parent = backup
    workflow._cleanup_staging_after_success = cleanup
    other = SquashfsBackupWorkflow(
        str(tmp_path / "other.sqsh"),
        staging_target=str(parent),
    )
    other.designate_local_path(str(tmp_path / "source.epub"), archive_path="other.epub")
    other_state = other.run_next()
    state = workflow.run_next()
    workspace = Path(state.declaration.option_map()["staging_workspace"])
    assert Path(state.declaration.staging_target).parent == workspace
    assert workspace.parent == parent
    assert {entry.key for entry in workflow._builder.iter_locations()} == {"book.epub"}

    result = workflow.run_to_completion()

    assert result.successful
    assert (parent / "keep.txt").read_bytes() == b"caller data"
    assert (
        Path(other_state.declaration.staging_target) / "other.epub"
    ).read_bytes() == b"book bytes"
    assert workspace.exists() is not cleanup
    assert (api.BackupWorkflowStepKind.CLEANUP in result.completed_steps) is cleanup
    assert (
        Path(result.output_artifact_reference).read_bytes() == b"fake-squashfs-archive"
    )


@pytest.mark.parametrize("boundary", ["cancel", "build", "verify"])
def test_non_success_retains_scratch_and_caller_files(backup, monkeypatch, boundary):
    """Cancellation and failures on either side of sealing retain recovery evidence."""
    workflow, parent = backup
    state = workflow.run_next()
    if boundary == "cancel":
        state = workflow.cancel()
        assert workflow.run_to_completion().status is api.WorkflowStatus.CANCELLED
    else:
        if boundary == "build":

            def fail_build(*args, **kwargs):
                raise api.StoreIntegrityError("injected build failure")

            monkeypatch.setattr(f"{BUILDER}._run_mksquashfs", fail_build)
        else:
            workflow._verify_after_build = True
            monkeypatch.setattr(
                f"{READER}.self_test",
                lambda self: SimpleNamespace(
                    available=False, message="injected verification failure"
                ),
            )
        state = workflow.run_next()
        assert state.status is api.WorkflowStatus.FAILED
        assert "injected" in state.last_error

    assert (parent / "keep.txt").read_bytes() == b"caller data"
    assert (
        Path(state.declaration.staging_target) / "book.epub"
    ).read_bytes() == b"book bytes"
    assert api.BackupWorkflowStepKind.CLEANUP not in state.completed_steps


def test_persisted_failed_checkpoint_retries_and_cleans_only_owned_workspace(
    backup,
    monkeypatch,
    tmp_path,
):
    """A database round trip retains ownership and the same staged source bytes."""
    workflow, parent = backup
    workflow.run_next()
    with monkeypatch.context() as fault:

        def fail_build(*args, **kwargs):
            raise api.StoreIntegrityError("retry this build")

        fault.setattr(f"{BUILDER}._run_mksquashfs", fail_build)
        failed = workflow.run_next()
    assert failed.status is api.WorkflowStatus.FAILED
    db = build_mini_db(tmp_path / "workflow.sqlite")
    try:
        repository = BackupWorkflowRepository(db)
        workflow_id = repository.save_workflow_declaration(failed.declaration)
        repository.save_checkpoint(workflow_id, failed)
        persisted = repository.load_checkpoint(workflow_id)
    finally:
        db.conn.close()
    resumed = SquashfsBackupWorkflow.from_checkpoint(persisted)
    assert resumed.build_declaration() == failed.declaration
    # Recovery must use retained bytes; the original is now unavailable.
    (tmp_path / "source.epub").unlink()
    resumed.run_next()
    result = resumed.run_to_completion()
    assert result.successful
    assert not Path(result.declaration.option_map()["staging_workspace"]).exists()
    assert (parent / "keep.txt").read_bytes() == b"caller data"


@pytest.mark.parametrize("with_identity", [False, True])
def test_legacy_checkpoint_never_acquires_cleanup_ownership(backup, with_identity):
    """Old flat staging remains borrowed even when its saved cleanup flag is true."""
    workflow, parent = backup
    declaration = workflow.build_declaration()
    options = declaration.option_map()
    options.pop("staging_workspace")
    if not with_identity:
        options.pop("builder_store_uuid")
    legacy = dataclasses.replace(
        declaration,
        staging_target=str(parent),
        options=tuple(options.items()),
    )
    resumed = SquashfsBackupWorkflow.from_checkpoint(
        api.BackupWorkflowCheckpoint(legacy, api.WorkflowStatus.RUNNING),
    )
    result = resumed.run_to_completion()
    assert result.successful
    assert (parent / "keep.txt").read_bytes() == b"caller data"
    assert (parent / "book.epub").read_bytes() == b"book bytes"
    assert "staging_workspace" not in result.declaration.option_map()
    assert api.BackupWorkflowStepKind.CLEANUP not in result.completed_steps


@pytest.mark.parametrize(
    "damage", ["missing", "marker", "workspace_symlink", "members_symlink", "redirect"]
)
def test_recovery_refuses_unverifiable_workspace(backup, tmp_path, damage):
    """A stale checkpoint cannot recreate ownership or redirect cleanup to caller data."""
    workflow, parent = backup
    checkpoint = workflow.run_next()
    workspace = Path(checkpoint.declaration.option_map()["staging_workspace"])
    if damage == "missing":
        workspace.rename(parent / "retained-original")
    elif damage == "marker":
        (workspace / ".liuxin-backup-owner").write_text("some other run")
    elif damage == "workspace_symlink":
        workspace.rename(parent / "retained-original")
        workspace.symlink_to(parent, target_is_directory=True)
    elif damage == "members_symlink":
        members = Path(checkpoint.declaration.staging_target)
        members.rename(workspace / "retained-members")
        members.symlink_to(parent, target_is_directory=True)
    else:
        checkpoint = dataclasses.replace(
            checkpoint,
            declaration=dataclasses.replace(
                checkpoint.declaration, staging_target=str(parent)
            ),
        )
    with pytest.raises(api.StorePreconditionFailed, match="ownership"):
        SquashfsBackupWorkflow.from_checkpoint(checkpoint)
    if damage != "redirect":
        assert workflow.run_next().status is api.WorkflowStatus.FAILED
    assert not (tmp_path / "backup.sqsh").exists()
    assert (parent / "keep.txt").read_bytes() == b"caller data"


def test_cleanup_rechecks_ownership_after_publication(backup, monkeypatch):
    """Changing ownership during verification cannot authorize later deletion."""
    workflow, parent = backup
    workflow._verify_after_build = True
    workspace = Path(workflow.build_declaration().option_map()["staging_workspace"])

    def change_owner(self):
        (workspace / ".liuxin-backup-owner").unlink()
        return SimpleNamespace(available=True)

    monkeypatch.setattr(f"{READER}.self_test", change_owner)
    result = workflow.run_to_completion()
    assert result.status is api.WorkflowStatus.FAILED
    assert "ownership" in result.last_error
    assert Path(result.output_artifact_reference).is_file()
    assert api.BackupWorkflowStepKind.CLEANUP not in result.completed_steps
    assert workspace.is_dir()
    assert (parent / "keep.txt").read_bytes() == b"caller data"


@pytest.mark.parametrize("retry_publication", [False, True])
def test_routed_success_releases_intermediate_image(
    backup, monkeypatch, tmp_path, retry_publication
):
    """After managed publication both staged members and the local image are removed."""
    original, parent = backup
    destination = FilesystemStore(tmp_path / "destination")
    manager = StorageManager(stores=[destination], startup_on_add=True)
    try:
        workflow = SquashfsBackupWorkflow(
            destination.locate("packs/nightly.sqsh"),
            staging_target=str(parent),
            storage_manager=manager,
            verify_after_build=False,
            cleanup_staging_after_success=True,
        )
        workflow.designate_local_path(str(tmp_path / "source.epub"))
        if retry_publication:
            with monkeypatch.context() as fault:

                def fail_publication(*args, **kwargs):
                    raise api.StoreUnavailable("destination unavailable")

                fault.setattr(manager, "put", fail_publication)
                failed = workflow.run_to_completion()
            assert failed.status is api.WorkflowStatus.FAILED
            assert workflow._builder.archive_path.is_file()
            assert Path(failed.declaration.staging_target).is_dir()
            assert (parent / "keep.txt").read_bytes() == b"caller data"
            workflow = SquashfsBackupWorkflow.from_checkpoint(
                failed.final_checkpoint,
                storage_manager=manager,
            )
            workflow.run_next()
        result = workflow.run_to_completion()
        assert result.successful
        assert (
            manager.read_bytes(result.output_artifact_reference)
            == b"fake-squashfs-archive"
        )
        assert not workflow._builder.archive_path.exists()
        assert not Path(result.declaration.option_map()["staging_workspace"]).exists()
        assert Path(original.build_declaration().staging_target).is_dir()
        assert (parent / "keep.txt").read_bytes() == b"caller data"
    finally:
        manager.close()


def test_reconstructed_local_output_cannot_be_inside_cleanup_workspace(backup):
    """A changed declaration cannot cause successful cleanup to remove its output."""
    workflow, parent = backup
    declaration = workflow.build_declaration()
    workspace = Path(declaration.option_map()["staging_workspace"])
    changed = dataclasses.replace(
        declaration, output_target=str(workspace / "keep.sqsh")
    )
    with pytest.raises(api.StorePreconditionFailed, match="outside"):
        SquashfsBackupWorkflow.from_declaration(changed)
    assert workspace.is_dir()
    assert (parent / "keep.txt").read_bytes() == b"caller data"


def test_cleanup_error_is_reported_and_can_be_retried(backup, monkeypatch):
    """A refused deletion cannot be reported as completed cleanup or lose publication evidence."""
    workflow, parent = backup
    with monkeypatch.context() as fault:

        def refuse_cleanup(path):
            raise PermissionError("scratch is busy")

        fault.setattr(
            "LiuXin_alpha.storage.backup._staging.shutil.rmtree", refuse_cleanup
        )
        failed = workflow.run_to_completion()
    assert failed.status is api.WorkflowStatus.FAILED
    assert "scratch is busy" in failed.last_error
    assert api.BackupWorkflowStepKind.CLEANUP not in failed.completed_steps
    assert Path(failed.output_artifact_reference).is_file()
    resumed = SquashfsBackupWorkflow.from_checkpoint(failed.final_checkpoint)
    assert resumed.run_next().status is api.WorkflowStatus.COMPLETE
    assert not Path(failed.declaration.option_map()["staging_workspace"]).exists()
    assert (parent / "keep.txt").read_bytes() == b"caller data"


def test_constructor_failure_releases_only_newly_allocated_scratch(
    backup, monkeypatch, tmp_path
):
    """Builder setup failure preserves the parent and previously allocated workspaces."""
    _, parent = backup
    before = set(parent.iterdir())

    def fail_setup(*args, **kwargs):
        raise ValueError("injected builder setup failure")

    monkeypatch.setattr(f"{BUILDER}.__init__", fail_setup)
    with pytest.raises(ValueError, match="injected"):
        SquashfsBackupWorkflow(
            str(tmp_path / "another.sqsh"), staging_target=str(parent)
        )
    assert set(parent.iterdir()) == before
    assert (parent / "keep.txt").read_bytes() == b"caller data"


def test_default_scratch_has_the_same_durable_ownership(monkeypatch, tmp_path):
    """Omitting the parent still produces resumable, explicitly owned scratch."""
    _install_fake_builder(monkeypatch)
    monkeypatch.setattr(
        "LiuXin_alpha.storage.backup._staging.tempfile.tempdir", str(tmp_path)
    )
    source = tmp_path / "source"
    source.write_bytes(b"default scratch")
    workflow = SquashfsBackupWorkflow(
        str(tmp_path / "default.sqsh"),
        verify_after_build=False,
        cleanup_staging_after_success=True,
    )
    workflow.designate_local_path(str(source))
    checkpoint = workflow.run_next()
    resumed = SquashfsBackupWorkflow.from_checkpoint(checkpoint)
    result = resumed.run_to_completion()
    assert result.successful
    assert not Path(checkpoint.declaration.option_map()["staging_workspace"]).exists()
    assert source.read_bytes() == b"default scratch"
