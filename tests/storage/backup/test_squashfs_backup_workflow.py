"""
Exercise backup staging, checkpoint reconstruction, and image publication boundaries.

Local files and filesystem Store routing are real. Successful sealing tests substitute
both the external SquashFS builder and candidate validator and disable post-build checks;
they do not claim genuine archive-format validation. Failure cases preserve source/output
and staging assertions around the actual workflow implementation.
"""

from __future__ import annotations

from pathlib import Path

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.backup import SquashfsBackupWorkflow
from LiuXin_alpha.storage.durable_manager import StorageManager
from LiuXin_alpha.storage.stores import FilesystemStore


def _fake_mksquashfs(self, output: Path, *, quiet: bool) -> None:
    """
    Replace the external builder with fixed bytes at the requested candidate path.

    Create the parent directory and write the marker. The unused receiver/quiet flag preserve the
    bound-method call shape. Output is deliberately not a real SquashFS image and needs the paired
    validation substitute.

    Example:
        >>> _fake_mksquashfs(builder, output, quiet=True)  # doctest: +SKIP


    :param self: Injected builder receiver, ignored by this test substitute.
    :param output: Candidate Path created or overwritten with the fixed marker bytes.
    :param quiet: Build flag accepted for method compatibility and otherwise ignored.
    :return: None after writing the fake candidate bytes.
    """
    del self, quiet
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(b"fake-squashfs-archive")


def _accept_fake_candidate(self, candidate: Path, manifest: object) -> None:
    """
    Accept only the paired fake archive marker with a nonempty staged manifest.

    The helper substitutes the real candidate validation boundary and does not parse SquashFS,
    enumerate archive members, or verify member hashes. Lower-level backend tests cover that
    validation separately.

    Example:
        >>> _accept_fake_candidate(builder, candidate, manifest)  # doctest: +SKIP


    :param self: Injected build Store receiver, ignored by this validator.
    :param candidate: Path whose contents must equal the paired fake builder marker.
    :param manifest: Captured staging manifest required to be truthy, without detailed comparison.
    :return: None when candidate marker and manifest assertions pass.
    """

    del self
    assert candidate.read_bytes() == b"fake-squashfs-archive"
    assert manifest


def _install_fake_builder(monkeypatch) -> None:
    """
    Patch both external build execution and candidate validation for workflow-only tests.

    Use the concrete SquashFS build backend method paths so real staging and publication remain
    exercised around the paired substitutes. Pytest restores both patched attributes at teardown.

    Example:
        >>> _install_fake_builder(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring substituted external-builder and candidate-validation methods after the test.
    :return: None after installing the paired fake builder/validator methods.
    """
    backend = (
        "LiuXin_alpha.storage.store_backend_plugins.squashfs_build."
        "squashfs_build_storage_backend.SquashfsBuildStorageBackend"
    )
    monkeypatch.setattr(f"{backend}._run_mksquashfs", _fake_mksquashfs)
    monkeypatch.setattr(f"{backend}._validate_candidate", _accept_fake_candidate)


def test_local_sources_checkpoint_resume_and_complete(monkeypatch, tmp_path: Path) -> None:
    """
    Verify real local staging resumes from a checkpoint and publishes the fake image.

    Stage alpha as the first of two sources, inspect the cursor and staged bytes, reconstruct with
    the same staging intent, and complete the beta source and finalization. Assertions cover output
    marker, reports, and stage/seal milestones; both external building and candidate validation are
    substituted and post-build verification is disabled.

    Example:
        >>> test_local_sources_checkpoint_resume_and_complete(monkeypatch, tmp_path)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring substituted external-builder and candidate-validation methods after the test.
    :param tmp_path: Pytest temporary directory for real source, staging, image, Store, or catalogue files.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Verify a managed source is designated and streamed into local staging through the manager.

    Store real source bytes in a FilesystemStore, designate its Location, and complete the workflow
    with paired fake archive boundaries and post-build verification disabled. Inspect the retained
    source Location, success flag, and real staged bytes.

    Example:
        >>> test_store_location_source_streams_through_manager(monkeypatch, tmp_path)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring substituted external-builder and candidate-validation methods after the test.
    :param tmp_path: Pytest temporary directory for real source, staging, image, Store, or catalogue files.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Verify a source size change fails staging without a member or image publication.

    Designate a local file while it contains five bytes, then replace it with a longer value before
    run_next. Assert FAILED with an expected-size error and absence of both the final image and
    staged member. Failure occurs before an external archive command is needed.

    Example:
        >>> test_source_snapshot_change_fails_without_publishing_archive(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real source, staging, image, Store, or catalogue files.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Verify final image bytes are published at a managed destination Location.

    A real local source and staging area feed paired fake archive boundaries. Finalization routes
    the marker through the transient manager into a FilesystemStore; reading the requested Location
    must reproduce those bytes. This does not validate a genuine SquashFS container.

    Example:
        >>> test_location_output_is_committed_through_manager(monkeypatch, tmp_path)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring substituted external-builder and candidate-validation methods after the test.
    :param tmp_path: Pytest temporary directory for real source, staging, image, Store, or catalogue files.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Verify separate routed-output workflows choose different local build-image paths.

    Construct two builders with distinct staging/output intent and compare their private archive
    paths. No source is staged, manager publication is attempted, or archive tool invoked.

    Example:
        >>> test_location_outputs_use_workflow_specific_local_artifacts(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real source, staging, image, Store, or catalogue files.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Verify an existing image without this workflow's seal milestone remains untouched.

    Seed unrelated output bytes, designate and stage a local source, and run to a terminal result
    with fake archive boundaries available. Assert FAILED with the missing-checkpoint explanation
    and exact preservation of the existing output.

    Example:
        >>> test_existing_output_without_sealed_checkpoint_is_never_adopted(monkeypatch, tmp_path)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring substituted external-builder and candidate-validation methods after the test.
    :param tmp_path: Pytest temporary directory for real source, staging, image, Store, or catalogue files.
    :return: None after the stated regression assertions pass.
    """
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
