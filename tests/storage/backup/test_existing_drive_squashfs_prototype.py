"""
Verify end-to-end prototype composition and optional genuine SquashFS persistence/readback.

One case uses a source-count/fixed-image double with real Library indexing and metadata
writes. The second requires installed SquashFS tools, builds and reads a real image after
Library/database reopen and source-directory rename, and checks byte and Store identity
persistence without claiming a separate operating-system process restart.
"""

from __future__ import annotations

import dataclasses
import shutil

from pathlib import Path, PurePosixPath

import pytest

from LiuXin_alpha.library import Library
from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.backup import (
    BackupArtifactRegistry,
    ExistingDriveSquashfsPrototype,
)


class _FakeWorkflow:
    """
    Model source-count checkpoint transitions and write a fixed output marker for prototype tests.

    The double retains declaration/output-root objects and replaces immutable checkpoints as each
    source is counted. It neither stages source bytes nor adds staging reports, verifies digests,
    executes archive tools, or checks recipe evidence. Only finalization writes a local marker at
    the declared Location key. It is a structural test double, not a BackupWorkflowAPI subclass.

    Example:
        >>> workflow = _FakeWorkflow(declaration, output_dir)  # doctest: +SKIP
    """

    def __init__(self, declaration, output_root: Path):
        """
        Retain test intent/output root and create the initial DRAFT checkpoint.

        No directory is created, source opened, or repository identity assigned. Checkpoint
        construction applies its ordinary selected value validation.

        Example:
            >>> workflow = _FakeWorkflow(declaration, output_dir)  # doctest: +SKIP


        :param declaration: Backup intent retained unchanged by the double.
        :param output_root: Local root used later to project the declared output Location key.
        :return: None after creating the initial checkpoint.
        """
        self.declaration = declaration
        self.output_root = output_root
        self.checkpoint = api.BackupWorkflowCheckpoint(
            declaration,
            api.WorkflowStatus.DRAFT,
        )

    def progress(self):
        """
        Return the currently retained checkpoint object without copying or advancing it.

        Example:
            >>> checkpoint = workflow.progress()  # doctest: +SKIP


        :return: Original current BackupWorkflowCheckpoint object.
        """
        return self.checkpoint

    def run_next(self):
        """
        Count one source as staged or write the fake image and set COMPLETE.

        Before source exhaustion, increment both source cursor and staged count without reading any
        source or adding reports. After exhaustion, require a Location output, join its key beneath
        the supplied root, create parents, overwrite marker bytes, and replace the checkpoint with
        COMPLETE and a seal milestone. There is no terminal-state guard, so a direct call after
        completion writes the marker again.

        Example:
            >>> state = workflow.run_next()  # doctest: +SKIP


        :return: Replaced RUNNING/COMPLETE checkpoint; file and value errors propagate without a FAILED transition.
        """
        index = self.checkpoint.next_source_index
        if index < len(self.declaration.sources):
            self.checkpoint = dataclasses.replace(
                self.checkpoint,
                status=api.WorkflowStatus.RUNNING,
                next_source_index=index + 1,
                staged_source_count=index + 1,
            )
            return self.checkpoint
        target = self.declaration.output_target
        assert isinstance(target, api.Location)
        output = self.output_root.joinpath(*PurePosixPath(target.key).parts)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(b"fake-squashfs-pack")
        self.checkpoint = dataclasses.replace(
            self.checkpoint,
            status=api.WorkflowStatus.COMPLETE,
            completed_steps=(api.BackupWorkflowStepKind.SEAL_ARTIFACT,),
            output_artifact_reference=target,
        )
        return self.checkpoint

    def run_to_completion(self):
        """
        Drive the fake workflow until its checkpoint is terminal and construct a result.

        Copy declaration, status, output, milestones, and final checkpoint into the result; source
        reports and workflow ID use their defaults. A preexisting terminal state stops the loop
        without another marker write.

        Example:
            >>> result = workflow.run_to_completion()  # doctest: +SKIP


        :return: BackupWorkflowResult containing the retained terminal checkpoint and fake output evidence.
        """
        while not self.checkpoint.status.terminal:
            self.run_next()
        return api.BackupWorkflowResult(
            self.declaration,
            self.checkpoint.status,
            output_artifact_reference=self.checkpoint.output_artifact_reference,
            completed_steps=self.checkpoint.completed_steps,
            final_checkpoint=self.checkpoint,
        )


def test_existing_drive_prototype_indexes_plans_and_registers_packs(tmp_path: Path) -> None:
    """
    Verify the prototype composes real indexing/planning/persistence around fake pack execution.

    Two source directories contain three ebooks and an excluded cover. A 16-byte target yields three
    singleton packs through the workflow double. Assert output files and distinct archive Store
    UUIDs, then reopen Library and inspect source/archive Stores, workflow/source rows, and presence
    links. The written images are markers; this case does not validate SquashFS content or run an
    external builder.

    Example:
        >>> test_existing_drive_prototype_indexes_plans_and_registers_packs(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real SQLite catalogue and local source/image paths.
    :return: None after the stated regression assertions pass.
    """
    source_a = tmp_path / "source_a"
    source_b = tmp_path / "source_b"
    source_a.mkdir()
    source_b.mkdir()
    (source_a / "book1.epub").write_bytes(b"a" * 12)
    (source_a / "cover.jpg").write_bytes(b"jpeg")
    (source_b / "book2.epub").write_bytes(b"b" * 11)
    (source_b / "book3.mobi").write_bytes(b"c" * 9)
    database_path = tmp_path / "prototype.sqlite"
    output_dir = tmp_path / "packs"
    prototype = ExistingDriveSquashfsPrototype(
        database_path=database_path,
        output_dir=output_dir,
        target_pack_size_bytes=16,
        verify_after_build=False,
        cleanup_staging_after_success=False,
        workflow_factory=lambda declaration: _FakeWorkflow(
            declaration,
            output_dir,
        ),
    )

    result = prototype.run([source_a, source_b])

    assert result.total_indexed_stores == 2
    assert result.total_executed_packs == 3
    assert all(Path(item.output_url).is_file() for item in result.executed_packs)
    assert len({item.backup_store_ref for item in result.executed_packs}) == 3

    with Library(
        database_path=database_path,
        create=False,
        backup=False,
        storage_startup_on_add=False,
    ) as library:
        stores = library.db.driver_wrapper.read("stores")
        store_names = {str(row["store_name"]) for row in stores}
        assert any(name.startswith("existing_disk_001_") for name in store_names)
        assert any(name.startswith("existing_disk_002_") for name in store_names)
        archive_rows = [
            row for row in stores if str(row["store_kind"]) == "squashfs_readonly"
        ]
        assert len(archive_rows) == 3
        assert all(row["store_uuid"] not in (None, "") for row in archive_rows)
        assert len(library.db.driver_wrapper.read("backup_workflows")) == 3
        assert len(library.db.driver_wrapper.read("backup_workflow_sources")) == 3
        assert len(library.db.driver_wrapper.read("backup_presence_links")) == 3


def test_existing_drive_prototype_pack_reads_after_database_restart_and_source_loss(
    tmp_path: Path,
) -> None:
    """
    Verify a genuine SquashFS pack remains readable after Library reopen and source-root removal.

    Skip only when mksquashfs or unsquashfs is unavailable. Build a real verified image containing
    two ebook paths with composed/decomposed Unicode and punctuation plus binary payload bytes,
    excluding a cover. Request staging cleanup, rename the original source directory, then reopen
    Library and read every archived member through the registered Store. Assert exact
    inventory/content and persisted registration/link count. The process remains the same;
    Library/database and Store objects are reconstructed.

    Example:
        >>> test_existing_drive_prototype_pack_reads_after_database_restart_and_source_loss(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real SQLite catalogue and local source/image paths.
    :return: None after the stated regression assertions pass.
    """
    if shutil.which("mksquashfs") is None or shutil.which("unsquashfs") is None:
        pytest.skip("squashfs-tools not available in environment")
    source = tmp_path / "indexed-source"
    expected = {
        "Author/Café-Café.epub": b"epub payload\x00\xff",
        "odd/100%-[draft]-question?.mobi": b"mobi payload",
    }
    for key, payload in expected.items():
        path = source.joinpath(*PurePosixPath(key).parts)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
    (source / "ignored-cover.jpg").write_bytes(b"not an ebook")
    database_path = tmp_path / "prototype-readback.sqlite"
    output_dir = tmp_path / "packs"
    prototype = ExistingDriveSquashfsPrototype(
        database_path=database_path,
        output_dir=output_dir,
        target_pack_size_bytes=1024 * 1024,
        verify_after_build=True,
        cleanup_staging_after_success=True,
    )

    result = prototype.run([source])

    assert result.total_indexed_stores == 1
    assert result.total_executed_packs == 1
    pack = result.executed_packs[0]
    assert pack.source_count == len(expected)
    assert Path(pack.output_url).is_file()

    # Make the original indexed Store unavailable.  The next process must be
    # reading the registered SquashFS Store, not accidentally falling back to
    # the source tree or retaining the workflow's in-process Store object.
    source.rename(tmp_path / "indexed-source-offline")
    with Library(
        database_path=database_path,
        create=False,
        backup=False,
        storage_startup_on_add=True,
    ) as library:
        archive_store = library.storage.get_store(pack.backup_store_ref)

        assert archive_store.status().available is True
        discovered = {
            location.key: location for location in archive_store.iter_locations()
        }
        assert set(discovered) == set(expected)
        for key, payload in expected.items():
            assert library.storage.read_bytes(discovered[key]) == payload

        registrations = tuple(
            BackupArtifactRegistry(
                library.db,
                storage_manager=library.storage,
            ).iter_artifact_registrations()
        )
        assert len(registrations) == 1
        assert registrations[0].backup_store_ref == pack.backup_store_ref
        assert registrations[0].presence_links_created == len(expected)
