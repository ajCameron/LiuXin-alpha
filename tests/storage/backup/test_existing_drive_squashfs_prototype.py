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
    """Fast workflow double preserving the new declaration/checkpoint contract."""

    def __init__(self, declaration, output_root: Path):
        self.declaration = declaration
        self.output_root = output_root
        self.checkpoint = api.BackupWorkflowCheckpoint(
            declaration,
            api.WorkflowStatus.DRAFT,
        )

    def progress(self):
        return self.checkpoint

    def run_next(self):
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
