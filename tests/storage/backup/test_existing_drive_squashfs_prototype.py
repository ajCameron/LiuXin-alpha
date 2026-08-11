from __future__ import annotations

import pathlib

from LiuXin_alpha.library import Library
from LiuXin_alpha.storage.api.workflow_apis.backup_api import BackupWorkflowResumeState, BackupWorkflowStatus
from LiuXin_alpha.storage.backup import ExistingDriveSquashfsPrototype


class _FakeWorkflow:
    def __init__(self, spec):
        self.spec = spec
        self._index = 0
        self._state = BackupWorkflowResumeState(spec=spec, status=BackupWorkflowStatus.DRAFT)

    def progress(self):
        return self._state

    def run_next(self):
        total = len(self.spec.sources)
        if self._index < total:
            self._index += 1
            self._state = BackupWorkflowResumeState(spec=self.spec, status=BackupWorkflowStatus.RUNNING, next_source_index=self._index, staged_source_count=self._index, output_artifact_url=None)
            return self._state
        output_path = pathlib.Path(self.spec.output_url)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b'fake-squashfs-pack')
        self._state = BackupWorkflowResumeState(spec=self.spec, status=BackupWorkflowStatus.COMPLETE, next_source_index=total, staged_source_count=total, output_artifact_url=str(output_path))
        return self._state


def test_existing_drive_prototype_indexes_plans_and_registers_packs(tmp_path):
    source_a = tmp_path / 'source_a'
    source_b = tmp_path / 'source_b'
    source_a.mkdir()
    source_b.mkdir()
    (source_a / 'book1.epub').write_bytes(b'a' * 12)
    (source_a / 'cover.jpg').write_bytes(b'jpeg')
    (source_b / 'book2.epub').write_bytes(b'b' * 11)
    (source_b / 'book3.mobi').write_bytes(b'c' * 9)
    db_path = tmp_path / 'prototype.sqlite'
    output_dir = tmp_path / 'packs'
    prototype = ExistingDriveSquashfsPrototype(database_path=db_path, output_dir=output_dir, target_pack_size_bytes=16, verify_after_build=False, cleanup_staging_after_success=False, workflow_factory=_FakeWorkflow)
    result = prototype.run([source_a, source_b])
    assert result.total_indexed_stores == 2
    assert result.total_executed_packs == 3
    assert all(pathlib.Path(item.output_url).exists() for item in result.executed_packs)
    with Library(database_path=db_path, create=False, backup=False, storage_startup_on_add=False) as lib:
        stores = lib.db.driver_wrapper.read('stores')
        store_names = {str(row['store_name']) for row in stores}
        assert any(name.startswith('existing_disk_001_') for name in store_names)
        assert any(name.startswith('existing_disk_002_') for name in store_names)
        backup_rows = [row for row in stores if str(row['store_access_protocol']) == 'squashfs']
        assert len(backup_rows) == 3
        workflow_rows = lib.db.driver_wrapper.read('backup_workflows')
        assert len(workflow_rows) == 3
        source_rows = lib.db.driver_wrapper.read('backup_workflow_sources')
        assert len(source_rows) == 3
        presence_rows = lib.db.driver_wrapper.read('backup_presence_links')
        assert len(presence_rows) == 3
