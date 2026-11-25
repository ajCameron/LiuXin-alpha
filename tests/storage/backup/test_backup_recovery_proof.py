"""A saved cursor alone must never authorize reuse of changed staged bytes."""

import dataclasses
import hashlib
from pathlib import Path

import pytest

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.backup import SquashfsBackupWorkflow
from LiuXin_alpha.storage.backup.recovery import can_resume_staging
from tests.storage.backup.test_squashfs_backup_workflow import _install_fake_builder


def test_only_pinned_owned_staging_is_eligible_and_restoration_rechecks_it(
    tmp_path, monkeypatch
):
    _install_fake_builder(monkeypatch)
    source = tmp_path / "source"
    source.write_bytes(b"original")
    workflow = SquashfsBackupWorkflow(
        str(tmp_path / "archive.sqsh"),
        staging_target=str(tmp_path / "staging"),
        verify_after_build=False,
    )
    workflow.designate_local_path(str(source), archive_path="first")
    workflow.designate_local_path(str(source), archive_path="second")
    saved = workflow.run_next()
    assert not can_resume_staging(saved)
    sources = tuple(
        dataclasses.replace(
            item,
            expected_size=8,
            expected_digest=api.Digest(
                "sha256", hashlib.sha256(b"original").hexdigest()
            ),
        )
        for item in saved.declaration.sources
    )
    pinned = dataclasses.replace(
        saved, declaration=dataclasses.replace(saved.declaration, sources=sources)
    )
    assert can_resume_staging(pinned)
    assert (
        SquashfsBackupWorkflow.from_checkpoint(pinned).progress().next_source_index == 1
    )
    (Path(saved.declaration.staging_target) / "first").write_bytes(b"tampered")
    with pytest.raises(api.StoreIntegrityError, match="digest"):
        SquashfsBackupWorkflow.from_checkpoint(pinned)
    assert not can_resume_staging(dataclasses.replace(pinned, next_source_index=2))
    assert not can_resume_staging(
        dataclasses.replace(pinned, output_artifact_reference="uncertain.sqsh")
    )
