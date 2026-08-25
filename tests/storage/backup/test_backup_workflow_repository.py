from __future__ import annotations

import dataclasses

from pathlib import Path
from uuid import uuid4

import pytest

from tests.storage._mini_db import build_mini_db

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.backup import BackupWorkflowRepository


def _declaration(tmp_path: Path) -> api.BackupWorkflowDeclaration:
    store_ref = uuid4()
    return api.BackupWorkflowDeclaration(
        "nightly-squashfs",
        api.BackupWorkflowKind.SQUASHFS_PACK,
        api.Location(uuid4(), "packs/nightly.sqsh"),
        sources=(
            api.BackupSourceDeclaration(
                api.BackupSourceKind.LOCAL_PATH,
                str(tmp_path / "local.epub"),
                archive_path="books/local.epub",
                expected_size=3,
                expected_digest=api.Digest("sha256", "a" * 64),
                source_digital_asset_id=11,
            ),
            api.BackupSourceDeclaration(
                api.BackupSourceKind.STORE_LOCATION,
                api.Location(store_ref, "objects/book.epub"),
                archive_path="books/managed.epub",
                expected_size=7,
                expected_digest=api.Digest("blake2b", "b" * 64),
                source_replica_id=22,
            ),
        ),
        staging_target=str(tmp_path / "staging"),
        options=(("compression", "zstd"), ("deterministic", "1")),
    )


def test_repository_roundtrips_declaration_checkpoint_and_result(tmp_path: Path) -> None:
    db = build_mini_db(tmp_path / "workflow.sqlite")
    try:
        repository = BackupWorkflowRepository(db)
        declaration = _declaration(tmp_path)
        workflow_id = repository.save_workflow_declaration(declaration)

        assert isinstance(workflow_id, int)
        assert repository.load_workflow_declaration(workflow_id) == declaration
        assert repository.load_checkpoint(workflow_id) == api.BackupWorkflowCheckpoint(
            declaration,
            api.WorkflowStatus.DRAFT,
            workflow_id=workflow_id,
        )

        report = api.BackupSourceStagingReport(
            0,
            declaration.sources[0].source_identifier,
            "books/local.epub",
            staged_location=api.Location(uuid4(), "books/local.epub"),
            bytes_staged=3,
            digest_verified=True,
        )
        checkpoint = api.BackupWorkflowCheckpoint(
            declaration,
            api.WorkflowStatus.RUNNING,
            workflow_id=workflow_id,
            next_source_index=1,
            staged_source_count=1,
            source_reports=(report,),
        )
        repository.save_checkpoint(workflow_id, checkpoint)
        assert repository.load_checkpoint(workflow_id) == checkpoint

        final = dataclasses.replace(
            checkpoint,
            status=api.WorkflowStatus.COMPLETE,
            next_source_index=2,
            staged_source_count=1,
            completed_steps=(api.BackupWorkflowStepKind.SEAL_ARTIFACT,),
            output_artifact_reference=declaration.output_target,
        )
        result = api.BackupWorkflowResult(
            declaration,
            api.WorkflowStatus.COMPLETE,
            workflow_id=workflow_id,
            output_artifact_reference=declaration.output_target,
            source_reports=(report,),
            completed_steps=final.completed_steps,
            final_checkpoint=final,
        )
        repository.record_result(workflow_id, result)

        assert repository.load_checkpoint(workflow_id) == final
        assert list(
            repository.iter_workflow_declarations(
                status=api.WorkflowStatus.COMPLETE
            )
        ) == [(workflow_id, declaration)]
        output = db.search(
            "backup_workflow_outputs",
            "backup_workflow_output_workflow_id",
            workflow_id,
        )
        assert len(output) == 1
        assert int(output[0]["backup_workflow_output_verified_ok"]) == 1
    finally:
        db.conn.close()


def test_repository_replacement_and_deletion_preconditions(tmp_path: Path) -> None:
    db = build_mini_db(tmp_path / "workflow-delete.sqlite")
    try:
        repository = BackupWorkflowRepository(db)
        declaration = _declaration(tmp_path)
        workflow_id = repository.save_workflow_declaration(declaration)

        changed = dataclasses.replace(declaration, workflow_name="changed")
        assert repository.save_workflow_declaration(
            changed,
            workflow_id=workflow_id,
        ) == workflow_id
        assert repository.load_workflow_declaration(workflow_id) == changed

        with pytest.raises(api.StorePreconditionFailed):
            repository.delete_workflow(workflow_id)
        assert repository.delete_workflow(workflow_id, require_terminal=False)
        assert not repository.delete_workflow(workflow_id, require_terminal=False)
    finally:
        db.conn.close()


def test_repository_rejects_checkpoint_for_different_intent(tmp_path: Path) -> None:
    db = build_mini_db(tmp_path / "workflow-mismatch.sqlite")
    try:
        repository = BackupWorkflowRepository(db)
        declaration = _declaration(tmp_path)
        workflow_id = repository.save_workflow_declaration(declaration)
        wrong = dataclasses.replace(declaration, workflow_name="wrong")

        with pytest.raises(api.StorePreconditionFailed, match="differs"):
            repository.save_checkpoint(
                workflow_id,
                api.BackupWorkflowCheckpoint(wrong, api.WorkflowStatus.DRAFT),
            )
    finally:
        db.conn.close()
