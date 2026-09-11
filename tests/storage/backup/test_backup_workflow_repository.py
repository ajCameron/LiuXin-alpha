"""
Exercise backup intent/checkpoint/result persistence through a real miniature SQLite schema.

Synthetic source identities and byte expectations test value encoding and reconstruction,
not physical backup verification. Cases cover replacement/deletion preconditions and
changed-intent rejection, with explicit database connection cleanup.
"""

from __future__ import annotations

import dataclasses

from pathlib import Path
from uuid import uuid4

import pytest

from tests.storage._mini_db import build_mini_db

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.backup import BackupWorkflowRepository


def _declaration(tmp_path: Path) -> api.BackupWorkflowDeclaration:
    """
    Build mixed local/routed backup intent with synthetic byte expectations and catalogue
    references.

    Allocate independent source/output Store UUIDs, retain two ordered member paths, and provide
    compression/deterministic options and a temporary staging path. No source files or Stores are
    created. Sizes/digests and Asset/Replica IDs exercise persistence encoding rather than verified
    physical provenance.

    Example:
        >>> declaration = _declaration(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real SQLite catalogue and local source/image paths.
    :return: BackupWorkflowDeclaration containing one local and one Location source with distinct expectation/provenance fields.
    """
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
    """
    Verify typed backup intent and staged/final evidence survive SQLite row reconstruction.

    Use the real miniature SQLite schema and repository adapter. Check initial DRAFT synthesis, a
    running checkpoint with a routed staging report, and a final checkpoint/result with stored
    status filtering and one output row. The successful flag is persisted metadata; no actual backup
    image, hash verification, or Store registration is performed. Close the SQLite connection in
    finally.

    Example:
        >>> test_repository_roundtrips_declaration_checkpoint_and_result(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real SQLite catalogue and local source/image paths.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Verify intent replacement retains an ID and deletion respects the stored lifecycle guard.

    Replace a DRAFT workflow name, reload the declaration, require default deletion to reject, then
    bypass the terminal check and confirm a repeated delete returns False. This does not claim
    checkpoint/output cleanup during replacement. The miniature SQLite connection closes in finally.

    Example:
        >>> test_repository_replacement_and_deletion_preconditions(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real SQLite catalogue and local source/image paths.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Verify a checkpoint with a changed declaration is rejected against durable intent.

    Persist one declaration in the miniature SQLite schema and construct a DRAFT checkpoint with a
    different name. save_checkpoint must raise StorePreconditionFailed identifying changed intent.
    The connection closes in finally.

    Example:
        >>> test_repository_rejects_checkpoint_for_different_intent(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real SQLite catalogue and local source/image paths.
    :return: None after the stated regression assertions pass.
    """
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
