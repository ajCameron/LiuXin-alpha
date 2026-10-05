"""
Verify Store registration reuse and protected presence metadata in miniature SQLite.

Opaque local image bytes stand in for SquashFS. Tests inspect persisted identity/link
behavior and database trigger rejection, without starting a reader or verifying archive
members. A failed-result case rejects before physical-path registration checks.
"""

from __future__ import annotations

import sqlite3

from pathlib import Path

import pytest

from tests.storage._mini_db import build_mini_db

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.backup import BackupArtifactRegistry, BackupWorkflowRepository


def test_registry_creates_stable_store_and_idempotent_protected_links(tmp_path: Path) -> None:
    """
    Verify repeated registration reuses Store identity and SQLite enforces link protection.

    Write opaque local image bytes, persist synthetic two-member backup intent and a COMPLETE
    checkpoint/result, then register twice. Assert equal registration values, one canonical-URI
    SquashFS Store row, two presence links, and iterator reconstruction. Direct SQL updates/deletes
    must be rejected by immutable/protected-link triggers. No manager is attached, member content is
    read, or genuine SquashFS image validated; close the connection in finally.

    Example:
        >>> test_registry_creates_stable_store_and_idempotent_protected_links(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real SQLite catalogue and local source/image paths.
    :return: None after the stated regression assertions pass.
    """
    db = build_mini_db(tmp_path / "registry.sqlite")
    try:
        artifact = tmp_path / "packs/nightly.sqsh"
        artifact.parent.mkdir()
        artifact.write_bytes(b"sealed-artifact")
        declaration = api.BackupWorkflowDeclaration(
            "nightly",
            api.BackupWorkflowKind.SQUASHFS_PACK,
            str(artifact),
            sources=(
                api.BackupSourceDeclaration(
                    api.BackupSourceKind.LOCAL_PATH,
                    str(tmp_path / "a.epub"),
                    archive_path="books/a.epub",
                ),
                api.BackupSourceDeclaration(
                    api.BackupSourceKind.LOCAL_PATH,
                    str(tmp_path / "b.epub"),
                    archive_path="books/b.epub",
                ),
            ),
        )
        repository = BackupWorkflowRepository(db)
        workflow_id = repository.save_workflow_declaration(declaration)
        checkpoint = api.BackupWorkflowCheckpoint(
            declaration,
            api.WorkflowStatus.COMPLETE,
            workflow_id=workflow_id,
            next_source_index=2,
            staged_source_count=2,
            completed_steps=(api.BackupWorkflowStepKind.SEAL_ARTIFACT,),
            output_artifact_reference=str(artifact),
        )
        result = api.BackupWorkflowResult(
            declaration,
            api.WorkflowStatus.COMPLETE,
            workflow_id=workflow_id,
            output_artifact_reference=str(artifact),
            completed_steps=checkpoint.completed_steps,
            final_checkpoint=checkpoint,
        )
        registry = BackupArtifactRegistry(db)

        registered = registry.register_artifact(workflow_id, result)
        repeated = registry.register_artifact(workflow_id, result)

        assert repeated == registered
        assert registered.presence_links_created == 2
        store_rows = db.search("stores", "store_uuid", str(registered.backup_store_ref))
        assert len(store_rows) == 1
        assert store_rows[0]["store_kind"] == "squashfs_readonly"
        assert store_rows[0]["store_root_uri"] == artifact.resolve().as_uri()
        assert list(registry.iter_artifact_registrations()) == [registered]

        links = db.search(
            "backup_presence_links",
            "backup_presence_link_backup_store_id",
            int(store_rows[0]["store_id"]),
        )
        assert len(links) == 2
        link_id = int(links[0]["backup_presence_link_id"])
        with db.conn:
            try:
                db.conn.execute(
                    "UPDATE backup_presence_links SET backup_presence_link_archive_path=? WHERE backup_presence_link_id=?",
                    ("changed.epub", link_id),
                )
            except sqlite3.IntegrityError as error:
                assert "immutable" in str(error).lower()
            else:  # pragma: no cover
                raise AssertionError("immutable link accepted an update")
        with db.conn:
            try:
                db.conn.execute(
                    "DELETE FROM backup_presence_links WHERE backup_presence_link_id=?",
                    (link_id,),
                )
            except sqlite3.IntegrityError as error:
                assert "protected" in str(error).lower()
            else:  # pragma: no cover
                raise AssertionError("protected link accepted deletion")
    finally:
        db.conn.close()


def test_registry_rejects_failed_or_missing_artifacts(tmp_path: Path) -> None:
    """
    Verify a FAILED result rejects before artifact registration.

    Persist intent naming an absent local image and construct a FAILED outcome with error text.
    Registration must raise StoreIntegrityError requiring success. The absent pathname supplies
    context, but this case does not reach the missing-file check for a successful result. Close the
    miniature SQLite connection in finally.

    Example:
        >>> test_registry_rejects_failed_or_missing_artifacts(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real SQLite catalogue and local source/image paths.
    :return: None after the stated regression assertions pass.
    """
    db = build_mini_db(tmp_path / "registry-errors.sqlite")
    try:
        declaration = api.BackupWorkflowDeclaration(
            "failed",
            api.BackupWorkflowKind.SQUASHFS_PACK,
            str(tmp_path / "missing.sqsh"),
        )
        workflow_id = BackupWorkflowRepository(db).save_workflow_declaration(declaration)
        failed = api.BackupWorkflowResult(
            declaration,
            api.WorkflowStatus.FAILED,
            workflow_id=workflow_id,
            last_error="build failed",
        )
        with pytest.raises(api.StoreIntegrityError, match="successful"):
            BackupArtifactRegistry(db).register_artifact(workflow_id, failed)
    finally:
        db.conn.close()
