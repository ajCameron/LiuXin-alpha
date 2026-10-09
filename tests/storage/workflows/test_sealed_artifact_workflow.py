"""
Verify whole-image ingest/adoption and sealed-artifact provenance composition.

Filesystem bytes and catalogue operations are real; image fixtures are opaque markers,
executor references are synthetic, and the backup bridge uses paired build/validation
substitutes. Tests inspect recorded recipe evidence and manager-reload persistence without
claiming actual archive parsing or reproducible external-tool execution.
"""

from __future__ import annotations

import dataclasses

from pathlib import Path
from uuid import uuid4

import pytest

from LiuXin_alpha.storage import api
from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.storage.backup import (
    BackupWorkflowRepository,
    SquashfsBackupWorkflow,
)
from LiuXin_alpha.storage.durable_manager import StorageManager
from LiuXin_alpha.storage.stores import FilesystemStore
from LiuXin_alpha.storage.workflows import SealedArtifactWorkflow


def _executor(name: str = "mksquashfs") -> api.ReproductionRecipeArtifactReference:
    """
    Create a syntactically pinned tool reference without a real executable or retrieval probe.

    The fixed SHA-256 text, test version, and file URI supply recipe evidence for validation. The
    referenced file is not created or fetched; callers must not treat this fixture as executed-tool
    proof.

    Example:
        >>> _executor("rar").name
        'rar'


    :param name: Tool label used in the reference and synthetic file URI; defaults to mksquashfs.
    :return: ReproductionRecipeArtifactReference containing fixed test digest/version/retrieval metadata.
    """
    return api.ReproductionRecipeArtifactReference(
        name,
        api.Digest("sha256", "e" * 64),
        version="test-1",
        uri=f"file:///reproduction-tools/{name}",
    )


def _fake_mksquashfs(self, output: Path, *, quiet: bool) -> None:
    """
    Write the sealed-workflow test marker as the requested archive candidate.

    Create parent directories and replace candidate bytes. This is not a SquashFS encoder; the
    accompanying validation substitute accepts the marker so tests can focus on staging and
    provenance.

    Example:
        >>> _fake_mksquashfs(builder, output, quiet=True)  # doctest: +SKIP


    :param self: Injected build Store receiver, ignored by the substitute.
    :param output: Candidate Path receiving the fixed fake SquashFS image bytes.
    :param quiet: Build flag accepted for signature compatibility and ignored.
    :return: None after writing the fake image marker.
    """
    del self, quiet
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(b"fake-squashfs-image")


def _accept_fake_candidate(self, candidate: Path, manifest: object) -> None:
    """
    Check the paired fake image marker and require a nonempty staging manifest.

    This replaces candidate validation in the end-to-end workflow test. It does not inspect real
    archive metadata or compare extracted member contents.

    Example:
        >>> _accept_fake_candidate(builder, candidate, manifest)  # doctest: +SKIP


    :param self: Injected build Store receiver, ignored here.
    :param candidate: Path expected to contain the fake sealed-image marker.
    :param manifest: Truthy staging manifest supplied by the actual builder path.
    :return: None when both substitute-boundary assertions pass.
    """

    del self
    assert candidate.read_bytes() == b"fake-squashfs-image"
    assert manifest


def test_managed_artifact_is_adopted_and_recorded_once(tmp_path: Path) -> None:
    """
    Verify routed image adoption records package evidence and reuses identical provenance.

    Use real filesystem bytes and a transient manager, retaining one operation ID and workflow ID
    across repeated calls. Assert image metadata, ARCHIVE Replica mode, PACKAGE derivation, pinned
    member path/identity, output digests, and one reused derivation. Exact-recreation flags are
    declared recipe evidence; no archive contents or replay command are executed.

    Example:
        >>> test_managed_artifact_is_adopted_and_recorded_once(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real source, staging, image, Store, or catalogue files.
    :return: None after the stated regression assertions pass.
    """
    store = FilesystemStore(tmp_path / "store")
    manager = StorageManager(stores=[store], startup_on_add=True)
    source = manager.ingest_bytes(b"source ebook")
    artifact = store.store_bytes(
        b"sealed bytes",
        location="packs/books.squashfs",
    ).location
    workflow = SealedArtifactWorkflow(manager)
    operation_id = uuid4()

    first = workflow.record_artifact(
        artifact,
        {"books/novel.epub": source},
        artifact_format="squashfs",
        executor=_executor(),
        command=("mksquashfs", ".", "artifact.squashfs", "-noappend"),
        parameters={"compression": "zstd"},
        reproducibility="exact",
        workflow_id=41,
        operation_id=operation_id,
    )
    repeated = workflow.record_artifact(
        artifact,
        {"books/novel.epub": source},
        artifact_format="squashfs",
        executor=_executor(),
        command=("mksquashfs", ".", "artifact.squashfs", "-noappend"),
        parameters={"compression": "zstd"},
        reproducibility="exact",
        workflow_id=41,
        operation_id=operation_id,
    )

    assert first.asset_record.metadata.media_type == "application/vnd.squashfs"
    assert first.replica_record.location == artifact
    assert first.replica_record.mode is api.ReplicaMode.ARCHIVE
    assert first.derivation_record.declaration.kind is api.DigitalAssetDerivationKind.PACKAGE
    assert first.derivation_record.can_recreate_exactly
    assert first.recipe.inputs[0].logical_path == "books/novel.epub"
    assert first.recipe.inputs[0].digital_asset_id == source.asset_record.digital_asset_id
    assert first.recipe.expected_output_digests == first.asset_record.digests
    assert repeated.derivation_record == first.derivation_record
    assert len(tuple(manager.iter_digital_asset_derivation_records())) == 1


def test_local_rar_artifact_is_ingested_into_managed_storage(tmp_path: Path) -> None:
    """
    Verify an external local image is ingested with the expected RAR recipe and mode.

    Opaque fixture bytes stand in for a RAR image. Assert their managed readback, selected media
    type, ARCHIVE storage route, BEST_EFFORT classification, and recorded RAR 4 compression command
    prefix. The test neither executes RAR nor validates a real container.

    Example:
        >>> test_local_rar_artifact_is_ingested_into_managed_storage(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real source, staging, image, Store, or catalogue files.
    :return: None after the stated regression assertions pass.
    """
    store = FilesystemStore(tmp_path / "managed")
    manager = StorageManager(stores=[store], startup_on_add=True)
    source = manager.store_bytes(b"source")
    artifact = tmp_path / "outside" / "books.rar"
    artifact.parent.mkdir()
    artifact.write_bytes(b"rar image")

    registration = SealedArtifactWorkflow(manager).record_rar_artifact(
        artifact,
        {"books/source.epub": source},
        executor=_executor("rar"),
        compression_level=4,
    )

    assert registration.artifact_format is api.SealedArtifactFormat.RAR
    assert registration.asset_record.metadata.media_type == "application/vnd.rar"
    assert registration.replica_record.location.store_ref == store.store_ref
    assert manager.read_file(registration.asset_record, mode="archive") == b"rar image"
    assert registration.recipe.reproducibility is api.Reproducibility.BEST_EFFORT
    assert registration.recipe.command[:5] == ("rar", "a", "-ma4", "-m4", "-s-")


def test_squashfs_backup_result_uses_catalogued_sources_and_build_settings(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """
    Verify backup intent becomes complete SquashFS provenance with namespaced workflow identity.

    Paired fake build/validation methods surround real source adoption, staging, and routed image
    publication. Designation captures Asset/Replica IDs; gzip/deterministic settings become
    command/JSON evidence. Assert the fixed build flags, same image Location, and backup:73 external
    reference with catalogue workflow_id unset. The EXACT flag is a recipe claim rather than
    demonstrated replay.

    Example:
        >>> test_squashfs_backup_result_uses_catalogued_sources_and_build_settings(monkeypatch, tmp_path)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring substituted external-builder and candidate-validation methods after the test.
    :param tmp_path: Pytest temporary directory for real source, staging, image, Store, or catalogue files.
    :return: None after the stated regression assertions pass.
    """
    monkeypatch.setattr(
        "LiuXin_alpha.storage.store_backend_plugins.squashfs_build."
        "squashfs_build_storage_backend.SquashfsBuildStorageBackend._run_mksquashfs",
        _fake_mksquashfs,
    )
    monkeypatch.setattr(
        "LiuXin_alpha.storage.store_backend_plugins.squashfs_build."
        "squashfs_build_storage_backend.SquashfsBuildStorageBackend._validate_candidate",
        _accept_fake_candidate,
    )
    store = FilesystemStore(tmp_path / "store")
    manager = StorageManager(stores=[store], startup_on_add=True)
    source = manager.ingest_bytes(b"managed source")
    output = store.locate("packs/nightly.squashfs")
    backup = SquashfsBackupWorkflow(
        output,
        staging_target=str(tmp_path / "staging"),
        storage_manager=manager,
        verify_after_build=False,
        compression="gzip",
        deterministic=True,
    )
    designated = backup.designate_location(
        source.replica_record.location,
        archive_path="library/book.epub",
    )
    result = dataclasses.replace(backup.run_to_completion(), workflow_id=73)

    registration = SealedArtifactWorkflow(manager).record_backup_result(
        result,
        executor=_executor(),
    )

    assert designated.source_digital_asset_id == source.asset_record.digital_asset_id
    assert designated.source_replica_id == source.replica_record.replica_id
    assert registration.replica_record.location == output
    assert registration.recipe.can_recreate_exactly
    assert '"compression":"gzip"' in registration.recipe.parameters_json
    assert '"deterministic":true' in registration.recipe.parameters_json
    assert registration.recipe.command[-7:] == (
        "-all-root",
        "-no-xattrs",
        "-all-time",
        "0",
        "-mkfs-time",
        "0",
        "-quiet",
    )
    assert registration.derivation_record.declaration.workflow_id is None
    assert (
        registration.derivation_record.declaration.workflow_reference
        == "backup:73"
    )


def test_backup_adapter_requires_catalogue_identity_or_explicit_override(
    tmp_path: Path,
) -> None:
    """
    Verify uncatalogued backup members require an explicit matching-path Asset override.

    Build a terminal result around real opaque image bytes and a local source designation without an
    Asset ID. The first call must reject with source_assets guidance; after ingesting the member, an
    exact-path override records that Asset as the recipe input.

    Example:
        >>> test_backup_adapter_requires_catalogue_identity_or_explicit_override(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real source, staging, image, Store, or catalogue files.
    :return: None after the stated regression assertions pass.
    """
    artifact = tmp_path / "pack.squashfs"
    artifact.write_bytes(b"pack")
    source_path = tmp_path / "book.epub"
    source_path.write_bytes(b"book")
    declaration = api.BackupWorkflowDeclaration(
        "uncatalogued",
        api.BackupWorkflowKind.SQUASHFS_PACK,
        str(artifact),
        sources=(
            api.BackupSourceDeclaration(
                api.BackupSourceKind.LOCAL_PATH,
                str(source_path),
                archive_path="book.epub",
            ),
        ),
    )
    result = api.BackupWorkflowResult(
        declaration,
        api.WorkflowStatus.COMPLETE,
        output_artifact_reference=str(artifact),
    )
    store = FilesystemStore(tmp_path / "store")
    manager = StorageManager(stores=[store], startup_on_add=True)
    source = manager.store_bytes(b"book")
    recorder = SealedArtifactWorkflow(manager)

    with pytest.raises(api.StoragePreconditionFailed, match="source_assets"):
        recorder.record_backup_result(result, executor=_executor())

    registered = recorder.record_backup_result(
        result,
        executor=_executor(),
        source_assets={"book.epub": source},
    )
    assert registered.recipe.inputs[0].digital_asset_id == source.digital_asset_id


def test_derivation_recipe_survives_database_manager_restart(
    driver_spec,
    tmp_path: Path,
) -> None:
    """
    Verify backup-derived provenance persists across manager reconstruction over one open catalogue.

    Use the selected real database adapter and filesystem Store, persist backup intent, catalogue an
    opaque image, then close and reconstruct the manager against the same open Database and Store
    UUID. Assert retained recipe, member identity/path, namespaced backup reference, and reuse of
    the same derivation on repetition. This is a manager reload, not a process or database reopen,
    and it does not execute an archive tool.

    Example:
        >>> test_derivation_recipe_survives_database_manager_restart(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Parameterized database-driver specification selecting the real catalogue adapter for the test.
    :param tmp_path: Pytest temporary directory for real source, staging, image, Store, or catalogue files.
    :return: None after the stated regression assertions pass.
    """
    store_ref = uuid4()
    root = tmp_path / "store"
    with Database(
        metadata={"database_path": str(tmp_path / "catalogue.sqlite")},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        enable_storage_manager=False,
    ) as db:
        first_store = FilesystemStore(root, uuid=store_ref)
        manager = StorageManager(db=db, stores=[first_store], startup_on_add=True)
        source = manager.ingest_bytes(b"database source")
        artifact = first_store.store_bytes(
            b"database artifact",
            location="packs/archive.squashfs",
        ).location
        declaration = api.BackupWorkflowDeclaration(
            "database-pack",
            api.BackupWorkflowKind.SQUASHFS_PACK,
            artifact,
            sources=(
                api.BackupSourceDeclaration(
                    api.BackupSourceKind.STORE_LOCATION,
                    source.replica_record.location,
                    archive_path="source.bin",
                    expected_size=source.asset_record.size_bytes,
                    expected_digest=source.asset_record.digests[0],
                    source_digital_asset_id=source.asset_record.digital_asset_id,
                    source_replica_id=source.replica_record.replica_id,
                ),
            ),
            verify_after_build=False,
            options=(("compression", "zstd"), ("deterministic", "1")),
        )
        workflow_id = BackupWorkflowRepository(db).save_workflow_declaration(
            declaration
        )
        result = api.BackupWorkflowResult(
            declaration,
            api.WorkflowStatus.COMPLETE,
            workflow_id=workflow_id,
            output_artifact_reference=artifact,
        )
        recorded = SealedArtifactWorkflow(manager).record_backup_result(
            result,
            executor=_executor(),
        )
        derivation_id = recorded.derivation_record.digital_asset_derivation_id
        asset_id = recorded.asset_record.digital_asset_id
        manager.close()

        reloaded_store = FilesystemStore(root, uuid=store_ref)
        reloaded = StorageManager(
            db=db,
            stores=[reloaded_store],
            startup_on_add=True,
        )
        persisted = reloaded.get_digital_asset_derivation_record(derivation_id)

        assert persisted.declaration.result_digital_asset_id == asset_id
        assert persisted.declaration.recipe == recorded.recipe
        assert persisted.declaration.workflow_id is None
        assert persisted.declaration.workflow_reference == f"backup:{workflow_id}"
        assert persisted.declaration.recipe is not None
        assert persisted.declaration.recipe.inputs[0].logical_path == "source.bin"
        repeated = SealedArtifactWorkflow(reloaded).record_backup_result(
            result,
            executor=_executor(),
        )
        assert repeated.derivation_record.digital_asset_derivation_id == derivation_id
        assert len(tuple(reloaded.iter_digital_asset_derivation_records())) == 1
        reloaded.close()


def test_pin_local_executor_records_content_identity(tmp_path: Path) -> None:
    """
    Verify a local tool file becomes a digest-pinned reference with supplied version metadata.

    The temporary file contains arbitrary tool bytes and need not be executable. Assert its label,
    caller-provided version, resolved file URI, SHA-256 algorithm, and digest width; the tool is
    never invoked.

    Example:
        >>> test_pin_local_executor_records_content_identity(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real source, staging, image, Store, or catalogue files.
    :return: None after the stated regression assertions pass.
    """
    tool = tmp_path / "packer"
    tool.write_bytes(b"tool bytes")

    reference = SealedArtifactWorkflow.pin_local_executor(
        tool,
        version="1.2.3",
    )

    assert reference.name == "packer"
    assert reference.version == "1.2.3"
    assert reference.uri == tool.resolve().as_uri()
    assert reference.digest.algorithm == "sha256"
    assert len(reference.digest.value) == 64
