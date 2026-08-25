"""Catalogue sealed container images as derived Digital Assets."""

from __future__ import annotations

import hashlib
import json
import os
import pathlib
import shutil

from collections.abc import Iterable, Mapping
from typing import cast, override
from urllib.parse import unquote, urlparse
from uuid import UUID

from LiuXin_alpha.storage import api


class SealedArtifactWorkflow(api.SealedArtifactWorkflowAPI):
    """Bridge physical archive creation to catalogue-level provenance."""

    @staticmethod
    def pin_local_executor(
        executable: str | os.PathLike[str],
        *,
        name: str | None = None,
        version: str | None = None,
    ) -> api.ReproductionRecipeArtifactReference:
        """Pin a local executable by SHA-256 and a resolvable file URI."""

        supplied = os.fspath(executable)
        resolved_name = shutil.which(supplied)
        path = pathlib.Path(resolved_name or supplied).expanduser()
        if not path.is_file():
            raise api.StoragePreconditionFailed(
                f"sealed-artifact executor is not a readable file: {supplied}."
            )
        path = path.resolve()
        return api.ReproductionRecipeArtifactReference(
            name=name or path.name,
            digest=_file_digest(path),
            version=version,
            uri=path.as_uri(),
        )

    @override
    def record_artifact(
        self,
        artifact: str | os.PathLike[str] | api.Location,
        sources: api.SealedArtifactSources,
        *,
        artifact_format: api.SealedArtifactFormat | str,
        executor: api.ReproductionRecipeArtifactReference | None,
        command: Iterable[str],
        parameters: Mapping[str, object] | None = None,
        environment: Mapping[str, object] | None = None,
        dependencies: Iterable[
            api.ReproductionRecipeArtifactReference
        ] = (),
        reproducibility: (
            api.Reproducibility | str
        ) = api.Reproducibility.BEST_EFFORT,
        complete: bool = True,
        workflow_id: int | None = None,
        workflow_reference: str | None = None,
        operation_id: UUID | None = None,
        preferred_store_ref: api.StoreUUID | None = None,
        replica_mode: api.ReplicaMode = api.ReplicaMode.ARCHIVE,
        metadata: api.DigitalAssetMetadata | None = None,
        operator: str | None = None,
        notes: str | None = None,
        verify: bool = True,
    ) -> api.SealedArtifactRegistration:
        """Adopt or ingest a sealed image and record its package derivation."""

        selected_format = api.SealedArtifactFormat(artifact_format)
        selected_reproducibility = api.Reproducibility(reproducibility)
        source_records = self._source_records(sources)
        command_tuple = tuple(str(argument) for argument in command)
        dependency_tuple = tuple(dependencies)
        parameters_document = dict(parameters or ())
        supplied_format = parameters_document.get("artifact_format")
        if supplied_format not in (None, selected_format.value):
            raise api.StoragePreconditionFailed(
                "parameters.artifact_format conflicts with artifact_format."
            )
        parameters_document["artifact_format"] = selected_format.value
        parameters_json = _canonical_json(parameters_document, "parameters")
        environment_json = _canonical_json(dict(environment or ()), "environment")
        recipe_inputs = tuple(
            api.ReproductionRecipeInputReference(
                sequence_number=index,
                digital_asset_id=record.digital_asset_id,
                size_bytes=record.size_bytes,
                digests=record.digests,
                logical_path=logical_path,
                role="archive_member",
            )
            for index, (logical_path, record) in enumerate(source_records)
        )
        if complete:
            if selected_reproducibility is api.Reproducibility.NOT_REPRODUCIBLE:
                raise api.StoragePreconditionFailed(
                    "a non-reproducible sealed-artifact recipe cannot be complete."
                )
            if executor is None or not executor.has_retrieval_source:
                raise api.StoragePreconditionFailed(
                    "a complete sealed-artifact recipe requires a retrievable pinned executor."
                )
            if any(
                not dependency.has_retrieval_source
                for dependency in dependency_tuple
            ):
                raise api.StoragePreconditionFailed(
                    "complete sealed-artifact dependencies must be retrievable."
                )
            if not command_tuple:
                raise api.StoragePreconditionFailed(
                    "a complete sealed-artifact recipe requires a replay command."
                )
        if any(not argument for argument in command_tuple):
            raise ValueError("sealed-artifact command arguments must not be empty.")

        chosen_metadata = metadata or _artifact_metadata(
            artifact,
            selected_format,
        )
        ingest_result = self._catalogue_bytes(
            artifact,
            operation_id=operation_id,
            preferred_store_ref=preferred_store_ref,
            replica_mode=replica_mode,
            metadata=chosen_metadata,
            verify=verify,
        )
        output = ingest_result.asset_record
        recipe = api.ReproductionRecipe(
            recipe_type=f"sealed_{selected_format.value}_artifact",
            reproducibility=selected_reproducibility,
            complete=complete,
            inputs=recipe_inputs,
            executor=executor,
            dependencies=dependency_tuple,
            parameters_json=parameters_json,
            environment_json=environment_json,
            command=command_tuple,
            working_directory=".",
            output_path=selected_format.default_output_name,
            expected_output_size=output.size_bytes,
            expected_output_digests=output.digests,
            instructions=(
                "Materialize each input at its logical path, then run the "
                "recorded command from that directory."
            ),
        )
        declaration = api.DigitalAssetDerivationDeclaration(
            result_digital_asset_id=output.digital_asset_id,
            sources=tuple(
                api.DigitalAssetDerivationSourceReference(
                    sequence_number=index,
                    digital_asset_id=record.digital_asset_id,
                    role="archive_member",
                )
                for index, (_logical_path, record) in enumerate(source_records)
            ),
            kind=api.DigitalAssetDerivationKind.PACKAGE,
            recipe=recipe,
            output_role=f"sealed_{selected_format.value}_artifact",
            operator=operator,
            notes=notes,
            workflow_id=workflow_id,
            workflow_reference=workflow_reference,
        )
        derivation = self._record_once(declaration)
        return api.SealedArtifactRegistration(
            selected_format,
            ingest_result,
            derivation,
            recipe,
        )

    @override
    def record_backup_result(
        self,
        result: api.BackupWorkflowResult,
        *,
        executor: api.ReproductionRecipeArtifactReference,
        source_assets: api.SealedArtifactSources | None = None,
        environment: Mapping[str, object] | None = None,
        dependencies: Iterable[
            api.ReproductionRecipeArtifactReference
        ] = (),
        operation_id: UUID | None = None,
        preferred_store_ref: api.StoreUUID | None = None,
        operator: str | None = None,
        notes: str | None = None,
        verify: bool = True,
    ) -> api.SealedArtifactRegistration:
        """Record a completed SquashFS backup using its captured build intent."""

        if not result.successful or result.output_artifact_reference is None:
            raise api.StoragePreconditionFailed(
                "only a successful backup result can become a derived artifact."
            )
        declaration = result.declaration
        if declaration.workflow_kind is not api.BackupWorkflowKind.SQUASHFS_PACK:
            raise api.StorageUnsupportedOperation(
                f"no sealed-artifact adapter exists for {declaration.workflow_kind}."
            )
        sources = (
            self._backup_sources(declaration)
            if source_assets is None
            else self._validated_source_override(declaration, source_assets)
        )
        options = declaration.option_map()
        compression = options.get("compression", "zstd")
        deterministic = options.get("deterministic", "0") == "1"
        command = [
            executor.name,
            ".",
            api.SealedArtifactFormat.SQUASHFS.default_output_name,
            "-noappend",
            "-comp",
            compression,
        ]
        if deterministic:
            command.extend(
                ["-all-root", "-no-xattrs", "-all-time", "0", "-mkfs-time", "0"]
            )
        command.append("-quiet")
        parameters: dict[str, object] = {
            "compression": compression,
            "deterministic": deterministic,
            "workflow_kind": declaration.workflow_kind.value,
            "workflow_name": declaration.workflow_name,
        }
        return self.record_artifact(
            result.output_artifact_reference,
            sources,
            artifact_format=api.SealedArtifactFormat.SQUASHFS,
            executor=executor,
            command=command,
            parameters=parameters,
            environment=environment,
            dependencies=dependencies,
            reproducibility=(
                api.Reproducibility.EXACT
                if deterministic
                else api.Reproducibility.BEST_EFFORT
            ),
            complete=True,
            workflow_reference=(
                None
                if result.workflow_id is None
                else f"backup:{result.workflow_id}"
            ),
            operation_id=operation_id,
            preferred_store_ref=preferred_store_ref,
            replica_mode=api.ReplicaMode.ARCHIVE,
            operator=operator,
            notes=notes,
            verify=verify,
        )

    def record_rar_artifact(
        self,
        artifact: str | os.PathLike[str] | api.Location,
        sources: api.SealedArtifactSources,
        *,
        executor: api.ReproductionRecipeArtifactReference,
        compression_level: int = 3,
        quiet: bool = True,
        environment: Mapping[str, object] | None = None,
        dependencies: Iterable[
            api.ReproductionRecipeArtifactReference
        ] = (),
        workflow_id: int | None = None,
        workflow_reference: str | None = None,
        operation_id: UUID | None = None,
        preferred_store_ref: api.StoreUUID | None = None,
        operator: str | None = None,
        notes: str | None = None,
        verify: bool = True,
    ) -> api.SealedArtifactRegistration:
        """Catalogue output from the build-once RAR 4 backend."""

        if compression_level not in range(0, 6):
            raise ValueError("compression_level must be between 0 and 5.")
        command = [
            executor.name,
            "a",
            "-ma4",
            f"-m{compression_level}",
            "-s-",
            "-r",
            "-ep1",
            "-p-",
        ]
        if quiet:
            command.append("-inul")
        command.extend((api.SealedArtifactFormat.RAR.default_output_name, "."))
        return self.record_artifact(
            artifact,
            sources,
            artifact_format=api.SealedArtifactFormat.RAR,
            executor=executor,
            command=command,
            parameters={
                "archive_version": 4,
                "compression_level": compression_level,
                "solid": False,
            },
            environment=environment,
            dependencies=dependencies,
            reproducibility=api.Reproducibility.BEST_EFFORT,
            complete=True,
            workflow_id=workflow_id,
            workflow_reference=workflow_reference,
            operation_id=operation_id,
            preferred_store_ref=preferred_store_ref,
            replica_mode=api.ReplicaMode.ARCHIVE,
            operator=operator,
            notes=notes,
            verify=verify,
        )

    def _source_records(
        self,
        sources: api.SealedArtifactSources,
    ) -> tuple[tuple[str, api.DigitalAssetRecord], ...]:
        values = _source_items(sources)
        if not values:
            raise ValueError("a sealed artifact requires at least one source Asset.")
        paths = tuple(str(path) for path, _asset in values)
        if len(paths) != len(set(paths)):
            raise ValueError("sealed artifact logical paths must be unique.")
        records: list[tuple[str, api.DigitalAssetRecord]] = []
        for path, asset in values:
            asset_id = _asset_id(asset)
            records.append(
                (str(path), self.storage_manager.get_digital_asset_record(asset_id))
            )
        return tuple(records)

    def _catalogue_bytes(
        self,
        artifact: str | os.PathLike[str] | api.Location,
        *,
        operation_id: UUID | None,
        preferred_store_ref: api.StoreUUID | None,
        replica_mode: api.ReplicaMode,
        metadata: api.DigitalAssetMetadata,
        verify: bool,
    ) -> api.DigitalAssetIngestResult:
        if isinstance(artifact, api.Location):
            if (
                preferred_store_ref is not None
                and preferred_store_ref != artifact.store_ref
            ):
                raise api.StoragePreconditionFailed(
                    "preferred_store_ref cannot redirect an adopted Location."
                )
            return self.storage_manager.adopt_location(
                artifact,
                operation_id=operation_id,
                metadata=metadata,
                replica_mode=replica_mode,
                verify=verify,
            )
        path = _local_artifact_path(artifact)
        if not path.is_file():
            raise api.StorageNotFound(f"sealed artifact does not exist: {path}.")
        return self.storage_manager.ingest_file(
            path,
            operation_id=operation_id,
            metadata=metadata,
            preferred_store_ref=preferred_store_ref,
            replica_mode=replica_mode,
            verify=verify,
        )

    def _record_once(
        self,
        declaration: api.DigitalAssetDerivationDeclaration,
    ) -> api.DigitalAssetDerivationRecord:
        existing = tuple(
            self.storage_manager.iter_digital_asset_derivation_records(
                result_digital_asset_id=declaration.result_digital_asset_id,
            )
        )
        for record in existing:
            if record.declaration == declaration:
                return record
        if (
            declaration.workflow_id is not None
            or declaration.workflow_reference is not None
        ):
            conflicting = tuple(
                record
                for record in existing
                if (
                    declaration.workflow_id is not None
                    and record.declaration.workflow_id
                    == declaration.workflow_id
                )
                or (
                    declaration.workflow_reference is not None
                    and record.declaration.workflow_reference
                    == declaration.workflow_reference
                )
            )
            if conflicting:
                raise api.StoragePreconditionFailed(
                    "this workflow already recorded different provenance for the sealed artifact."
                )
        return self.storage_manager.record_digital_asset_derivation(declaration)

    def _backup_sources(
        self,
        declaration: api.BackupWorkflowDeclaration,
    ) -> tuple[tuple[str, api.DigitalAssetID], ...]:
        sources: list[tuple[str, api.DigitalAssetID]] = []
        for index, source in enumerate(declaration.sources):
            if source.source_digital_asset_id is None:
                raise api.StoragePreconditionFailed(
                    "".join(
                        (
                            "backup source ",
                            f"{index} ({source.archive_path or source.source_identifier}) ",
                            "has no Digital Asset identity; supply source_assets or ",
                            "designate a catalogued source.",
                        )
                    )
                )
            assert source.archive_path is not None
            sources.append((source.archive_path, source.source_digital_asset_id))
        return tuple(sources)

    def _validated_source_override(
        self,
        declaration: api.BackupWorkflowDeclaration,
        source_assets: api.SealedArtifactSources,
    ) -> tuple[tuple[str, api.SealedArtifactAssetInput], ...]:
        values = _source_items(source_assets)
        expected_paths = tuple(
            source.archive_path for source in declaration.sources
        )
        supplied_paths = tuple(str(path) for path, _asset in values)
        if supplied_paths != expected_paths:
            raise api.StoragePreconditionFailed(
                "source_assets paths and order must match the backup declaration."
            )
        return values


def _asset_id(value: api.SealedArtifactAssetInput) -> api.DigitalAssetID:
    if isinstance(value, api.DigitalAssetRecord):
        return value.digital_asset_id
    if isinstance(value, api.DigitalAssetIngestResult):
        return value.asset_record.digital_asset_id
    if isinstance(value, api.DigitalAssetResolution):
        return value.asset_record.digital_asset_id
    candidate = cast(object, value)
    if (
        isinstance(candidate, int)
        and not isinstance(candidate, bool)
        and candidate > 0
    ):
        return api.DigitalAssetID(candidate)
    raise TypeError(
        "source assets must be positive IDs or atomic Asset records/results."
    )


def _canonical_json(value: Mapping[str, object], field_name: str) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError) as error:
        raise ValueError(f"{field_name} must contain JSON-compatible values.") from error


def _source_items(
    sources: api.SealedArtifactSources,
) -> tuple[tuple[str, api.SealedArtifactAssetInput], ...]:
    if isinstance(sources, Mapping):
        mapping = cast(
            Mapping[str, api.SealedArtifactAssetInput],
            sources,
        )
        return tuple(mapping.items())
    return tuple(sources)


def _local_artifact_path(
    artifact: str | os.PathLike[str],
) -> pathlib.Path:
    value = os.fspath(artifact)
    parsed = urlparse(value)
    if parsed.scheme == "file":
        return pathlib.Path(unquote(parsed.path)).resolve()
    if parsed.scheme:
        raise api.StorageUnsupportedOperation(
            "sealed artifact strings must be local paths or file URIs; use a Location for managed Store bytes."
        )
    return pathlib.Path(value).expanduser().resolve()


def _artifact_metadata(
    artifact: str | os.PathLike[str] | api.Location,
    artifact_format: api.SealedArtifactFormat,
) -> api.DigitalAssetMetadata:
    if isinstance(artifact, api.Location):
        original_name = pathlib.PurePosixPath(artifact.key).name or None
    else:
        original_name = _local_artifact_path(artifact).name or None
    return api.DigitalAssetMetadata(
        name=f"sealed {artifact_format.value} artifact",
        media_type=artifact_format.media_type,
        original_name=original_name,
        attributes=(
            ("artifact_format", artifact_format.value),
            ("artifact_kind", "sealed_container"),
        ),
    )


def _file_digest(path: pathlib.Path) -> api.Digest:
    hasher = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            hasher.update(chunk)
    return api.Digest("sha256", hasher.hexdigest())


__all__ = ["SealedArtifactWorkflow"]
