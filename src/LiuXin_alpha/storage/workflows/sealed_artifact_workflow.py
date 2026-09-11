"""
Record existing sealed container images as derived atomic Digital Assets.

The concrete workflow bridges local-image ingest or routed adoption with ordered member
provenance and replay descriptions. It does not build archives, execute recipes, or inspect
members. Late recipe/provenance failures can follow completed byte cataloguing.
"""

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
    """
    Catalogue an existing container image and record its package derivation.

    The inherited initializer borrows a manager. Each operation captures current atomic source
    records, represents logical member paths and a replay recipe, then adopts or ingests the whole
    image and records provenance. No archive builder or replay command runs here, and member
    contents are not opened to prove the supplied relationship.

    Recipe completeness and reproducibility describe recorded evidence, not a demonstrated
    reproduction. Byte cataloguing precedes some recipe/provenance validation; later errors can
    leave a managed image without the requested derivation. Identical declarations are reused, while
    same-output workflow conflicts reject without an enclosing transaction or concurrency lock.

    Example:
        >>> recorder = SealedArtifactWorkflow(manager)  # doctest: +SKIP
        >>> registration = recorder.record_artifact(  # doctest: +SKIP
        ...     "books.zip", {"book.epub": book}, artifact_format="zip",
        ...     executor=tool, command=("zip", "artifact.zip", "book.epub"),
        ... )
    """

    @staticmethod
    def pin_local_executor(
        executable: str | os.PathLike[str],
        *,
        name: str | None = None,
        version: str | None = None,
    ) -> api.ReproductionRecipeArtifactReference:
        """
        Hash a local tool file and record a resolved file URI with optional version metadata.

        Try shutil.which first, then interpret the supplied spelling as a local path, expand user
        syntax, require a file, and resolve it. A direct file need not be executable. SHA-256 is
        computed without running the tool, probing its version, or pinning a file descriptor for
        later replay. Open/read errors propagate after the existence check, and file content can
        change after hashing.

        Example:
            >>> reference = SealedArtifactWorkflow.pin_local_executor(  # doctest: +SKIP
            ...     "/opt/tools/packer", name="packer", version="1.2",
            ... )
            >>> reference.digest.algorithm  # doctest: +SKIP
            'sha256'


        :param executable: Local path/PathLike or executable name searched through PATH.
        :param name: Optional tool label; falsey input uses the resolved filename.
        :param version: Optional caller-provided version text; not obtained by executing the file.
        :return: ReproductionRecipeArtifactReference containing the observed SHA-256 and resolved file URI.
        """

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
        """
        Adopt or ingest an existing image and attach an ordered package derivation.

        Coerce format/reproducibility through their enums, materialize and refresh source records,
        stringify command arguments, and collect dependencies. Copy parameters/environment to
        compact key-sorted JSON; insert the format parameter and reject a conflicting supplied
        value. Recipe input values validate lexical paths and retain current Asset size/digest
        evidence before byte cataloguing.

        A truthy complete flag requires a reproducible classification, executor/dependency retrieval
        hints, and a nonempty command. These checks do not resolve tools or test replay; empty
        command arguments reject for incomplete recipes too. Supplied metadata is used as-is,
        otherwise derive the container description. Adopt a Location in place or ingest a local
        image through the manager.

        Only after byte cataloguing, construct the final recipe and PACKAGE declaration using the
        output's recorded size/digests. Duplicate dependency names, invalid optional provenance
        text/IDs, or later manager failures can therefore leave bytes/records without new
        provenance. Reuse an exactly equal derivation; reject conflicting same-output workflow
        identity. No operation spans both effects atomically, and the archive itself is not
        inspected.

        Example:
            >>> registration = recorder.record_artifact(  # doctest: +SKIP
            ...     output, {"books/novel.epub": novel}, artifact_format="squashfs",
            ...     executor=tool, command=("mksquashfs", ".", "artifact.squashfs"),
            ...     parameters={"compression": "zstd"}, workflow_reference="backup:17",
            ... )


        :param artifact: Existing local path/PathLike or file URI to ingest, or managed Location to adopt; remote URI strings are unsupported by the concrete implementation.
        :param sources: Ordered mapping or iterable of logical member paths to existing atomic Asset inputs; must be nonempty with unique valid paths.
        :param artifact_format: Container format enum or exact enum value used for metadata and recipe naming.
        :param executor: Pinned tool reference used by replay; None is allowed only for an incomplete recipe.
        :param command: Replay argument iterable, materialized as strings; nonempty arguments are required and complete recipes need at least one.
        :param parameters: Optional JSON-compatible build settings; artifact_format is inserted and a conflicting supplied value is rejected.
        :param environment: Optional JSON-compatible replay environment description; None records an empty object.
        :param dependencies: Additional pinned recipe artifacts; complete recipes require retrieval sources for each.
        :param reproducibility: Declared reproducibility enum or value; does not run a reproducibility check.
        :param complete: Whether the recipe is claimed complete and must satisfy the complete-reference preconditions.
        :param workflow_id: Optional catalogue provenance workflow ID used in derivation identity/conflict checks.
        :param workflow_reference: Optional external workflow identity string recorded with provenance.
        :param operation_id: Optional ingest/adoption operation token forwarded to the manager; not a transaction for the whole workflow.
        :param preferred_store_ref: Preferred ingest destination; for a Location it must be omitted or equal that Location's Store.
        :param replica_mode: Mode forwarded for the container copy, default ARCHIVE.
        :param metadata: Optional image description; None derives name, media type, basename, and format/kind attributes.
        :param operator: Optional attribution recorded on the package derivation.
        :param notes: Optional human-readable provenance notes.
        :param verify: Verification request forwarded to manager ingest/adoption; does not inspect archive members or test replay.
        :return: SealedArtifactRegistration retaining the ingest result, selected format, recorded/reused derivation, and recipe.
        """

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
        """
        Translate a successful SquashFS backup result into a complete package recipe.

        Require successful result fields and the SQUASHFS_PACK enum member. Use declared source
        Asset IDs or validate a complete override with exactly matching path order. Source records
        are refreshed during delegation; this method does not compare them with earlier staging
        reports or prove their correspondence to the finished archive.

        Rebuild the command using executor.name, compression default zstd, -noappend, and -quiet.
        Exact deterministic option text "1" adds fixed ownership/time/no-xattr flags and selects
        EXACT; otherwise select BEST_EFFORT. These are recipe claims rather than replay results.
        Delegate in ARCHIVE mode, converting a backup repository ID to
        workflow_reference="backup:<id>" and leaving catalogue workflow_id unset. Adoption/ingest
        can precede a later provenance failure.

        Example:
            >>> registration = recorder.record_backup_result(  # doctest: +SKIP
            ...     result, executor=tool, source_assets={"book.epub": book},
            ... )


        :param result: Successful SquashFS backup result containing a non-None output reference and intended source order.
        :param executor: Retrievable pinned tool reference supplying the replay command name; not automatically inferred from the local builder executable.
        :param source_assets: Optional complete ordered path/Asset override; None requires an Asset ID on every declared source.
        :param environment: Optional JSON-compatible replay environment description forwarded to recipe recording.
        :param dependencies: Pinned additional artifacts required for the complete recipe.
        :param operation_id: Optional manager ingest/adoption token for cataloguing the output image.
        :param preferred_store_ref: Preferred destination for local-image ingest; cannot redirect adoption of a Location.
        :param operator: Optional attribution for the derived image.
        :param notes: Optional provenance notes attached to the derivation.
        :param verify: Manager byte-verification request, independent of backup verification milestones or recipe replay.
        :return: SealedArtifactRegistration for the result image and captured SquashFS replay settings.
        """

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
        """
        Describe a build-once RAR 4 image with a complete best-effort package recipe.

        Reject compression levels outside range(0, 6), without coercing accepted numeric equivalents
        to int. Record executor.name followed by archive creation, RAR 4, compression,
        nonsolid/recursive/relative-path, and no-password flags; add -inul when quiet. The recipe
        writes artifact.rar from the current directory and records archive_version=4, compression
        level, and solid=False.

        Delegate existing bytes to record_artifact in ARCHIVE mode with a complete BEST_EFFORT
        recipe. The method does not invoke RAR, validate image headers, or prove supplied bytes were
        built with these flags.

        Example:
            >>> registration = recorder.record_rar_artifact(  # doctest: +SKIP
            ...     "books.rar", {"book.epub": book}, executor=rar_tool,
            ...     compression_level=4, quiet=True,
            ... )


        :param artifact: Existing local path/PathLike or file URI to ingest, or managed Location to adopt; remote URI strings are unsupported by the concrete implementation.
        :param sources: Ordered mapping or iterable of logical member paths to existing atomic Asset inputs; must be nonempty with unique valid paths.
        :param executor: Pinned tool reference whose name starts the command; its retrieval hint is required by this complete recipe.
        :param compression_level: Value accepted by membership in range(0, 6); interpolated unchanged into the RAR -m argument.
        :param quiet: Whether the recorded command includes -inul; no command is executed by this method.
        :param environment: Optional JSON-compatible replay environment description; None records an empty object.
        :param dependencies: Additional pinned recipe artifacts; complete recipes require retrieval sources for each.
        :param workflow_id: Optional catalogue provenance workflow ID used in derivation identity/conflict checks.
        :param workflow_reference: Optional external workflow identity string recorded with provenance.
        :param operation_id: Optional ingest/adoption operation token forwarded to the manager; not a transaction for the whole workflow.
        :param preferred_store_ref: Preferred ingest destination; for a Location it must be omitted or equal that Location's Store.
        :param operator: Optional attribution recorded on the package derivation.
        :param notes: Optional human-readable provenance notes.
        :param verify: Verification request forwarded to manager ingest/adoption; does not inspect archive members or test replay.
        :return: SealedArtifactRegistration with the recorded RAR package recipe and manager byte outcome.
        """

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
        """
        Collect ordered member/Asset pairs, reject duplicate stringified names, and refresh records.

        Require at least one pair. Stringify paths for duplicate checks before any Asset lookup,
        then resolve each accepted atomic ID through the manager. Path syntax is validated later by
        recipe input construction. Duplicate Asset IDs at different member paths are allowed; this
        helper neither reads source bytes nor compares them with archive contents.

        Example:
            >>> sources = recorder._source_records({"book.epub": book})  # doctest: +SKIP


        :param sources: Ordered mapping or iterable of member-name/atomic-Asset pairs.
        :return: Tuple of stringified member paths and current DigitalAssetRecord objects in supplied order.
        """
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
        """
        Adopt a routed image in place or ingest an existing local image file.

        For Location input, reject a non-None preferred Store differing from its Store before
        delegation. Adoption receives operation ID, metadata, mode, and verify without a destination
        override. Other input goes through local path/file-URI parsing and an is_file check, then
        manager.ingest_file. No archive signature or member validation occurs; manager
        byte/verification outcomes are returned unchanged.

        Example:
            >>> outcome = recorder._catalogue_bytes(  # doctest: +SKIP
            ...     output, operation_id=None, preferred_store_ref=None,
            ...     replica_mode=api.ReplicaMode.ARCHIVE, metadata=metadata, verify=True,
            ... )


        :param artifact: Existing local path/PathLike or file URI to ingest, or managed Location to adopt; remote URI strings are unsupported by the concrete implementation.
        :param operation_id: Optional ingest/adoption operation token forwarded to the manager; not a transaction for the whole workflow.
        :param preferred_store_ref: Preferred ingest destination; for a Location it must be omitted or equal that Location's Store.
        :param replica_mode: Required copy mode forwarded unchanged to manager ingest or adoption.
        :param metadata: Prepared image description forwarded unchanged; this helper does not derive defaults.
        :param verify: Verification request forwarded to manager ingest/adoption; does not inspect archive members or test replay.
        :return: Original DigitalAssetIngestResult from manager adoption or file ingest; lookup/read/publication failures propagate.
        """
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
        """
        Reuse equal same-output provenance or reject a conflicting workflow association.

        Materialize existing derivations for the result Asset. Return the first exactly equal
        declaration before checking workflow conflicts. Otherwise, if a numeric workflow ID or
        external reference was supplied, reject any same-output record sharing either value.
        Unrelated outputs are not searched, and the read-then-record sequence has no concurrency
        lock or transaction here.

        Example:
            >>> first = recorder._record_once(declaration)  # doctest: +SKIP
            >>> recorder._record_once(declaration) == first  # doctest: +SKIP
            True


        :param declaration: Fully constructed package derivation whose output and optional workflow association define reuse/conflict checks.
        :return: Existing equal DigitalAssetDerivationRecord or the manager-created record; conflicting workflow provenance raises StoragePreconditionFailed.
        """
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
        """
        Extract ordered member paths and required catalogue IDs from backup intent.

        Every source must carry source_digital_asset_id; absence raises a precondition error
        suggesting an override. Assert a non-None archive path for each retained ID. No source
        lookup, path normalization, or digest comparison occurs here; an empty declaration yields an
        empty tuple for later validation.

        Example:
            >>> sources = recorder._backup_sources(result.declaration)  # doctest: +SKIP


        :param declaration: Backup intent whose sources carry member paths and atomic Asset provenance IDs.
        :return: Tuple of (archive_path, DigitalAssetID) pairs in declaration order.
        """
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
        """
        Require override path spelling and order to equal the backup declaration exactly.

        Materialize input pairs and stringify only the override paths for comparison. Counts and
        order must match, but Asset identity, bytes, and duplicate/path validity are deferred to
        later record_artifact validation. Return the collected pairs without replacing their path or
        Asset objects.

        Example:
            >>> sources = recorder._validated_source_override(  # doctest: +SKIP
            ...     result.declaration, {"book.epub": book},
            ... )


        :param declaration: Backup intent supplying the exact expected archive-path sequence.
        :param source_assets: Ordered override pairs that must cover those paths in the same order.
        :return: Collected original override pairs; a path/order mismatch raises StoragePreconditionFailed.
        """
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
    """
    Extract an atomic Asset ID or accept a strictly positive non-bool integer.

    IDs taken from accepted records/results/resolutions are returned without scalar revalidation.
    Direct strings, floats, zero, negatives, and bool reject. This helper does not query the manager
    or distinguish nominal int aliases at runtime.

    Example:
        >>> _asset_id(7)
        7


    :param value: Atomic Asset ID, record, ingest result, or resolution accepted by the public workflow.
    :return: Extracted DigitalAssetID, or TypeError for an unsupported direct value.
    """
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
    """
    Encode a mapping with sorted keys and compact separators, wrapping value/type failures.

    Use standard json.dumps defaults, including ASCII escaping and permissive NaN/Infinity encoding.
    This is deterministic key-sorted text for supported inputs, not an independent finite-number or
    JSON-object validator. TypeError and ValueError become a field-specific ValueError retaining the
    cause; other exceptions propagate.

    Example:
        >>> _canonical_json({"b": 2, "a": 1}, "parameters")
        '{"a":1,"b":2}'


    :param value: Mapping passed to the standard JSON encoder.
    :param field_name: Field label included when encoding fails.
    :return: Compact JSON text; unsupported values or sortable-key failures raise a labelled ValueError.
    """
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError) as error:
        raise ValueError(f"{field_name} must contain JSON-compatible values.") from error


def _source_items(
    sources: api.SealedArtifactSources,
) -> tuple[tuple[str, api.SealedArtifactAssetInput], ...]:
    """
    Materialize source pairs while preserving mapping insertion order or iterable order.

    Mapping input is collected from items(); other iterables are tuple-collected directly. This
    helper does not unpack entries, stringify paths, resolve Assets, or validate emptiness and
    uniqueness.

    Example:
        >>> _source_items({"book.epub": 7})
        (('book.epub', 7),)


    :param sources: Mapping or iterable of member-path/Asset pairs to consume once.
    :return: Tuple containing the supplied pairs in encounter order.
    """
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
    """
    Resolve local path spelling or a file URI to an absolute image Path.

    Apply os.fspath, parse the URI, and reject non-file schemes with StorageUnsupportedOperation.
    For file URIs, decode and resolve the path component only; authority/query/fragment are ignored
    and user syntax is not expanded in that branch. Ordinary paths expand user syntax before
    resolution. The image need not exist yet, and platform drive spellings may be interpreted as
    unsupported schemes.

    Example:
        >>> _local_artifact_path("file:///tmp/image%20one.zip").name
        'image one.zip'


    :param artifact: Local string/PathLike or file URI spelling; managed remote objects require a Location elsewhere.
    :return: Resolved local Path without an existence/readability check.
    """
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
    """
    Build default whole-image description from the declared format and source basename.

    Locations use their POSIX key basename; other inputs use local path/file-URI parsing. Assign a
    sealed-format label, conservative media type, optional original name, and
    artifact_format/artifact_kind attributes. Do not inspect image headers or source members.

    Example:
        >>> _artifact_metadata(
        ...     api.Location(UUID(int=1), "packs/books.zip"), api.SealedArtifactFormat.ZIP,
        ... ).media_type
        'application/zip'


    :param artifact: Existing-image route or local spelling used only to derive an original filename.
    :param artifact_format: Selected format enum providing the label and conservative media type.
    :return: New DigitalAssetMetadata for the container image; invalid local URI spelling can raise.
    """
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
    """
    Read a local tool file in 1 MiB chunks and return its SHA-256 identity.

    Context management closes the reader after normal completion or failure. No size ceiling or
    file-version pin is applied; the digest describes bytes observed during this traversal, and
    later changes remain possible.

    Example:
        >>> digest = _file_digest(pathlib.Path("/opt/tools/packer"))  # doctest: +SKIP
        >>> digest.algorithm  # doctest: +SKIP
        'sha256'


    :param path: Local file Path opened in binary mode for hashing.
    :return: SHA-256 Digest of the streamed content; file open/read/close errors propagate.
    """
    hasher = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            hasher.update(chunk)
    return api.Digest("sha256", hasher.hexdigest())


__all__ = ["SealedArtifactWorkflow"]
