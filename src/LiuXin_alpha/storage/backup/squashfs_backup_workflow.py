"""
Execute explicit source staging and resumable SquashFS artifact publication.

Checkpoint values capture in-memory progress but require separate repository persistence.
The build Store owns member writes and local image sealing; a borrowed manager handles
routed reads/publication. Retry evidence, cancellation, and finalization preserve partial
physical effects rather than promising a transaction across an entire backup.
"""

from __future__ import annotations

import hashlib
import pathlib
import shutil

from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from LiuXin_alpha.storage.api import (
    BackupSourceDeclaration,
    BackupSourceKind,
    BackupSourceStagingReport,
    BackupWorkflowAPI,
    BackupWorkflowCheckpoint,
    BackupWorkflowDeclaration,
    BackupWorkflowKind,
    BackupWorkflowResult,
    BackupWorkflowStepKind,
    DigitalAssetID,
    DigitalAssetIngestResult,
    DigitalAssetRecord,
    DigitalAssetResolution,
    Digest,
    Location,
    ReplicaState,
    StoreAlreadyExists,
    StoreIntegrityError,
    StorePreconditionFailed,
    StoreUnsupportedOperation,
    WorkflowStatus,
)
from LiuXin_alpha.storage.store_backend_plugins.squashfs_build import (
    SquashfsBuildStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.squashfs_readonly import (
    SquashfsReadOnlyStorageBackend,
)

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api import StorageManagerAPI


class SquashfsBackupWorkflow(BackupWorkflowAPI):
    """
    Stage designated sources, seal a SquashFS image, and expose explicit resume evidence.

    This workflow borrows a manager for routed reads/publication and owns a build Store for local
    staging. It keeps checkpoints in memory; callers persist them through a repository. Sources
    become immutable after DRAFT. Each successful staging step advances one source, while
    finalization can seal, inspect availability, publish, and clean staging in one call.

    Ordinary Exceptions become FAILED state with retained progress, but BaseException propagates. A
    failure can follow physical publication. Reconstruction depends on retained staging and recorded
    milestones, and neither cancellation nor failure rolls back completed copies. Artifact Store
    registration and Asset provenance recording remain separate services. The class supplies no
    concurrent-call synchronization or automatic manager shutdown.

    Example:
        >>> workflow = SquashfsBackupWorkflow(  # doctest: +SKIP
        ...     "nightly.sqsh", staging_target="nightly-stage", verify_after_build=True,
        ... )
        >>> workflow.designate_local_path("book.epub")  # doctest: +SKIP
        >>> checkpoint = workflow.run_next()  # doctest: +SKIP
    """

    def __init__(
        self,
        output_target: str | Location,
        *,
        workflow_name: str | None = None,
        verify_after_build: bool = True,
        cleanup_staging_after_success: bool = False,
        staging_target: str | None = None,
        mksquashfs_exe: str = "mksquashfs",
        compression: str = "zstd",
        deterministic: bool = False,
        storage_manager: StorageManagerAPI | None = None,
        _builder_store_uuid: UUID | None = None,
    ) -> None:
        """
        Initialize a fresh DRAFT workflow and its local build Store.

        Retain the output target and borrowed manager, choose a display name and builder UUID, and
        delegate output-parent/staging-directory creation to the builder. Flags are converted to
        bool. A Location output requires explicit persistent local staging, with a UUID-specific
        build image beside that directory. Local outputs can use builder-owned temporary staging.

        Builder creation can leave directories before a later constructor error; no enclosing
        cleanup guard is added here. This does not check source availability, execute mksquashfs,
        persist intent, or assign a repository workflow ID.

        Example:
            >>> workflow = SquashfsBackupWorkflow(  # doctest: +SKIP
            ...     "nightly.sqsh", workflow_name="nightly", staging_target="stage",
            ...     compression="zstd", deterministic=True,
            ... )


        :param output_target: Local image path or managed destination Location; the original value is retained in intent.
        :param workflow_name: Truthy display name, otherwise derived from the local output basename or a fixed Location-output label.
        :param verify_after_build: Whether finalization additionally checks the sealed reader self-test availability.
        :param cleanup_staging_after_success: Whether to request best-effort staging removal after publication.
        :param staging_target: Optional local staging directory; required for a Location output and created when necessary.
        :param mksquashfs_exe: Builder executable name or path forwarded to the build Store.
        :param compression: Compression argument forwarded to the builder.
        :param deterministic: Request builder flags fixing ownership/timestamps and excluding xattrs.
        :param storage_manager: Optional borrowed manager required by routed sources, catalogue identity, and Location output.
        :param _builder_store_uuid: Optional retained builder UUID for reconstruction; a falsey value allocates a fresh UUID.
        :return: None after builder setup and initialization of empty source/report/milestone lists and DRAFT state.
        """
        self._output_target = output_target
        self._storage_manager = storage_manager
        self._workflow_name = workflow_name or _default_workflow_name(output_target)
        self._verify_after_build = bool(verify_after_build)
        self._cleanup_staging_after_success = bool(cleanup_staging_after_success)
        builder_store_uuid = _builder_store_uuid or uuid4()
        build_output = _local_build_output(
            output_target,
            staging_target,
            builder_store_uuid=builder_store_uuid,
        )
        self._builder = SquashfsBuildStorageBackend(
            url=str(build_output),
            name=self._workflow_name,
            uuid=builder_store_uuid,
            staging_root=staging_target,
            mksquashfs_exe=mksquashfs_exe,
            compression=compression,
            deterministic=deterministic,
        )
        self._sources: list[BackupSourceDeclaration] = []
        self._source_reports: list[BackupSourceStagingReport] = []
        self._status = WorkflowStatus.DRAFT
        self._workflow_id: int | None = None
        self._next_source_index = 0
        self._completed_steps: list[BackupWorkflowStepKind] = []
        self._output_artifact_reference: str | Location | None = None
        self._last_error: str | None = None

    @property
    def workflow_kind(self) -> BackupWorkflowKind:
        """
        Return the fixed implementation discriminator used by declaration reconstruction.

        Example:
            >>> SquashfsBackupWorkflow.workflow_kind.fget(None)
            <BackupWorkflowKind.SQUASHFS_PACK: 'squashfs_pack'>


        :return: BackupWorkflowKind.SQUASHFS_PACK.
        """
        return BackupWorkflowKind.SQUASHFS_PACK

    @property
    def workflow_name(self) -> str:
        """
        Expose the stored display name without deriving it again or checking uniqueness.

        Example:
            >>> workflow.workflow_name  # doctest: +SKIP


        :return: Original selected workflow name.
        """
        return self._workflow_name

    def build_declaration(self) -> BackupWorkflowDeclaration:
        """
        Snapshot current source designations and builder settings as durable intent.

        Sources become a tuple, flags and the original output target are retained, and staging is
        represented by its resolved local path. Options capture executable, compression,
        deterministic spelling, and builder UUID; declaration construction canonicalizes their
        order. No repository write or source-byte inspection occurs.

        Example:
            >>> declaration = workflow.build_declaration()  # doctest: +SKIP
            >>> declaration.option_map()["compression"]  # doctest: +SKIP
            'zstd'


        :return: New BackupWorkflowDeclaration describing the current sources, output, and reconstructible builder settings.
        """
        return BackupWorkflowDeclaration(
            workflow_name=self.workflow_name,
            workflow_kind=self.workflow_kind,
            output_target=self._output_target,
            sources=tuple(self._sources),
            verify_after_build=self._verify_after_build,
            cleanup_staging_after_success=self._cleanup_staging_after_success,
            staging_target=str(self._builder.staging_root),
            options=(
                ("mksquashfs_exe", self._builder._mksquashfs_exe),
                ("compression", self._builder._compression),
                ("deterministic", "1" if self._builder._deterministic else "0"),
                ("builder_store_uuid", str(self._builder.store_ref)),
            ),
        )

    def progress(self) -> BackupWorkflowCheckpoint:
        """
        Snapshot in-memory execution position and successful-source evidence.

        Rebuild the declaration and tuple-copy reports/milestones. The staged count is the sum of
        report.ok values, while the cursor is stored independently. No timestamp is assigned and no
        repository or staged-file check occurs. Value-construction errors propagate if internal
        state is inconsistent.

        Example:
            >>> checkpoint = workflow.progress()  # doctest: +SKIP


        :return: New BackupWorkflowCheckpoint carrying current state, optional workflow ID, output reference, and last error.
        """
        return BackupWorkflowCheckpoint(
            declaration=self.build_declaration(),
            status=self._status,
            workflow_id=self._workflow_id,
            next_source_index=self._next_source_index,
            staged_source_count=sum(report.ok for report in self._source_reports),
            source_reports=tuple(self._source_reports),
            completed_steps=tuple(self._completed_steps),
            output_artifact_reference=self._output_artifact_reference,
            last_error=self._last_error,
        )

    def designate_local_path(
        self,
        source_path: str,
        *,
        archive_path: str | None = None,
        asset: (
            DigitalAssetID
            | DigitalAssetRecord
            | DigitalAssetIngestResult
            | DigitalAssetResolution
            | None
        ) = None,
    ) -> BackupSourceDeclaration:
        """
        Append a local source after resolving its name and any catalogue expectations.

        Require DRAFT, expand user syntax, and resolve without requiring existence. Record size only
        when is_file succeeds; a missing path with no Asset association can be designated for later
        failure at staging. A supplied Asset is refreshed through the manager and requires matching
        observed size; prefer its SHA-256 digest, otherwise its first digest. No content hash is
        read at designation.

        A falsey archive_path falls back to the basename. The source value normalizes that name
        before duplicate-member rejection, and the stored path remains absolute.

        Example:
            >>> source = workflow.designate_local_path(  # doctest: +SKIP
            ...     "incoming/book.epub", archive_path="books/book.epub", asset=record,
            ... )


        :param source_path: Local source path expanded and resolved for durable designation.
        :param archive_path: Optional member name; a falsey value selects the source basename.
        :param asset: Optional atomic Asset ID/record/ingest result/resolution whose current catalogue size and digest constrain staging.
        :return: Appended BackupSourceDeclaration; state, metadata lookup, size, and duplicate-path failures propagate.
        """
        self._require_draft()
        source = pathlib.Path(source_path).expanduser().resolve(strict=False)
        expected_size = source.stat().st_size if source.is_file() else None
        source_asset = self._source_asset_record(asset)
        expected_digest = None
        if source_asset is not None:
            if expected_size != source_asset.size_bytes:
                raise StoreIntegrityError(
                    "local backup source size differs from its Digital Asset."
                )
            expected_digest = _preferred_digest(source_asset.digests)
        declaration = BackupSourceDeclaration(
            source_kind=BackupSourceKind.LOCAL_PATH,
            source_identifier=str(source),
            archive_path=archive_path or source.name,
            expected_size=expected_size,
            expected_digest=expected_digest,
            source_digital_asset_id=(
                None
                if source_asset is None
                else source_asset.digital_asset_id
            ),
        )
        self._append_source(declaration)
        return declaration

    def designate_location(
        self,
        source_location: Location,
        *,
        archive_path: str | None = None,
        asset: (
            DigitalAssetID
            | DigitalAssetRecord
            | DigitalAssetIngestResult
            | DigitalAssetResolution
            | None
        ) = None,
    ) -> BackupSourceDeclaration:
        """
        Append a routed source with current file and optional catalogue evidence.

        Require DRAFT and Location. Refresh an explicit Asset through the manager. When a manager is
        present, stat the source and materialize non-deleted Replicas at that exact Location,
        selecting the first match without a health filter. A match must agree with any explicit
        Asset; refresh its record, check size, and prefer its catalogue digest over stat evidence.

        Without a manager and explicit Asset, retain the Location without size/digest expectations;
        staging later requires a manager. A falsey member name uses source- plus the six-digit
        current source count. These observations do not pin a backend version.

        Example:
            >>> source = workflow.designate_location(  # doctest: +SKIP
            ...     Location(UUID(int=1), "objects/book"), archive_path="book.epub",
            ... )


        :param source_location: Exact managed route retained for later staging reads.
        :param archive_path: Optional member path; falsey input uses a zero-padded source ordinal.
        :param asset: Optional atomic catalogue identity, checked against any registered Replica at the Location.
        :return: Appended declaration with captured byte expectations and any associated Asset/Replica IDs.
        """
        self._require_draft()
        if not isinstance(source_location, Location):
            raise TypeError("source_location must be a Location.")
        expected_size = None
        expected_digest = None
        source_asset = self._source_asset_record(asset)
        source_replica_id = None
        if self._storage_manager is not None:
            info = self._storage_manager.stat(source_location)
            expected_size = info.size
            expected_digest = info.digest
            matching_replicas = tuple(
                replica
                for replica in self._storage_manager.iter_replica_records(
                    store_ref=source_location.store_ref,
                )
                if replica.location == source_location
                and replica.state is not ReplicaState.DELETED
            )
            if matching_replicas:
                replica = matching_replicas[0]
                if (
                    source_asset is not None
                    and source_asset.digital_asset_id != replica.digital_asset_id
                ):
                    raise StoreIntegrityError(
                        "backup Location belongs to a different Digital Asset."
                    )
                source_asset = self._storage_manager.get_digital_asset_record(
                    replica.digital_asset_id
                )
                source_replica_id = replica.replica_id
        if source_asset is not None:
            if (
                expected_size is not None
                and expected_size != source_asset.size_bytes
            ):
                raise StoreIntegrityError(
                    "backup Location size differs from its Digital Asset."
                )
            expected_size = source_asset.size_bytes
            expected_digest = _preferred_digest(source_asset.digests)
        declaration = BackupSourceDeclaration(
            source_kind=BackupSourceKind.STORE_LOCATION,
            source_identifier=source_location,
            archive_path=archive_path or f"source-{len(self._sources):06d}",
            expected_size=expected_size,
            expected_digest=expected_digest,
            source_digital_asset_id=(
                None
                if source_asset is None
                else source_asset.digital_asset_id
            ),
            source_replica_id=source_replica_id,
            source_store_ref=source_location.store_ref,
        )
        self._append_source(declaration)
        return declaration

    def _source_asset_record(
        self,
        asset: (
            DigitalAssetID
            | DigitalAssetRecord
            | DigitalAssetIngestResult
            | DigitalAssetResolution
            | None
        ),
    ) -> DigitalAssetRecord | None:
        """
        Resolve an optional atomic Asset input to its current manager record.

        None returns before any manager access. Accepted records/results/resolutions supply IDs
        without scalar revalidation; direct integers must be positive and must not be bool. Every
        non-None input requires a manager, including an already supplied record. Lookup errors
        propagate.

        Example:
            >>> workflow._source_asset_record(None) is None  # doctest: +SKIP
            True


        :param asset: Optional Asset ID, record, ingest result, or resolution supplying an atomic identity.
        :return: Freshly looked-up DigitalAssetRecord, or None for absent association.
        """
        if asset is None:
            return None
        if isinstance(asset, DigitalAssetRecord):
            asset_id = asset.digital_asset_id
        elif isinstance(asset, DigitalAssetIngestResult):
            asset_id = asset.asset_record.digital_asset_id
        elif isinstance(asset, DigitalAssetResolution):
            asset_id = asset.asset_record.digital_asset_id
        elif isinstance(asset, int) and not isinstance(asset, bool) and asset > 0:
            asset_id = DigitalAssetID(asset)
        else:
            raise TypeError(
                "asset must be a positive ID or an atomic Asset result/record."
            )
        if self._storage_manager is None:
            raise StorePreconditionFailed(
                "catalogued backup sources require a storage manager."
            )
        return self._storage_manager.get_digital_asset_record(asset_id)

    def run_next(self) -> BackupWorkflowCheckpoint:
        """
        Attempt one source stage or the remaining finalization and expose updated state.

        COMPLETE and CANCELLED return current progress immediately. Otherwise set RUNNING and clear
        the previous error, allowing an explicit retry of FAILED. Successful staging appends one
        report and advances the cursor; the final source also records STAGE_SOURCES. A staging
        exception leaves that source position unadvanced and adds no failure report.

        When no sources remain, seal/publish and then set COMPLETE. Exceptions inside the attempt
        become FAILED with type-prefixed error text; BaseException is not caught. Checkpoint
        construction can itself fail, and finalization errors can follow physical side effects.
        State is not persisted or protected against concurrent calls.

        Example:
            >>> state = workflow.run_next()  # doctest: +SKIP
            >>> state.next_source_index  # doctest: +SKIP
            1


        :return: Current checkpoint after success, represented failure, or an already complete/cancelled no-op.
        """
        if self._status in {WorkflowStatus.COMPLETE, WorkflowStatus.CANCELLED}:
            return self.progress()
        try:
            self._status = WorkflowStatus.RUNNING
            self._last_error = None
            if self._next_source_index < len(self._sources):
                report = self._stage_one(
                    self._sources[self._next_source_index],
                    self._next_source_index,
                )
                self._source_reports.append(report)
                self._next_source_index += 1
                if self._next_source_index == len(self._sources):
                    self._mark_complete(BackupWorkflowStepKind.STAGE_SOURCES)
                return self.progress()
            self._seal_and_publish()
            self._status = WorkflowStatus.COMPLETE
        except Exception as error:
            self._status = WorkflowStatus.FAILED
            self._last_error = f"{type(error).__name__}: {error}"
        return self.progress()

    def run_to_completion(self) -> BackupWorkflowResult:
        """
        Run successive units until completion, failure, or cancellation, then snapshot the outcome.

        An already FAILED instance returns a failure result immediately. Reconstructing from a
        failed checkpoint retains this behavior; call run_next explicitly to retry first. The result
        includes the final checkpoint and its reports/milestones but does not save it, register an
        artifact Store, or create provenance.

        Example:
            >>> result = workflow.run_to_completion()  # doctest: +SKIP
            >>> result.successful  # doctest: +SKIP
            True


        :return: Terminal BackupWorkflowResult copied from the current checkpoint; failed/cancelled results are ordinary returns.
        """
        while self._status not in {
            WorkflowStatus.COMPLETE,
            WorkflowStatus.FAILED,
            WorkflowStatus.CANCELLED,
        }:
            self.run_next()
        checkpoint = self.progress()
        return BackupWorkflowResult(
            declaration=checkpoint.declaration,
            status=checkpoint.status,
            workflow_id=checkpoint.workflow_id,
            output_artifact_reference=checkpoint.output_artifact_reference,
            source_reports=checkpoint.source_reports,
            completed_steps=checkpoint.completed_steps,
            last_error=checkpoint.last_error,
            final_checkpoint=checkpoint,
        )

    def cancel(self) -> BackupWorkflowCheckpoint:
        """
        Set CANCELLED and clear error text while preserving sources, reports, output, and staging.

        Reject cancellation after COMPLETE. Repeated cancellation is accepted. This is a synchronous
        state update; it neither interrupts a concurrent unit nor removes physical or catalogue
        work.

        Example:
            >>> cancelled = workflow.cancel()  # doctest: +SKIP


        :return: Cancelled checkpoint, or StorePreconditionFailed if the workflow already completed.
        """
        if self._status is WorkflowStatus.COMPLETE:
            raise StorePreconditionFailed("a completed workflow cannot be cancelled.")
        self._status = WorkflowStatus.CANCELLED
        self._last_error = None
        return self.progress()

    @classmethod
    def from_declaration(
        cls,
        declaration: BackupWorkflowDeclaration,
        *,
        storage_manager: StorageManagerAPI | None = None,
    ) -> SquashfsBackupWorkflow:
        """
        Construct DRAFT execution from SquashFS intent and its supported options.

        Require the SQUASHFS_PACK enum member and reject a Location staging target. Restore
        executable, compression, builder UUID, and the exact deterministic="1" spelling, defaulting
        missing options; unrecognized options are ignored. Constructor setup can create directories,
        and the declaration's sources are list-copied without re-designating or restatting them. No
        progress, workflow ID, or repository association is restored.

        Example:
            >>> workflow = SquashfsBackupWorkflow.from_declaration(  # doctest: +SKIP
            ...     declaration, storage_manager=manager,
            ... )


        :param declaration: SquashFS backup intent with local staging and optional captured builder options.
        :param storage_manager: Optional borrowed manager for subsequent routed operations and Asset lookups.
        :return: Fresh DRAFT instance of cls with the declared ordered sources.
        """
        if declaration.workflow_kind is not BackupWorkflowKind.SQUASHFS_PACK:
            raise ValueError("declaration is not a SquashFS pack workflow.")
        if isinstance(declaration.staging_target, Location):
            raise StoreUnsupportedOperation(
                "SquashFS build staging currently requires a local path."
            )
        options = declaration.option_map()
        workflow = cls(
            declaration.output_target,
            workflow_name=declaration.workflow_name,
            verify_after_build=declaration.verify_after_build,
            cleanup_staging_after_success=declaration.cleanup_staging_after_success,
            staging_target=declaration.staging_target,
            mksquashfs_exe=options.get("mksquashfs_exe", "mksquashfs"),
            compression=options.get("compression", "zstd"),
            deterministic=options.get("deterministic", "0") == "1",
            storage_manager=storage_manager,
            _builder_store_uuid=(
                None
                if "builder_store_uuid" not in options
                else UUID(options["builder_store_uuid"])
            ),
        )
        workflow._sources = list(declaration.sources)
        return workflow

    @classmethod
    def from_checkpoint(
        cls,
        checkpoint: BackupWorkflowCheckpoint,
        *,
        storage_manager: StorageManagerAPI | None = None,
    ) -> SquashfsBackupWorkflow:
        """
        Reconstruct eligible execution and retain the checkpoint's position and failure state.

        Reject COMPLETE or CANCELLED through status.resumable. First reconstruct the declaration and
        builder, then restore workflow ID, status, cursor, reports, milestones, output, and error. A
        FAILED checkpoint remains FAILED until run_next explicitly retries. Reconstruction does not
        validate staged bytes or persist the restored object, and updated_at is not retained.

        Example:
            >>> resumed = SquashfsBackupWorkflow.from_checkpoint(  # doctest: +SKIP
            ...     checkpoint, storage_manager=manager,
            ... )
            >>> state = resumed.run_next()  # doctest: +SKIP


        :param checkpoint: DRAFT, RUNNING, or FAILED checkpoint whose physical staging remains available.
        :param storage_manager: Optional borrowed manager needed by the restored routed operations.
        :return: Reconstructed concrete workflow at the supplied resume position; invalid state/options/setup may raise.
        """
        if not checkpoint.status.resumable:
            raise StorePreconditionFailed("workflow checkpoint is not resumable.")
        workflow = cls.from_declaration(
            checkpoint.declaration,
            storage_manager=storage_manager,
        )
        workflow._workflow_id = checkpoint.workflow_id
        workflow._status = checkpoint.status
        workflow._next_source_index = checkpoint.next_source_index
        workflow._source_reports = list(checkpoint.source_reports)
        workflow._completed_steps = list(checkpoint.completed_steps)
        workflow._output_artifact_reference = checkpoint.output_artifact_reference
        workflow._last_error = checkpoint.last_error
        return workflow

    def _append_source(self, source: BackupSourceDeclaration) -> None:
        """
        Reject an already designated member path and append the source object.

        Compare archive_path values exactly, including None, without re-normalizing or validating
        lifecycle. Public designation performs those earlier checks. The original source value is
        retained.

        Example:
            >>> workflow._append_source(source)  # doctest: +SKIP


        :param source: Already constructed source declaration to add after earlier designation checks.
        :return: None after appending; duplicate member paths raise ValueError without modifying the list.
        """
        if any(
            existing.archive_path == source.archive_path
            for existing in self._sources
        ):
            raise ValueError(f"duplicate backup archive path: {source.archive_path!r}.")
        self._sources.append(source)

    def _stage_one(
        self,
        source: BackupSourceDeclaration,
        source_index: int,
    ) -> BackupSourceStagingReport:
        """
        Reuse verified staged bytes or copy one designated source into the build Store.

        Require a designated member path. If a staged entry exists, check only the declaration's
        available size/digest expectations and reuse it without reopening the original source.
        Otherwise a local designation must currently be a file. A Location source requires a
        manager, is statted for size agreement, and is opened in a context that closes the returned
        reader.

        The builder owns transactional member publication and byte verification. For routed reads, a
        current stat digest is used when the declaration omitted one, but digest_verified reports
        only whether the declaration had an expected digest. No source version precondition joins
        stat and read. This helper returns evidence without advancing workflow counters;
        read/close/copy failures propagate.

        Example:
            >>> report = workflow._stage_one(source, 0)  # doctest: +SKIP


        :param source: Designation with a non-None archive path and optional size/digest expectations.
        :param source_index: Source ordinal copied into the returned report; validation occurs in the report constructor.
        :return: Successful BackupSourceStagingReport for the reused or newly staged member.
        """
        assert source.archive_path is not None
        destination = self._builder.locate(source.archive_path)
        existing = self._builder.try_stat(destination)
        if existing is not None:
            self._verify_staged(existing.location, source)
            return BackupSourceStagingReport(
                source_index,
                source.source_identifier,
                source.archive_path,
                staged_location=existing.location,
                bytes_staged=existing.size,
                digest_verified=source.expected_digest is not None,
            )

        if source.source_kind is BackupSourceKind.LOCAL_PATH:
            path = pathlib.Path(str(source.source_identifier))
            if not path.is_file():
                raise FileNotFoundError(str(path))
            info = self._builder.store_file(
                path,
                location=destination,
                expected_size=source.expected_size,
                expected_digest=source.expected_digest,
            )
        else:
            if self._storage_manager is None:
                raise StorePreconditionFailed(
                    "store-location sources require a storage manager."
                )
            location = source.location
            assert location is not None
            current = self._storage_manager.stat(location)
            if source.expected_size is not None and current.size != source.expected_size:
                raise StoreIntegrityError("backup source size changed before staging.")
            with self._storage_manager.get(location) as input_stream:
                info = self._builder.store_stream(
                    input_stream,
                    location=destination,
                    expected_size=current.size,
                    expected_digest=source.expected_digest or current.digest,
                )
        return BackupSourceStagingReport(
            source_index,
            source.source_identifier,
            source.archive_path,
            staged_location=info.location,
            bytes_staged=info.size,
            digest_verified=source.expected_digest is not None,
        )

    def _verify_staged(
        self,
        staged_location: Location,
        source: BackupSourceDeclaration,
    ) -> None:
        """
        Check a staged object against the designation's available size and digest expectations.

        Always stat the staged Location. Compare size only when expected_size is present, and
        compute the requested algorithm only when expected_digest is present. With neither
        expectation, an existing staged object passes after stat alone; original-source identity and
        later file changes are not checked.

        Example:
            >>> workflow._verify_staged(staged_location, source)  # doctest: +SKIP


        :param staged_location: Build Store Location whose existing staged bytes are being reused.
        :param source: Designation supplying optional expected size and digest.
        :return: None when stated expectations match; mismatches raise StoreIntegrityError and backend failures propagate.
        """
        info = self._builder.stat(staged_location)
        if source.expected_size is not None and info.size != source.expected_size:
            raise StoreIntegrityError("existing staged source has the wrong size.")
        if source.expected_digest is not None:
            observed = self._builder.compute_digest(
                staged_location,
                source.expected_digest.algorithm,
            )
            if observed != source.expected_digest:
                raise StoreIntegrityError("existing staged source has the wrong digest.")

    def _seal_and_publish(self) -> None:
        """
        Seal or reuse the local image, optionally inspect it, publish output, and request cleanup.

        A current local file is reused only when SEAL_ARTIFACT is already recorded; otherwise reject
        it without adoption. A new build delegates create-only candidate publication to the builder,
        then records the seal milestone. Builder failure after publication can leave an image
        without that milestone. Optional verification calls the reader self-test and requires
        availability, rather than independently replaying every source hash here.

        Location output is SHA-256 hashed and streamed through manager.put. On StoreAlreadyExists
        only, accept the destination if a separately computed digest matches. Hash/stat/read and
        destination checks are not version-pinned. Local output records the resolved image pathname.
        The output reference is assigned before optional best-effort staging removal; CLEANUP
        records the attempt even if rmtree ignores filesystem errors.

        No aggregate transaction rolls back an image, routed copy, or milestone after a later error.
        This helper does not set COMPLETE or save the checkpoint, and leaves artifact
        registration/provenance to other services.

        Example:
            >>> workflow._seal_and_publish()  # doctest: +SKIP


        :return: None after assigning the output reference and optional milestones; errors can leave physical output or earlier state updates.
        """
        local_archive = self._builder.archive_path
        if local_archive.is_file():
            if BackupWorkflowStepKind.SEAL_ARTIFACT not in self._completed_steps:
                raise StoreAlreadyExists(
                    "backup output already exists without a checkpoint proving "
                    "that this workflow sealed it."
                )
            built = SquashfsReadOnlyStorageBackend(
                str(local_archive),
                name=f"{self.workflow_name} (resumed)",
            )
        else:
            built = self._builder.seal(force=False, quiet=True)
        self._mark_complete(BackupWorkflowStepKind.SEAL_ARTIFACT)

        if self._verify_after_build:
            status = built.self_test()
            if not status.available:
                raise StoreIntegrityError(
                    status.message or "sealed SquashFS artifact is unreadable."
                )
            self._mark_complete(BackupWorkflowStepKind.VERIFY_ARTIFACT)

        if isinstance(self._output_target, Location):
            if self._storage_manager is None:
                raise StorePreconditionFailed(
                    "Location output targets require a storage manager."
                )
            digest = _file_digest(local_archive)
            try:
                with local_archive.open("rb") as source:
                    self._storage_manager.put(
                        self._output_target,
                        source,
                        expected_size=local_archive.stat().st_size,
                        expected_digest=digest,
                    )
            except StoreAlreadyExists:
                target_store = self._storage_manager.get_store(
                    self._output_target.store_ref
                )
                if target_store.compute_digest(
                    self._output_target,
                    digest.algorithm,
                ) != digest:
                    raise
            self._output_artifact_reference = self._output_target
        else:
            self._output_artifact_reference = str(local_archive)

        if self._cleanup_staging_after_success:
            # A local output is deliberately outside staging. A Location output
            # has already been committed before cleanup begins.
            shutil.rmtree(self._builder.staging_root, ignore_errors=True)
            self._mark_complete(BackupWorkflowStepKind.CLEANUP)

    def _mark_complete(self, step: BackupWorkflowStepKind) -> None:
        """
        Append a milestone only when it is not already recorded.

        Membership equality controls deduplication. The helper does not enforce milestone order,
        validate a step type, or persist state.

        Example:
            >>> workflow._mark_complete(BackupWorkflowStepKind.SEAL_ARTIFACT)  # doctest: +SKIP


        :param step: Milestone value to retain once in first-recorded order.
        :return: None after an append or an already-present no-op.
        """
        if step not in self._completed_steps:
            self._completed_steps.append(step)

    def _require_draft(self) -> None:
        """
        Require the DRAFT enum singleton before permitting public source designation.

        This check changes no state. Any other value, including an equivalent raw string, raises
        StorePreconditionFailed.

        Example:
            >>> workflow._require_draft()  # doctest: +SKIP


        :return: None in DRAFT; otherwise StorePreconditionFailed indicating that sources are immutable.
        """
        if self._status is not WorkflowStatus.DRAFT:
            raise StorePreconditionFailed(
                "backup sources are immutable after execution starts."
            )


def _default_workflow_name(output_target: str | Location) -> str:
    """
    Derive a fallback display name from a local basename or the fixed routed-output label.

    A Location always selects squashfs-backup. Local spelling is passed to Path without user
    expansion or resolution; prefer stem, then name, then the same fixed fallback.

    Example:
        >>> _default_workflow_name("/backups/nightly.sqsh")
        'nightly'


    :param output_target: Local output spelling or managed Location whose workflow needs a default display name.
    :return: Basename-derived display text, or squashfs-backup when a usable basename is absent.
    """
    if isinstance(output_target, Location):
        return "squashfs-backup"
    path = pathlib.Path(output_target)
    return path.stem or path.name or "squashfs-backup"


def _local_build_output(
    output_target: str | Location,
    staging_target: str | None,
    *,
    builder_store_uuid: UUID,
) -> pathlib.Path:
    """
    Choose the absolute local image path used before any routed publication.

    String outputs are expanded and resolved directly. A routed output requires non-None local
    staging; the intermediate image is named with the builder UUID and placed in the staging
    directory's parent. This avoids sharing a fixed intermediate name across builders, but does not
    reserve a path or create directories.

    Example:
        >>> _local_build_output(
        ...     Location(UUID(int=1), "packs/nightly"), "/tmp/stage",
        ...     builder_store_uuid=UUID(int=2),
        ... ).name
        '.liuxin-backup-output-00000000000000000000000000000002.squashfs'


    :param output_target: Local image path or managed output Location.
    :param staging_target: Local staging directory required for routed output; ignored for a string output.
    :param builder_store_uuid: Builder identity used to distinguish the routed-output intermediate filename.
    :return: Resolved local image Path; absent routed-output staging raises StorePreconditionFailed.
    """
    if isinstance(output_target, str):
        return pathlib.Path(output_target).expanduser().resolve(strict=False)
    if staging_target is None:
        raise StorePreconditionFailed(
            "Location output targets require a persistent local staging_target."
        )
    return (
        pathlib.Path(staging_target).expanduser().resolve(strict=False).parent
        / f".liuxin-backup-output-{builder_store_uuid.hex}.squashfs"
    )


def _file_digest(path: pathlib.Path) -> Digest:
    """
    Hash a local image with SHA-256 in 1 MiB reads and close the file on exit.

    The file is opened from its current pathname without a size cap or version pin. Open/read/close
    errors propagate; a successful digest describes bytes read during this traversal.

    Example:
        >>> digest = _file_digest(pathlib.Path("nightly.sqsh"))  # doctest: +SKIP
        >>> digest.algorithm  # doctest: +SKIP
        'sha256'


    :param path: Local image Path to open in binary read mode.
    :return: SHA-256 Digest containing the lowercase hexadecimal hash of the streamed bytes.
    """
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return Digest("sha256", digest.hexdigest())


def _preferred_digest(digests: tuple[Digest, ...]) -> Digest:
    """
    Select the first SHA-256 digest, otherwise retain the collection's first digest.

    No hash is computed and digest values are not revalidated. The fallback is indexed eagerly, so
    an empty collection raises IndexError before selection.

    Example:
        >>> _preferred_digest((Digest("md5", "aa"), Digest("sha256", "bb"))).algorithm
        'sha256'


    :param digests: Nonempty ordered catalogue digest tuple.
    :return: Original first SHA-256 Digest object, or the original first entry when SHA-256 is absent.
    """

    return next(
        (digest for digest in digests if digest.algorithm == "sha256"),
        digests[0],
    )


__all__ = ["SquashfsBackupWorkflow"]
