"""
Register successful backup images as configured Stores with source-presence metadata.

The registry reuses persisted Store/image associations and can attach them to a borrowed
manager. Registration writes can have partial effects, and reuse does not revalidate bytes
or complete a previously interrupted attachment/link pass. Physical image creation and
whole-image Asset provenance remain separate operations.
"""

from __future__ import annotations

import pathlib

from collections.abc import Iterator
from urllib.parse import unquote, urlparse
from uuid import UUID, uuid4

from LiuXin_alpha.databases import Row
from LiuXin_alpha.storage.api import (
    BackupArtifactRegistration,
    BackupArtifactRegistryAPI,
    BackupWorkflowResult,
    Location,
    StoreConfiguration,
    StoreConfigurationNotFound,
    StoreIntegrityError,
    StoreUnsupportedOperation,
)
from LiuXin_alpha.storage.backup.backup_workflow_repository import (
    BackupWorkflowRepository,
)


class BackupArtifactRegistry(BackupArtifactRegistryAPI):
    """
    Associate completed image references with stable Store rows and protected member-presence links.

    The registry borrows a database and optional manager and creates a repository adapter over that
    database. It configures a SquashFS read-only Store for an image without parsing archive contents
    or recording the entire image as a derived atomic Asset. New registration can perform Store,
    checkpoint/output, presence, and manager updates in sequence with no encompassing transaction.

    Existing registrations are reused before current path checks or new options are applied. Failed
    later attachment or linking can leave earlier metadata visible; retrying a now-visible
    registration does not automatically finish the interrupted work. RegisteredBackupArtifact
    remains an alias for the public registration value.

    Example:
        >>> registry = BackupArtifactRegistry(db, storage_manager=manager)  # doctest: +SKIP
        >>> registration = registry.register_artifact(3, result)  # doctest: +SKIP
    """

    def __init__(self, db, *, storage_manager=None) -> None:
        """
        Retain database/manager references and construct the workflow repository adapter.

        No schema check, startup, transaction, or ownership transfer occurs. The caller controls
        both borrowed objects' lifetimes.

        Example:
            >>> registry = BackupArtifactRegistry(db, storage_manager=manager)  # doctest: +SKIP


        :param db: Borrowed database containing workflow, Store, and presence-link tables.
        :param storage_manager: Optional borrowed manager used to resolve routed local images and attach registered Stores.
        :return: None after retaining dependencies and creating BackupWorkflowRepository over the same database.
        """
        self.db = db
        self.storage_manager = storage_manager
        self.repository = BackupWorkflowRepository(db)

    def register_artifact(
        self,
        workflow_id: int,
        result: BackupWorkflowResult,
        *,
        store_name: str | None = None,
        link_sources: bool = True,
    ) -> BackupArtifactRegistration:
        """
        Register a successful result's image and optionally insert source-presence evidence.

        Int-convert workflow_id, require successful fields and a compatible optional result ID, then
        return any existing registration immediately. For a new registration, resolve a local image
        path and require a file. Reuse/create its Store row, record the result, attach that Store ID
        to the last output row, then optionally insert member links and attach the Store to the
        manager.

        A falsey store_name falls back to image stem/name. Existing Store rows retain their own
        name/configuration. Member fallback names use the number of links newly inserted so far, not
        the source ordinal; a duplicate that inserts nothing leaves that fallback counter unchanged.
        The returned new-registration count reports insertions from this call, whereas later lookup
        counts current links for the whole Store.

        This does not validate a SquashFS signature, read members, or establish rollback across
        writes. A late link/attachment error can leave a discoverable registration, and a retry then
        takes the early-return path without repairing unfinished work.

        Example:
            >>> registration = registry.register_artifact(  # doctest: +SKIP
            ...     3, result, store_name="nightly", link_sources=True,
            ... )


        :param workflow_id: Int-convertible durable workflow ID; the result ID must be None or equal it.
        :param result: Successful terminal result with a non-None image reference and source declaration.
        :param store_name: Optional name for a newly inserted Store row; falsey input derives a basename.
        :param link_sources: Whether a new registration should record source-member presence; existing registrations ignore this option.
        :return: Existing or newly completed BackupArtifactRegistration; errors may follow partial metadata/manager effects.
        """
        workflow_id = int(workflow_id)
        if not result.successful or result.output_artifact_reference is None:
            raise StoreIntegrityError(
                "only a successful workflow result can be registered."
            )
        if result.workflow_id not in (None, workflow_id):
            raise StoreIntegrityError("workflow result id does not match registration id.")

        existing = self.get_artifact_registration(workflow_id)
        if existing is not None:
            return existing

        artifact_path = self._artifact_path(result.output_artifact_reference)
        if not artifact_path.is_file():
            raise StoreIntegrityError(
                f"backup artifact does not exist: {artifact_path}."
            )
        chosen_name = store_name or artifact_path.stem or artifact_path.name
        store_row = self._find_or_create_store(
            artifact_path,
            store_name=chosen_name,
            workflow_id=workflow_id,
        )
        store_id = int(store_row["store_id"])
        store_ref = UUID(str(store_row["store_uuid"]))
        registration = BackupArtifactRegistration(
            workflow_id=workflow_id,
            backup_store_ref=store_ref,
            backup_store_name=str(store_row["store_name"]),
            artifact_reference=result.output_artifact_reference,
        )

        self.repository.record_result(workflow_id, result)
        output_rows = self.db.search(
            "backup_workflow_outputs",
            "backup_workflow_output_workflow_id",
            workflow_id,
        )
        output_row = output_rows[-1]
        output_row["backup_workflow_output_store_id"] = store_id
        output_row.sync()

        links_created = 0
        if link_sources:
            for source in result.declaration.sources:
                archive_path = source.archive_path or f"source-{links_created:06d}"
                if self.repository.record_backup_presence(
                    workflow_id,
                    registration,
                    source,
                    archive_path=archive_path,
                ):
                    links_created += 1
        registration = BackupArtifactRegistration(
            workflow_id=workflow_id,
            backup_store_ref=store_ref,
            backup_store_name=str(store_row["store_name"]),
            artifact_reference=result.output_artifact_reference,
            presence_links_created=links_created,
        )
        self._attach_to_manager(artifact_path, registration)
        return registration

    def get_artifact_registration(
        self,
        workflow_id: int,
    ) -> BackupArtifactRegistration | None:
        """
        Reconstruct the last search-result output associated with a Store for one workflow.

        Ignore output rows with None/empty Store IDs and return None when none remain. Require the
        referenced Store and a nonempty UUID, then decode the original artifact reference and count
        all current presence links for that Store, across workflows. Search order determines the
        selected output; it is not explicitly sorted by creation time.

        Lookup does not inspect image existence, readability, or member contents. Missing Store
        identity raises StoreIntegrityError; invalid UUID/reference spelling or malformed rows can
        raise their own errors.

        Example:
            >>> registration = registry.get_artifact_registration(3)  # doctest: +SKIP


        :param workflow_id: Int-convertible workflow ID whose Store-associated output is requested.
        :return: Reconstructed registration with current Store-wide link count, or None when no output has a Store association.
        """
        rows = self.db.search(
            "backup_workflow_outputs",
            "backup_workflow_output_workflow_id",
            int(workflow_id),
        )
        rows = [
            row
            for row in rows
            if row["backup_workflow_output_store_id"] not in (None, "")
        ]
        if not rows:
            return None
        output = rows[-1]
        store = self.db.get_row_from_id(
            "stores",
            int(output["backup_workflow_output_store_id"]),
        )
        if store is None or store["store_uuid"] in (None, ""):
            raise StoreIntegrityError(
                "registered backup output has no stable Store UUID."
            )
        links = self.db.search(
            "backup_presence_links",
            "backup_presence_link_backup_store_id",
            int(store["store_id"]),
        )
        return BackupArtifactRegistration(
            workflow_id=int(workflow_id),
            backup_store_ref=UUID(str(store["store_uuid"])),
            backup_store_name=str(store["store_name"]),
            artifact_reference=_decode_artifact_reference(
                str(output["backup_workflow_output_url"])
            ),
            presence_links_created=len(links),
        )

    def iter_artifact_registrations(self) -> Iterator[BackupArtifactRegistration]:
        """
        Yield reconstructed registrations once per successfully resolved workflow ID.

        Materialize output rows in adapter order, skip missing IDs, and int-convert the rest. A
        workflow enters the seen set only when lookup returns a registration; an unregistered
        duplicate may therefore be queried repeatedly. Reconstruction failures propagate after any
        earlier yields, without a pinned metadata snapshot or image check.

        Example:
            >>> registrations = tuple(registry.iter_artifact_registrations())  # doctest: +SKIP


        :return: Iterator of available workflow registrations with duplicate successful workflow IDs suppressed.
        """
        seen: set[int] = set()
        for row in self._all_output_rows():
            workflow_id = row["backup_workflow_output_workflow_id"]
            if workflow_id in (None, ""):
                continue
            workflow_id = int(workflow_id)
            if workflow_id in seen:
                continue
            registration = self.get_artifact_registration(workflow_id)
            if registration is not None:
                seen.add(workflow_id)
                yield registration

    def _artifact_path(self, reference: str | Location) -> pathlib.Path:
        """
        Resolve an image reference to a local pathname suitable for Store configuration.

        Strings accept ordinary paths or file URIs; other URI schemes reject. File URIs use the
        decoded path component only, ignoring authority/query/fragment, while ordinary paths expand
        user syntax. No file existence check occurs in this helper.

        Routed input needs a manager and a Store exposing root_path. Pass the reference through
        Store.locate, resolve the root and joined key, and require current containment beneath that
        root. This rejects a presently escaping symlink but does not lock pathname state against
        later changes.

        Example:
            >>> registry = BackupArtifactRegistry(None)
            >>> registry._artifact_path("file:///tmp/nightly%20pack.sqsh").name
            'nightly pack.sqsh'


        :param reference: Local path/file-URI text or managed Location backed by a Store exposing a local root.
        :return: Resolved local Path; unsupported routing/schemes or current containment failure raise typed Store errors.
        """
        if isinstance(reference, str):
            parsed = urlparse(reference)
            if parsed.scheme == "file":
                return pathlib.Path(unquote(parsed.path)).resolve()
            if parsed.scheme:
                raise StoreUnsupportedOperation(
                    "only local SquashFS artifacts can become mounted Stores."
                )
            return pathlib.Path(reference).expanduser().resolve()
        if self.storage_manager is None:
            raise StoreUnsupportedOperation(
                "a Location artifact requires a storage manager for registration."
            )
        store = self.storage_manager.get_store(reference.store_ref)
        root = getattr(store, "root_path", None)
        if root is None:
            raise StoreUnsupportedOperation(
                "the artifact Store cannot expose a safe local archive path."
            )
        # The Store has already validated this Location. Resolving again keeps
        # the registry from following a later symlink outside the Store root.
        validated = store.locate(reference)
        root_path = pathlib.Path(root).resolve()
        candidate = root_path.joinpath(*pathlib.PurePosixPath(validated.key).parts)
        resolved = candidate.resolve()
        try:
            resolved.relative_to(root_path)
        except ValueError as error:
            raise StoreIntegrityError("artifact Location escapes its Store.") from error
        return resolved

    def _find_or_create_store(
        self,
        artifact_path: pathlib.Path,
        *,
        store_name: str,
        workflow_id: int,
    ):
        """
        Reuse the first Store row for an image path or insert its read-only SquashFS configuration.

        Search canonical file URI first, then historical raw path text. An existing row is returned
        without checking or replacing name, kind, capabilities, or workflow metadata; only a missing
        UUID is generated and synchronized. A new row uses an archive role, configured read-only
        capabilities, backup/archive modes, and a workflow scratch reference, filtered to columns
        the schema reports.

        The caller supplies an absolute image path. No image parsing, manager registration,
        concurrency lock, or enclosing transaction occurs.

        Example:
            >>> store_row = registry._find_or_create_store(  # doctest: +SKIP
            ...     image_path, store_name="nightly", workflow_id=3,
            ... )


        :param artifact_path: Absolute image Path used for canonical URI and legacy-path searches.
        :param store_name: Display name assigned only when inserting a new Store row.
        :param workflow_id: Workflow ID embedded in new-row scratch metadata.
        :return: Original reused Store row or newly inserted Row with a stable UUID; schema/persistence errors propagate.
        """
        root_uri = artifact_path.as_uri()
        existing = self.db.search("stores", "store_root_uri", root_uri)
        if not existing:
            # Read historical rows written before root URIs were canonical.
            existing = self.db.search("stores", "store_root_uri", str(artifact_path))
        if existing:
            row = existing[0]
            if row["store_uuid"] in (None, ""):
                row["store_uuid"] = str(uuid4())
                row.sync()
            return row

        store_ref = uuid4()
        allowed = set(self.db.get_column_headings("stores"))
        values = {
            "store_uuid": str(store_ref),
            "store_name": store_name,
            "store_kind": "squashfs_readonly",
            "store_access_protocol": "squashfs",
            "store_root_uri": root_uri,
            "store_operational_role": "archive",
            "store_is_read_only": 1,
            "store_supports_folders": 1,
            "store_supports_hierarchical_list": 1,
            "store_supports_random_read": 1,
            "store_supports_random_write": 0,
            "store_supports_delete": 0,
            "store_supports_immutable_objects": 1,
            "store_online_status": "online",
            "store_supports_active_replica_mode": 0,
            "store_supports_backup_replica_mode": 1,
            "store_supports_archive_replica_mode": 1,
            "store_scratch": (
                '{"created_by_backup_workflow_id":%d}' % workflow_id
            ),
        }
        return Row.from_idless_row_dict(
            self.db,
            row_dict={key: value for key, value in values.items() if key in allowed},
            table="stores",
        )

    def _attach_to_manager(
        self,
        artifact_path: pathlib.Path,
        registration: BackupArtifactRegistration,
    ) -> None:
        """
        Ensure the borrowed manager knows the registered Store without requesting startup.

        No manager is a no-op. Any existing get_store result is accepted without comparing
        configuration or health. Only StoreConfigurationNotFound triggers creation with the
        registration UUID/name and a read-only SquashFS URI. Other lookup errors and create_store
        failures propagate after prior registry writes.

        Example:
            >>> registry._attach_to_manager(image_path, registration)  # doctest: +SKIP


        :param artifact_path: Absolute image Path whose file URI configures a newly attached Store.
        :param registration: Stable Store identity/name retained by the registry.
        :return: None after an absent-manager/existing-Store no-op or successful create_store(startup=False).
        """
        if self.storage_manager is None:
            return
        try:
            self.storage_manager.get_store(registration.backup_store_ref)
            return
        except StoreConfigurationNotFound:
            pass
        self.storage_manager.create_store(
            StoreConfiguration(
                store_uuid=registration.backup_store_ref,
                store_name=registration.backup_store_name,
                store_kind="squashfs_readonly",
                store_root_uri=artifact_path.as_uri(),
                store_access_protocol="squashfs",
                read_only=True,
                supports_folders=True,
            ),
            startup=False,
        )

    def _all_output_rows(self):
        """
        Materialize output rows through the first exposed database enumeration interface.

        Prefer driver_wrapper.read, then get_all_rows(iterator_return=False), then connection SQL
        with column headings and strict positional pairing. Selection checks attribute presence
        without callability validation. No sort, streaming bound, or snapshot transaction is added;
        missing interfaces and adapter failures propagate.

        Example:
            >>> rows = registry._all_output_rows()  # doctest: +SKIP


        :return: List of output rows or reconstructed dictionaries in adapter query order.
        """
        wrapper = getattr(self.db, "driver_wrapper", None)
        if wrapper is not None and hasattr(wrapper, "read"):
            return list(wrapper.read("backup_workflow_outputs"))
        if hasattr(self.db, "get_all_rows"):
            return list(
                self.db.get_all_rows(
                    "backup_workflow_outputs",
                    iterator_return=False,
                )
            )
        connection = getattr(self.db, "conn")
        columns = list(self.db.get_column_headings("backup_workflow_outputs"))
        return [
            dict(zip(columns, values, strict=True))
            for values in connection.execute(
                "SELECT * FROM `backup_workflow_outputs`"
            ).fetchall()
        ]


def _decode_artifact_reference(value: str) -> str | Location:
    # Keep the persistence envelope private to the repository module while
    # sharing its exact decoder within the concrete backup package.
    """
    Delegate artifact-reference reconstruction to the repository's exact private decoder.

    Import the decoder lazily to share its versioned-envelope and historical raw-path behavior
    without exposing the encoding through the public API. Recognized-envelope errors propagate
    unchanged.

    Example:
        >>> _decode_artifact_reference("/backups/legacy.sqsh")
        '/backups/legacy.sqsh'


    :param value: Persisted reference string from a workflow output row.
    :return: Decoded text/Location or original unrecognized spelling according to the repository decoder.
    """
    from LiuXin_alpha.storage.backup.backup_workflow_repository import (
        _decode_reference,
    )

    return _decode_reference(value)


RegisteredBackupArtifact = BackupArtifactRegistration


__all__ = ["BackupArtifactRegistry", "RegisteredBackupArtifact"]
