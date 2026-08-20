"""Register successful SquashFS artifacts as stable read-only Stores."""

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
    """Persist artifact Store identity and protected source-presence links."""

    def __init__(self, db, *, storage_manager=None) -> None:
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
    from LiuXin_alpha.storage.backup.backup_workflow_repository import (
        _decode_reference,
    )

    return _decode_reference(value)


RegisteredBackupArtifact = BackupArtifactRegistration


__all__ = ["BackupArtifactRegistry", "RegisteredBackupArtifact"]
