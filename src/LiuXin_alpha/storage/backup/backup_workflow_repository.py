"""Database-backed persistence for the public backup workflow values."""

from __future__ import annotations

import dataclasses
import json

from collections.abc import Iterator
from typing import Any
from uuid import UUID

from LiuXin_alpha.databases import Row
from LiuXin_alpha.storage.api import (
    BackupArtifactRegistration,
    BackupSourceDeclaration,
    BackupSourceKind,
    BackupSourceStagingReport,
    BackupWorkflowCheckpoint,
    BackupWorkflowDeclaration,
    BackupWorkflowKind,
    BackupWorkflowRepositoryAPI,
    BackupWorkflowResult,
    BackupWorkflowStepKind,
    Digest,
    Location,
    StorePreconditionFailed,
    WorkflowID,
    WorkflowStatus,
)


class BackupWorkflowRepository(BackupWorkflowRepositoryAPI):
    """Persist immutable declarations and typed checkpoints behind one facade."""

    def __init__(self, db) -> None:
        self.db = db

    def save_workflow_declaration(
        self,
        declaration: BackupWorkflowDeclaration,
        *,
        workflow_id: WorkflowID | None = None,
        status: WorkflowStatus = WorkflowStatus.DRAFT,
    ) -> WorkflowID:
        if not isinstance(declaration, BackupWorkflowDeclaration):
            raise TypeError("declaration must be a BackupWorkflowDeclaration.")
        status = WorkflowStatus(status)
        row_values = {
            "backup_workflow_name": declaration.workflow_name,
            "backup_workflow_kind": declaration.workflow_kind.value,
            "backup_workflow_output_url": _encode_reference(declaration.output_target),
            "backup_workflow_verify_after_build": int(declaration.verify_after_build),
            "backup_workflow_cleanup_staging_after_success": int(
                declaration.cleanup_staging_after_success
            ),
            "backup_workflow_staging_root": (
                None
                if declaration.staging_target is None
                else _encode_reference(declaration.staging_target)
            ),
            "backup_workflow_options_json": json.dumps(
                dict(declaration.options), sort_keys=True
            ),
            "backup_workflow_status": status.value,
            "backup_workflow_last_error": None,
        }
        if workflow_id is None:
            row = Row.from_idless_row_dict(
                self.db,
                row_dict=row_values,
                table="backup_workflows",
            )
            workflow_id = int(row["backup_workflow_id"])
        else:
            workflow_id = int(workflow_id)
            row = self._require_row("backup_workflows", workflow_id)
            self._update_row(row, row_values)
            self._delete_sources(workflow_id)

        for ordinal, source in enumerate(declaration.sources):
            Row.from_idless_row_dict(
                self.db,
                row_dict={
                    "backup_workflow_source_workflow_id": workflow_id,
                    "backup_workflow_source_ordinal": ordinal,
                    "backup_workflow_source_kind": source.source_kind.value,
                    # The complete typed source lives in this versioned JSON
                    # envelope. Legacy scalar columns remain indexing aids.
                    "backup_workflow_source_identifier": _encode_source(source),
                    "backup_workflow_source_archive_path": source.archive_path,
                    "backup_workflow_source_expected_size": source.expected_size,
                    "backup_workflow_source_expected_hash": (
                        None
                        if source.expected_digest is None
                        else _encode_digest(source.expected_digest)
                    ),
                    "backup_workflow_source_file_id": None,
                    "backup_workflow_source_asset_replica_id": source.source_replica_id,
                    "backup_workflow_source_store_id": None,
                },
                table="backup_workflow_sources",
            )
        return workflow_id

    def load_workflow_declaration(
        self,
        workflow_id: WorkflowID,
    ) -> BackupWorkflowDeclaration:
        workflow_id = int(workflow_id)
        row = self._require_row("backup_workflows", workflow_id)
        source_rows = sorted(
            self.db.search(
                "backup_workflow_sources",
                "backup_workflow_source_workflow_id",
                workflow_id,
            ),
            key=lambda item: int(_value(item, "backup_workflow_source_ordinal") or 0),
        )
        options_raw = _value(row, "backup_workflow_options_json")
        options = json.loads(str(options_raw)) if options_raw not in (None, "") else {}
        staging_raw = _value(row, "backup_workflow_staging_root")
        return BackupWorkflowDeclaration(
            workflow_name=str(_value(row, "backup_workflow_name")),
            workflow_kind=BackupWorkflowKind(
                str(_value(row, "backup_workflow_kind"))
            ),
            output_target=_decode_reference(
                str(_value(row, "backup_workflow_output_url"))
            ),
            sources=tuple(_decode_source(item) for item in source_rows),
            verify_after_build=bool(
                _value(row, "backup_workflow_verify_after_build")
            ),
            cleanup_staging_after_success=bool(
                _value(row, "backup_workflow_cleanup_staging_after_success")
            ),
            staging_target=(
                None
                if staging_raw in (None, "")
                else _decode_reference(str(staging_raw))
            ),
            options=tuple((str(key), str(value)) for key, value in options.items()),
        )

    def iter_workflow_declarations(
        self,
        *,
        status: WorkflowStatus | None = None,
    ) -> Iterator[tuple[WorkflowID, BackupWorkflowDeclaration]]:
        rows = (
            self._all_rows("backup_workflows")
            if status is None
            else self.db.search(
                "backup_workflows",
                "backup_workflow_status",
                WorkflowStatus(status).value,
            )
        )
        for row in sorted(
            rows,
            key=lambda item: int(_value(item, "backup_workflow_id")),
        ):
            workflow_id = int(_value(row, "backup_workflow_id"))
            yield workflow_id, self.load_workflow_declaration(workflow_id)

    def save_checkpoint(
        self,
        workflow_id: WorkflowID,
        checkpoint: BackupWorkflowCheckpoint,
    ) -> None:
        workflow_id = int(workflow_id)
        if checkpoint.workflow_id not in (None, workflow_id):
            raise StorePreconditionFailed(
                "checkpoint belongs to a different workflow id."
            )
        if checkpoint.declaration != self.load_workflow_declaration(workflow_id):
            raise StorePreconditionFailed(
                "checkpoint declaration differs from durable workflow intent."
            )
        values = {
            "backup_workflow_state_workflow_id": workflow_id,
            "backup_workflow_state_status": checkpoint.status.value,
            "backup_workflow_state_next_source_index": checkpoint.next_source_index,
            "backup_workflow_state_staged_source_count": checkpoint.staged_source_count,
            "backup_workflow_state_completed_steps_json": json.dumps(
                [step.value for step in checkpoint.completed_steps]
            ),
            "backup_workflow_state_source_results_json": json.dumps(
                [_report_to_json(report) for report in checkpoint.source_reports],
                sort_keys=True,
            ),
            "backup_workflow_state_output_artifact_url": (
                None
                if checkpoint.output_artifact_reference is None
                else _encode_reference(checkpoint.output_artifact_reference)
            ),
            "backup_workflow_state_last_error": checkpoint.last_error,
        }
        existing = self.db.search(
            "backup_workflow_state",
            "backup_workflow_state_workflow_id",
            workflow_id,
        )
        if existing:
            self._update_row(existing[0], values)
        else:
            Row.from_idless_row_dict(
                self.db,
                row_dict=values,
                table="backup_workflow_state",
            )
        self._update_workflow_status(
            workflow_id,
            checkpoint.status,
            checkpoint.last_error,
        )

    def load_checkpoint(self, workflow_id: WorkflowID) -> BackupWorkflowCheckpoint:
        workflow_id = int(workflow_id)
        declaration = self.load_workflow_declaration(workflow_id)
        rows = self.db.search(
            "backup_workflow_state",
            "backup_workflow_state_workflow_id",
            workflow_id,
        )
        if not rows:
            workflow = self._require_row("backup_workflows", workflow_id)
            return BackupWorkflowCheckpoint(
                declaration,
                WorkflowStatus(str(_value(workflow, "backup_workflow_status"))),
                workflow_id=workflow_id,
                last_error=_optional_text(
                    _value(workflow, "backup_workflow_last_error")
                ),
            )
        row = rows[0]
        steps_raw = _value(row, "backup_workflow_state_completed_steps_json")
        reports_raw = _value(row, "backup_workflow_state_source_results_json")
        output_raw = _value(row, "backup_workflow_state_output_artifact_url")
        return BackupWorkflowCheckpoint(
            declaration=declaration,
            status=WorkflowStatus(
                str(_value(row, "backup_workflow_state_status"))
            ),
            workflow_id=workflow_id,
            next_source_index=int(
                _value(row, "backup_workflow_state_next_source_index") or 0
            ),
            staged_source_count=int(
                _value(row, "backup_workflow_state_staged_source_count") or 0
            ),
            source_reports=tuple(
                _report_from_json(item)
                for item in (
                    json.loads(str(reports_raw))
                    if reports_raw not in (None, "")
                    else []
                )
            ),
            completed_steps=tuple(
                BackupWorkflowStepKind(value)
                for value in (
                    json.loads(str(steps_raw))
                    if steps_raw not in (None, "")
                    else []
                )
            ),
            output_artifact_reference=(
                None
                if output_raw in (None, "")
                else _decode_reference(str(output_raw))
            ),
            last_error=_optional_text(
                _value(row, "backup_workflow_state_last_error")
            ),
        )

    def record_result(
        self,
        workflow_id: WorkflowID,
        result: BackupWorkflowResult,
    ) -> None:
        workflow_id = int(workflow_id)
        if result.workflow_id not in (None, workflow_id):
            raise StorePreconditionFailed("result belongs to a different workflow id.")
        checkpoint = result.final_checkpoint or BackupWorkflowCheckpoint(
            declaration=result.declaration,
            status=result.status,
            workflow_id=workflow_id,
            next_source_index=len(result.declaration.sources),
            staged_source_count=sum(report.ok for report in result.source_reports),
            source_reports=result.source_reports,
            completed_steps=result.completed_steps,
            output_artifact_reference=result.output_artifact_reference,
            last_error=result.last_error,
        )
        self.save_checkpoint(workflow_id, checkpoint)
        output = result.output_artifact_reference
        if output is None:
            return
        values = {
            "backup_workflow_output_workflow_id": workflow_id,
            "backup_workflow_output_url": _encode_reference(output),
            "backup_workflow_output_verified_ok": int(result.successful),
        }
        existing = self.db.search(
            "backup_workflow_outputs",
            "backup_workflow_output_workflow_id",
            workflow_id,
        )
        if existing:
            self._update_row(existing[-1], values)
        else:
            Row.from_idless_row_dict(
                self.db,
                row_dict=values,
                table="backup_workflow_outputs",
            )

    def record_backup_presence(
        self,
        workflow_id: WorkflowID,
        registration: BackupArtifactRegistration,
        source: BackupSourceDeclaration,
        *,
        archive_path: str,
        protected: bool = True,
        immutable: bool = True,
    ) -> bool:
        workflow_id = int(workflow_id)
        store_id = self._store_id_for_uuid(registration.backup_store_ref)
        existing = self.db.search(
            "backup_presence_links",
            "backup_presence_link_backup_store_id",
            store_id,
        )
        for row in existing:
            if (
                str(_value(row, "backup_presence_link_archive_path"))
                == archive_path
            ):
                return False
        Row.from_idless_row_dict(
            self.db,
            row_dict={
                "backup_presence_link_backup_store_id": store_id,
                "backup_presence_link_workflow_id": workflow_id,
                "backup_presence_link_source_identifier": _encode_source(source),
                "backup_presence_link_source_kind": source.source_kind.value,
                "backup_presence_link_source_file_id": None,
                "backup_presence_link_source_asset_replica_id": source.source_replica_id,
                "backup_presence_link_source_store_id": None,
                "backup_presence_link_archive_path": archive_path,
                "backup_presence_link_type": "packed_presence",
                "backup_presence_link_output_url": _encode_reference(
                    registration.artifact_reference
                ),
                "backup_presence_link_is_protected": int(protected),
                "backup_presence_link_is_immutable": int(immutable),
            },
            table="backup_presence_links",
        )
        return True

    def delete_workflow(
        self,
        workflow_id: WorkflowID,
        *,
        require_terminal: bool = True,
    ) -> bool:
        workflow_id = int(workflow_id)
        row = self.db.get_row_from_id("backup_workflows", workflow_id)
        if row is None:
            return False
        status = WorkflowStatus(str(_value(row, "backup_workflow_status")))
        if require_terminal and not status.terminal:
            raise StorePreconditionFailed("workflow is not terminal.")
        self.db.delete(row)
        return True

    def _delete_sources(self, workflow_id: int) -> None:
        for row in self.db.search(
            "backup_workflow_sources",
            "backup_workflow_source_workflow_id",
            workflow_id,
        ):
            self.db.delete(row)

    def _update_workflow_status(
        self,
        workflow_id: int,
        status: WorkflowStatus,
        last_error: str | None,
    ) -> None:
        self._update_row(
            self._require_row("backup_workflows", workflow_id),
            {
                "backup_workflow_status": status.value,
                "backup_workflow_last_error": last_error,
            },
        )

    def _store_id_for_uuid(self, store_uuid: UUID) -> int:
        rows = self.db.search("stores", "store_uuid", str(store_uuid))
        if not rows:
            raise KeyError(f"No database Store row for UUID {store_uuid}.")
        return int(_value(rows[0], "store_id"))

    def _require_row(self, table: str, row_id: int):
        row = self.db.get_row_from_id(table, int(row_id))
        if row is None:
            raise KeyError(f"Unknown {table} row id: {row_id}.")
        return row

    def _update_row(self, row, values: dict[str, Any]) -> None:
        if hasattr(row, "allowed_columns"):
            for key, value in values.items():
                if key in row.allowed_columns:
                    row[key] = value
            row.sync()
            return
        updated = dict(row)
        updated.update(values)
        self.db.driver_wrapper.update_row(updated)

    def _all_rows(self, table: str) -> list[Any]:
        wrapper = getattr(self.db, "driver_wrapper", None)
        if wrapper is not None and hasattr(wrapper, "read"):
            return list(wrapper.read(table))
        if hasattr(self.db, "get_all_rows"):
            return list(self.db.get_all_rows(table, iterator_return=False))
        connection = getattr(self.db, "conn", None)
        if connection is None:
            raise TypeError("database cannot enumerate workflow rows.")
        columns = list(self.db.get_column_headings(table))
        rows = connection.execute(f"SELECT * FROM `{table}`").fetchall()
        return [dict(zip(columns, values, strict=True)) for values in rows]


def _value(row: Any, key: str) -> Any:
    return row[key]


def _optional_text(value: Any) -> str | None:
    return None if value in (None, "") else str(value)


def _encode_reference(reference: str | Location) -> str:
    payload = (
        {"v": 1, "kind": "text", "value": reference}
        if isinstance(reference, str)
        else {
            "v": 1,
            "kind": "location",
            "store_ref": str(reference.store_ref),
            "key": reference.key,
        }
    )
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _decode_reference(encoded: str) -> str | Location:
    try:
        payload = json.loads(encoded)
    except (TypeError, json.JSONDecodeError):
        # Existing databases stored local paths directly.
        return encoded
    if not isinstance(payload, dict) or payload.get("v") != 1:
        return encoded
    if payload.get("kind") == "text":
        return str(payload["value"])
    if payload.get("kind") == "location":
        return Location(UUID(str(payload["store_ref"])), str(payload["key"]))
    raise ValueError("unknown persisted backup reference kind.")


def _encode_digest(digest: Digest) -> str:
    return f"{digest.algorithm}:{digest.value}"


def _decode_digest(value: Any) -> Digest | None:
    if value in (None, ""):
        return None
    algorithm, separator, digest = str(value).partition(":")
    if not separator:
        # Historical rows implicitly meant SHA-256.
        algorithm, digest = "sha256", algorithm
    return Digest(algorithm, digest)


def _encode_source(source: BackupSourceDeclaration) -> str:
    identifier = _encode_reference(source.source_identifier)
    return json.dumps(
        {
            "v": 1,
            "identifier": json.loads(identifier),
            "source_digital_asset_id": source.source_digital_asset_id,
            "source_replica_id": source.source_replica_id,
            "source_store_ref": (
                None if source.source_store_ref is None else str(source.source_store_ref)
            ),
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def _decode_source(row: Any) -> BackupSourceDeclaration:
    raw = str(_value(row, "backup_workflow_source_identifier"))
    source_kind = BackupSourceKind(
        str(_value(row, "backup_workflow_source_kind"))
    )
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        payload = None
    if isinstance(payload, dict) and payload.get("v") == 1:
        identifier = _decode_reference(json.dumps(payload["identifier"]))
        asset_id = payload.get("source_digital_asset_id")
        replica_id = payload.get("source_replica_id")
        store_ref_raw = payload.get("source_store_ref")
        store_ref = None if store_ref_raw is None else UUID(str(store_ref_raw))
    else:
        identifier = raw
        asset_id = None
        replica_id = _value(row, "backup_workflow_source_asset_replica_id")
        store_ref = None
    return BackupSourceDeclaration(
        source_kind=source_kind,
        source_identifier=identifier,
        archive_path=_optional_text(
            _value(row, "backup_workflow_source_archive_path")
        ),
        expected_size=(
            None
            if _value(row, "backup_workflow_source_expected_size") in (None, "")
            else int(_value(row, "backup_workflow_source_expected_size"))
        ),
        expected_digest=_decode_digest(
            _value(row, "backup_workflow_source_expected_hash")
        ),
        source_digital_asset_id=(None if asset_id is None else int(asset_id)),
        source_replica_id=(None if replica_id is None else int(replica_id)),
        source_store_ref=store_ref,
    )


def _report_to_json(report: BackupSourceStagingReport) -> dict[str, Any]:
    return {
        "source_index": report.source_index,
        "source_identifier": json.loads(_encode_reference(report.source_identifier)),
        "archive_path": report.archive_path,
        "staged_location": (
            None
            if report.staged_location is None
            else json.loads(_encode_reference(report.staged_location))
        ),
        "bytes_staged": report.bytes_staged,
        "digest_verified": report.digest_verified,
        "ok": report.ok,
        "error": report.error,
    }


def _report_from_json(payload: dict[str, Any]) -> BackupSourceStagingReport:
    staged = payload.get("staged_location")
    staged_reference = (
        None if staged is None else _decode_reference(json.dumps(staged))
    )
    if staged_reference is not None and not isinstance(staged_reference, Location):
        raise ValueError("persisted staged_location is not a Location.")
    return BackupSourceStagingReport(
        source_index=int(payload["source_index"]),
        source_identifier=_decode_reference(json.dumps(payload["source_identifier"])),
        archive_path=str(payload["archive_path"]),
        staged_location=staged_reference,
        bytes_staged=(
            None
            if payload.get("bytes_staged") is None
            else int(payload["bytes_staged"])
        ),
        digest_verified=payload.get("digest_verified"),
        ok=bool(payload.get("ok", True)),
        error=_optional_text(payload.get("error")),
    )


__all__ = ["BackupWorkflowRepository"]
