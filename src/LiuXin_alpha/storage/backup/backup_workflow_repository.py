"""Database persistence helpers for backup workflows.

This repository keeps workflow persistence separate from raw store plugins and
from the workflow runtime object itself. It stores durable intent, designated
sources, resumable checkpoint state, and optional output records.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from typing import Any

from LiuXin_alpha.storage.api.backup_api.backup_workflow_models import (
    BackupSourceKind,
    BackupSourceResult,
    BackupSourceSpec,
    BackupWorkflowResumeState,
    BackupWorkflowSpec,
    BackupWorkflowStatus,
    BackupWorkflowStepKind,
)
from LiuXin_alpha.storage.api.backup_api.backup_workflow_rows import (
    BackupWorkflowOutputRow,
    BackupWorkflowRow,
    BackupWorkflowSourceRow,
    BackupWorkflowStateRow,
)


class BackupWorkflowRepository:
    """Persist and reload backup workflow specifications and resume state."""

    def __init__(self, db) -> None:
        self.db = db

    # ------------------------------------------------------------------
    # Spec persistence
    # ------------------------------------------------------------------
    def save_workflow_spec(
        self,
        spec: BackupWorkflowSpec,
        *,
        workflow_id: int | None = None,
        destination_store_id: int | None = None,
        staging_store_id: int | None = None,
        status: BackupWorkflowStatus = BackupWorkflowStatus.DRAFT,
        last_error: str | None = None,
    ) -> BackupWorkflowRow:
        row_dict = {
            "backup_workflow_name": spec.workflow_name,
            "backup_workflow_kind": spec.workflow_kind.value,
            "backup_workflow_destination_store_id": destination_store_id,
            "backup_workflow_staging_store_id": staging_store_id,
            "backup_workflow_output_url": spec.output_url,
            "backup_workflow_verify_after_build": 1 if spec.verify_after_build else 0,
            "backup_workflow_cleanup_staging_after_success": 1 if spec.cleanup_staging_after_success else 0,
            "backup_workflow_staging_root": spec.staging_root,
            "backup_workflow_options_json": json.dumps(dict(spec.options)),
            "backup_workflow_status": status.value,
            "backup_workflow_last_error": last_error,
        }

        if workflow_id is None:
            workflow_row = BackupWorkflowRow.from_idless_row_dict(self.db, row_dict=row_dict)
        else:
            workflow_row = BackupWorkflowRow.from_row_id(self.db, workflow_id)
            for key, value in row_dict.items():
                if key in workflow_row.allowed_columns and value is not None:
                    workflow_row[key] = value
            workflow_row.sync()
            self._delete_existing_sources(int(workflow_id))

        workflow_id = int(workflow_row.backup_workflow_id)
        for ordinal, source in enumerate(spec.sources):
            BackupWorkflowSourceRow.from_idless_row_dict(
                self.db,
                row_dict={
                    "backup_workflow_source_workflow_id": workflow_id,
                    "backup_workflow_source_ordinal": ordinal,
                    "backup_workflow_source_kind": source.source_kind.value,
                    "backup_workflow_source_identifier": source.source_identifier,
                    "backup_workflow_source_archive_path": source.archive_path or "",
                    "backup_workflow_source_expected_size": source.expected_size,
                    "backup_workflow_source_expected_hash": source.expected_hash,
                },
            )
        return workflow_row

    def load_workflow_spec(self, workflow_id: int) -> BackupWorkflowSpec:
        workflow_row = BackupWorkflowRow.from_row_id(self.db, workflow_id, read_only=True)
        source_rows = sorted(
            self.db.search("backup_workflow_sources", "backup_workflow_source_workflow_id", int(workflow_id)),
            key=lambda row: int(row["backup_workflow_source_ordinal"]),
        )
        options_raw = workflow_row["backup_workflow_options_json"]
        options_map = json.loads(str(options_raw)) if options_raw not in (None, "") else {}
        return BackupWorkflowSpec(
            workflow_name=str(workflow_row["backup_workflow_name"]),
            workflow_kind=self._parse_workflow_kind(workflow_row["backup_workflow_kind"]),
            output_url=str(workflow_row["backup_workflow_output_url"]),
            sources=tuple(self._source_spec_from_row(row) for row in source_rows),
            verify_after_build=bool(workflow_row["backup_workflow_verify_after_build"]),
            cleanup_staging_after_success=bool(workflow_row["backup_workflow_cleanup_staging_after_success"]),
            staging_root=self._coerce_optional_str(workflow_row["backup_workflow_staging_root"]),
            options=tuple((str(k), str(v)) for k, v in dict(options_map).items()),
        )

    # ------------------------------------------------------------------
    # Resume-state persistence
    # ------------------------------------------------------------------
    def save_resume_state(
        self,
        workflow_id: int,
        resume_state: BackupWorkflowResumeState,
    ) -> BackupWorkflowStateRow:
        existing = self.db.search("backup_workflow_state", "backup_workflow_state_workflow_id", int(workflow_id))
        row_dict = {
            "backup_workflow_state_workflow_id": int(workflow_id),
            "backup_workflow_state_status": resume_state.status.value,
            "backup_workflow_state_next_source_index": int(resume_state.next_source_index),
            "backup_workflow_state_staged_source_count": int(resume_state.staged_source_count),
            "backup_workflow_state_completed_steps_json": json.dumps([step.value for step in resume_state.completed_steps]),
            "backup_workflow_state_source_results_json": json.dumps([self._source_result_to_jsonable(item) for item in resume_state.source_results]),
            "backup_workflow_state_output_artifact_url": resume_state.output_artifact_url,
            "backup_workflow_state_last_error": resume_state.last_error,
        }
        if existing:
            state_row = BackupWorkflowStateRow.from_row_id(self.db, int(existing[0]["backup_workflow_state_id"]))
            for key, value in row_dict.items():
                if key in state_row.allowed_columns:
                    state_row[key] = value
            state_row.sync()
            return state_row
        return BackupWorkflowStateRow.from_idless_row_dict(self.db, row_dict=row_dict)

    def load_resume_state(self, workflow_id: int) -> BackupWorkflowResumeState:
        spec = self.load_workflow_spec(int(workflow_id))
        rows = self.db.search("backup_workflow_state", "backup_workflow_state_workflow_id", int(workflow_id))
        if not rows:
            return BackupWorkflowResumeState(spec=spec, status=BackupWorkflowStatus.DRAFT)
        row = rows[0]
        steps_raw = row["backup_workflow_state_completed_steps_json"]
        results_raw = row["backup_workflow_state_source_results_json"]
        steps = []
        if steps_raw not in (None, ""):
            steps = [BackupWorkflowStepKind(str(item)) for item in json.loads(str(steps_raw))]
        results = []
        if results_raw not in (None, ""):
            results = [self._source_result_from_jsonable(item) for item in json.loads(str(results_raw))]
        return BackupWorkflowResumeState(
            spec=spec,
            status=BackupWorkflowStatus(str(row["backup_workflow_state_status"])),
            next_source_index=int(row["backup_workflow_state_next_source_index"] or 0),
            staged_source_count=int(row["backup_workflow_state_staged_source_count"] or 0),
            source_results=tuple(results),
            completed_steps=tuple(steps),
            output_artifact_url=self._coerce_optional_str(row["backup_workflow_state_output_artifact_url"]),
            last_error=self._coerce_optional_str(row["backup_workflow_state_last_error"]),
        )

    # ------------------------------------------------------------------
    # Output persistence
    # ------------------------------------------------------------------
    def record_output(
        self,
        workflow_id: int,
        *,
        output_url: str,
        output_digital_asset_id: int | None = None,
        output_asset_replica_id: int | None = None,
        output_store_id: int | None = None,
        verified_ok: bool | None = None,
    ) -> BackupWorkflowOutputRow:
        return BackupWorkflowOutputRow.from_idless_row_dict(
            self.db,
            row_dict={
                "backup_workflow_output_workflow_id": int(workflow_id),
                "backup_workflow_output_url": str(output_url),
                "backup_workflow_output_digital_asset_id": output_digital_asset_id,
                "backup_workflow_output_asset_replica_id": output_asset_replica_id,
                "backup_workflow_output_store_id": output_store_id,
                "backup_workflow_output_verified_ok": None if verified_ok is None else (1 if verified_ok else 0),
            },
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _delete_existing_sources(self, workflow_id: int) -> None:
        for row in self.db.search("backup_workflow_sources", "backup_workflow_source_workflow_id", int(workflow_id)):
            self.db.delete(row)

    def _source_spec_from_row(self, row: Any) -> BackupSourceSpec:
        return BackupSourceSpec(
            source_kind=BackupSourceKind(str(row["backup_workflow_source_kind"])),
            source_identifier=str(row["backup_workflow_source_identifier"]),
            archive_path=self._coerce_optional_str(row["backup_workflow_source_archive_path"]),
            expected_size=self._coerce_optional_int(row["backup_workflow_source_expected_size"]),
            expected_hash=self._coerce_optional_str(row["backup_workflow_source_expected_hash"]),
        )

    def _source_result_to_jsonable(self, result: BackupSourceResult) -> dict[str, Any]:
        return {
            "source_index": int(result.source_index),
            "source_identifier": result.source_identifier,
            "archive_path": result.archive_path,
            "staged_location_url": result.staged_location_url,
            "ok": bool(result.ok),
            "error": result.error,
        }

    def _source_result_from_jsonable(self, payload: dict[str, Any]) -> BackupSourceResult:
        return BackupSourceResult(
            source_index=int(payload.get("source_index", 0)),
            source_identifier=str(payload.get("source_identifier", "")),
            archive_path=str(payload.get("archive_path", "")),
            staged_location_url=self._coerce_optional_str(payload.get("staged_location_url")),
            ok=bool(payload.get("ok", True)),
            error=self._coerce_optional_str(payload.get("error")),
        )

    @staticmethod
    def _parse_workflow_kind(value: Any):
        from LiuXin_alpha.storage.api.backup_api.backup_workflow_models import BackupWorkflowKind
        return BackupWorkflowKind(str(value))

    @staticmethod
    def _coerce_optional_str(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value)
        return text if text != "" else None

    @staticmethod
    def _coerce_optional_int(value: Any) -> int | None:
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None


__all__ = ["BackupWorkflowRepository"]
