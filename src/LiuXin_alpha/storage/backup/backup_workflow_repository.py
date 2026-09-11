"""
Persist backup workflow declarations, checkpoints, outcomes, and source presence.

Domain values are encoded into workflow/source/state/output/link rows through a borrowed
database. Multi-row writes have no transaction boundary of their own. Private codecs retain
versioned routed references and selected historical local-path/digest fallbacks without
claiming fresh physical verification or complete consistency among persisted evidence.
"""

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
    """
    Persist backup intent and evidence through database rows and private reference envelopes.

    The repository borrows a database exposing Row-compatible mutation, lookup, and enumeration
    operations. Declarations, checkpoints, results, and presence records remain separate writes.
    Methods do not open an enclosing transaction, lock competing writers, run a workflow, or verify
    physical bytes. Multi-row failures can leave earlier changes visible or committed according to
    the adapter.

    Versioned source/reference JSON preserves managed Locations and catalogue provenance while
    scalar columns retain source ordering, member names, and expected byte evidence. Historical
    local-path and digest spellings have explicit decode fallbacks. Returned values validate
    selected structure rather than proving that stored metadata or physical state is mutually
    consistent.

    Example:
        >>> repository = BackupWorkflowRepository(db)  # doctest: +SKIP
        >>> workflow_id = repository.save_workflow_declaration(declaration)  # doctest: +SKIP
        >>> repository.save_checkpoint(workflow_id, checkpoint)  # doctest: +SKIP
    """

    def __init__(self, db) -> None:
        """
        Retain the database adapter without opening it or taking ownership.

        Runtime conformance, schema readiness, transaction policy, and database lifetime remain the
        caller's responsibilities.

        Example:
            >>> repository = BackupWorkflowRepository(db)  # doctest: +SKIP


        :param db: Borrowed database supporting the workflow tables and Row-compatible persistence operations.
        :return: None after storing the original database reference.
        """
        self.db = db

    def save_workflow_declaration(
        self,
        declaration: BackupWorkflowDeclaration,
        *,
        workflow_id: WorkflowID | None = None,
        status: WorkflowStatus = WorkflowStatus.DRAFT,
    ) -> WorkflowID:
        """
        Create intent or replace an existing workflow row and its ordered source rows.

        Require BackupWorkflowDeclaration and coerce status through WorkflowStatus. Encode
        targets/options before writes. A new row allocates the ID; a supplied ID is int-converted,
        looked up, and updated before its old source rows are deleted. Then insert sources in
        declaration order with zero-based ordinals, typed identifier/provenance envelopes, and
        scalar path/size/digest evidence.

        Reset the workflow row's last error and status, but retain any prior checkpoint, output, and
        presence rows. Replacement can therefore leave incompatible older execution state. A later
        source or adapter failure can follow earlier row updates/deletions; no transaction or
        compensating restore is established here.

        Example:
            >>> workflow_id = repository.save_workflow_declaration(  # doctest: +SKIP
            ...     declaration, status=WorkflowStatus.DRAFT,
            ... )


        :param declaration: Validated backup intent whose targets, options, and ordered sources are encoded.
        :param workflow_id: None to allocate a new row, or an int-convertible existing workflow ID to replace.
        :param status: Enum member or value written to the workflow row; defaults to DRAFT.
        :return: Integer ID of the created/replaced workflow; missing replacement rows and persistence/encoding failures raise.
        """
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
        """
        Reconstruct intent with sources sorted by their persisted ordinal.

        Int-convert the ID and require its workflow row. Missing/falsey ordinals sort as zero;
        decode source envelopes and scalar expectations. Empty options become an empty mapping;
        other JSON must support items(). Persisted flags use Python truthiness, so nonempty text
        such as "0" is true. Targets and optional staging use the reference decoder, then the
        declaration canonicalizes options and validates selected names/paths.

        No transaction joins these reads, and decoding/construction errors propagate rather than
        producing incomplete intent.

        Example:
            >>> declaration = repository.load_workflow_declaration(3)  # doctest: +SKIP


        :param workflow_id: Int-convertible identifier whose workflow and source rows are read.
        :return: BackupWorkflowDeclaration reconstructed from current persisted rows; an absent workflow raises KeyError.
        """
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
        """
        Enumerate workflow IDs in numeric order and reconstruct each selected declaration.

        An omitted status uses the adapter's full-row enumeration; otherwise coerce the status and
        search its stored value. Materialize/sort workflow rows before yielding, but load each
        declaration at iteration time. Later reads can fail after earlier yields and do not form a
        pinned snapshot.

        Example:
            >>> drafts = list(repository.iter_workflow_declarations(  # doctest: +SKIP
            ...     status=WorkflowStatus.DRAFT,
            ... ))


        :param status: Optional lifecycle filter; None selects every workflow row.
        :return: Iterator of (integer workflow ID, reconstructed declaration) pairs in numeric-ID order.
        """
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
        """
        Write checkpoint state, then update the workflow row's status and error.

        Int-convert the target ID. Require a matching optional checkpoint ID and declaration equal
        to current durable intent before writes. Encode reports, milestones, and output, then
        replace the first matching state row or insert one. Only afterward update workflow
        status/error; a later failure can leave state and workflow rows disagreeing.

        There is no revision comparison or enclosing transaction. updated_at is not encoded, and
        checkpoint construction's selected validation does not establish agreement among reports,
        counters, and physical staging.

        Example:
            >>> repository.save_checkpoint(3, checkpoint)  # doctest: +SKIP


        :param workflow_id: Existing int-convertible workflow ID whose state is replaced.
        :param checkpoint: Checkpoint with matching declaration and either no workflow ID or the same ID.
        :return: None after state and status writes; mismatches raise StorePreconditionFailed and persistence errors propagate.
        """
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
        """
        Reconstruct state using current intent and the first matching checkpoint row.

        If no state row exists, use the workflow row's status/error with default counters and empty
        evidence, rather than forcing DRAFT. Existing state decodes report/milestone JSON,
        int-converts falsey counters through zero, and normalizes empty output/error values.
        updated_at remains None. Replacing intent without replacing state can make the resulting
        checkpoint fail value validation.

        Example:
            >>> checkpoint = repository.load_checkpoint(3)  # doctest: +SKIP


        :param workflow_id: Int-convertible workflow ID whose intent and stored evidence are reconstructed.
        :return: BackupWorkflowCheckpoint without a restored timestamp; absent intent or malformed/inconsistent rows raise.
        """
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
        """
        Persist a terminal result's selected checkpoint and any non-None output reference.

        Require a compatible optional result ID. Use a truthy final_checkpoint as supplied;
        otherwise synthesize one with the cursor at the source count and staged count summed from
        report.ok. Checkpoint persistence performs its own intent/ID checks but does not reconcile a
        supplied checkpoint with every result field.

        After saving state, a None result output returns without deleting an older output row.
        Otherwise update the last matching output row or insert one, recording
        int(result.successful) as verified_ok without reading bytes. An output failure can follow a
        completed checkpoint/status write; Store registration and presence links are separate
        operations.

        Example:
            >>> repository.record_result(3, result)  # doctest: +SKIP


        :param workflow_id: Int-convertible existing workflow identifier associated with the result.
        :param result: Terminal outcome supplying checkpoint evidence and optional output metadata.
        :return: None after checkpoint persistence and any output-row update; partial writes may remain on failure.
        """
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
        """
        Insert source-presence evidence unless this Store already has the exact member key.

        Int-convert workflow_id and look up the registration's Store UUID. Compare existing member
        text to archive_path exactly; the first match returns False without updating source,
        workflow, artifact, or protection fields. Otherwise encode source provenance and artifact
        reference, insert packed_presence metadata, and int-convert protection flags.

        No path normalization, member read, registration/workflow-ID agreement check, concurrency
        lock, or enclosing transaction occurs. Physical archive write protection is separate from
        the stored metadata flags.

        Example:
            >>> created = repository.record_backup_presence(  # doctest: +SKIP
            ...     3, registration, source, archive_path="books/a.epub",
            ... )


        :param workflow_id: Int-convertible workflow ID recorded on a newly inserted link.
        :param registration: Backup Store UUID and image reference used for the presence association.
        :param source: Source identifier/provenance to encode without verifying physical bytes.
        :param archive_path: Exact archive member key, retained without normalization.
        :param protected: Flag converted to int for the metadata deletion-protection field.
        :param immutable: Flag converted to int for the metadata mutation-protection field.
        :return: True after inserting a link; false for an existing Store/member key; lookup/schema failures propagate.
        """
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
        """
        Delete an existing workflow row subject to lifecycle and database constraints.

        Int-convert the ID, return False when absent, and decode stored status even when
        require_terminal is false. The default check accepts FAILED as terminal. Delegate deletion
        to the database; associated-row cascades and protected-link errors follow its schema.
        Physical images and configured Stores are not explicitly removed here.

        Example:
            >>> removed = repository.delete_workflow(3)  # doctest: +SKIP


        :param workflow_id: Int-convertible workflow row identifier to remove.
        :param require_terminal: Require terminal stored status before deletion; false bypasses that lifecycle test only.
        :return: True after database deletion, false when absent; a nonterminal default request raises StorePreconditionFailed.
        """
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
        """
        Delete every source row returned for one workflow, in search order.

        Each deletion is independent at this layer. A later adapter error leaves earlier deletions
        applied according to database transaction policy; no replacement or rollback occurs here.

        Example:
            >>> repository._delete_sources(3)  # doctest: +SKIP


        :param workflow_id: Workflow identifier used to search its source rows.
        :return: None after all returned source rows are deleted.
        """
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
        """
        Require the workflow row and write only its lifecycle value and error text.

        The existing row is updated through the shared Row/mapping helper. Checkpoint rows are not
        touched, and there is no cross-row consistency check or transaction.

        Example:
            >>> repository._update_workflow_status(3, WorkflowStatus.FAILED, "copy failed")  # doctest: +SKIP


        :param workflow_id: Workflow ID whose main row must exist.
        :param status: WorkflowStatus whose value is written to the row.
        :param last_error: Error text or None to retain without normalization.
        :return: None after the row update; lookup, attribute, or persistence errors propagate.
        """
        self._update_row(
            self._require_row("backup_workflows", workflow_id),
            {
                "backup_workflow_status": status.value,
                "backup_workflow_last_error": last_error,
            },
        )

    def _store_id_for_uuid(self, store_uuid: UUID) -> int:
        """
        Resolve the first Store row matching a stringified UUID to its integer database ID.

        UUID syntax and uniqueness are not validated here. Search order chooses among duplicate
        matches; an absent match raises KeyError.

        Example:
            >>> store_id = repository._store_id_for_uuid(UUID(int=1))  # doctest: +SKIP


        :param store_uuid: Public Store UUID stringified for the database search.
        :return: Int-converted store_id from the first matching row.
        """
        rows = self.db.search("stores", "store_uuid", str(store_uuid))
        if not rows:
            raise KeyError(f"No database Store row for UUID {store_uuid}.")
        return int(_value(rows[0], "store_id"))

    def _require_row(self, table: str, row_id: int):
        """
        Fetch a row by an int-converted ID and reject absence with a table-specific KeyError.

        Return the adapter's original row object without copying or checking fields. ID conversion
        is permissive int conversion, not a positive-integer validator.

        Example:
            >>> row = repository._require_row("backup_workflows", 3)  # doctest: +SKIP


        :param table: Internal database table name passed to row lookup.
        :param row_id: Value converted to int before lookup.
        :return: Original database row, or KeyError when lookup returns None.
        """
        row = self.db.get_row_from_id(table, int(row_id))
        if row is None:
            raise KeyError(f"Unknown {table} row id: {row_id}.")
        return row

    def _update_row(self, row, values: dict[str, Any]) -> None:
        """
        Apply changes through a Row's allowed columns or a copied mapping's driver update.

        Presence of allowed_columns selects attribute-based handling: assign only admitted keys and
        call sync once even if nothing changed. Otherwise copy row into a dict, merge all values,
        and delegate to driver_wrapper.update_row. Attribute/callable conformance is not separately
        checked, and partial in-memory mutation or adapter writes can precede errors.

        Example:
            >>> repository._update_row(row, {"backup_workflow_last_error": None})  # doctest: +SKIP


        :param row: Mutable Row-like object with allowed_columns/sync, or a mapping accepted by the driver wrapper.
        :param values: Field changes; unknown columns are skipped only for the Row-like branch.
        :return: None after the selected persistence call returns.
        """
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
        """
        Materialize a table using the first available database enumeration interface.

        Prefer driver_wrapper.read, then get_all_rows(iterator_return=False), then a connection
        SELECT using reported column order and strict zip. Attribute presence selects a branch
        without callability checks. The SQL fallback interpolates an internal table name; row
        ordering is left to the adapter and all results are collected in memory.

        Example:
            >>> rows = repository._all_rows("backup_workflows")  # doctest: +SKIP


        :param table: Trusted internal schema table name requested from the adapter or interpolated into fallback SQL.
        :return: List of adapter rows or reconstructed dictionaries; no usable connection fallback raises TypeError.
        """
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
    """
    Return a row field through direct subscription without defaults or error translation.

    Example:
        >>> _value({"count": 3}, "count")
        3


    :param row: Row or mapping supporting subscription by column name.
    :param key: Exact field key to request.
    :return: Original field value; missing-key and adapter errors propagate.
    """
    return row[key]


def _optional_text(value: Any) -> str | None:
    """
    Normalize None and empty text to None, stringifying other values unchanged.

    Whitespace is retained; numeric zero and false become their ordinary text spellings. Equality
    with the two empty sentinels determines normalization.

    Example:
        >>> _optional_text(0)
        '0'
        >>> _optional_text("") is None
        True


    :param value: Persisted optional value to normalize at reconstruction.
    :return: None for None/empty text, otherwise str(value).
    """
    return None if value in (None, "") else str(value)


def _encode_reference(reference: str | Location) -> str:
    """
    Encode text or a routed Location in a compact sorted version-1 JSON envelope.

    Strings are retained as text values without URI parsing or path validation. Non-string inputs
    provide store_ref and key attributes, with only the Store reference stringified. JSON
    serialization and attribute errors propagate.

    Example:
        >>> _decode_reference(_encode_reference(Location(UUID(int=1), "book")))
        Location(store_ref=UUID('00000000-0000-0000-0000-000000000001'), key='book')


    :param reference: Local/reference text or Location whose route should survive persistence.
    :return: Compact JSON string identifying text or a Store UUID/key route.
    """
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
    """
    Decode a recognized reference envelope while preserving historical unwrapped input.

    TypeError/JSON syntax failures, non-object JSON, and versions unequal to 1 return the original
    input without coercion. Recognized text stringifies its value; recognized locations construct
    UUID and Location values. Unknown kinds in a version-1 object raise ValueError, and malformed
    required fields propagate. The version comparison uses equality, so True also compares as
    version 1.

    Example:
        >>> _decode_reference("/backups/old.sqsh")
        '/backups/old.sqsh'
        >>> _decode_reference(_encode_reference("nightly.sqsh"))
        'nightly.sqsh'


    :param encoded: Persisted reference envelope or historical raw local-path text.
    :return: Decoded text/Location, or the original unrecognized input; malformed recognized envelopes raise.
    """
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
    """
    Join a digest's current algorithm and value with a colon without additional validation.

    Example:
        >>> _encode_digest(Digest("sha256", "ab"))
        'sha256:ab'


    :param digest: Digest supplying algorithm and value attributes.
    :return: Algorithm-prefixed digest text suitable for the scalar expectation column.
    """
    return f"{digest.algorithm}:{digest.value}"


def _decode_digest(value: Any) -> Digest | None:
    """
    Decode an optional algorithm-prefixed digest or historical bare SHA-256 spelling.

    None/empty text return None. Stringify other input and split at the first colon; when absent,
    use sha256 as the algorithm. Digest construction supplies normalization and selected validation,
    without checking bytes or the historical algorithm assumption.

    Example:
        >>> _decode_digest("abcd")
        Digest(algorithm='sha256', value='abcd')
        >>> _decode_digest(None) is None
        True


    :param value: Optional persisted digest spelling, including historical bare hash text.
    :return: Constructed Digest or None for an empty sentinel; invalid digest fields propagate.
    """
    if value in (None, ""):
        return None
    algorithm, separator, digest = str(value).partition(":")
    if not separator:
        # Historical rows implicitly meant SHA-256.
        algorithm, digest = "sha256", algorithm
    return Digest(algorithm, digest)


def _encode_source(source: BackupSourceDeclaration) -> str:
    """
    Encode a source's identifier and catalogue provenance in a version-1 JSON object.

    Nest the parsed reference envelope and retain Asset/Replica IDs and optional stringified Store
    UUID. Source kind, archive path, size, and digest remain separate scalar source-row columns
    rather than fields in this envelope. No catalogue or byte validation occurs.

    Example:
        >>> source = BackupSourceDeclaration(BackupSourceKind.LOCAL_PATH, "/books/a")
        >>> json.loads(_encode_source(source))["identifier"]["kind"]
        'text'


    :param source: Source declaration whose identifier and provenance are persisted.
    :return: Compact key-sorted JSON string containing identifier and optional catalogue references.
    """
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
    """
    Reconstruct one source from its identifier envelope and scalar row evidence.

    Coerce the stored kind through BackupSourceKind. A version-1 source object must contain its
    nested identifier; obtain provenance from that object, ignoring the legacy Replica scalar. For
    unrecognized/non-JSON identifier text, retain the raw text, omit Asset/Store provenance, and use
    the legacy Replica column. A historical raw string cannot satisfy a STORE_LOCATION designation's
    required Location type.

    Int-convert optional sizes and IDs, decode expected digest, normalize optional member text, then
    let BackupSourceDeclaration validate its selected kind/path/reference constraints. Missing
    fields, malformed recognized envelopes, and value errors propagate.

    Example:
        >>> source = _decode_source(source_row)  # doctest: +SKIP


    :param row: Source row exposing identifier, kind, member path, expectations, and legacy Replica columns.
    :return: BackupSourceDeclaration reconstructed from envelope/scalar evidence without physical inspection.
    """
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
    """
    Project a staging report into JSON-compatible fields and nested reference envelopes.

    Preserve report counters, verification/success flags, and error without coercion. Encode the
    original source identifier and optional staged Location as parsed reference objects. This
    returns a mapping; serialization into checkpoint JSON occurs at the caller.

    Example:
        >>> report = BackupSourceStagingReport(0, "/books/a", "a", bytes_staged=4)
        >>> _report_to_json(report)["bytes_staged"]
        4


    :param report: Staging evidence value to project into the checkpoint payload.
    :return: New dictionary retaining report fields and nested encoded reference objects.
    """
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
    """
    Decode one staging report and require any staged reference to be a Location.

    Decode nested reference objects, int-convert index and optional byte count, stringify the
    archive path, and retain digest_verified unchanged. Missing ok defaults to True; supplied values
    use bool, so nonempty text such as "false" is true. Empty error text becomes None. Report
    construction then checks selected counters/path/error consistency; no declaration or staged
    bytes are consulted.

    Example:
        >>> report = BackupSourceStagingReport(0, "/books/a", "a", bytes_staged=4)
        >>> _report_from_json(_report_to_json(report)) == report
        True


    :param payload: Decoded checkpoint mapping for one source report.
    :return: BackupSourceStagingReport, or an error for malformed fields, a non-Location staged reference, or inconsistent report values.
    """
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
