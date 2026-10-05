"""
Execute SquashFS publication jobs and project stored file-derivation edges through Core.

Publication delegates archive work to named Core start commands and waits without
a CLI deadline. This runner differs from common job helpers: state matching is
exact and it requires a successful mapping-shaped execution result. Provenance
reads legacy derivation metadata and file rows; it does not inspect archive bytes,
validate lineage, or infer derivations from replicas. Output uses direct print,
not the common buffered/no-clobber file publisher.
"""

from __future__ import annotations

import argparse
import json
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any, TypedDict

from LiuXin_alpha.core import CoreClientAPI
from LiuXin_alpha.surfaces.core import (
    CoreRow,
    CoreSurfaceModel,
    open_surface_core_from_args,
)


class _ProvenanceFile(TypedDict):
    """
    Describe six file values included at either endpoint of a provenance edge.

    Only file_id is integer-coerced by the projector. Other values, including
    None, remain unchanged. This TypedDict is a static contract, not a runtime
    validator or complete file-record representation.

    Example:
        >>> file = _ProvenanceFile(file_id=1, file_store_id=None, file_storage_key=None,
        ...     file_name=None, file_size_bytes=None, file_hash_sha256=None)
        >>> file['file_name'] is None
        True


    :ivar file_id: Integer legacy files-row identity.
    :ivar file_store_id: Original store reference, possibly None.
    :ivar file_storage_key: Unmodified backend-relative key metadata.
    :ivar file_name: Original display filename, including None when unset.
    :ivar file_size_bytes: Original size metadata without numeric coercion.
    :ivar file_hash_sha256: Stored digest metadata, not a newly computed hash.
    """

    file_id: int
    file_store_id: object
    file_storage_key: object
    file_name: object
    file_size_bytes: object
    file_hash_sha256: object


class _ProvenanceEdge(TypedDict):
    """
    Describe one stored derivation and its resolved parent/child file projections.

    Kind and note remain original metadata, including None. The type does not
    enforce distinct endpoints, valid lineage, or semantic derivation kinds.

    Example:
        >>> edge = _ProvenanceEdge(file_derivation_id=1, kind=None, note=None,
        ...     parent_file=parent, child_file=child)  # doctest: +SKIP


    :ivar file_derivation_id: Integer identity of the stored derivation row.
    :ivar kind: Unmodified derivation-kind metadata.
    :ivar note: Unmodified derivation-note metadata.
    :ivar parent_file: Selected metadata from the resolved source file row.
    :ivar child_file: Selected metadata from the resolved derived file row.
    """

    file_derivation_id: int
    kind: object
    note: object
    parent_file: _ProvenanceFile
    child_file: _ProvenanceFile


class _ProvenanceQuery(TypedDict):
    """
    Echo the optional integer file/store selectors used for provenance filtering.

    The payload builder requires at least one selector, but constructing this
    TypedDict alone does not validate that rule or referenced-row existence.

    Example:
        >>> _ProvenanceQuery(store_id=7, file_id=None)
        {'store_id': 7, 'file_id': None}


    :ivar store_id: Required store at either endpoint, or None to omit this condition.
    :ivar file_id: Required file at either endpoint, or None to omit this condition.
    """

    store_id: int | None
    file_id: int | None


class _ProvenancePayload(TypedDict):
    """
    Describe a provenance query and its ordered, filtered derivation-edge list.

    The builder sets edge_count to the list length; the TypedDict itself does
    not enforce consistency, sorting, uniqueness, or a transaction snapshot.

    Example:
        >>> payload = _ProvenancePayload(query={'store_id': 7, 'file_id': None}, edge_count=0, edges=[])
        >>> payload['edge_count']
        0


    :ivar query: Echoed file/store selectors, integer-coerced when present.
    :ivar edge_count: Number of returned edges after endpoint resolution and filtering.
    :ivar edges: Selected edges in the model's original derivation order.
    """

    query: _ProvenanceQuery
    edge_count: int
    edges: list[_ProvenanceEdge]


def _print_publish_report(report: Mapping[str, Any], *, as_json: bool) -> None:
    """
    Print the complete report as Unicode JSON or a compact human-readable summary.

    JSON shallow-copies the outer mapping, sorts keys, and uses two-space
    indentation with the standard encoder's nonfinite-number behavior. Text
    mode prints counts and lengths of error/mismatch collections, not their
    contents, then an optional duration rounded to three decimals. Missing
    fields use defaults; malformed values can raise after earlier lines print.
    Output is not staged or rolled back.

    Example:
        >>> _print_publish_report({'verified_files': 1}, as_json=True)
        {
          "verified_files": 1
        }


    :param report: Publication mapping containing optional store, count, error, and duration fields.
    :param as_json: Emit the whole report as JSON when true, otherwise selected summary lines.
    :return: None after printing to current stdout.
    """
    if as_json:
        print(json.dumps(dict(report), ensure_ascii=False, indent=2, sort_keys=True))
        return

    print(
        "Store: {} ({})".format(
            report.get("store_name", ""), report.get("store_root_uri", "")
        )
    )
    print("Store row id: {}".format(report.get("store_row_id", "")))
    print("Designated files: {}".format(report.get("designated_files", 0)))
    print("Packed files: {}".format(report.get("packed_files", 0)))
    print("Verified files: {}".format(report.get("verified_files", 0)))
    print("Duplicated files: {}".format(report.get("duplicated_files", 0)))
    print(
        "Skipped existing duplicates: {}".format(
            report.get("skipped_existing_duplicates", 0)
        )
    )
    print("Hash mismatches: {}".format(len(report.get("hash_mismatches", ()) or ())))
    print("Errors: {}".format(len(report.get("errors", ()) or ())))
    duration_seconds = report.get("duration_seconds")
    if duration_seconds is not None:
        print(f"Duration (seconds): {float(duration_seconds):.3f}")


def _run_job(
    core: CoreClientAPI, operation: str, payload: Mapping[str, Any]
) -> dict[str, Any]:
    """
    Submit a publication job, poll exact terminal states, and unwrap its successful result.

    Shallow-copy the request payload and require an object submission with a
    truthy stringified job ID; whitespace IDs are not stripped. Validate each
    jobs.get envelope and nested job mapping, sleeping 0.1 seconds between
    nonterminal states. State spelling is case-sensitive and untrimmed. There
    is no CLI wait deadline, detach option, or cancellation on interruption.

    Once terminal, fetch jobs.result with timeout_s=0.0, coerce its execution
    value to dict, and require truthy ok plus a mapping result. Malformed final
    envelopes can raise ordinary attribute/conversion errors. Query errors and
    interruptions propagate; accepted jobs and earlier writes are not undone.

    Example:
        >>> report = _run_job(core, 'backup.squashfs.publish-store.start', {'store_id': 7})  # doctest: +SKIP


    :param core: Client providing the named start command and jobs.get/result queries.
    :param operation: Publication start operation forwarded without allowlist validation here.
    :param payload: Command data shallow-copied before submission.
    :return: New outer dictionary containing the successful execution's result mapping.
    :raises RuntimeError: Required submission/state/result shape is absent or execution is not successful.
    """
    submitted = core.command(operation, dict(payload))
    if not isinstance(submitted, Mapping):
        raise RuntimeError("Core job submission did not return an object.")
    job_id = str(submitted.get("job_id") or "")
    if not job_id:
        raise RuntimeError("Core job submission did not return a job id.")

    while True:
        job = core.query("jobs.get", {"job_id": job_id})
        if not isinstance(job, Mapping):
            raise RuntimeError("Core jobs.get did not return an object.")
        job_info = job.get("job")
        if not isinstance(job_info, Mapping):
            raise RuntimeError("Core jobs.get did not return job details.")
        if str(job_info.get("state") or "") in {
            "succeeded",
            "failed",
            "cancelled",
            "timed_out",
            "aborted",
        }:
            break
        time.sleep(0.1)

    completed = core.query(
        "jobs.result",
        {"job_id": job_id, "timeout_s": 0.0},
    )
    execution = dict(completed.get("execution", {}) or {})
    if not bool(execution.get("ok", False)):
        raise RuntimeError(str(execution.get("traceback") or "Core job failed."))
    result = execution.get("result")
    if not isinstance(result, Mapping):
        raise RuntimeError("Core job result did not return an object.")
    return dict(result)


def _collect_file_ids(args: argparse.Namespace) -> list[int]:
    """
    Combine explicit and file-supplied IDs, keeping the first occurrence of each integer.

    Process file_id values first, then read the complete optional UTF-8 ID file
    on the CLI host after user-prefix expansion. Ignore blank lines and lines
    whose stripped text starts with #; inline comments are not supported. No
    size limit, positive-ID check, or row-existence query is applied.

    Example:
        >>> _collect_file_ids(argparse.Namespace(file_id=[3, '2', 3, -1]))
        [3, 2, -1]


    :param args: Namespace with file_id values and optional file_ids_file path.
    :return: Integer IDs in first-seen order, possibly empty.
    :raises ValueError: An explicit value or noncomment file line cannot convert to int.
    """
    file_ids: list[int] = []
    for value in args.file_id or []:
        file_ids.append(int(value))

    file_ids_file = getattr(args, "file_ids_file", None)
    if file_ids_file:
        raw_lines = (
            Path(file_ids_file).expanduser().read_text(encoding="utf-8").splitlines()
        )
        for raw in raw_lines:
            text = raw.strip()
            if not text or text.startswith("#"):
                continue
            file_ids.append(int(text))

    deduped: list[int] = []
    seen: set[int] = set()
    for file_id in file_ids:
        if file_id in seen:
            continue
        seen.add(file_id)
        deduped.append(file_id)
    return deduped


def cmd_publish_store(args: argparse.Namespace) -> int:
    """
    Submit a designated-store publication job and print its report after session exit.

    Forward archive, codec, determinism, overwrite, duplication, strictness, and
    refresh settings to Core; this adapter does not implement those guarantees.
    The dedicated runner waits without a local timeout. Print a successful
    execution's report before interpreting report errors, so status two can
    accompany completed publication or a partially successful report. Exceptions
    propagate to the application dispatcher without adapter-level rollback.

    Example:
        >>> status = cmd_publish_store(args)  # doctest: +SKIP


    :param args: Publish-store namespace with Core selectors, store/options, and output/error flags.
    :return: Two for truthy report errors when strict or fail_on_report_errors is enabled; otherwise zero.
    """
    with open_surface_core_from_args(args) as session:
        report = _run_job(
            session.client,
            "backup.squashfs.publish-store.start",
            {
                "store_id": int(args.store_id),
                "output_archive": args.output_archive,
                "compression": args.compression,
                "deterministic": bool(args.deterministic),
                "force": bool(args.force),
                "duplicate_verified_files": bool(args.duplicate_verified_files),
                "strict": bool(args.strict),
                "refresh_storage_manager": not bool(args.no_refresh_storage_manager),
            },
        )

    _print_publish_report(report, as_json=bool(args.json))
    if (args.fail_on_report_errors or args.strict) and report.get("errors"):
        return 2
    return 0


def cmd_publish_from_ids(args: argparse.Namespace) -> int:
    """
    Collect source IDs before opening Core, then designate/publish them through a job.

    The ID-list file is read on the CLI host; archive and store-name values are
    forwarded to Core without local resolution. Publication guarantees belong
    to Core, not this adapter. Wait without a local deadline, exit the session,
    print the report, then apply strict/report-error status policy. No adapter
    rollback is attempted for failures or report errors.

    Example:
        >>> status = cmd_publish_from_ids(args)  # doctest: +SKIP


    :param args: Publish-from-ids namespace with ID sources, Core selectors, publication and report options.
    :return: Two for truthy report errors under strict/fail_on_report_errors policy; otherwise zero.
    :raises ValueError: No IDs remain after collection, or an ID cannot convert to int.
    """
    file_ids = _collect_file_ids(args)
    if not file_ids:
        raise ValueError("No file ids supplied. Use --file-id and/or --file-ids-file.")

    with open_surface_core_from_args(args) as session:
        report = _run_job(
            session.client,
            "backup.squashfs.publish-files.start",
            {
                "file_ids": file_ids,
                "archive": args.archive,
                "store_name": args.store_name,
                "compression": args.compression,
                "deterministic": bool(args.deterministic),
                "force": bool(args.force),
                "strict": bool(args.strict),
                "refresh_storage_manager": not bool(args.no_refresh_storage_manager),
            },
        )

    _print_publish_report(report, as_json=bool(args.json))
    if (args.fail_on_report_errors or args.strict) and report.get("errors"):
        return 2
    return 0


def _file_payload(file_row: CoreRow) -> _ProvenanceFile:
    """
    Project six legacy file fields without normalizing optional metadata.

    Coerce only file_id to int. All six column keys must exist, even when their
    values are None. Other values are retained by reference; no byte reads,
    hash validation, or store resolution occurs here.

    Example:
        >>> row = CoreRow(table='files', row_id=1, values={'file_id': '1', 'file_store_id': None,
        ...     'file_storage_key': None, 'file_name': None, 'file_size_bytes': None, 'file_hash_sha256': None})
        >>> _file_payload(row)['file_name'] is None
        True


    :param file_row: Core row containing all six projected keys and a numeric file_id.
    :return: New outer file dictionary with original optional values.
    :raises KeyError: A required projected column key is missing from the row.
    """
    return {
        "file_id": int(file_row["file_id"]),
        "file_store_id": file_row["file_store_id"],
        "file_storage_key": file_row["file_storage_key"],
        "file_name": file_row["file_name"],
        "file_size_bytes": file_row["file_size_bytes"],
        "file_hash_sha256": file_row["file_hash_sha256"],
    }


def _build_provenance_payload(
    model: CoreSurfaceModel,
    *,
    store_id: int | None,
    file_id: int | None,
) -> _ProvenancePayload:
    """
    Select stored derivations whose resolved endpoints meet all requested filters.

    Require the derivation table before checking that a file/store selector was
    supplied. Read all derivations and resolve both endpoint files before
    applying filters, skipping edges with a missing endpoint. Existing endpoint
    rows are cached locally; missing rows are queried again on later occurrences.

    Each supplied condition may match either endpoint, and both conditions must
    hold when both selectors are supplied, even on different endpoints. Preserve
    derivation order and duplicates without validating cycles or lineage. This
    is not an atomic snapshot, archive-byte inspection, or replica-to-derivation
    inference. Read/coercion errors remain visible.

    Example:
        >>> payload = _build_provenance_payload(model, store_id=7, file_id=None)  # doctest: +SKIP


    :param model: Core-backed schema and row reader for file_derivations and files.
    :param store_id: Optional store ID matched against either endpoint's non-None store reference.
    :param file_id: Optional file ID matched against either endpoint identity.
    :return: Echoed integer selectors, selected edge count, and ordered file projections.
    :raises ValueError: The derivation table or required selector is absent, or an ID conversion fails.
    """
    tables = set(model.table_names())
    if "file_derivations" not in tables:
        raise ValueError("Database does not contain `file_derivations` table.")
    if store_id is None and file_id is None:
        raise ValueError("Provide at least one filter: --store-id and/or --file-id.")

    derivations = model.rows("file_derivations")
    file_cache: dict[int, CoreRow | None] = {}

    def get_file_row(target_file_id: int) -> CoreRow | None:
        """
        Reuse a cached file row or fetch it again when the cached value is None.

        Negative lookups are stored but not treated as a cache hit. Errors
        propagate rather than recording a missing row.

        Example:
            >>> row = get_file_row(7)  # doctest: +SKIP


        :param target_file_id: Endpoint identity used as the cache key and int-coerced for lookup.
        :return: Existing Core file row by identity, or None when the model reports absence.
        """
        row = file_cache.get(target_file_id)
        if row is None:
            row = model.row("files", int(target_file_id))
            file_cache[target_file_id] = row
        return row

    edges: list[_ProvenanceEdge] = []
    for row in derivations:
        parent_id = int(row["file_derivation_parent_file_id"])
        child_id = int(row["file_derivation_child_file_id"])
        parent_row = get_file_row(parent_id)
        child_row = get_file_row(child_id)
        if parent_row is None or child_row is None:
            continue

        if file_id is not None and (
            parent_id != int(file_id) and child_id != int(file_id)
        ):
            continue
        if store_id is not None:
            parent_store_id = parent_row["file_store_id"]
            child_store_id = child_row["file_store_id"]
            parent_store_matches = parent_store_id is not None and int(
                parent_store_id
            ) == int(store_id)
            child_store_matches = child_store_id is not None and int(
                child_store_id
            ) == int(store_id)
            if not parent_store_matches and not child_store_matches:
                continue

        edges.append(
            {
                "file_derivation_id": int(row["file_derivation_id"]),
                "kind": row["file_derivation_kind"],
                "note": row["file_derivation_note"],
                "parent_file": _file_payload(parent_row),
                "child_file": _file_payload(child_row),
            }
        )

    return {
        "query": {
            "store_id": int(store_id) if store_id is not None else None,
            "file_id": int(file_id) if file_id is not None else None,
        },
        "edge_count": len(edges),
        "edges": edges,
    }


def cmd_provenance(args: argparse.Namespace) -> int:
    """
    Read filtered provenance through a Core session and print after session cleanup.

    JSON preserves optional None values and literal Unicode. Text substitutes
    unknown for a falsey kind and empty text for a falsey note, but prints only
    selected endpoint metadata. No archive bytes or lineage validation are read.
    Model, serialization, and output errors propagate to the application owner.

    Example:
        >>> status = cmd_provenance(args)  # doctest: +SKIP


    :param args: Namespace with Core selectors, optional integer file/store IDs, and json flag.
    :return: Zero after successful query projection and printing, including an empty edge list.
    """
    with open_surface_core_from_args(args) as session:
        payload = _build_provenance_payload(
            CoreSurfaceModel(session.client),
            store_id=int(args.store_id) if args.store_id is not None else None,
            file_id=int(args.file_id) if args.file_id is not None else None,
        )

    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        return 0

    query = payload["query"]
    print(
        "Provenance query: store_id={}, file_id={}".format(
            query["store_id"], query["file_id"]
        )
    )
    print("Edges: {}".format(payload["edge_count"]))
    for edge in payload["edges"]:
        parent = edge["parent_file"]
        child = edge["child_file"]
        print(
            "[{}] {} :: {}:{} -> {}:{} ({})".format(
                edge["file_derivation_id"],
                edge["kind"] or "unknown",
                parent["file_id"],
                parent["file_storage_key"],
                child["file_id"],
                child["file_storage_key"],
                edge["note"] or "",
            ).strip()
        )
    return 0


__all__ = ["cmd_publish_store", "cmd_publish_from_ids", "cmd_provenance"]
