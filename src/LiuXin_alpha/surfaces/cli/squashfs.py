from __future__ import annotations

import argparse
import json
import sys
import time

from pathlib import Path
from typing import Any, Mapping, Optional

from LiuXin_alpha.surfaces.cli.postgres import build_postgres_parser
from LiuXin_alpha.surfaces.core import (
    CoreRow,
    CoreSurfaceModel,
    add_core_client_arguments,
    open_surface_core_from_args,
)


def _print_publish_report(report: Mapping[str, Any], *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(dict(report), ensure_ascii=False, indent=2, sort_keys=True))
        return

    print("Store: {} ({})".format(report.get("store_name", ""), report.get("store_root_uri", "")))
    print("Store row id: {}".format(report.get("store_row_id", "")))
    print("Designated files: {}".format(report.get("designated_files", 0)))
    print("Packed files: {}".format(report.get("packed_files", 0)))
    print("Verified files: {}".format(report.get("verified_files", 0)))
    print("Duplicated files: {}".format(report.get("duplicated_files", 0)))
    print("Skipped existing duplicates: {}".format(report.get("skipped_existing_duplicates", 0)))
    print("Hash mismatches: {}".format(len(report.get("hash_mismatches", ()) or ())))
    print("Errors: {}".format(len(report.get("errors", ()) or ())))
    duration_seconds = report.get("duration_seconds")
    if duration_seconds is not None:
        print("Duration (seconds): {:.3f}".format(float(duration_seconds)))


def _run_job(core, operation: str, payload: Mapping[str, Any]) -> dict[str, Any]:
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
        }:
            break
        time.sleep(0.1)

    completed = core.query(
        "jobs.result",
        {"job_id": job_id, "timeout_s": 0.0},
    )
    execution = dict(completed.get("execution", {}) or {})
    if not bool(execution.get("ok", False)):
        raise RuntimeError(
            str(execution.get("traceback") or "Core job failed.")
        )
    result = execution.get("result")
    if not isinstance(result, Mapping):
        raise RuntimeError("Core job result did not return an object.")
    return dict(result)


def _collect_file_ids(args: argparse.Namespace) -> list[int]:
    file_ids: list[int] = []
    for value in args.file_id or []:
        file_ids.append(int(value))

    file_ids_file = getattr(args, "file_ids_file", None)
    if file_ids_file:
        raw_lines = Path(file_ids_file).expanduser().read_text(encoding="utf-8").splitlines()
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


def _file_payload(file_row: CoreRow) -> dict[str, object]:
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
    store_id: Optional[int],
    file_id: Optional[int],
) -> dict[str, object]:
    tables = set(model.table_names())
    if "file_derivations" not in tables:
        raise ValueError("Database does not contain `file_derivations` table.")
    if store_id is None and file_id is None:
        raise ValueError("Provide at least one filter: --store-id and/or --file-id.")

    derivations = model.rows("file_derivations")
    file_cache: dict[int, CoreRow | None] = {}

    def get_file_row(target_file_id: int) -> CoreRow | None:
        row = file_cache.get(target_file_id)
        if row is None:
            row = model.row("files", int(target_file_id))
            file_cache[target_file_id] = row
        return row

    edges: list[dict[str, object]] = []
    for row in derivations:
        parent_id = int(row["file_derivation_parent_file_id"])
        child_id = int(row["file_derivation_child_file_id"])
        parent_row = get_file_row(parent_id)
        child_row = get_file_row(child_id)
        if parent_row is None or child_row is None:
            continue

        if file_id is not None and (parent_id != int(file_id) and child_id != int(file_id)):
            continue
        if store_id is not None:
            parent_store_id = parent_row["file_store_id"]
            child_store_id = child_row["file_store_id"]
            parent_store_matches = parent_store_id is not None and int(parent_store_id) == int(store_id)
            child_store_matches = child_store_id is not None and int(child_store_id) == int(store_id)
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
    print("Provenance query: store_id={}, file_id={}".format(query["store_id"], query["file_id"]))
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


def build_squashfs_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "squashfs",
        help="SquashFS archival workflows (designated files -> archive -> locked store).",
    )
    squashfs_subparsers = parser.add_subparsers(dest="squashfs_command", required=True)

    publish_store = squashfs_subparsers.add_parser(
        "publish-store",
        help="Publish an already-designated open SquashFS store by store id.",
    )
    add_core_client_arguments(publish_store)
    publish_store.add_argument("--db-type", default="SQLite", help="Database backend type (default: SQLite).")
    publish_store.add_argument("--store-id", required=True, type=int, help="Open SquashFS store row id.")
    publish_store.add_argument(
        "--output-archive",
        default=None,
        help="Optional output archive path override (defaults to stores.store_root_uri).",
    )
    publish_store.add_argument("--compression", default="zstd", help="mksquashfs compression codec.")
    publish_store.add_argument("--deterministic", action="store_true", help="Enable deterministic squashfs flags.")
    publish_store.add_argument("--force", action="store_true", help="Overwrite archive output path if it exists.")
    publish_store.add_argument(
        "--no-duplicate-verified-files",
        action="store_false",
        dest="duplicate_verified_files",
        help="Do not duplicate verified files into the archive store in `files`.",
    )
    publish_store.set_defaults(duplicate_verified_files=True)
    publish_store.add_argument("--strict", action="store_true", help="Fail fast on any verification/publish error.")
    publish_store.add_argument("--json", action="store_true", help="Print report as JSON.")
    publish_store.add_argument(
        "--fail-on-report-errors",
        action="store_true",
        help="Return exit code 2 when report.errors is non-empty.",
    )
    publish_store.add_argument(
        "--no-refresh-storage-manager",
        action="store_true",
        help="Skip db.bootstrap_storage_manager(...) after publish.",
    )
    publish_store.set_defaults(handler=cmd_publish_store)

    publish_from_ids = squashfs_subparsers.add_parser(
        "publish-from-ids",
        help="Designate file ids and publish SquashFS archive in one command.",
    )
    add_core_client_arguments(publish_from_ids)
    publish_from_ids.add_argument("--db-type", default="SQLite", help="Database backend type (default: SQLite).")
    publish_from_ids.add_argument("--archive", required=True, help="Output archive path (.squashfs/.sqfs).")
    publish_from_ids.add_argument("--store-name", default=None, help="Optional open store name override.")
    publish_from_ids.add_argument(
        "--file-id",
        action="append",
        type=int,
        default=[],
        help="Source file row id to include (repeatable).",
    )
    publish_from_ids.add_argument(
        "--file-ids-file",
        default=None,
        help="Path to newline-separated file ids (blank lines and '#' comments allowed).",
    )
    publish_from_ids.add_argument("--compression", default="zstd", help="mksquashfs compression codec.")
    publish_from_ids.add_argument("--deterministic", action="store_true", help="Enable deterministic squashfs flags.")
    publish_from_ids.add_argument("--force", action="store_true", help="Overwrite archive output path if it exists.")
    publish_from_ids.add_argument("--strict", action="store_true", help="Fail fast on any verification/publish error.")
    publish_from_ids.add_argument("--json", action="store_true", help="Print report as JSON.")
    publish_from_ids.add_argument(
        "--fail-on-report-errors",
        action="store_true",
        help="Return exit code 2 when report.errors is non-empty.",
    )
    publish_from_ids.add_argument(
        "--no-refresh-storage-manager",
        action="store_true",
        help="Skip db.bootstrap_storage_manager(...) after publish.",
    )
    publish_from_ids.set_defaults(handler=cmd_publish_from_ids)

    provenance = squashfs_subparsers.add_parser(
        "provenance",
        help="Inspect file provenance edges (file_derivations) for a store and/or file.",
    )
    add_core_client_arguments(provenance)
    provenance.add_argument("--db-type", default="SQLite", help="Database backend type (default: SQLite).")
    provenance.add_argument("--store-id", type=int, default=None, help="Filter by store id.")
    provenance.add_argument("--file-id", type=int, default=None, help="Filter by file id.")
    provenance.add_argument("--json", action="store_true", help="Print report as JSON.")
    provenance.set_defaults(handler=cmd_provenance)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="LiuXin CLI surfaces")
    subparsers = parser.add_subparsers(dest="surface", required=True)
    build_squashfs_parser(subparsers)
    build_postgres_parser(subparsers)
    args = parser.parse_args(argv)

    handler = getattr(args, "handler", None)
    if handler is None:
        parser.print_help()
        return 2

    try:
        return int(handler(args))
    except Exception as exc:
        print("ERROR: {}".format(exc), file=sys.stderr)
        return 2


__all__ = [
    "main",
    "build_squashfs_parser",
    "cmd_publish_store",
    "cmd_publish_from_ids",
    "cmd_provenance",
]
