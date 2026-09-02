#!/usr/bin/env python3
"""Build and verify optional LiuXin data artifacts.

The large artifacts live in the separate private data checkout. This script is
the single entry point from the main repo: it wraps existing builders, pins the
child build environment, and writes/verifies a manifest using content hashes.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST_NAME = "artifacts_manifest.json"
DEFAULT_ISFDB_DUMP_NAME = "backup-MySQL-55-2026-04-18.zip"

DETERMINISTIC_CHILD_ENV = {
    "PYTHONHASHSEED": "0",
    "TZ": "UTC",
    "LC_ALL": "C.UTF-8",
    "LANG": "C.UTF-8",
}

DETERMINISM_NOTES = (
    "Child builders are launched with PYTHONHASHSEED=0, TZ=UTC, LC_ALL=C.UTF-8, and LANG=C.UTF-8.",
    "Build recipes must use explicit ordering for database reads and writes.",
    "Build recipes must not use platform random backends, secrets, uuid4, or unseeded random state for persisted values.",
    "Absolute paths, hostnames, and elapsed times are not reproducibility inputs; artifact bytes and logical counts are.",
)


def _log(message: str) -> None:
    print(f"[artifacts] {message}", file=sys.stderr, flush=True)


def _format_bytes(size: int) -> str:
    value = float(size)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024.0 or unit == "TiB":
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024.0
    return f"{size} B"


@dataclass(frozen=True)
class ArtifactSpec:
    """Declarative build, provenance, and validation rules for one artifact."""

    name: str
    relative_path: Path
    tracked_in_git: bool
    description: str
    buildable: bool
    source_zip_required: bool = False
    count_tables: tuple[str, ...] = ()
    legacy_manifest_only_reason: str = ""


ARTIFACTS: dict[str, ArtifactSpec] = {
    "benchmark-smoke": ArtifactSpec(
        name="benchmark-smoke",
        relative_path=Path("benchmarks/databases/benchmark_db_smoke.test_db"),
        tracked_in_git=True,
        description="Small deterministic benchmark fixture safe to store in Git.",
        buildable=True,
        count_tables=("works", "books", "folders", "files"),
    ),
    "isfdb-current": ArtifactSpec(
        name="isfdb-current",
        relative_path=Path("test_databases/isfdb_mysql_55_2026_04_18/isfdb_mysql_55_2026_04_18.test_db"),
        tracked_in_git=False,
        description="Current large FRBR-native ISFDB corpus; payload is too large for Git/GitHub LFS.",
        buildable=True,
        source_zip_required=True,
    ),
    "isfdb-full-legacy": ArtifactSpec(
        name="isfdb-full-legacy",
        relative_path=Path("test_databases/isfdb_full_2026_04_22/isfdb_full_2026_04_22.test_db"),
        tracked_in_git=False,
        description="Archived earlier large ISFDB corpus kept for comparison.",
        buildable=False,
        source_zip_required=True,
        legacy_manifest_only_reason=(
            "This artifact was built by an older version of scripts/build_isfdb_test_db.py. "
            "The current builder emits richer metadata, so this payload is preserved by hash "
            "rather than advertised as bit-for-bit rebuildable from the current code."
        ),
    ),
}


def _resolve_data_root(explicit: Optional[str]) -> Path:
    candidates: list[Path] = []
    if explicit:
        _log(f"data root requested explicitly: {explicit}")
        candidates.append(Path(explicit).expanduser())
    env = os.environ.get("LIUXIN_ALPHA_DATA_DIR")
    if env:
        _log(f"data root candidate from LIUXIN_ALPHA_DATA_DIR: {env}")
        candidates.append(Path(env).expanduser())
    candidates.extend(
        [
            REPO_ROOT / "LiuXin_alpha_data",
            REPO_ROOT.parent / "LiuXin_alpha_data",
            REPO_ROOT / "LiuXin_data",
        ]
    )

    for candidate in candidates:
        path = candidate if candidate.is_absolute() else (REPO_ROOT / candidate)
        _log(f"checking data root candidate: {path}")
        if path.exists():
            resolved = path.resolve()
            _log(f"using data root: {resolved}")
            return resolved

    fallback = REPO_ROOT / "LiuXin_alpha_data"
    fallback.mkdir(parents=True, exist_ok=True)
    resolved = fallback.resolve()
    _log(f"created fallback data root: {resolved}")
    return resolved


def _resolve_manifest_path(data_root: Path, explicit: Optional[str]) -> Path:
    if explicit:
        path = Path(explicit).expanduser()
        if not path.is_absolute():
            path = (REPO_ROOT / path).resolve()
        _log(f"using explicit manifest path: {path}")
        return path
    path = data_root / DEFAULT_MANIFEST_NAME
    _log(f"using default manifest path: {path}")
    return path


def _dump_zip_candidates(explicit: Optional[str]) -> list[Path]:
    candidates: list[Path] = []
    if explicit:
        candidates.append(Path(explicit).expanduser())
    env = os.environ.get("LIUXIN_ISFDB_DUMP_ZIP")
    if env:
        candidates.append(Path(env).expanduser())
    candidates.extend(
        [
            REPO_ROOT / DEFAULT_ISFDB_DUMP_NAME,
            REPO_ROOT.parent / DEFAULT_ISFDB_DUMP_NAME,
            Path.home() / "Downloads" / DEFAULT_ISFDB_DUMP_NAME,
        ]
    )
    resolved: list[Path] = []
    for candidate in candidates:
        path = candidate if candidate.is_absolute() else (REPO_ROOT / candidate)
        resolved.append(path.resolve())
    return resolved


def _resolve_dump_zip(explicit: Optional[str], *, required: bool) -> Optional[Path]:
    candidates = _dump_zip_candidates(explicit)
    for candidate in candidates:
        _log(f"checking ISFDB dump zip candidate: {candidate}")
        if candidate.is_file():
            _log(f"using ISFDB dump zip: {candidate}")
            return candidate
    if required:
        tried = "\n".join(f"  - {candidate}" for candidate in candidates)
        raise SystemExit(
            "Could not locate ISFDB dump zip. Pass --dump-zip or set LIUXIN_ISFDB_DUMP_ZIP.\n"
            f"Tried:\n{tried}"
        )
    _log("no ISFDB dump zip found; continuing because it is optional for this command")
    return None


def _selected_specs(artifact: str, *, build_only: bool = False) -> list[ArtifactSpec]:
    if artifact == "all":
        values = list(ARTIFACTS.values())
    else:
        values = [ARTIFACTS[artifact]]
    if build_only:
        requested = values
        values = [spec for spec in values if spec.buildable]
        if not values and artifact != "all":
            spec = requested[0]
            reason = spec.legacy_manifest_only_reason or "artifact is manifest-only"
            raise SystemExit(f"{spec.name} is not buildable: {reason}")
    _log(
        "selected artifacts: "
        + ", ".join(spec.name for spec in values)
        + (" (buildable only)" if build_only else "")
    )
    return values


def _child_env(data_root: Path) -> dict[str, str]:
    env = os.environ.copy()
    env.update(DETERMINISTIC_CHILD_ENV)
    env["LIUXIN_ALPHA_DATA_DIR"] = str(data_root)
    env["LIUXIN_DATA_DIR"] = str(data_root)
    return env


def _run(command: Sequence[str], *, data_root: Path) -> None:
    _log(f"deterministic child env: {DETERMINISTIC_CHILD_ENV}")
    _log(f"child data root: {data_root}")
    _log("$ " + " ".join(command))
    subprocess.run(
        list(command),
        cwd=str(REPO_ROOT),
        env=_child_env(data_root),
        check=True,
    )


def build_artifact(
    spec: ArtifactSpec,
    *,
    data_root: Path,
    dump_zip: Optional[Path],
    force: bool,
    regenerate: bool,
) -> None:
    if not spec.buildable:
        raise SystemExit(f"{spec.name} is manifest-only: {spec.legacy_manifest_only_reason}")

    output_path = data_root / spec.relative_path
    _log(f"building artifact {spec.name}: {spec.description}")
    _log(f"expected output path: {output_path}")

    if spec.name == "benchmark-smoke":
        command = [
            sys.executable,
            str(REPO_ROOT / "scripts" / "build_benchmark_test_db.py"),
            "--name",
            "benchmark_db_smoke",
            "--output",
            str(data_root / spec.relative_path),
        ]
        if regenerate:
            command.append("--regenerate")
        _run(command, data_root=data_root)
        _log(f"finished build for {spec.name}")
        return

    if spec.name == "isfdb-current":
        if dump_zip is None:
            dump_zip = _resolve_dump_zip(None, required=True)
        command = [
            sys.executable,
            str(REPO_ROOT / "scripts" / "build_isfdb_test_db.py"),
            "--data-root",
            str(data_root),
            "--dump-zip",
            str(dump_zip),
            "--bundle-name",
            "isfdb_mysql_55_2026_04_18",
        ]
        if force:
            command.append("--force")
        _run(command, data_root=data_root)
        _log(f"finished build for {spec.name}")
        return

    raise AssertionError(f"Unhandled buildable artifact: {spec.name}")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    size = path.stat().st_size
    processed = 0
    next_report = 1024 * 1024 * 1024
    _log(f"hashing {path} ({_format_bytes(size)})")
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            processed += len(chunk)
            if size >= next_report and processed >= next_report:
                _log(f"hashed {processed / (1024 ** 3):.1f} GiB / {size / (1024 ** 3):.1f} GiB: {path}")
                next_report += 1024 * 1024 * 1024
    hexdigest = digest.hexdigest()
    _log(f"hash complete {path}: {hexdigest}")
    return hexdigest


def _file_payload(path: Path) -> dict[str, object]:
    _log(f"collecting file payload: {path}")
    return {
        "size_bytes": path.stat().st_size,
        "sha256": _sha256_file(path),
    }


def _sqlite_counts(path: Path, tables: Iterable[str]) -> dict[str, int]:
    _log(f"collecting SQLite counts from {path}")
    counts: dict[str, int] = {}
    conn = sqlite3.connect(str(path))
    try:
        available = {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table', 'view');"
            ).fetchall()
        }
        for table in sorted(set(tables)):
            if table not in available:
                continue
            row = conn.execute(f"SELECT COUNT(*) FROM `{table}`;").fetchone()
            if row is not None:
                counts[table] = int(row[0])
                _log(f"count {path.name}:{table}={counts[table]}")
    finally:
        conn.close()
    return counts


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _summary_payload(spec: ArtifactSpec, data_root: Path) -> dict[str, object]:
    summary_path = data_root / spec.relative_path.parent / "build_summary.json"
    if not summary_path.is_file():
        _log(f"no build summary found for {spec.name}: {summary_path}")
        return {}
    _log(f"reading build summary for {spec.name}: {summary_path}")
    raw = _read_json(summary_path)
    keys = ("options", "stage_counts", "selection_counts", "target_counts")
    payload = {key: raw[key] for key in keys if key in raw}
    if "bundle_name" in raw:
        payload["bundle_name"] = raw["bundle_name"]
    return payload


def _source_zip_payload(dump_zip: Optional[Path]) -> Optional[dict[str, object]]:
    if dump_zip is None or not dump_zip.is_file():
        _log("source zip payload skipped; no source zip available")
        return None
    _log(f"recording source zip payload: {dump_zip}")
    payload = _file_payload(dump_zip)
    payload["filename"] = dump_zip.name
    return payload


def _rebuild_command(spec: ArtifactSpec) -> str:
    if spec.name == "benchmark-smoke":
        return "python3 scripts/build_artifacts.py build --artifact benchmark-smoke --regenerate"
    if spec.name == "isfdb-current":
        return (
            "python3 scripts/build_artifacts.py build --artifact isfdb-current "
            "--dump-zip <backup-MySQL-55-2026-04-18.zip> --force"
        )
    raise AssertionError(f"No rebuild command for {spec.name}")


def _manifest_entry(
    spec: ArtifactSpec,
    *,
    data_root: Path,
    dump_zip: Optional[Path],
) -> dict[str, object]:
    artifact_path = data_root / spec.relative_path
    _log(f"building manifest entry for {spec.name}")
    entry: dict[str, object] = {
        "name": spec.name,
        "description": spec.description,
        "relative_path": spec.relative_path.as_posix(),
        "tracked_in_git": spec.tracked_in_git,
    }
    if spec.buildable:
        entry["rebuild"] = {
            "command": _rebuild_command(spec),
            "deterministic_child_env": DETERMINISTIC_CHILD_ENV,
        }
    else:
        entry["rebuild"] = {
            "status": "manifest-only",
            "reason": spec.legacy_manifest_only_reason,
        }

    if spec.source_zip_required:
        source_payload = _source_zip_payload(dump_zip)
        if source_payload is not None:
            entry["source_zip"] = source_payload
        else:
            entry["source_zip"] = {
                "filename": DEFAULT_ISFDB_DUMP_NAME,
                "status": "not-found-when-manifest-was-written",
            }

    if artifact_path.is_file():
        _log(f"artifact payload exists for {spec.name}: {artifact_path}")
        entry["file"] = _file_payload(artifact_path)
        if spec.count_tables:
            entry["sqlite_counts"] = _sqlite_counts(artifact_path, spec.count_tables)
    else:
        _log(f"artifact payload missing for {spec.name}: {artifact_path}")
        entry["file"] = {"status": "missing"}

    summary = _summary_payload(spec, data_root)
    if summary:
        entry["build_summary"] = summary

    return entry


def write_manifest(
    *,
    data_root: Path,
    manifest_path: Path,
    specs: Sequence[ArtifactSpec],
    dump_zip: Optional[Path],
) -> None:
    _log(f"writing manifest: {manifest_path}")
    selected_names = {spec.name for spec in specs}
    existing_entries: dict[str, dict[str, object]] = {}
    if manifest_path.is_file():
        _log(f"preserving unselected entries from existing manifest: {manifest_path}")
        existing_payload = _read_json(manifest_path)
        for entry in existing_payload.get("artifacts", []):
            if isinstance(entry, dict) and entry.get("name"):
                existing_entries[str(entry["name"])] = entry

    for spec in specs:
        _log(f"refreshing manifest entry: {spec.name}")
        existing_entries[spec.name] = _manifest_entry(spec, data_root=data_root, dump_zip=dump_zip)

    if selected_names == set(ARTIFACTS):
        entries = [existing_entries[spec.name] for spec in ARTIFACTS.values()]
    else:
        entries = [
            existing_entries[name]
            for name in ARTIFACTS
            if name in existing_entries
        ]

    payload = {
        "schema_version": 1,
        "generated_by": "scripts/build_artifacts.py write-manifest",
        "determinism_notes": list(DETERMINISM_NOTES),
        "artifacts": entries,
    }
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _log(f"wrote manifest with {len(entries)} artifact entries: {manifest_path}")


def verify_manifest(
    *,
    data_root: Path,
    manifest_path: Path,
    specs: Sequence[ArtifactSpec],
    allow_missing: bool,
    dump_zip: Optional[Path],
    require_source: bool,
) -> int:
    _log(f"verifying manifest: {manifest_path}")
    payload = _read_json(manifest_path)
    requested = {spec.name for spec in specs}
    entries = [
        entry
        for entry in payload.get("artifacts", [])
        if isinstance(entry, dict) and str(entry.get("name")) in requested
    ]
    failures: list[str] = []
    hash_cache: dict[Path, str] = {}
    _log(f"manifest entries selected for verification: {len(entries)}")

    def cached_hash(path: Path) -> str:
        key = path.resolve()
        if key not in hash_cache:
            hash_cache[key] = _sha256_file(path)
        return hash_cache[key]

    for entry in entries:
        name = str(entry["name"])
        relative_path = Path(str(entry["relative_path"]))
        path = data_root / relative_path
        _log(f"verifying artifact {name}: {path}")
        expected_file = entry.get("file") if isinstance(entry.get("file"), dict) else {}
        expected_hash = str(expected_file.get("sha256") or "")
        expected_size = expected_file.get("size_bytes")

        if not path.is_file():
            message = f"{name}: missing {relative_path.as_posix()}"
            if allow_missing:
                _log(f"warning: {message}")
            else:
                failures.append(message)
            continue

        actual_size = path.stat().st_size
        _log(f"{name}: size {_format_bytes(actual_size)}")
        if expected_size is not None and actual_size != int(expected_size):
            failures.append(f"{name}: size mismatch expected={expected_size} actual={actual_size}")
            continue

        actual_hash = cached_hash(path)
        if expected_hash and actual_hash != expected_hash:
            failures.append(f"{name}: sha256 mismatch expected={expected_hash} actual={actual_hash}")
        else:
            _log(f"ok: {name} {actual_hash}")

        source = entry.get("source_zip") if isinstance(entry.get("source_zip"), dict) else None
        source_hash = str((source or {}).get("sha256") or "")
        if source_hash:
            _log(f"{name}: verifying source zip hash")
            source_path = dump_zip or _resolve_dump_zip(None, required=False)
            if source_path is None or not source_path.is_file():
                if require_source:
                    failures.append(f"{name}: source zip is required but missing")
            else:
                actual_source_hash = cached_hash(source_path)
                if actual_source_hash != source_hash:
                    failures.append(
                        f"{name}: source zip sha256 mismatch expected={source_hash} actual={actual_source_hash}"
                    )

    if failures:
        for failure in failures:
            _log(f"error: {failure}")
        return 1
    _log("manifest verification passed")
    return 0


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--data-root", help="Path to the LiuXin_alpha_data checkout.")
    parser.add_argument("--manifest", help=f"Manifest path; defaults to <data-root>/{DEFAULT_MANIFEST_NAME}.")
    parser.add_argument("--dump-zip", help=f"Path to {DEFAULT_ISFDB_DUMP_NAME}.")
    parser.add_argument(
        "--artifact",
        default="all",
        choices=(*ARTIFACTS.keys(), "all"),
        help="Artifact to act on.",
    )


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List known artifacts.")
    _add_common_args(list_parser)

    build_parser = subparsers.add_parser("build", help="Build one or more buildable artifacts.")
    _add_common_args(build_parser)
    build_parser.add_argument("--force", action="store_true", help="Replace existing large output bundles.")
    build_parser.add_argument(
        "--regenerate",
        action="store_true",
        help="Regenerate cached benchmark templates before provisioning.",
    )

    manifest_parser = subparsers.add_parser("write-manifest", help="Write a hash manifest from local artifacts.")
    _add_common_args(manifest_parser)

    verify_parser = subparsers.add_parser("verify", help="Verify local artifacts against the manifest.")
    _add_common_args(verify_parser)
    verify_parser.add_argument("--allow-missing", action="store_true", help="Warn instead of failing on missing payloads.")
    verify_parser.add_argument("--require-source", action="store_true", help="Fail if a source zip hash exists but the zip is missing.")

    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    _log(f"command: {args.command}")
    _log(f"repo root: {REPO_ROOT}")
    data_root = _resolve_data_root(args.data_root)
    manifest_path = _resolve_manifest_path(data_root, args.manifest)

    if args.command == "list":
        _log("listing known artifacts")
        for spec in ARTIFACTS.values():
            mode = "buildable" if spec.buildable else "manifest-only"
            tracking = "tracked" if spec.tracked_in_git else "manifest"
            print(f"{spec.name}\t{mode}\t{tracking}\t{spec.relative_path.as_posix()}")
        return 0

    selected = _selected_specs(args.artifact, build_only=args.command == "build")

    if args.command == "build":
        dump_zip = _resolve_dump_zip(args.dump_zip, required=any(spec.source_zip_required for spec in selected))
        for spec in selected:
            build_artifact(
                spec,
                data_root=data_root,
                dump_zip=dump_zip,
                force=bool(args.force),
                regenerate=bool(args.regenerate),
            )
        return 0

    if args.command == "write-manifest":
        dump_zip = _resolve_dump_zip(args.dump_zip, required=False)
        write_manifest(
            data_root=data_root,
            manifest_path=manifest_path,
            specs=selected,
            dump_zip=dump_zip,
        )
        return 0

    if args.command == "verify":
        dump_zip = _resolve_dump_zip(args.dump_zip, required=False)
        return verify_manifest(
            data_root=data_root,
            manifest_path=manifest_path,
            specs=selected,
            allow_missing=bool(args.allow_missing),
            dump_zip=dump_zip,
            require_source=bool(args.require_source),
        )

    raise AssertionError(f"Unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
