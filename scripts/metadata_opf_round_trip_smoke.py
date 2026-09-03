#!/usr/bin/env python3
"""Smoke-test DB -> metadata -> OPF -> metadata round trips."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
import time
from collections.abc import Iterable, Mapping
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, TypeVar


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
T = TypeVar("T")


def _ensure_importable() -> None:
    for candidate in (str(REPO_ROOT), str(SRC_ROOT)):
        if candidate not in sys.path:
            sys.path.insert(0, candidate)


def _ensure_quiet_calibre_config() -> None:
    config_dir = Path(tempfile.gettempdir()) / "liuxin-calibre-config"
    config_dir.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("CALIBRE_CONFIG_DIRECTORY", str(config_dir))


_ensure_quiet_calibre_config()
_ensure_importable()


@dataclass(frozen=True)
class MetadataOPFRoundTripResult:
    """Serializable comparison and diagnostics for one OPF round trip."""

    item_id: int
    ok: bool
    errors: tuple[str, ...]
    before: dict[str, Any]
    after: dict[str, Any]
    opf_bytes: int
    opf_path: str | None = None
    write_report: dict[str, Any] | None = None

    def to_mapping(self) -> dict[str, Any]:
        return asdict(self)


def _existing_path(raw: str | os.PathLike[str] | None) -> Path | None:
    text = str(raw or "").strip()
    if not text:
        return None
    path = Path(text).expanduser()
    if not path.is_absolute():
        path = (REPO_ROOT / path).resolve()
    return path if path.is_file() else None


def _candidate_data_roots(explicit: str | None) -> list[Path]:
    raw_roots = [
        explicit,
        os.environ.get("LIUXIN_ALPHA_DATA_DIR"),
        os.environ.get("LIUXIN_DATA_DIR"),
        str(REPO_ROOT / "LiuXin_data"),
        str(REPO_ROOT / "LiuXin_alpha_data"),
        str(REPO_ROOT.parent / "LiuXin_alpha_data"),
    ]
    roots: list[Path] = []
    seen: set[Path] = set()
    for raw in raw_roots:
        text = str(raw or "").strip()
        if not text:
            continue
        path = Path(text).expanduser()
        if not path.is_absolute():
            path = (REPO_ROOT / path).resolve()
        if path in seen or not path.exists():
            continue
        seen.add(path)
        roots.append(path)
    return roots


def _bundle_candidates(data_root: Path, bundle_name: str) -> list[Path]:
    test_databases = data_root / "test_databases"
    candidates: list[Path] = []
    if bundle_name:
        candidates.extend(
            [
                test_databases / bundle_name / f"{bundle_name}.test_db",
                test_databases / f"{bundle_name}.test_db",
                data_root / bundle_name / f"{bundle_name}.test_db",
                data_root / f"{bundle_name}.test_db",
            ]
        )
        candidates.extend(sorted((test_databases / bundle_name).glob("*.test_db")))
        return candidates

    candidates.extend(sorted(test_databases.glob("isfdb*/*.test_db")))
    candidates.extend(sorted(test_databases.glob("*isfdb*/*.test_db")))
    candidates.extend(sorted(test_databases.glob("isfdb*.test_db")))
    candidates.extend(sorted(test_databases.glob("*isfdb*.test_db")))
    return candidates


def resolve_database(
    *,
    database: str | os.PathLike[str] | None,
    data_root: str | None,
    bundle_name: str,
) -> Path:
    explicit = _existing_path(database) or _existing_path(os.environ.get("LIUXIN_ISFDB_TEST_DB"))
    if explicit is not None:
        return explicit

    found: list[Path] = []
    for root in _candidate_data_roots(data_root):
        for candidate in _bundle_candidates(root, bundle_name):
            if candidate.is_file():
                found.append(candidate.resolve())

    if found:
        return max(found, key=lambda path: path.stat().st_mtime)

    roots = "\n".join(f"  - {root}" for root in _candidate_data_roots(data_root))
    raise FileNotFoundError(
        "Could not find an ISFDB .test_db. Pass DATABASE, pass --database, "
        "set LIUXIN_ISFDB_TEST_DB, or build one with scripts/build_isfdb_test_db.py.\n"
        f"Data roots searched:\n{roots or '  - <none found>'}"
    )


def open_database(
    database_path: Path,
    db_type: str,
    *,
    enable_maintenance: bool = False,
    repair_bootstrap_rows: bool = False,
) -> Any:
    from LiuXin_alpha.databases.database import Database

    return Database(
        metadata={"database_path": str(database_path)},
        db_type=db_type,
        create=False,
        backup=False,
        enable_storage_manager=False,
        enable_maintenance=bool(enable_maintenance),
        repair_bootstrap_rows=repair_bootstrap_rows,
    )


def prepare_write_back_database(
    database_path: Path,
    *,
    write_back: bool,
    scratch_db: str | os.PathLike[str] | None,
    allow_write_original: bool,
) -> Path:
    """
    Resolve the database path to open for the smoke run.

    OPF round-trip checks are read-oriented by default. When write-back is
    requested, require either a scratch copy destination or an explicit opt-in
    to writing the original file.
    """
    if not write_back:
        return database_path

    if scratch_db:
        target = Path(scratch_db).expanduser()
        if not target.is_absolute():
            target = (REPO_ROOT / target).resolve()
        if target.exists():
            raise FileExistsError(f"Scratch database already exists: {target}")
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(database_path, target)
        return target

    if allow_write_original:
        return database_path

    raise ValueError(
        "Refusing metadata write-back against the original database. "
        "Pass --scratch-db PATH or --allow-write-original."
    )


def quiet_call(func: Callable[..., T], *args: object, quiet: bool = True, **kwargs: object) -> T:
    if not quiet:
        return func(*args, **kwargs)
    with open(os.devnull, "w", encoding="utf-8") as sink:
        with redirect_stdout(sink), redirect_stderr(sink):
            return func(*args, **kwargs)


def select_item_ids(db: Any, explicit_item_ids: Iterable[int], *, limit: int) -> tuple[int, ...]:
    explicit = tuple(int(item_id) for item_id in explicit_item_ids if int(item_id) > 0)
    if explicit:
        return explicit

    rows = db.get_all_rows("items")
    selected: list[int] = []
    for row in rows:
        item_id = _row_id(row, "item_id")
        if item_id is None:
            continue
        selected.append(item_id)
        if len(selected) >= max(1, int(limit)):
            break
    if not selected:
        raise RuntimeError("The database has no item rows to smoke-test.")
    return tuple(selected)


def round_trip_item(
    db: Any,
    item_id: int,
    *,
    source: str = "database",
    lazy: bool = False,
    strict: bool = False,
    opf_dir: str | os.PathLike[str] | None = None,
    default_lang: str | None = None,
    replace_metadata: bool = False,
    quiet: bool = True,
    progress: bool = False,
    write_back: bool = False,
    write_back_fields: Iterable[str] | None = None,
    write_back_add_tags: Iterable[str] = (),
    write_back_replace: bool = False,
) -> MetadataOPFRoundTripResult:
    opf_path: Path | None = None
    write_report: dict[str, Any] | None = None
    started = time.monotonic()
    try:
        _progress(f"item {int(item_id)}: hydrating metadata", enabled=progress)
        metadata_facade = quiet_call(_metadata_facade, quiet=quiet)
        hydrated = quiet_call(
            metadata_facade.metadata_from_database,
            db,
            item_id=int(item_id),
            source=source,
            lazy=lazy,
            quiet=quiet,
        )
        before = metadata_snapshot(hydrated)
        _progress(f"item {int(item_id)}: serializing OPF", enabled=progress)
        opf_bytes = quiet_call(
            metadata_facade.metadata_to_opf_bytes,
            hydrated,
            default_lang=default_lang,
            quiet=quiet,
        )
        if opf_dir is not None:
            target_dir = Path(opf_dir)
            target_dir.mkdir(parents=True, exist_ok=True)
            opf_path = target_dir / f"item_{int(item_id)}.opf"
            opf_path.write_bytes(opf_bytes)
            opf_source: Any = opf_path
        else:
            opf_source = opf_bytes

        _progress(f"item {int(item_id)}: hydrating OPF metadata", enabled=progress)
        round_tripped = quiet_call(
            metadata_facade.metadata_from_opf,
            opf_source,
            kind="wemi",
            database=db,
            item_id=int(item_id),
            replace_metadata=replace_metadata,
            quiet=quiet,
        )
        after = metadata_snapshot(round_tripped)
        errors = list(compare_snapshots(before, after, strict=strict))
        added_tags = tuple(str(tag).strip() for tag in write_back_add_tags if str(tag).strip())
        if write_back:
            _add_tags(round_tripped, added_tags)
            _progress(f"item {int(item_id)}: writing metadata back", enabled=progress)
            report = quiet_call(
                round_tripped.write_to_database,
                db,
                fields=tuple(write_back_fields or ("tags", "series", "identifiers")),
                item_id=int(item_id),
                replace=write_back_replace,
                quiet=quiet,
            )
            to_mapping = getattr(report, "to_mapping", None)
            write_report = to_mapping() if callable(to_mapping) else {"repr": repr(report)}
            for error in write_report.get("errors", ()) if isinstance(write_report, Mapping) else ():
                errors.append(f"write-back error: {error}")
            if added_tags and isinstance(write_report, Mapping) and not write_report.get("changed"):
                errors.append("write-back did not apply requested tag additions")
        status = "OK" if not errors else "FAIL"
        _progress(
            f"item {int(item_id)}: {status} in {time.monotonic() - started:.1f}s",
            enabled=progress,
        )
        return MetadataOPFRoundTripResult(
            item_id=int(item_id),
            ok=not bool(errors),
            errors=tuple(errors),
            before=before,
            after=after,
            opf_bytes=len(opf_bytes),
            opf_path=str(opf_path) if opf_path is not None else None,
            write_report=write_report,
        )
    except Exception as err:
        _progress(
            f"item {int(item_id)}: ERROR in {time.monotonic() - started:.1f}s",
            enabled=progress,
        )
        return MetadataOPFRoundTripResult(
            item_id=int(item_id),
            ok=False,
            errors=(f"{err.__class__.__name__}: {err}",),
            before={},
            after={},
            opf_bytes=0,
            opf_path=str(opf_path) if opf_path is not None else None,
            write_report=write_report,
        )


def metadata_snapshot(metadata: Any) -> dict[str, Any]:
    return {
        "title": _scalar(_first_present(metadata, "title", "display_title", "canonical_title")),
        "authors": _values(_first_present(metadata, "authors")),
        "tags": _values(_first_present(metadata, "tags")),
        "series": _values(_first_present(metadata, "series")),
        "identifiers": _identifier_snapshot(metadata),
    }


def compare_snapshots(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    *,
    strict: bool = False,
) -> list[str]:
    errors: list[str] = []
    if before.get("title") and before.get("title") != after.get("title"):
        errors.append(
            "title changed from {!r} to {!r}".format(
                before.get("title"),
                after.get("title"),
            )
        )

    for field in ("authors", "identifiers"):
        if before.get(field) and not after.get(field):
            errors.append(f"{field} were dropped")

    if strict:
        for field in ("authors", "tags", "series", "identifiers"):
            if before.get(field) != after.get(field):
                errors.append(
                    "{} changed from {!r} to {!r}".format(
                        field,
                        before.get(field),
                        after.get(field),
                    )
                )
    return errors


def run_smoke_on_database(
    db: Any,
    *,
    item_ids: Iterable[int],
    limit: int,
    source: str,
    lazy: bool,
    strict: bool,
    opf_dir: str | os.PathLike[str] | None,
    default_lang: str | None,
    replace_metadata: bool,
    quiet: bool,
    progress: bool,
    write_back: bool,
    write_back_fields: Iterable[str] | None,
    write_back_add_tags: Iterable[str],
    write_back_replace: bool,
) -> list[MetadataOPFRoundTripResult]:
    _progress("Selecting item ids", enabled=progress)
    selected = select_item_ids(db, item_ids, limit=limit)
    _progress(
        "Selected item ids: {}".format(", ".join(str(item_id) for item_id in selected)),
        enabled=progress,
    )
    return [
        round_trip_item(
            db,
            item_id,
            source=source,
            lazy=lazy,
            strict=strict,
            opf_dir=opf_dir,
            default_lang=default_lang,
            replace_metadata=replace_metadata,
            quiet=quiet,
            progress=progress,
            write_back=write_back,
            write_back_fields=write_back_fields,
            write_back_add_tags=write_back_add_tags,
            write_back_replace=write_back_replace,
        )
        for item_id in selected
    ]


def _metadata_facade() -> Any:
    from LiuXin_alpha import metadata as metadata_facade

    return metadata_facade


def _progress(message: str, *, enabled: bool) -> None:
    if enabled:
        print(message, file=sys.stderr, flush=True)


def _add_tags(metadata: Any, tags: Iterable[str]) -> None:
    requested = [str(tag).strip() for tag in tags if str(tag).strip()]
    if not requested:
        return
    existing = list(_values(_first_present(metadata, "tags")))
    for tag in requested:
        if tag not in existing:
            existing.append(tag)
    setattr(metadata, "tags", existing)


def _row_id(row: Any, column: str) -> int | None:
    row_dict = getattr(row, "row_dict", None)
    value = row_dict.get(column) if isinstance(row_dict, Mapping) else None
    if value in (None, ""):
        value = getattr(row, "row_id", None)
    if value in (None, ""):
        return None
    return int(value)


def _first_present(obj: Any, *names: str) -> Any:
    for name in names:
        try:
            value = getattr(obj, name)
        except Exception:
            value = None
        if value not in (None, ""):
            return value
    return None


def _scalar(value: Any) -> str | None:
    if value in (None, ""):
        return None
    if isinstance(value, Mapping):
        value = next(iter(value.keys()), None)
    elif not isinstance(value, (str, bytes)):
        try:
            value = next(iter(value), None)
        except TypeError:
            pass
    if value in (None, ""):
        return None
    return str(value)


def _values(raw: Any) -> tuple[str, ...]:
    if raw in (None, ""):
        return ()
    if isinstance(raw, Mapping):
        iterable = raw.keys()
    elif isinstance(raw, (str, bytes)):
        iterable = (raw,)
    else:
        try:
            iterable = iter(raw)
        except TypeError:
            iterable = (raw,)
    return tuple(str(value) for value in iterable if value not in (None, ""))


def _identifier_snapshot(metadata: Any) -> dict[str, tuple[str, ...]]:
    getter = getattr(metadata, "get_identifiers", None)
    if not callable(getter):
        return {}
    try:
        identifiers = getter() or {}
    except Exception:
        return {}
    if not isinstance(identifiers, Mapping):
        return {}
    return {
        str(scheme): tuple(sorted(_values(values)))
        for scheme, values in sorted(identifiers.items(), key=lambda item: str(item[0]))
        if scheme and _values(values)
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Smoke-test DB -> metadata -> OPF -> metadata against a real LiuXin database."
    )
    parser.add_argument("database", nargs="?", default="", help="Explicit .test_db/.sqlite path.")
    parser.add_argument("--database", dest="database_option", default="", help=argparse.SUPPRESS)
    parser.add_argument("--data-root", default="", help="Data root containing test_databases/ bundles.")
    parser.add_argument(
        "--bundle-name",
        default="",
        help="Specific ISFDB test database bundle. Defaults to the newest isfdb*.test_db found.",
    )
    parser.add_argument("--db-type", default="sqlite", help="Database driver type. Default: sqlite.")
    parser.add_argument(
        "--item-id",
        action="append",
        type=int,
        default=[],
        help="Item id to smoke-test. Can be repeated. Defaults to the first --limit item rows.",
    )
    parser.add_argument("--limit", type=int, default=1, help="Number of item rows to test when --item-id is omitted.")
    parser.add_argument("--source", choices=("database", "cache"), default="database")
    parser.add_argument("--lazy", action="store_true", help="Hydrate lazy metadata objects.")
    parser.add_argument("--strict", action="store_true", help="Require tags, series, and identifiers to match exactly.")
    parser.add_argument("--replace-metadata", action="store_true", help="Replace hydrated fields when applying OPF data.")
    parser.add_argument("--default-lang", default=None, help="Default language to use when writing OPF.")
    parser.add_argument("--opf-dir", default=None, help="Directory to write OPF files into.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    parser.add_argument("--json-out", default=None, help="Optional path to write the JSON report.")
    parser.add_argument("--no-quiet", action="store_true", help="Do not suppress noisy metadata/database output.")
    parser.add_argument("--no-progress", action="store_true", help="Do not print progress messages to stderr.")
    parser.add_argument(
        "--write-back",
        action="store_true",
        help="Write the OPF-read metadata back to the database after comparison.",
    )
    parser.add_argument(
        "--write-back-field",
        action="append",
        default=[],
        help="Field to write back. Can be repeated. Defaults to tags, series, and identifiers.",
    )
    parser.add_argument(
        "--write-back-replace",
        action="store_true",
        help="Use replace semantics for write-back fields.",
    )
    parser.add_argument(
        "--add-tag",
        action="append",
        default=[],
        help="Tag to add before write-back. Can be repeated. Requires --write-back.",
    )
    parser.add_argument(
        "--scratch-db",
        default=None,
        help="Copy DATABASE here and run write-back against the copy.",
    )
    parser.add_argument(
        "--allow-write-original",
        action="store_true",
        help="Allow --write-back to mutate DATABASE directly.",
    )
    parser.add_argument(
        "--repair-bootstrap-rows",
        action="store_true",
        help="Run startup rating/null-row repairs before the smoke test. This may write to the database.",
    )
    return parser


def _report_mapping(results: list[MetadataOPFRoundTripResult], database_path: Path) -> dict[str, Any]:
    return {
        "database": str(database_path),
        "ok": all(result.ok for result in results),
        "count": len(results),
        "failures": sum(1 for result in results if not result.ok),
        "results": [result.to_mapping() for result in results],
    }


def _print_text_report(report: Mapping[str, Any]) -> None:
    print("Database:", report["database"])
    print("Round trips:", report["count"], "Failures:", report["failures"])
    for result in report["results"]:
        status = "OK" if result["ok"] else "FAIL"
        title = result.get("after", {}).get("title") or result.get("before", {}).get("title") or "<untitled>"
        print(f"[{status}] item {result['item_id']}: {title}")
        for error in result.get("errors", ()):
            print("  -", error)
        if result.get("opf_path"):
            print("  OPF:", result["opf_path"])


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    quiet = not bool(args.no_quiet)
    database_arg = args.database_option or args.database
    database_path = resolve_database(
        database=database_arg,
        data_root=args.data_root,
        bundle_name=str(args.bundle_name or "").strip(),
    )
    if args.add_tag and not args.write_back:
        raise ValueError("--add-tag requires --write-back.")

    progress = not bool(args.no_progress)
    database_path = prepare_write_back_database(
        database_path,
        write_back=bool(args.write_back),
        scratch_db=args.scratch_db,
        allow_write_original=bool(args.allow_write_original),
    )
    _progress(f"Opening database: {database_path}", enabled=progress)
    with quiet_call(
        open_database,
        database_path,
        str(args.db_type),
        enable_maintenance=bool(args.write_back),
        repair_bootstrap_rows=bool(args.repair_bootstrap_rows),
        quiet=quiet,
    ) as db:
        results = run_smoke_on_database(
            db,
            item_ids=tuple(args.item_id or ()),
            limit=int(args.limit),
            source=str(args.source),
            lazy=bool(args.lazy),
            strict=bool(args.strict),
            opf_dir=args.opf_dir,
            default_lang=args.default_lang,
            replace_metadata=bool(args.replace_metadata),
            quiet=quiet,
            progress=progress,
            write_back=bool(args.write_back),
            write_back_fields=tuple(args.write_back_field or ()) or None,
            write_back_add_tags=tuple(args.add_tag or ()),
            write_back_replace=bool(args.write_back_replace),
        )

    report = _report_mapping(results, database_path)
    if args.json_out:
        Path(args.json_out).write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        _print_text_report(report)
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
