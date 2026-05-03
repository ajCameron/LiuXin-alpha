#!/usr/bin/env python3
"""Open the local ISFDB test DB with a LiuXin WEMI metadata hydrator."""

from __future__ import annotations

import argparse
import code
import os
import sys
import tempfile
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Callable, TypeVar


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

from LiuXin_alpha.databases.database import Database  # noqa: E402
from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadataHydrator  # noqa: E402


def _existing_path(raw: str | None) -> Path | None:
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


def resolve_isfdb_database(
    *,
    database: str | None,
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
        "Could not find an ISFDB .test_db. Pass --database, set "
        "LIUXIN_ISFDB_TEST_DB, or build one with scripts/build_isfdb_test_db.py.\n"
        f"Data roots searched:\n{roots or '  - <none found>'}"
    )


def open_database(database_path: Path, db_type: str) -> Database:
    return Database(
        metadata={"database_path": str(database_path)},
        db_type=db_type,
        create=False,
        backup=False,
    )


def first_item_id(db: Database) -> int:
    rows = db.get_all_rows("items")
    first = next(iter(rows), None)
    if first is None:
        raise RuntimeError("The ISFDB test database has no item rows.")
    return int(first.row_dict.get("item_id") or first.row_id)


def quiet_call(func: Callable[..., T], *args: object, quiet: bool = True, **kwargs: object) -> T:
    if not quiet:
        return func(*args, **kwargs)
    with open(os.devnull, "w", encoding="utf-8") as sink:
        with redirect_stdout(sink), redirect_stderr(sink):
            return func(*args, **kwargs)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Open the ISFDB test database and drop into a metadata-hydrator Python shell."
    )
    parser.add_argument("--database", default="", help="Explicit .test_db path. Overrides auto-discovery.")
    parser.add_argument("--data-root", default="", help="Data root containing test_databases/ bundles.")
    parser.add_argument(
        "--bundle-name",
        default="",
        help="Specific ISFDB test database bundle name. Defaults to the newest isfdb*.test_db found.",
    )
    parser.add_argument("--db-type", default="sqlite", help="Database driver type. Default: sqlite.")
    parser.add_argument("--item-id", type=int, default=0, help="Sample item id. Defaults to the first item row.")
    parser.add_argument("--no-sample", action="store_true", help="Do not pre-hydrate md/liuxin_md/calibre_md.")
    parser.add_argument("--no-quiet", action="store_true", help="Do not suppress noisy relation logs during hydration.")
    parser.add_argument("--no-console", action="store_true", help="Prepare the objects, print a summary, and exit.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    database_path = resolve_isfdb_database(
        database=args.database,
        data_root=args.data_root,
        bundle_name=str(args.bundle_name or "").strip(),
    )
    quiet = not bool(args.no_quiet)
    db: Database | None = None
    try:
        db = quiet_call(open_database, database_path, str(args.db_type), quiet=quiet)
        hydrator = LiuXinWEMIMetadataHydrator(db)
        item_id = int(args.item_id) if int(args.item_id) > 0 else first_item_id(db)

        def get_md(target_item_id: int = item_id):
            return quiet_call(
                hydrator.get_liuxin_wemi_metadata,
                item_id=int(target_item_id),
                quiet=quiet,
            )

        def get_liuxin_md(target_item_id: int = item_id):
            return get_md(target_item_id).as_liuxin_metadata()

        def get_calibre_md(target_item_id: int = item_id):
            return get_md(target_item_id).as_calibre_metadata()

        namespace: dict[str, object] = {
            "db": db,
            "database_path": database_path,
            "hydrator": hydrator,
            "item_id": item_id,
            "get_md": get_md,
            "get_liuxin_md": get_liuxin_md,
            "get_calibre_md": get_calibre_md,
            "LiuXinWEMIMetadataHydrator": LiuXinWEMIMetadataHydrator,
        }

        if not bool(args.no_sample):
            md = get_md(item_id)
            namespace["md"] = md
            namespace["liuxin_md"] = md.as_liuxin_metadata()
            namespace["calibre_md"] = md.as_calibre_metadata()

        print(f"Database: {database_path}")
        print(f"Items: {db.get_record_count('items')}")
        print(f"Sample item_id: {item_id}")
        if "md" in namespace:
            print(f"Sample title: {getattr(namespace['md'], 'title', None)}")
        print("Bound names: db, hydrator, item_id, get_md, get_liuxin_md, get_calibre_md")
        if "md" in namespace:
            print("Also bound: md, liuxin_md, calibre_md")

        if bool(args.no_console):
            return 0

        banner = (
            "ISFDB metadata shell\n"
            "Examples:\n"
            "  md = get_md(item_id)\n"
            "  md.database_ids\n"
            "  md.work, md.expression, md.manifestation, md.item\n"
            "  calibre_md = get_calibre_md(item_id)\n"
            "Call db.close() when done if you exit unusually."
        )
        code.interact(banner=banner, local=namespace)
        return 0
    finally:
        if db is not None:
            db.close()


if __name__ == "__main__":
    raise SystemExit(main())
