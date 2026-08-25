from __future__ import annotations

import argparse
import os
import shutil
import sys
import tempfile

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

EXAMPLES_ROOT = Path(__file__).resolve().parents[1]
if str(EXAMPLES_ROOT) not in sys.path:
    sys.path.insert(0, str(EXAMPLES_ROOT))

from _example_utils import bootstrap_src_path, dump_json

bootstrap_src_path()

from LiuXin_alpha.catalog import Catalog
from LiuXin_alpha.databases.database import Database


@dataclass(frozen=True, slots=True)
class CatalogExampleSession:
    """Live catalog and database details supplied to one example."""

    catalog: Catalog
    database_path: Path
    database_retained: bool


def add_database_argument(parser: argparse.ArgumentParser) -> None:
    """Add the common isolated-database option to an example parser."""

    parser.add_argument(
        "--database",
        type=Path,
        help=(
            "Create and retain the example database at this path. The path "
            "must not already exist. Without this option a temporary database "
            "is removed when the example finishes."
        ),
    )


@contextmanager
def open_catalog_example(
    database_path: Path | None,
) -> Iterator[CatalogExampleSession]:
    """Open a fresh FRBR database and expose its catalog facade.

    :param database_path: Optional path at which to retain the example database.
    :return: Context manager yielding the live catalog and path details.
    :raises FileExistsError: If a retained database path already exists.
    """

    temporary_directory: tempfile.TemporaryDirectory[str] | None = None
    if database_path is None:
        temporary_directory = tempfile.TemporaryDirectory(
            prefix="liuxin-catalog-example-"
        )
        resolved_path = Path(temporary_directory.name) / "catalog.sqlite"
        retained = False
    else:
        resolved_path = database_path.expanduser().resolve()
        if resolved_path.exists():
            raise FileExistsError(
                f"example database already exists: {resolved_path}"
            )
        resolved_path.parent.mkdir(parents=True, exist_ok=True)
        retained = True

    template_value = os.environ.get("LIUXIN_CATALOG_EXAMPLE_TEMPLATE")
    template_path = (
        None
        if not template_value
        else Path(template_value).expanduser().resolve()
    )
    if template_path is not None:
        if not template_path.is_file():
            raise FileNotFoundError(
                f"catalog example template does not exist: {template_path}"
            )
        if template_path == resolved_path:
            raise ValueError("catalog example template and output paths must differ")
        shutil.copy2(template_path, resolved_path)

    db: Database | None = None
    try:
        db = Database(
            metadata={"database_path": str(resolved_path)},
            db_type="SQLite",
            create=template_path is None,
            backup=False,
            enable_storage_manager=False,
            enable_maintenance=False,
        )
        yield CatalogExampleSession(
            catalog=Catalog(db),
            database_path=resolved_path,
            database_retained=retained,
        )
    finally:
        if db is not None:
            db.close()
        if temporary_directory is not None:
            temporary_directory.cleanup()


__all__ = [
    "CatalogExampleSession",
    "add_database_argument",
    "dump_json",
    "open_catalog_example",
]
