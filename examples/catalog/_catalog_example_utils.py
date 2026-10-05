"""
Share database allocation and catalogue lifetime across the command-line examples.

An omitted output path uses a temporary directory; an explicit path retains the
resulting database and must be absent at the initial check. The optional
LIUXIN_CATALOG_EXAMPLE_TEMPLATE environment variable supplies a database file to
copy rather than creating a new schema. Expose a Catalog through a context manager
and re-export the shared diagnostic JSON renderer.
"""

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
    """
    Describe a live Catalog and the database allocation used by one example. This frozen, slotted
    record does not own or close the database. Its Catalog is usable during the enclosing
    open_catalog_example context; retaining this record does not extend that lifetime or reopen the
    connection.

    Example:
        >>> with open_catalog_example(None) as session:  # doctest: +SKIP
        ...     catalog = session.catalog


    :ivar catalog: Facade over the database opened by the enclosing example context.
    :ivar database_path: Resolved retained path or database path inside the allocated temporary directory.
    :ivar database_retained: Whether an explicit output path was requested rather than temporary cleanup.
    """

    catalog: Catalog
    database_path: Path
    database_retained: bool


def add_database_argument(parser: argparse.ArgumentParser) -> None:
    """
    Add the shared optional --database argument to an existing parser. Convert supplied text to Path
    without expanding or checking it. Omission produces None, selecting temporary storage when the
    example later opens its context. Help explains that an explicit output path is retained and must
    not already exist; enforcement belongs to open_catalog_example. Parser conflict errors
    propagate.

    Example:
        >>> parser = argparse.ArgumentParser()
        >>> add_database_argument(parser)
        >>> parser.parse_args([]).database is None
        True
        >>> str(parser.parse_args(["--database", "demo.sqlite"]).database)
        'demo.sqlite'


    :param parser: Mutable argument parser to receive the shared database option.
    :return: None after registering the argument.
    """

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
    """
    Open an example catalogue and yield its allocation details until context exit. Work begins on
    context entry. With no path, allocate a temporary directory and use catalog.sqlite within it.
    Otherwise expand/resolve the output, refuse it if it currently exists, and create its parents.
    This existence check is not an atomic path reservation.

    A truthy LIUXIN_CATALOG_EXAMPLE_TEMPLATE setting is expanded/resolved, checked as a file
    distinct from the output, and copied with shutil.copy2. Its existing contents are kept; the
    helper does not clear template records. Without a template, request schema creation. Open SQLite
    with backup, automatic storage management, and maintenance disabled, then yield a
    CatalogExampleSession wrapping Catalog(db).

    Finally close an assigned database, then explicitly clean up a temporary directory. Retained
    output and created parents remain even after later failures. Template validation and copying
    happen before this finally block, and a db.close error prevents the explicit temporary cleanup
    call; no stronger cleanup guarantee is supplied here. Body errors propagate unless replaced by
    cleanup errors.

    Example:
        >>> with open_catalog_example(None) as session:  # doctest: +SKIP
        ...     work_id = session.catalog.works.create({"title": "Example"})


    :param database_path: Optional retained output path; None chooses a disposable temporary catalogue.
    :return: Context manager yielding one live CatalogExampleSession and owning its database lifetime.
    :raises FileExistsError: If an explicitly retained output path exists at the initial check.
    :raises FileNotFoundError: If the configured template is not a file.
    :raises ValueError: If the resolved template and output paths are equal.
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
