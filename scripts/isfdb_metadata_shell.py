#!/usr/bin/env python3
"""
Query ISFDB metadata from the command line.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise isfdb metadata shell through a consuming regression::

        python -m pytest -q tests/scripts/test_ci_workflow_contracts.py
"""

from __future__ import annotations

import argparse
import code
import os
import sys
import tempfile
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Callable, TYPE_CHECKING, TypeVar


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
T = TypeVar("T")


def _ensure_importable() -> None:
    """
    Perform the ensure importable operation under explicit file-format and conversion rules.

    Example:
        Exercise  ensure importable through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    for candidate in (str(REPO_ROOT), str(SRC_ROOT)):
        if candidate not in sys.path:
            sys.path.insert(0, candidate)


def _ensure_quiet_calibre_config() -> None:
    """
    Perform the ensure quiet calibre config operation under explicit file-format and conversion rules.

    Example:
        Exercise  ensure quiet calibre config through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    config_dir = Path(tempfile.gettempdir()) / "liuxin-calibre-config"
    config_dir.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("CALIBRE_CONFIG_DIRECTORY", str(config_dir))


_ensure_quiet_calibre_config()
_ensure_importable()

if TYPE_CHECKING:
    from LiuXin_alpha.databases.database import Database


_METADATA_CONTAINER_CLASSES: dict[str, object] | None = None


def _metadata_container_classes() -> dict[str, object]:
    """
    Perform the metadata container classes operation under explicit file-format and conversion rules.

    Example:
        Exercise  metadata container classes through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    global _METADATA_CONTAINER_CLASSES
    if _METADATA_CONTAINER_CLASSES is None:
        from LiuXin_alpha.metadata.containers import (
            LazyLiuXinWEMIMetadataHydrator,
            LiuXinWEMIMetadataHydrator,
            LiuXinWEMIMetadataWriter,
        )

        _METADATA_CONTAINER_CLASSES = {
            "LazyLiuXinWEMIMetadataHydrator": LazyLiuXinWEMIMetadataHydrator,
            "LiuXinWEMIMetadataHydrator": LiuXinWEMIMetadataHydrator,
            "LiuXinWEMIMetadataWriter": LiuXinWEMIMetadataWriter,
        }
    return dict(_METADATA_CONTAINER_CLASSES)


class LazyShellBinding:
    """
    Small REPL helper that defers expensive sample hydration until use.

    Example:
        Exercise LazyShellBinding through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py
    """

    def __init__(self, label: str, factory: Callable[[], object]) -> None:
        """
        Initialize and validate the lazyshellbinding state.

        Example:
            Exercise LazyShellBinding.  init   through a consuming regression::

                python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


        :param label: Value supplied for label under the utility contract.
        :param factory: Value supplied for factory under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        object.__setattr__(self, "_label", str(label))
        object.__setattr__(self, "_factory", factory)
        object.__setattr__(self, "_loaded", False)
        object.__setattr__(self, "_value", None)

    @property
    def is_loaded(self) -> bool:
        """
        Return whether is loaded holds for the supplied ebook data.

        Example:
            Exercise LazyShellBinding.is loaded through a consuming regression::

                python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


        :return: True when the documented condition holds; otherwise False.
        """
        return bool(object.__getattribute__(self, "_loaded"))

    def load(self):
        """
        Perform the load operation under explicit file-format and conversion rules.

        Example:
            Exercise LazyShellBinding.load through a consuming regression::

                python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not bool(object.__getattribute__(self, "_loaded")):
            factory = object.__getattribute__(self, "_factory")
            object.__setattr__(self, "_value", factory())
            object.__setattr__(self, "_loaded", True)
        return object.__getattribute__(self, "_value")

    def __getattr__(self, name: str):
        """
        Perform the getattr operation under explicit file-format and conversion rules.

        Example:
            Exercise LazyShellBinding.  getattr   through a consuming regression::

                python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return getattr(self.load(), name)

    def __setattr__(self, name: str, value: object) -> None:
        """
        Perform the setattr operation under explicit file-format and conversion rules.

        Example:
            Exercise LazyShellBinding.  setattr   through a consuming regression::

                python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param value: Value normalized, stored, formatted or returned.
        :return: None; validated state is stored on the receiving object.
        """
        if str(name).startswith("_"):
            object.__setattr__(self, name, value)
            return
        setattr(self.load(), name, value)

    def __getitem__(self, key: object):
        """
        Perform the getitem operation under explicit file-format and conversion rules.

        Example:
            Exercise LazyShellBinding.  getitem   through a consuming regression::

                python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


        :param key: Metadata, identifier or local-variable key.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.load()[key]

    def __iter__(self):
        """
        Perform the iter operation under explicit file-format and conversion rules.

        Example:
            Exercise LazyShellBinding.  iter   through a consuming regression::

                python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return iter(self.load())

    def __len__(self) -> int:
        """
        Perform the len operation under explicit file-format and conversion rules.

        Example:
            Exercise LazyShellBinding.  len   through a consuming regression::

                python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return len(self.load())

    def __bool__(self) -> bool:
        """
        Perform the bool operation under explicit file-format and conversion rules.

        Example:
            Exercise LazyShellBinding.  bool   through a consuming regression::

                python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


        :return: True when the documented condition holds; otherwise False.
        """
        return bool(self.load())

    def __call__(self, *args: object, **kwargs: object):
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise LazyShellBinding.  call   through a consuming regression::

                python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.load()(*args, **kwargs)

    def __int__(self) -> int:
        """
        Perform the int operation under explicit file-format and conversion rules.

        Example:
            Exercise LazyShellBinding.  int   through a consuming regression::

                python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return int(self.load())

    def __str__(self) -> str:
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise LazyShellBinding.  str   through a consuming regression::

                python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return str(self.load())

    def __repr__(self) -> str:
        """
        Perform the repr operation under explicit file-format and conversion rules.

        Example:
            Exercise LazyShellBinding.  repr   through a consuming regression::

                python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        label = object.__getattribute__(self, "_label")
        if not bool(object.__getattribute__(self, "_loaded")):
            return "<lazy {}: access attributes or call .load() to hydrate>".format(label)
        return repr(object.__getattribute__(self, "_value"))


def _realize(value: object) -> object:
    """
    Perform the realize operation under explicit file-format and conversion rules.

    Example:
        Exercise  realize through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param value: Value normalized, stored, formatted or returned.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(value, LazyShellBinding):
        return value.load()
    return value


def _existing_path(raw: str | None) -> Path | None:
    """
    Perform the existing path operation under explicit file-format and conversion rules.

    Example:
        Exercise  existing path through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param raw: Value supplied for raw under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    text = str(raw or "").strip()
    if not text:
        return None
    path = Path(text).expanduser()
    if not path.is_absolute():
        path = (REPO_ROOT / path).resolve()
    return path if path.is_file() else None


def _candidate_data_roots(explicit: str | None) -> list[Path]:
    """
    Perform the candidate data roots operation under explicit file-format and conversion rules.

    Example:
        Exercise  candidate data roots through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param explicit: Value supplied for explicit under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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
    """
    Perform the bundle candidates operation under explicit file-format and conversion rules.

    Example:
        Exercise  bundle candidates through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param data_root: Value supplied for data root under the utility contract.
    :param bundle_name: Value supplied for bundle name under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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
    """
    Perform the resolve isfdb database operation under explicit file-format and conversion rules.

    Example:
        Exercise resolve isfdb database through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param database: Value supplied for database under the utility contract.
    :param data_root: Value supplied for data root under the utility contract.
    :param bundle_name: Value supplied for bundle name under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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


def open_database(
    database_path: Path,
    db_type: str,
    *,
    enable_storage_manager: bool = False,
    enable_maintenance: bool = False,
    repair_bootstrap_rows: bool = False,
) -> Database:
    """
    Perform the open database operation under explicit file-format and conversion rules.

    Example:
        Exercise open database through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param database_path: Value supplied for database path under the utility contract.
    :param db_type: Value supplied for db type under the utility contract.
    :param enable_storage_manager: Value supplied for enable storage manager under the
        utility contract.
    :param enable_maintenance: Value supplied for enable maintenance under the utility
        contract.
    :param repair_bootstrap_rows: Value supplied for repair bootstrap rows under the
        utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from LiuXin_alpha.databases.database import Database

    return Database(
        metadata={"database_path": str(database_path)},
        db_type=db_type,
        create=False,
        backup=False,
        enable_storage_manager=bool(enable_storage_manager),
        enable_maintenance=bool(enable_maintenance),
        repair_bootstrap_rows=bool(repair_bootstrap_rows),
    )


def first_item_id(db: "Database") -> int:
    """
    Perform the first item id operation under explicit file-format and conversion rules.

    Example:
        Exercise first item id through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param db: Value supplied for db under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    rows = db.get_all_rows("items")
    first = next(iter(rows), None)
    if first is None:
        raise RuntimeError("The ISFDB test database has no item rows.")
    return int(first.row_dict.get("item_id") or first.row_id)


def quiet_call(func: Callable[..., T], *args: object, quiet: bool = True, **kwargs: object) -> T:
    """
    Perform the quiet call operation under explicit file-format and conversion rules.

    Example:
        Exercise quiet call through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param func: Value supplied for func under the utility contract.
    :param args: Positional values forwarded to the compatibility implementation.
    :param quiet: Value supplied for quiet under the utility contract.
    :param kwargs: Keyword values forwarded to the compatibility implementation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not quiet:
        return func(*args, **kwargs)
    with open(os.devnull, "w", encoding="utf-8") as sink:
        with redirect_stdout(sink), redirect_stderr(sink):
            return func(*args, **kwargs)


def build_arg_parser() -> argparse.ArgumentParser:
    """
    Perform the build arg parser operation under explicit file-format and conversion rules.

    Example:
        Exercise build arg parser through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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
    parser.add_argument(
        "--enable-storage-manager",
        action="store_true",
        help="Bootstrap the storage manager at startup. Slower; not needed for metadata hydration.",
    )
    parser.add_argument(
        "--enable-maintenance",
        action="store_true",
        help="Start the background maintenance service. Slower; not needed for read-only metadata inspection.",
    )
    parser.add_argument(
        "--repair-bootstrap-rows",
        action="store_true",
        help="Run startup rating/null-row repairs before opening the shell. Slower and may write to the DB.",
    )
    parser.add_argument("--item-id", type=int, default=0, help="Sample item id. Defaults to the first item row.")
    parser.add_argument("--lazy", action="store_true", help="Use the lazy metadata hydrator.")
    parser.add_argument(
        "--load-lazy",
        action="append",
        default=[],
        metavar="FIELDS",
        help=(
            "Comma-separated lazy fields to materialize after hydration, or 'all'. "
            "Can be repeated. Applies to lazy metadata objects."
        ),
    )
    parser.add_argument(
        "--no-sample",
        action="store_true",
        help="Do not bind md/lazy_md/liuxin_md/calibre_md sample helpers.",
    )
    parser.add_argument(
        "--eager-sample",
        action="store_true",
        help="Hydrate md/lazy_md/liuxin_md/calibre_md before opening the shell. Slower startup.",
    )
    parser.add_argument(
        "--eager-open",
        action="store_true",
        help="Open the database and resolve the default item before opening the shell. Slower startup.",
    )
    parser.add_argument("--no-quiet", action="store_true", help="Do not suppress noisy relation logs during hydration.")
    parser.add_argument("--no-console", action="store_true", help="Prepare the objects, print a summary, and exit.")
    return parser


def _parse_lazy_fields(raw_values: list[str]) -> tuple[str, ...] | None:
    """
    Parse lazy fields under the format's safety and compatibility rules.

    Example:
        Exercise  parse lazy fields through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param raw_values: Value supplied for raw values under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    fields: list[str] = []
    for raw_value in raw_values:
        for token in str(raw_value or "").split(","):
            field = token.strip()
            if not field:
                continue
            if field.lower() == "all":
                return None
            fields.append(field)
    return tuple(fields)


def main(argv: list[str] | None = None) -> int:
    """
    Perform the main operation under explicit file-format and conversion rules.

    Example:
        Exercise main through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param argv: Value supplied for argv under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    args = build_arg_parser().parse_args(argv)
    database_path = resolve_isfdb_database(
        database=args.database,
        data_root=args.data_root,
        bundle_name=str(args.bundle_name or "").strip(),
    )
    quiet = not bool(args.no_quiet)
    db: object | None = None
    try:
        db = LazyShellBinding(
            "db",
            lambda: quiet_call(
                open_database,
                database_path,
                str(args.db_type),
                enable_storage_manager=bool(args.enable_storage_manager),
                enable_maintenance=bool(args.enable_maintenance),
                repair_bootstrap_rows=bool(args.repair_bootstrap_rows),
                quiet=quiet,
            ),
        )
        eager_hydrator = LazyShellBinding(
            "eager_hydrator",
            lambda: _metadata_container_classes()["LiuXinWEMIMetadataHydrator"](_realize(db)),
        )
        lazy_hydrator = LazyShellBinding(
            "lazy_hydrator",
            lambda: _metadata_container_classes()["LazyLiuXinWEMIMetadataHydrator"](_realize(db)),
        )
        metadata_writer = LazyShellBinding(
            "metadata_writer",
            lambda: _metadata_container_classes()["LiuXinWEMIMetadataWriter"](_realize(db)),
        )
        hydrator = lazy_hydrator if bool(args.lazy) else eager_hydrator
        hydrator_class_name = "LazyLiuXinWEMIMetadataHydrator" if bool(args.lazy) else "LiuXinWEMIMetadataHydrator"
        lazy_fields_to_load = _parse_lazy_fields(args.load_lazy)
        item_id = (
            int(args.item_id)
            if int(args.item_id) > 0
            else LazyShellBinding("item_id", lambda: first_item_id(_realize(db)))
        )

        if bool(args.eager_open):
            _realize(db)
            _realize(item_id)

        def resolve_item_id(target_item_id: object | None = None) -> int:
            """
            Perform the resolve item id operation under explicit file-format and conversion rules.

            Example:
                Exercise main.resolve item id through a consuming regression::

                    python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


            :param target_item_id: Value supplied for target item id under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if target_item_id is None:
                return int(_realize(item_id))
            return int(_realize(target_item_id))

        def maybe_load_lazy_fields(metadata):
            """
            Perform the maybe load lazy fields operation under explicit file-format and conversion rules.

            Example:
                Exercise main.maybe load lazy fields through a consuming regression::

                    python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


            :param metadata: Value supplied for metadata under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            force_hydrate = getattr(metadata, "force_hydrate", None)
            if args.load_lazy and callable(force_hydrate):
                force_hydrate(fields=lazy_fields_to_load)
            return metadata

        def get_eager_md(target_item_id: object | None = None):
            """
            Return eager md under the format's safety and compatibility rules.

            Example:
                Exercise main.get eager md through a consuming regression::

                    python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


            :param target_item_id: Value supplied for target item id under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return quiet_call(
                _realize(eager_hydrator).get_liuxin_wemi_metadata,
                item_id=resolve_item_id(target_item_id),
                quiet=quiet,
            )

        def get_lazy_md(target_item_id: object | None = None):
            """
            Return lazy md under the format's safety and compatibility rules.

            Example:
                Exercise main.get lazy md through a consuming regression::

                    python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


            :param target_item_id: Value supplied for target item id under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            metadata = quiet_call(
                _realize(lazy_hydrator).get_liuxin_wemi_metadata,
                item_id=resolve_item_id(target_item_id),
                quiet=quiet,
            )
            return maybe_load_lazy_fields(metadata)

        def get_md(target_item_id: object | None = None):
            """
            Return md under the format's safety and compatibility rules.

            Example:
                Exercise main.get md through a consuming regression::

                    python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


            :param target_item_id: Value supplied for target item id under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if bool(args.lazy):
                return get_lazy_md(target_item_id)
            return get_eager_md(target_item_id)

        def get_liuxin_md(target_item_id: object | None = None):
            """
            Return liuxin md under the format's safety and compatibility rules.

            Example:
                Exercise main.get liuxin md through a consuming regression::

                    python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


            :param target_item_id: Value supplied for target item id under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return get_md(target_item_id).as_liuxin_metadata()

        def get_calibre_md(target_item_id: object | None = None):
            """
            Return calibre md under the format's safety and compatibility rules.

            Example:
                Exercise main.get calibre md through a consuming regression::

                    python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


            :param target_item_id: Value supplied for target item id under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return get_md(target_item_id).as_calibre_metadata()

        def get_lazy_liuxin_md(target_item_id: object | None = None):
            """
            Return lazy liuxin md under the format's safety and compatibility rules.

            Example:
                Exercise main.get lazy liuxin md through a consuming regression::

                    python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


            :param target_item_id: Value supplied for target item id under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return get_lazy_md(target_item_id).as_liuxin_metadata()

        def get_lazy_calibre_md(target_item_id: object | None = None):
            """
            Return lazy calibre md under the format's safety and compatibility rules.

            Example:
                Exercise main.get lazy calibre md through a consuming regression::

                    python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


            :param target_item_id: Value supplied for target item id under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return get_lazy_md(target_item_id).as_calibre_metadata()

        def write_md(metadata=None, **kwargs):
            """
            Write md under the format's safety and compatibility rules.

            Example:
                Exercise main.write md through a consuming regression::

                    python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


            :param metadata: Value supplied for metadata under the utility contract.
            :param kwargs: Keyword values forwarded to the compatibility implementation.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            target_metadata = _realize(metadata) if metadata is not None else get_md()
            kwargs.setdefault("item_id", resolve_item_id())
            return _realize(metadata_writer).write(target_metadata, **kwargs)

        namespace: dict[str, object] = {
            "db": db,
            "database_path": database_path,
            "hydrator": hydrator,
            "eager_hydrator": eager_hydrator,
            "lazy_hydrator": lazy_hydrator,
            "metadata_writer": metadata_writer,
            "item_id": item_id,
            "get_md": get_md,
            "get_eager_md": get_eager_md,
            "get_lazy_md": get_lazy_md,
            "get_liuxin_md": get_liuxin_md,
            "get_calibre_md": get_calibre_md,
            "get_lazy_liuxin_md": get_lazy_liuxin_md,
            "get_lazy_calibre_md": get_lazy_calibre_md,
            "write_md": write_md,
            "LiuXinWEMIMetadataHydrator": LazyShellBinding(
                "LiuXinWEMIMetadataHydrator",
                lambda: _metadata_container_classes()["LiuXinWEMIMetadataHydrator"],
            ),
            "LazyLiuXinWEMIMetadataHydrator": LazyShellBinding(
                "LazyLiuXinWEMIMetadataHydrator",
                lambda: _metadata_container_classes()["LazyLiuXinWEMIMetadataHydrator"],
            ),
            "LiuXinWEMIMetadataWriter": LazyShellBinding(
                "LiuXinWEMIMetadataWriter",
                lambda: _metadata_container_classes()["LiuXinWEMIMetadataWriter"],
            ),
        }

        sample_mode = "none"
        if not bool(args.no_sample) and bool(args.eager_sample):
            md = get_md()
            namespace["md"] = md
            namespace["liuxin_md"] = md.as_liuxin_metadata()
            namespace["calibre_md"] = md.as_calibre_metadata()
            namespace["lazy_md"] = get_lazy_md()
            sample_mode = "eager"
        elif not bool(args.no_sample):
            md = LazyShellBinding("md", lambda: get_md())
            namespace["md"] = md
            namespace["liuxin_md"] = LazyShellBinding(
                "liuxin_md",
                lambda: _realize(md).as_liuxin_metadata(),
            )
            namespace["calibre_md"] = LazyShellBinding(
                "calibre_md",
                lambda: _realize(md).as_calibre_metadata(),
            )
            namespace["lazy_md"] = LazyShellBinding("lazy_md", lambda: get_lazy_md())
            sample_mode = "lazy"

        print(f"Database: {database_path}")
        print(f"Hydrator: {hydrator_class_name}")
        db_is_loaded = bool(getattr(db, "is_loaded", False))
        if db_is_loaded:
            resolved_db = _realize(db)
            print(f"Items: {resolved_db.get_record_count('items')}")
            print(f"Sample item_id: {resolve_item_id()}")
        else:
            print("Database handle: lazy; call db.load(), get_md(), or int(item_id) to open it.")
            if int(args.item_id) > 0:
                print(f"Sample item_id: {item_id}")
            else:
                print("Sample item_id: lazy first item; call int(item_id) to resolve it.")
        if sample_mode == "eager":
            print(f"Sample title: {getattr(namespace['md'], 'title', None)}")
            lazy_fields = getattr(namespace["md"], "lazy_fields", None)
            if callable(lazy_fields):
                print(f"Lazy fields: {', '.join(lazy_fields()) or '<none>'}")
            lazy_sample_fields = getattr(namespace.get("lazy_md"), "lazy_fields", None)
            if namespace.get("lazy_md") is not namespace["md"] and callable(lazy_sample_fields):
                print(f"Lazy sample fields: {', '.join(lazy_sample_fields()) or '<none>'}")
        elif sample_mode == "lazy":
            print("Sample metadata: lazy bindings; access md.title or call md.load() to hydrate.")
        print(
            "Bound names: db, hydrator, eager_hydrator, lazy_hydrator, metadata_writer, "
            "item_id, get_md, get_eager_md, get_lazy_md, get_liuxin_md, "
            "get_calibre_md, get_lazy_liuxin_md, get_lazy_calibre_md, write_md"
        )
        if "md" in namespace:
            print("Also bound: md, lazy_md, liuxin_md, calibre_md")

        if bool(args.no_console):
            return 0

        banner = (
            "ISFDB metadata shell\n"
            "Examples:\n"
            "  md.load()  # when using the default lazy sample binding\n"
            "  md = get_md(item_id)\n"
            "  db.load()\n"
            "  lazy_md = get_lazy_md(item_id)\n"
            "  md.database_ids\n"
            "  lazy_md.lazy_fields()\n"
            "  lazy_md.hydrate_field('tags')\n"
            "  write_md(md, fields=('tags', 'labels'))\n"
            "  write_md(calibre_md, fields=('tags',), item_id=item_id)\n"
            "  md.work, md.expression, md.manifestation, md.item\n"
            "  calibre_md = get_calibre_md(item_id)\n"
            "Call db.close() when done if you exit unusually."
        )
        code.interact(banner=banner, local=namespace)
        return 0
    finally:
        if db is not None:
            if isinstance(db, LazyShellBinding):
                if db.is_loaded:
                    _realize(db).close()
            elif hasattr(db, "close"):
                db.close()


if __name__ == "__main__":
    raise SystemExit(main())
