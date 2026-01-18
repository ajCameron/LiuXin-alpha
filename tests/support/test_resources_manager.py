"""Test resource provisioning for LiuXin_alpha.

This is a modernized port of the old LiuXin `LiuXin_tests.test_setup` helpers.

The intent is to let tests request a *named* resource (e.g. a legacy-style
``test_db_13``) and receive a fresh, writable copy.

For test databases, provisioning is now provider-driven:

* Copy from prebuilt bundles when available (from configured directories).
* Import a builder module for a given name (opt-in, avoids huge binaries).
* Fall back to small built-in generators for a couple of common DBs.

Providers are registered via a small API so projects can add more without
editing this file.
"""

from __future__ import annotations

import os
import re
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, Mapping, Optional, Protocol, Sequence

import importlib
import pkgutil


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ProvisionedTestDatabase:
    """A concrete test database instance provisioned for a single test."""

    name: str
    root: Path
    db_path: Path


Builder = Callable[[Path], None]


@dataclass(frozen=True)
class TestDatabaseSpec:
    """Specification for a named test database."""

    name: str
    builder: Builder
    description: str = ""

    @property
    def bundle_dirname(self) -> str:
        return self.name

    @property
    def db_filename(self) -> str:
        # Keep compatibility with the historic naming.
        return f"{self.name}.test_db"


class TestResourcesManager:
    """Manages cached templates and per-test copies of test resources."""

    def __init__(
        self,
        *,
        cache_dir: Path,
        prebuilt_dir: Optional[Path] = None,
        regenerate: bool = False,
        specs: Optional[Mapping[str, TestDatabaseSpec]] = None,
        db_registry: Optional["TestDatabaseRegistry"] = None,
    ) -> None:
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        self.prebuilt_dir = Path(prebuilt_dir) if prebuilt_dir is not None else _env_prebuilt_dir()
        self.regenerate = regenerate

        self._specs: Dict[str, TestDatabaseSpec] = dict(specs or default_test_database_specs())

        # Provider-driven database resolution.
        self._db_registry = db_registry or default_test_database_registry(
            specs=self._specs,
            prebuilt_dir=self.prebuilt_dir,
        )

        # A distinct subtree makes it easy to nuke caches.
        self._templates_root = self.cache_dir / "templates" / "test_databases"
        self._templates_root.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    def available_test_databases(self) -> list[str]:
        discovered = set(self._db_registry.list_available())
        return sorted(discovered, key=_package_good_sort_key)

    def get_spec(self, name: str) -> TestDatabaseSpec:
        try:
            return self._specs[name]
        except KeyError as e:
            raise KeyError(
                f"Unknown built-in test database spec '{name}'. "
                f"Available built-ins: {sorted(self._specs.keys(), key=_package_good_sort_key)}"
            ) from e

    # ------------------------------------------------------------------
    # Provisioning
    # ------------------------------------------------------------------

    def provision_test_database(self, *, name: str, dst_dir: Path) -> ProvisionedTestDatabase:
        """Provision a fresh writable copy of *name* inside *dst_dir*."""

        dst_dir = Path(dst_dir)
        dst_dir.mkdir(parents=True, exist_ok=True)

        template_bundle, db_filename = self._ensure_template_bundle(name)

        provision_root = dst_dir / name
        if provision_root.exists():
            shutil.rmtree(provision_root)
        shutil.copytree(template_bundle, provision_root)

        db_path = provision_root / db_filename
        if not db_path.exists():
            raise FileNotFoundError(f"Provisioned bundle missing db file: {db_path}")

        return ProvisionedTestDatabase(name=name, root=provision_root, db_path=db_path)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _ensure_template_bundle(self, name: str) -> tuple[Path, str]:
        """Ensure a cached template bundle for *name*.

        Returns (bundle_path, db_filename).
        """

        bundle = self._templates_root / name
        db_filename = f"{name}.test_db"
        db_path = bundle / db_filename
        lock_dir = self._templates_root / f".{name}.lock"

        if not self.regenerate and db_path.exists():
            return bundle, db_filename

        # Best-effort cross-platform lock using mkdir.
        _acquire_dir_lock(lock_dir)
        try:
            # Re-check under the lock.
            if not self.regenerate and db_path.exists():
                return bundle, db_filename

            if bundle.exists():
                shutil.rmtree(bundle)
            bundle.mkdir(parents=True, exist_ok=True)

            provider = self._db_registry.resolve(name)

            # Provider populates the bundle and returns the db filename.
            produced_db = provider.provide_template(name=name, bundle_dir=bundle)
            produced_db = Path(produced_db)
            if not produced_db.is_absolute():
                produced_db = bundle / produced_db

            if not produced_db.exists():
                raise FileNotFoundError(
                    f"Provider {provider!r} did not create expected DB for '{name}': {produced_db}"
                )

            # Normalize to our conventional filename if needed.
            if produced_db.name != db_filename:
                shutil.copy2(produced_db, db_path)
            return bundle, db_filename
        finally:
            _release_dir_lock(lock_dir)


# ---------------------------------------------------------------------------
# Provider registry
# ---------------------------------------------------------------------------


class TestDatabaseProviderAPI(Protocol):
    """API for providing named test databases.

    Providers may create a DB by copying a prebuilt file, importing a builder
    module, or generating a small DB on demand.
    """

    def list_available(self) -> Iterable[str]:
        """Return names this provider can supply (best-effort)."""

    def can_provide(self, name: str) -> bool:
        """Fast predicate: can this provider supply *name*?"""

    def provide_template(self, *, name: str, bundle_dir: Path) -> Path:
        """Populate *bundle_dir* with required files.

        Returns the DB path (absolute or relative to bundle_dir).
        """


class TestDatabaseRegistry:
    """A simple provider registry with resolution rules."""

    def __init__(self, providers: Sequence[TestDatabaseProviderAPI]) -> None:
        self._providers = list(providers)

    def list_available(self) -> Iterable[str]:
        out: set[str] = set()
        for p in self._providers:
            try:
                out.update(p.list_available())
            except Exception:
                # Discovery should never be fatal to the test suite.
                continue
        return out

    def resolve(self, name: str) -> TestDatabaseProviderAPI:
        for p in self._providers:
            try:
                if p.can_provide(name):
                    return p
            except Exception:
                continue
        raise KeyError(
            f"No provider could supply test database '{name}'. "
            f"Discovered: {sorted(self.list_available(), key=_package_good_sort_key)}"
        )


def default_test_database_registry(
    *,
    specs: Mapping[str, TestDatabaseSpec],
    prebuilt_dir: Optional[Path],
) -> TestDatabaseRegistry:
    """Default provider chain (highest priority first)."""

    providers: list[TestDatabaseProviderAPI] = []

    data_dir = _default_liuxin_data_dir()
    prebuilt_dirs: list[Path] = []
    if prebuilt_dir is not None:
        prebuilt_dirs.append(prebuilt_dir)
    if data_dir is not None:
        # Conventional location for DB bundles.
        candidate = data_dir / "test_databases"
        if candidate.exists():
            prebuilt_dirs.append(candidate)
        # Also allow <LiuXin_data>/... direct.
        prebuilt_dirs.append(data_dir)

    if prebuilt_dirs:
        providers.append(PrebuiltDirectoryDatabaseProvider(prebuilt_dirs=prebuilt_dirs))

    providers.append(ImportedModuleDatabaseProvider(prefixes=_env_module_prefixes()))
    providers.append(BuiltinSpecDatabaseProvider(specs=specs))
    return TestDatabaseRegistry(providers)


class PrebuiltDirectoryDatabaseProvider:
    """Provides databases by copying from prebuilt directories."""

    def __init__(self, *, prebuilt_dirs: Sequence[Path]) -> None:
        self._dirs = [Path(p) for p in prebuilt_dirs if Path(p).exists()]

    def list_available(self) -> Iterable[str]:
        out: set[str] = set()
        for root in self._dirs:
            # Directory bundles: <root>/<name>/<name>.test_db
            try:
                for child in root.iterdir():
                    if not child.is_dir():
                        continue
                    name = child.name
                    candidate = child / f"{name}.test_db"
                    if candidate.is_file():
                        out.add(name)
            except Exception:
                pass

            # Single files: <root>/<name>.test_db
            try:
                for file in root.glob("*.test_db"):
                    out.add(file.stem)
            except Exception:
                pass
        return out

    def can_provide(self, name: str) -> bool:
        return any(_find_prebuilt_db(root, name) is not None for root in self._dirs)

    def provide_template(self, *, name: str, bundle_dir: Path) -> Path:
        bundle_dir.mkdir(parents=True, exist_ok=True)
        for root in self._dirs:
            found = _find_prebuilt_db(root, name)
            if found is None:
                continue
            # If this is a directory bundle, copy the entire directory.
            if found.is_dir():
                shutil.copytree(found, bundle_dir, dirs_exist_ok=True)
                preferred = bundle_dir / f"{name}.test_db"
                if preferred.exists():
                    return Path(preferred.name)
                dbs = sorted(bundle_dir.glob("*.test_db"))
                if len(dbs) == 1:
                    return Path(dbs[0].name)
                raise FileNotFoundError(
                    f"Prebuilt bundle for '{name}' copied but no unique .test_db file found in {bundle_dir}"
                )
            # Else it's a single file.
            dst = bundle_dir / f"{name}.test_db"
            shutil.copy2(found, dst)
            return dst
        raise FileNotFoundError(f"No prebuilt DB found for '{name}' in {self._dirs}")


class ImportedModuleDatabaseProvider:
    """Provides databases by importing a builder module named after the DB."""

    def __init__(self, *, prefixes: Sequence[str]) -> None:
        # Prefixes are module packages under which submodules are named after DBs.
        self._prefixes = [p for p in prefixes if p]

    def list_available(self) -> Iterable[str]:
        out: set[str] = set()
        for prefix in self._prefixes:
            try:
                pkg = importlib.import_module(prefix)
            except Exception:
                continue
            pkg_path = getattr(pkg, "__path__", None)
            if not pkg_path:
                continue
            try:
                for mod in pkgutil.iter_modules(pkg_path):
                    out.add(mod.name)
            except Exception:
                continue
        return out

    def can_provide(self, name: str) -> bool:
        for prefix in self._prefixes:
            try:
                importlib.import_module(f"{prefix}.{name}")
                return True
            except Exception:
                continue
        return False

    def provide_template(self, *, name: str, bundle_dir: Path) -> Path:
        bundle_dir.mkdir(parents=True, exist_ok=True)
        last_exc: Optional[BaseException] = None
        for prefix in self._prefixes:
            modname = f"{prefix}.{name}"
            try:
                module = importlib.import_module(modname)
            except Exception as e:
                last_exc = e
                continue

            # Preferred: module.populate_bundle(bundle_dir)
            populate = getattr(module, "populate_bundle", None)
            if callable(populate):
                populate(Path(bundle_dir))
                preferred = Path(bundle_dir) / f"{name}.test_db"
                if preferred.exists():
                    return Path(preferred.name)
                dbs = sorted(Path(bundle_dir).glob("*.test_db"))
                if len(dbs) == 1:
                    return Path(dbs[0].name)
                raise FileNotFoundError(
                    f"populate_bundle() for '{name}' ran but did not create a unique .test_db in {bundle_dir}"
                )

            # Common builder names.
            builder = (
                getattr(module, "build", None)
                or getattr(module, "build_database", None)
                or getattr(module, "build_test_database", None)
            )
            if callable(builder):
                db_path = Path(bundle_dir) / f"{name}.test_db"
                builder(Path(db_path))
                return db_path

            raise AttributeError(
                f"Module {modname!r} found but has neither populate_bundle() nor a build*() function"
            )

        raise ImportError(f"Could not import builder module for '{name}'. Last error: {last_exc}")


class BuiltinSpecDatabaseProvider:
    """Provides databases from a mapping of built-in specs."""

    def __init__(self, *, specs: Mapping[str, TestDatabaseSpec]) -> None:
        self._specs = dict(specs)

    def list_available(self) -> Iterable[str]:
        return list(self._specs.keys())

    def can_provide(self, name: str) -> bool:
        return name in self._specs

    def provide_template(self, *, name: str, bundle_dir: Path) -> Path:
        spec = self._specs[name]
        bundle_dir.mkdir(parents=True, exist_ok=True)
        tmp_db = bundle_dir / f".{name}.test_db.building"
        if tmp_db.exists():
            tmp_db.unlink()
        spec.builder(tmp_db)
        final = bundle_dir / f"{name}.test_db"
        tmp_db.replace(final)
        return final


# ---------------------------------------------------------------------------
# Default database specs (small, but easy to extend)
# ---------------------------------------------------------------------------


def default_test_database_specs() -> Dict[str, TestDatabaseSpec]:
    return {
        "test_db_0": TestDatabaseSpec(
            name="test_db_0",
            builder=_build_test_db_0_minimal,
            description="Minimal database with one title row.",
        ),
        "test_db_13": TestDatabaseSpec(
            name="test_db_13",
            builder=_build_test_db_13_blank,
            description="Schema + helper tables only (keeps required null rows).",
        ),
    }


def _build_test_db_0_minimal(db_path: Path) -> None:
    """Create a tiny but valid database with one title row.

    We intentionally do *not* rely on the higher-level Database class here,
    since the legacy APSW-backed driver is optional.
    """

    import sqlite3

    from LiuXin_alpha.databases.database_driver_plugins.SQLite.database_generator.database_generator import (
        create_new_database,
    )

    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    try:
        _register_sqlite_test_functions(conn)
        create_new_database(conn)
        _ensure_required_null_rows(conn)

        # Insert a single, constraint-compliant title row.
        _insert_minimal_row(conn, table="titles", preferred_text_value="Test Book")
        conn.commit()
    finally:
        conn.close()


def _build_test_db_13_blank(db_path: Path) -> None:
    """Create a schema-only database.

    Historically `test_db_13` was used as a "blank" DB. For LiuXin_alpha we
    keep this lightweight: schema created + required null rows present.
    """

    import sqlite3

    from LiuXin_alpha.databases.database_driver_plugins.SQLite.database_generator.database_generator import (
        create_new_database,
    )

    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    try:
        _register_sqlite_test_functions(conn)
        create_new_database(conn)
        _ensure_required_null_rows(conn)
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


_DB_NAME_RE = re.compile(r"^test_db_(\d+)$")


def _package_good_sort_key(name: str) -> tuple[int, str]:
    m = _DB_NAME_RE.match(name)
    if m:
        return (int(m.group(1)), name)
    return (10**9, name)


def _env_prebuilt_dir() -> Optional[Path]:
    """Optional folder containing prebuilt test DB bundles."""

    v = os.environ.get("LIUXIN_TEST_DATABASES_DIR")
    if not v:
        return None
    p = Path(v).expanduser()
    return p if p.exists() else None

def _repo_root() -> Path:
    # tests/_support/test_resources_manager.py -> repo root is two parents up.
    return Path(__file__).resolve().parents[2]


def _default_liuxin_data_dir() -> Optional[Path]:
    """Locate a `LiuXin_data` directory (best-effort).

    Resolution order:
    1) $LIUXIN_DATA_DIR if it exists.
    2) <repo root>/LiuXin_data if it exists.
    """

    env = os.environ.get("LIUXIN_DATA_DIR")
    if env:
        p = Path(env).expanduser()
        if p.exists():
            return p

    candidate = _repo_root() / "LiuXin_data"
    if candidate.exists():
        return candidate
    return None


def _env_module_prefixes() -> list[str]:
    """Builder module prefixes used by ImportedModuleDatabaseProvider.

    Defaults to `tests._support.test_databases` and can be extended via:
    $LIUXIN_TEST_DATABASE_BUILDER_PREFIXES (separator: ; , :)
    """

    defaults = ["tests._support.test_databases"]
    raw = os.environ.get("LIUXIN_TEST_DATABASE_BUILDER_PREFIXES", "")
    extra: list[str] = []
    for part in re.split(r"[;,:]", raw):
        part = part.strip()
        if part:
            extra.append(part)

    out: list[str] = []
    for p in [*defaults, *extra]:
        if p not in out:
            out.append(p)
    return out


def _find_prebuilt_db(root: Path, name: str) -> Optional[Path]:
    """Find a prebuilt DB by *name* under *root*.

    Supports:
    * Directory bundle: <root>/<name>/... (returns the directory)
    * Single file: <root>/<name>.test_db (returns the file)
    """

    root = Path(root)

    candidate_dir = root / name
    if candidate_dir.is_dir():
        # Allow any bundle dir; provider will validate presence of .test_db.
        return candidate_dir

    candidate_file = root / f"{name}.test_db"
    if candidate_file.is_file():
        return candidate_file

    return None


def _acquire_dir_lock(lock_dir: Path, *, timeout_s: float = 30.0) -> None:
    start = time.time()
    while True:
        try:
            lock_dir.mkdir(parents=False, exist_ok=False)
            return
        except FileExistsError:
            if (time.time() - start) > timeout_s:
                raise TimeoutError(f"Timed out waiting for lock: {lock_dir}")
            time.sleep(0.05)


def _release_dir_lock(lock_dir: Path) -> None:
    try:
        lock_dir.rmdir()
    except FileNotFoundError:
        return


def _table_exists(conn, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1;", (table,)
    ).fetchone()
    return row is not None


def _table_info(conn, table: str):
    return conn.execute(f"PRAGMA table_info({table});").fetchall()


def _register_sqlite_test_functions(conn) -> None:
    """Register sqlite functions referenced by triggers/views.

    The historical schema expects a couple of custom functions (implemented
    by the APSW driver in production). For test DB generation using the
    stdlib sqlite3 module we stub them out.
    """

    # Used by `update_callback_on_titles` trigger.
    conn.create_function("DIRTY_RECORD", 2, lambda _table, _row_id: 0)


def _detect_pk_column(conn, table: str) -> Optional[str]:
    for _cid, name, _typ, _notnull, _dflt, pk in _table_info(conn, table):
        if int(pk) == 1:
            return str(name)
    return None


def _ensure_required_null_rows(conn) -> None:
    """Ensure historic required null rows exist.

    LiuXin uses id=0 in some tables as a "null" record for link tables.
    """

    for table, preferred_text_col in (
        ("series", "series"),
        ("publishers", "publisher"),
    ):
        if not _table_exists(conn, table):
            continue

        pk_col = _detect_pk_column(conn, table)
        if pk_col is None:
            continue

        cols = {str(name) for _cid, name, *_rest in _table_info(conn, table)}
        text_col = preferred_text_col if preferred_text_col in cols else None

        existing = conn.execute(
            f"SELECT {pk_col} FROM {table} WHERE {pk_col} = 0 LIMIT 1;"
        ).fetchone()
        if existing is None:
            if text_col is None:
                conn.execute(f"INSERT INTO {table} ({pk_col}) VALUES (0);")
            else:
                conn.execute(
                    f"INSERT INTO {table} ({pk_col}, {text_col}) VALUES (0, NULL);"
                )
        else:
            if text_col is not None:
                conn.execute(
                    f"UPDATE {table} SET {text_col} = NULL WHERE {pk_col} = 0;"
                )


def _default_value_for_type(col_name: str, col_type: str, preferred_text_value: str):
    n = col_name.lower()
    t = (col_type or "").upper()

    # Simple heuristics that satisfy typical NOT NULL constraints.
    if "UUID" in t or n.endswith("_uuid"):
        return "00000000-0000-0000-0000-000000000000"
    if "DATE" in n or "TIME" in n:
        return "2000-01-01 00:00:00"

    if "INT" in t:
        return 0
    if "REAL" in t or "FLOA" in t or "DOUB" in t:
        return 0.0
    if "BLOB" in t:
        return b""
    if "CHAR" in t or "TEXT" in t or "CLOB" in t:
        # Prefer a stable, readable value for common name fields.
        if n in {"title", "series", "publisher", "name"}:
            return preferred_text_value
        return ""
    # Unknown type: fall back to empty string.
    return ""


def _insert_minimal_row(conn, *, table: str, preferred_text_value: str) -> None:
    """Insert a single row into *table* satisfying NOT NULL + no-default columns."""

    cols = _table_info(conn, table)
    required: list[str] = []
    values: list[object] = []

    for _cid, name, col_type, notnull, dflt, pk in cols:
        name = str(name)
        if int(pk) == 1:
            continue
        if int(notnull) == 1 and dflt is None:
            required.append(name)
            values.append(_default_value_for_type(name, str(col_type), preferred_text_value))

    if not required:
        # If there are no strict requirements, try the lightest possible insert.
        conn.execute(f"INSERT INTO {table} DEFAULT VALUES;")
        return

    placeholders = ",".join(["?"] * len(required))
    cols_sql = ",".join(required)
    conn.execute(f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders});", values)
