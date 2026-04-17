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
import sqlite3
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, Mapping, Optional, Protocol, Sequence

import importlib
import pkgutil

from LiuXin_alpha.databases.bootstrap_constants import AGENTS_NULL_CANONICAL_NAME


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ProvisionedTestDatabase:
    """A concrete test database instance provisioned for a single test."""

    name: str
    root: Path
    db_path: Path


@dataclass(frozen=True)
class ProvisionedTestAssets:
    """A concrete set of copied test assets provisioned for a single test."""

    root: Path
    paths: tuple[Path, ...]


Builder = Callable[[Path], None]
TEST_DB_BUNDLE_ROOT_TOKEN = "__LIUXIN_TEST_BUNDLE_ROOT__"


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

    def provision_named_test_database(self, *, name: str, dst_dir: Path) -> ProvisionedTestDatabase:
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

        _rewrite_bundle_root_tokens(db_path=db_path, provision_root=provision_root)

        return ProvisionedTestDatabase(name=name, root=provision_root, db_path=db_path)

    def provision_test_books(self, *, dst_dir: Path, names: Optional[Sequence[str]] = None) -> ProvisionedTestAssets:
        """Provision test ebook files into *dst_dir*."""

        source_dir = _resolve_test_asset_source(
            env_key="LIUXIN_TEST_BOOKS_DIR",
            fallback_dirnames=("test_books", "md_test_books", "md_test_files"),
        )
        return _provision_test_assets(source_dir=source_dir, dst_dir=Path(dst_dir) / "test_books", names=names)

    def provision_test_covers(self, *, dst_dir: Path, names: Optional[Sequence[str]] = None) -> ProvisionedTestAssets:
        """Provision test cover image files into *dst_dir*."""

        source_dir = _resolve_test_asset_source(
            env_key="LIUXIN_TEST_COVERS_DIR",
            fallback_dirnames=("test_covers", "covers"),
        )
        return _provision_test_assets(source_dir=source_dir, dst_dir=Path(dst_dir) / "test_covers", names=names)

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
                    if mod.name.startswith("_"):
                        continue
                    try:
                        module = importlib.import_module(f"{prefix}.{mod.name}")
                    except Exception:
                        continue
                    if _module_has_supported_db_entrypoint(module):
                        out.add(mod.name)
            except Exception:
                continue
        return out

    def can_provide(self, name: str) -> bool:
        for prefix in self._prefixes:
            try:
                module = importlib.import_module(f"{prefix}.{name}")
                if _module_has_supported_db_entrypoint(module):
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

            populate = getattr(module, "populate_bundle", None)
            builder = (
                getattr(module, "build", None)
                or getattr(module, "build_database", None)
                or getattr(module, "build_test_database", None)
            )

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
    specs: Dict[str, TestDatabaseSpec] = {
        "test_db_0": TestDatabaseSpec(
            name="test_db_0",
            builder=_build_test_db_0_minimal,
            description="Minimal database with one title row.",
        ),
        "test_db_2": TestDatabaseSpec(
            name="test_db_2",
            builder=_build_test_db_2_small,
            description="Minimal FRBR-native fixture with exactly one title/book projection.",
        ),
        "test_db_3": TestDatabaseSpec(
            name="test_db_3",
            builder=_build_test_db_3_formats_fixture,
            description="FRBR-native format fixture: deterministic high-volume folders/files + link rows.",
        ),
        "test_db_13": TestDatabaseSpec(
            name="test_db_13",
            builder=_build_test_db_13_blank,
            description="Schema + helper tables only (keeps required null rows).",
        ),
        "benchmark_db_smoke": TestDatabaseSpec(
            name="benchmark_db_smoke",
            builder=_make_profiled_builder(
                db_name="benchmark_db_smoke",
                books=250,
                folders=1000,
                files=4000,
            ),
            description="Opt-in benchmark fixture: 250 books, 1000 folders, 4000 files.",
        ),
        "benchmark_db_medium": TestDatabaseSpec(
            name="benchmark_db_medium",
            builder=_make_profiled_builder(
                db_name="benchmark_db_medium",
                books=2500,
                folders=10000,
                files=40000,
            ),
            description="Benchmark fixture: 2500 books, 10000 folders, 40000 files.",
        ),
        "benchmark_db_large": TestDatabaseSpec(
            name="benchmark_db_large",
            builder=_make_profiled_builder(
                db_name="benchmark_db_large",
                books=10000,
                folders=40000,
                files=160000,
            ),
            description="Large benchmark fixture: 10000 books, 40000 folders, 160000 files.",
        ),
    }

    # Cover the full historical `test_db_0..test_db_25` range with FRBR-native
    # synthetic fixtures. More specialised DBs above keep explicit builders.
    for db_num in range(26):
        name = f"test_db_{db_num}"
        if name in specs:
            continue

        profile = _legacy_test_db_profiles().get(name, {"books": 12, "folders": 0, "files": 0})
        books = int(profile.get("books", 12))
        folders = int(profile.get("folders", 0))
        files = int(profile.get("files", 0))

        specs[name] = TestDatabaseSpec(
            name=name,
            builder=_make_profiled_builder(
                db_name=name,
                books=books,
                folders=folders,
                files=files,
            ),
            description=(
                f"Synthetic FRBR-native fixture for {name}: "
                f"{books} books, {folders} folders, {files} files."
            ),
        )

    return specs


def _legacy_test_db_profiles() -> Dict[str, Dict[str, int]]:
    """Profiles for synthetic re-implementations of legacy test DB names."""

    return {
        "test_db_1": {"books": 25, "folders": 0, "files": 0},
        "test_db_4": {"books": 40, "folders": 0, "files": 0},
        "test_db_5": {"books": 40, "folders": 120, "files": 360},
        "test_db_6": {"books": 20, "folders": 0, "files": 0},
        "test_db_7": {"books": 6, "folders": 0, "files": 0},
        "test_db_8": {"books": 6, "folders": 0, "files": 0},
        "test_db_9": {"books": 17, "folders": 0, "files": 0},
        "test_db_10": {"books": 20, "folders": 0, "files": 0},
        "test_db_11": {"books": 20, "folders": 40, "files": 120},
        "test_db_12": {"books": 21, "folders": 0, "files": 0},
        "test_db_14": {"books": 10, "folders": 0, "files": 0},
        "test_db_15": {"books": 20, "folders": 0, "files": 0},
        "test_db_16": {"books": 1, "folders": 0, "files": 0},
        "test_db_17": {"books": 10, "folders": 0, "files": 0},
        "test_db_18": {"books": 30, "folders": 0, "files": 0},
        "test_db_19": {"books": 30, "folders": 0, "files": 0},
        "test_db_20": {"books": 60, "folders": 0, "files": 0},
        "test_db_21": {"books": 30, "folders": 0, "files": 0},
        "test_db_22": {"books": 30, "folders": 0, "files": 0},
        "test_db_23": {"books": 30, "folders": 0, "files": 0},
        "test_db_24": {"books": 30, "folders": 0, "files": 0},
        "test_db_25": {"books": 30, "folders": 0, "files": 0},
    }


def _make_profiled_builder(*, db_name: str, books: int, folders: int, files: int) -> Builder:
    def _builder(db_path: Path) -> None:
        build_profiled_test_database(
            db_path=db_path,
            db_name=db_name,
            books=books,
            folders=folders,
            files=files,
        )

    return _builder


def build_profiled_test_database(
    *,
    db_path: Path,
    db_name: str,
    books: int,
    folders: int,
    files: int,
) -> None:
    """Public helper for deterministic synthetic FRBR-native test DBs.

    This is intended for opt-in benchmark and profiling workflows that want a
    larger synthetic fixture without needing a dedicated legacy-style builder
    module.
    """

    _build_profiled_test_db(
        db_path=db_path,
        db_name=db_name,
        books=books,
        folders=folders,
        files=files,
    )


def _module_has_supported_db_entrypoint(module: object) -> bool:
    return any(
        callable(getattr(module, attr, None))
        for attr in ("populate_bundle", "build", "build_database", "build_test_database")
    )


def _rewrite_bundle_root_tokens(*, db_path: Path, provision_root: Path) -> None:
    db_path = Path(db_path)
    if not db_path.exists():
        return

    replacement = str(Path(provision_root).resolve())
    token = TEST_DB_BUNDLE_ROOT_TOKEN

    conn = sqlite3.connect(str(db_path))
    try:
        rewrites = (
            ("stores", "store_root_uri"),
            ("files", "file_original_path"),
            ("images", "image_original_path"),
            ("items", "item_source_path"),
        )
        for table, column in rewrites:
            if not _table_exists(conn, table):
                continue
            columns = {str(row[1]) for row in _table_info(conn, table)}
            if column not in columns:
                continue
            conn.execute(
                f"UPDATE {table} "
                f"SET {column} = REPLACE({column}, ?, ?) "
                f"WHERE {column} IS NOT NULL AND instr({column}, ?) > 0;",
                (token, replacement, token),
            )
        conn.commit()
    finally:
        conn.close()


def _bundled_test_db_1_csv_dir() -> Path:
    """Locate the bundled CSV fixture for test_db_1.

    We use test_db_1 as a canonical, richer dataset and derive smaller DBs
    (like test_db_2) by pruning rows after import.
    """

    # Prefer import-based resolution so this works both in editable installs
    # and in sdist/wheel layouts.
    try:  # pragma: no cover
        import importlib.util

        spec = importlib.util.find_spec("tests.support.test_databases.test_db_1")
        if spec is not None and spec.origin:
            p = Path(spec.origin).resolve().parent
            if p.is_dir():
                return p
    except Exception:
        pass

    # Fallback to conventional repo layout.
    return _repo_root() / "tests" / "support" / "test_databases" / "test_db_1"


def _load_csv_fixture_into_db(conn, *, csv_dir: Path) -> None:
    """Load a CSV fixture folder into an already-created schema."""

    import csv
    import re

    csv_dir = Path(csv_dir)
    if not csv_dir.is_dir():
        raise FileNotFoundError(f"CSV fixture directory not found: {csv_dir}")

    def _table_info_map(table: str) -> dict[str, tuple]:
        return {str(row[1]): row for row in conn.execute(f"PRAGMA table_info({table});").fetchall()}

    _int_re = re.compile(r"^-?\d+$")
    _float_re = re.compile(r"^-?\d+(?:\.\d+)?$")

    def _coerce(value: str | None, decl_type: str) -> object:
        if value is None:
            return None
        v = str(value).strip()
        if v == "" or v.lower() == "none":
            return None

        t = (decl_type or "").upper()
        if "INT" in t and _int_re.match(v):
            try:
                return int(v)
            except Exception:
                return v
        if any(x in t for x in ("REAL", "FLOA", "DOUB")) and _float_re.match(v):
            try:
                return float(v)
            except Exception:
                return v
        return v

    # Ensure pragma changes are effective (sqlite ignores foreign_keys toggles mid-transaction).
    conn.commit()
    # Disable FK enforcement during bulk load for speed and to avoid ordering issues.
    conn.execute("PRAGMA foreign_keys=OFF;")

    for csv_path in sorted(csv_dir.glob("*.csv")):
        table = csv_path.stem
        if not _table_exists(conn, table):
            continue
        if conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='trigger' AND name=? LIMIT 1;",
            (f"block_insert_on_{table}",),
        ).fetchone():
            # FRBR constant tables (for now: `languages`) are seeded/locked by generator triggers.
            continue

        with csv_path.open("r", newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            if reader.fieldnames is None:
                continue
            rows = list(reader)

        if not rows:
            continue

        info = _table_info_map(table)
        cols = [c for c in reader.fieldnames if c in info]
        if not cols:
            continue

        col_sql = ",".join([f'"{c}"' for c in cols])
        placeholders = ",".join(["?"] * len(cols))
        sql = f'INSERT INTO "{table}" ({col_sql}) VALUES ({placeholders})'

        values: list[list[object]] = []
        for r in rows:
            values.append([_coerce(r.get(c), str(info[c][2])) for c in cols])
        conn.executemany(sql, values)

    conn.commit()

    # Re-enable and validate.
    conn.execute("PRAGMA foreign_keys=ON;")
    violations = conn.execute("PRAGMA foreign_key_check;").fetchall()
    if violations:
        raise AssertionError(f"Foreign key violations after CSV import: {violations[:10]}")


def _build_test_db_2_small(db_path: Path) -> None:
    """Create a small FRBR-native test DB with exactly one title/book projection."""
    _build_profiled_test_db(
        db_path=db_path,
        db_name="test_db_2",
        books=1,
        folders=0,
        files=0,
    )


def _build_test_db_3_formats_fixture(db_path: Path) -> None:
    """Create test_db_3 (FRBR-native): many folders/files linked to one seeded work/item."""
    _build_profiled_test_db(
        db_path=db_path,
        db_name="test_db_3",
        books=1,
        folders=497,
        files=2440,
    )


def _build_profiled_test_db(
    *,
    db_path: Path,
    db_name: str,
    books: int,
    folders: int,
    files: int,
) -> None:
    """Build a deterministic FRBR-native fixture from a small profile."""

    import sqlite3
    from itertools import cycle

    from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator import (
        create_new_database,
    )
    from tests.support._surface_storage_tables import ensure_surface_asset_tables_sqlite

    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    try:
        _register_sqlite_test_functions(conn)
        create_new_database(conn)
        _ensure_required_null_rows(conn)

        if books > 0:
                seeded = [
                    _insert_minimal_wemi_book(conn, title=f"{db_name} title {idx + 1:03d}")
                    for idx in range(books)
                ]

                if folders > 0:
                    ensure_surface_asset_tables_sqlite(
                        conn,
                        include_file_folder_links=True,
                    )
                    work_ids = [row[0] for row in seeded]
                    item_ids = [row[3] for row in seeded]
                    folder_ids: list[int] = []

                for folder_idx in range(folders):
                    cur = conn.execute(
                        "INSERT INTO folders (folder_scratch) VALUES (?);",
                        (f"{db_name}-folder-{folder_idx}",),
                    )
                    folder_id = int(cur.lastrowid)
                    folder_ids.append(folder_id)
                    conn.execute(
                        "INSERT INTO folder_work_links "
                        "(folder_work_link_folder_id, folder_work_link_work_id, folder_work_link_priority) "
                        "VALUES (?, ?, ?);",
                        (folder_id, work_ids[folder_idx % len(work_ids)], folder_idx + 1),
                    )

                file_count = files if files > 0 else folders
                ext_iter = cycle(["epub", "mobi", "pdf"])
                for file_idx in range(file_count):
                    folder_id = folder_ids[file_idx % len(folder_ids)]
                    item_id = item_ids[file_idx % len(item_ids)]
                    ext = next(ext_iter)
                    curf = conn.execute(
                        "INSERT INTO files "
                        "(file_item_id, file_folder_id, file_size_bytes, file_extension, file_base_name) "
                        "VALUES (?, ?, ?, ?, ?);",
                        (item_id, folder_id, 1234, ext, f"{db_name}-file-{file_idx}"),
                    )
                    file_id = int(curf.lastrowid)
                    conn.execute(
                        "INSERT INTO file_folder_links (file_folder_link_file_id, file_folder_link_folder_id) "
                        "VALUES (?, ?);",
                        (file_id, folder_id),
                    )

        _normalize_test_db_for_determinism(conn, db_name=db_name)
        conn.commit()

        viol = conn.execute("PRAGMA foreign_key_check;").fetchall()
        if viol:
            raise AssertionError(f"Foreign key violations after {db_name} build: {viol[:10]}")
    finally:
        conn.close()


def _insert_minimal_wemi_book(conn, *, title: str) -> tuple[int, int, int, int]:
    """Create a minimal Work+Expression+Manifestation+Item chain and link tables."""

    work_id = int(
        conn.execute(
            "INSERT INTO works (work_canonical_title, work_scratch) VALUES (?, ?);",
            (title, "generated-work"),
        ).lastrowid
    )
    expression_id = int(
        conn.execute(
            "INSERT INTO expressions (expression_label, expression_scratch) VALUES (?, ?);",
            ("Primary expression", "generated-expression"),
        ).lastrowid
    )
    manifestation_id = int(
        conn.execute(
            "INSERT INTO manifestations (manifestation_format_detail, manifestation_scratch) VALUES (?, ?);",
            ("epub", "generated-manifestation"),
        ).lastrowid
    )

    conn.execute(
        "INSERT INTO expression_work_links "
        "(expression_work_link_expression_id, expression_work_link_work_id, expression_work_link_primary) "
        "VALUES (?, ?, 1);",
        (expression_id, work_id),
    )
    conn.execute(
        "INSERT INTO expression_manifestation_links "
        "(expression_manifestation_link_expression_id, expression_manifestation_link_manifestation_id, expression_manifestation_link_primary) "
        "VALUES (?, ?, 1);",
        (expression_id, manifestation_id),
    )

    item_id = int(
        conn.execute(
            "INSERT INTO items (item_manifestation_id, item_scratch) VALUES (?, ?);",
            (manifestation_id, "generated-item"),
        ).lastrowid
    )

    return work_id, expression_id, manifestation_id, item_id


def _normalize_test_db_for_determinism(conn, *, db_name: str) -> None:
    """Normalize volatile timestamp-ish columns so DB builds are reproducible."""

    m = _DB_NAME_RE.match(db_name)
    db_num = int(m.group(1)) if m else 0
    ts_ms = 1700000000000 + (db_num * 1000)
    ts_s = ts_ms // 1000

    trigger_defs = conn.execute(
        "SELECT name, sql FROM sqlite_master "
        "WHERE type='trigger' "
        "ORDER BY name;"
    ).fetchall()

    for name, _sql in trigger_defs:
        safe_name = str(name).replace("`", "``")
        conn.execute(f"DROP TRIGGER IF EXISTS `{safe_name}`;")

    tables = [
        str(r[0])
        for r in conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
            "ORDER BY name;"
        ).fetchall()
    ]

    for table in tables:
        cols = conn.execute(f"PRAGMA table_info(`{table}`);").fetchall()
        assignments: list[str] = []
        params: list[object] = []
        for _cid, col_name, _decl_type, _notnull, _dflt, _pk in cols:
            col = str(col_name)
            low = col.lower()

            if low.endswith("_ep_k"):
                assignments.append(f"`{col}` = ?")
                params.append(ts_ms)
                continue

            if "datestamp" in low or low.endswith("_datestamp") or low.endswith("_timestamp"):
                assignments.append(f"`{col}` = ?")
                params.append(ts_s)

        if assignments:
            try:
                conn.execute(f"UPDATE `{table}` SET {', '.join(assignments)};", tuple(params))
            except sqlite3.OperationalError:
                # Some tables (notably `files`) carry update-time constraints that
                # rely on environment-specific runtime checks. Determinism tests
                # use a canonical dump that excludes volatile columns anyway.
                continue

    for _name, sql in trigger_defs:
        if sql:
            conn.execute(str(sql))

def _build_test_db_0_minimal(db_path: Path) -> None:
    """Create a tiny but valid database with one title row.

    We intentionally do *not* rely on the higher-level Database class here,
    since the legacy APSW-backed driver is optional.
    """

    import sqlite3

    from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator import (
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
        _normalize_test_db_for_determinism(conn, db_name="test_db_0")
        conn.commit()
    finally:
        conn.close()


def _build_test_db_13_blank(db_path: Path) -> None:
    """Create a schema-only database.

    Historically `test_db_13` was used as a "blank" DB. For LiuXin_alpha we
    keep this lightweight: schema created + required null rows present.
    """

    import sqlite3

    from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr.database_generator import (
        create_new_database,
    )

    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    try:
        _register_sqlite_test_functions(conn)
        create_new_database(conn)
        _ensure_required_null_rows(conn)
        _normalize_test_db_for_determinism(conn, db_name="test_db_13")
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

    Defaults to `tests.support.test_databases` and can be extended via:
    $LIUXIN_TEST_DATABASE_BUILDER_PREFIXES (separator: ; , :)
    """

    defaults = ["tests.support.test_databases", "tests._support.test_databases"]
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


def _resolve_test_asset_source(*, env_key: str, fallback_dirnames: Sequence[str]) -> Path:
    raw = os.environ.get(env_key)
    if raw:
        candidate = Path(raw).expanduser()
        if candidate.is_dir():
            return candidate
        raise FileNotFoundError(f"{env_key} points to a missing directory: {candidate}")

    data_dir = _default_liuxin_data_dir()
    if data_dir is not None:
        for dirname in fallback_dirnames:
            candidate = data_dir / dirname
            if candidate.is_dir():
                return candidate

    raise FileNotFoundError(
        f"Unable to locate test assets for {env_key}. "
        f"Set {env_key} or provide one of {list(fallback_dirnames)} under LIUXIN_DATA_DIR."
    )


def _provision_test_assets(
    *,
    source_dir: Path,
    dst_dir: Path,
    names: Optional[Sequence[str]] = None,
) -> ProvisionedTestAssets:
    dst_dir = Path(dst_dir)
    if dst_dir.exists():
        shutil.rmtree(dst_dir)
    dst_dir.mkdir(parents=True, exist_ok=True)

    selected: list[Path]
    if names is None:
        selected = sorted(path for path in source_dir.iterdir() if path.is_file())
    else:
        selected = []
        missing: list[str] = []
        for name in names:
            candidate = source_dir / name
            if candidate.is_file():
                selected.append(candidate)
            else:
                missing.append(str(name))
        if missing:
            raise FileNotFoundError(f"Missing requested test assets in {source_dir}: {missing}")

    copied: list[Path] = []
    for src in selected:
        dst = dst_dir / src.name
        shutil.copy2(src, dst)
        copied.append(dst)

    return ProvisionedTestAssets(root=dst_dir, paths=tuple(copied))


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
    """Ensure historic required null/sentinel rows exist.

    LiuXin uses id=0 in some tables as a "null" record for link tables.

    In the FRBR-first/WEMI schema, publishing entities are modelled via
    `agents` (+ subtype sidecars like `org_agents`) rather than a dedicated
    `publishers` table.
    """

    required = (
        # Classic Calibre/LiuXin sentinel row
        ("series", {"preferred_text_col": "series", "desired_text": None, "extra_values": {}}),

        # FRBR-first replacement for legacy `publishers`: an "organisation" agent at id=0.
        # Note: agents.agent_canonical_name is NOT NULL in the current schema.
        ("agents", {"preferred_text_col": "agent_canonical_name", "desired_text": AGENTS_NULL_CANONICAL_NAME, "extra_values": {"agent_type": "organisation"}}),
    )

    for table, spec in required:
        if not _table_exists(conn, table):
            continue

        pk_col = _detect_pk_column(conn, table)
        if pk_col is None:
            continue

        cols = {str(name) for _cid, name, *_rest in _table_info(conn, table)}

        preferred_text_col = str(spec.get("preferred_text_col") or "")
        desired_text = spec.get("desired_text", None)

        text_col = preferred_text_col if preferred_text_col and preferred_text_col in cols else None

        extra_values = {
            str(k): v for k, v in (spec.get("extra_values") or {}).items() if str(k) in cols
        }

        existing = conn.execute(
            f"SELECT {pk_col} FROM {table} WHERE {pk_col} = 0 LIMIT 1;"
        ).fetchone()

        if existing is None:
            insert_values = {pk_col: 0}
            insert_values.update(extra_values)

            # If we have a preferred text column, set it to desired_text (or NULL if None).
            if text_col is not None and text_col not in insert_values:
                insert_values[text_col] = desired_text

            # If desired_text is None, we still need a value for NOT NULL columns.
            # The only current case is agents.agent_canonical_name, where we use a sentinel string.
            if table == "agents":
                if "agent_type" in cols and "agent_type" not in insert_values:
                    insert_values["agent_type"] = "organisation"
                if "agent_canonical_name" in cols:
                    if insert_values.get("agent_canonical_name") is None:
                        insert_values["agent_canonical_name"] = AGENTS_NULL_CANONICAL_NAME

            col_list = ", ".join(insert_values.keys())
            placeholders = ", ".join(["?"] * len(insert_values))
            conn.execute(
                f"INSERT INTO {table} ({col_list}) VALUES ({placeholders});",
                tuple(insert_values.values()),
            )
        else:
            updates = dict(extra_values)

            if text_col is not None:
                if desired_text is None:
                    updates[text_col] = None
                else:
                    updates[text_col] = desired_text

            # Ensure agents sentinel row remains schema-valid.
            if table == "agents" and "agent_canonical_name" in cols:
                if updates.get("agent_canonical_name") is None:
                    updates["agent_canonical_name"] = AGENTS_NULL_CANONICAL_NAME

            if updates:
                set_clause = ", ".join([f"{col} = ?" for col in updates.keys()])
                conn.execute(
                    f"UPDATE {table} SET {set_clause} WHERE {pk_col} = 0;",
                    tuple(updates.values()),
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

    target_table = table
    row = conn.execute(
        "SELECT type FROM sqlite_master WHERE name = ? LIMIT 1;",
        (table,),
    ).fetchone()
    if row is not None and str(row[0]).lower() == "view":
        # FRBR compatibility views (e.g. `titles`) are read-only projections.
        # For fixture seeding, insert into their writable source tables.
        compat_insert_targets = {
            "titles": "works",
        }
        mapped = compat_insert_targets.get(table)
        if mapped and _table_exists(conn, mapped):
            target_table = mapped

    cols = _table_info(conn, target_table)
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
        conn.execute(f"INSERT INTO {target_table} DEFAULT VALUES;")
        return

    placeholders = ",".join(["?"] * len(required))
    cols_sql = ",".join(required)
    conn.execute(f"INSERT INTO {target_table} ({cols_sql}) VALUES ({placeholders});", values)
