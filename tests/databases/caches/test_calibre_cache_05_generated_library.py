from __future__ import annotations

from pathlib import Path

import pytest

import os


_ENABLE = os.environ.get("LIUXIN_ENABLE_LEGACY_CALIBRE_CACHE_TESTS", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

if not _ENABLE:
    pytest.skip(
        "Legacy CalibreCache tests are disabled under FRBR-first schema. "
        "Set LIUXIN_ENABLE_LEGACY_CALIBRE_CACHE_TESTS=1 to run them.",
        allow_module_level=True,
    )

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.interfaces.field_metadata import FieldMetadata

try:
    from LiuXin_alpha.library.caches.calibre.cache import CalibreCache
except ModuleNotFoundError as e:
    # Some minimal test environments omit optional calibre-compat deps.
    pytest.skip(f"Skipping CalibreCache smoke test; missing dependency: {e}", allow_module_level=True)


class TestPrefs(dict):
    def __init__(self, defaults=None):
        super().__init__()
        self.defaults = defaults or {}

    def __getitem__(self, key):
        if key in self:
            return super().__getitem__(key)
        if key in self.defaults:
            return self.defaults[key]
        raise KeyError(key)

    def get(self, key, default=None):
        if key in self:
            return super().get(key)
        return self.defaults.get(key, default)

    def set(self, key, value):
        self[key] = value


class DummyFSM:
    def __init__(self, root: Path):
        self.root = Path(root)

    def get_loc(self, *args, **kwargs):
        row = None
        if args:
            row = args[0]
        row = kwargs.get("asset_row", row)
        row = kwargs.get("book_folder_row", row)
        if row is None:
            return None
        for k in ("file_id", "cover_id", "folder_id", "id"):
            if isinstance(row, dict) and k in row:
                return str(self.root / f"{k}_{row[k]}")
        return str(self.root / "unknown")


def _backend_from_library(tmp_path: Path, metadata_db_path: Path) -> Database:
    db = Database(metadata={"database_path": str(metadata_db_path)})
    db.tables = {}
    db.conn = db.driver_wrapper.lock
    db.fsm = DummyFSM(tmp_path / "fsm_root")

    db.prefs = TestPrefs(
        defaults={
            "bools_are_tristate": False,
            "update_all_last_mod_dates_on_start": False,
            "metadata_backup_interval": 0,
        }
    )
    db.default_prefs = dict(db.prefs.defaults)
    db.pref_progress_callback = None
    db.restore_all_prefs = False

    def _init_prefs(default_prefs=None, restore_all_prefs=False, progress_callback=None):
        if default_prefs:
            db.prefs.defaults.update(default_prefs)
            for k, v in default_prefs.items():
                if k not in db.prefs:
                    db.prefs[k] = v
        if callable(progress_callback) and default_prefs is not None:
            progress_callback(None, len(default_prefs))

    db.initialize_prefs = _init_prefs
    db.field_metadata = FieldMetadata()
    db.custom_table_names = db.driver_wrapper.custom_table_names
    return db


def test_cache_init_on_generated_calibre_library(tmp_path, provision_populated_calibre_library):
    lib, builder = provision_populated_calibre_library(name="calibre_lib_for_cache")

    # Make the library non-empty (exercises more init paths).
    builder.add_book(
        title="Cache Smoke",
        authors=["Constance Thrane"],
        formats={"EPUB": b"epub"},
        tags=["smoke"],
    )

    backend = _backend_from_library(tmp_path, Path(lib.root) / "metadata.db")
    cache = CalibreCache(backend=backend)
    cache.init()

    assert "title" in cache.fields
