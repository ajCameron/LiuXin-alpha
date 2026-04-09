import sys
from pathlib import Path
import pytest

import os


# ---------------------------------------------------------------------------
# NOTE: legacy CalibreCache
# ---------------------------------------------------------------------------
#
# These tests exercise the historical CalibreCache layer, which targets the
# deprecated calibre-shaped schema (meta/books/authors/tags...). Under the
# FRBR-first generator, those writable tables no longer exist.
#
# Keep these tests opt-in while the cache layer is either removed or
# reimplemented on top of FRBR views.

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

# --- Make bundled libs importable (liuxin_dateutil is in utils/libraries) ---
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_LIBS = _PROJECT_ROOT / "src" / "LiuXin_alpha" / "utils" / "libraries"
if _LIBS.exists() and str(_LIBS) not in sys.path:
    sys.path.insert(0, str(_LIBS))

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.interfaces.field_metadata import FieldMetadata
from LiuXin_alpha.library.caches.calibre.cache import CalibreCache


class TestPrefs(dict):
    """
    Minimal prefs shim:
    - behaves like a dict
    - supports .set()
    - supplies a small defaults map for keys CalibreCache reads early
    """

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
    """
    Minimal fsm used by formats/covers/path tables during init().
    CalibreCache tables call fsm.get_loc(...) with either folder rows or asset rows.
    """

    def __init__(self, root: Path):
        self.root = Path(root)

    def get_loc(self, *args, **kwargs):
        row = None
        if args:
            row = args[0]
        row = kwargs.get("asset_row", row)
        row = kwargs.get("book_folder_row", row)
        # produce stable dummy locations
        if row is None:
            return None
        # try common id keys
        for k in ("file_id", "cover_id", "folder_id", "id"):
            if isinstance(row, dict) and k in row:
                return str(self.root / f"{k}_{row[k]}")
        return str(self.root / "unknown")


@pytest.fixture()
def calibre_backend_db(tmp_path, provision_named_test_database):
    # provision a real sqlite db copy
    prov = provision_named_test_database(name="test_db_0", dst_dir=tmp_path)

    db = Database(metadata={"database_path": str(prov.db_path)})

    # Patch in what CalibreCache expects on the backend
    db.tables = {}  # BaseCache expects it to exist; CalibreCache will replace it
    db.conn = db.driver_wrapper.lock  # used as `with backend.conn:`
    db.fsm = DummyFSM(tmp_path / "fsm_root")

    # preferences CalibreCache reads immediately
    db.prefs = TestPrefs(
        defaults={
            "bools_are_tristate": False,
            "update_all_last_mod_dates_on_start": False,
            "metadata_backup_interval": 0,
        }
    )
    db.default_prefs = dict(db.prefs.defaults)
    db.pref_progress_callback = None

    # CalibreCache calls: initialize_prefs(default_prefs, restore_all_prefs, progress_callback)
    db.restore_all_prefs = False

    def _init_prefs(default_prefs=None, restore_all_prefs=False, progress_callback=None):
        # keep defaults in sync with what the cache passes in
        if default_prefs:
            db.prefs.defaults.update(default_prefs)
            # optionally materialize defaults into prefs storage if missing
            for k, v in default_prefs.items():
                if k not in db.prefs:
                    db.prefs[k] = v

        # progress_callback is optional; mimic the shape Calibre expects
        if callable(progress_callback) and default_prefs is not None:
            progress_callback(None, len(default_prefs))

    db.initialize_prefs = _init_prefs

    db.field_metadata = FieldMetadata()

    # Some codepaths call backend.custom_table_names(...)
    db.custom_table_names = db.driver_wrapper.custom_table_names

    return db


@pytest.fixture()
def live_calibre_cache(calibre_backend_db):
    cache = CalibreCache(backend=calibre_backend_db)
    cache.init()
    return cache


def test_live_cache_fixture_initializes(live_calibre_cache):
    cache = live_calibre_cache
    assert cache is not None
    assert getattr(cache, "fields", None) is not None
    assert "title" in cache.fields  # core field should exist
    assert isinstance(cache.tables, dict)
