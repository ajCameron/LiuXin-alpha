"""Minimal test-object helper shim for legacy DB-support builders."""

from __future__ import annotations

import os
import random
import re
import tempfile
import uuid
from pathlib import Path

from LiuXin_alpha.utils.ptempfiles import DummyScratchFolderManager
from LiuXin_alpha.utils.storage.local.file_ops import ensure_folder, ensured_copy


def _find_project_root(start: Path) -> Path:
    start = start.resolve()
    for candidate in [start] + list(start.parents):
        if (candidate / "src" / "LiuXin_alpha").is_dir() and (candidate / "tests").is_dir():
            return candidate
    return start.parents[3] if len(start.parents) >= 4 else start


def _resolve_data_repo_root(project_root: Path) -> Path | None:
    env = os.environ.get("LIUXIN_ALPHA_DATA_DIR")
    if env:
        candidate = Path(env).expanduser()
        if not candidate.is_absolute():
            candidate = (project_root / candidate).resolve()
        if candidate.is_dir():
            return candidate

    for candidate in (
        project_root / "LiuXin_alpha_data",
        project_root.parent / "LiuXin_alpha_data",
        project_root / "LiuXin_data",
        project_root.parent / "LiuXin_data",
    ):
        if candidate.is_dir():
            return candidate

    return None


def _resolve_asset_source(project_root: Path, env_key: str, fallback_dirnames: tuple[str, ...]) -> Path:
    env = os.environ.get(env_key)
    if env:
        candidate = Path(env).expanduser()
        if not candidate.is_absolute():
            candidate = (project_root / candidate).resolve()
        if candidate.is_dir():
            return candidate

    data_root = _resolve_data_repo_root(project_root)
    if data_root is not None:
        for dirname in fallback_dirnames:
            candidate = data_root / dirname
            if candidate.is_dir():
                return candidate

    raise FileNotFoundError(
        "Unable to locate legacy DB-support asset directory for "
        f"{env_key}. Tried {fallback_dirnames!r} under the alpha data repo."
    )


def _discover_md_files(root: Path) -> list[Path]:
    return sorted(
        (
            path
            for path in root.iterdir()
            if path.is_file() and re.match(r"^[A-Za-z0-9]+_md_test_file_[0-9]+\.[A-Za-z0-9]+$", path.name)
        ),
        key=lambda path: path.name.casefold(),
    )


def _discover_cover_files(root: Path) -> list[Path]:
    return sorted(
        (
            path
            for path in root.iterdir()
            if path.is_file() and re.match(r"^book_id_[0-9]+\.[A-Za-z0-9]+$", path.name)
        ),
        key=lambda path: path.name.casefold(),
    )


class TestObjectsHandler(object):
    """Small compatibility surface for the legacy DB builders.

    Only the methods still referenced by `tests/support/test_databases` are
    implemented here. This intentionally avoids the old ramdisk/cache system.
    """

    def __init__(self, scratch_file_handler=None, try_for_ramdisk_cache=True):
        del try_for_ramdisk_cache
        self.sf_handler = scratch_file_handler if scratch_file_handler is not None else _EphemeralScratchFolderManager()
        self.rng = random

        project_root = _find_project_root(Path(__file__))
        self._md_source_dir = _resolve_asset_source(
            project_root,
            "LIUXIN_TEST_BOOKS_DIR",
            ("md_test_files", "md_test_books", "test_books"),
        )
        self._cover_source_dir = _resolve_asset_source(
            project_root,
            "LIUXIN_TEST_COVERS_DIR",
            ("test_covers", "covers"),
        )

        self.available_test_md_files = _discover_md_files(self._md_source_dir)
        self.available_test_book_covers = _discover_cover_files(self._cover_source_dir)

    def _new_scratch_folder(self, filename=None):
        return Path(self.sf_handler.get_scratch_folder(filename=filename))

    @staticmethod
    def get_md_file_name(file_ext, file_num):
        if file_ext.startswith("."):
            file_ext = file_ext[1:]
        return "{0}_md_test_file_{1}.{0}".format(file_ext, file_num)

    def get_rand_md_test_file(self):
        path = self.rng.choice(self.available_test_md_files)
        match = re.match(r"^([A-Za-z0-9]+)_md_test_file_([0-9]+)\.[A-Za-z0-9]+$", path.name)
        if match is None:
            raise ValueError("Unexpected metadata test file name: {}".format(path.name))
        return match.group(1), match.group(2)

    def _find_md_source(self, file_ext, file_num) -> Path:
        candidate = self._md_source_dir / self.get_md_file_name(file_ext, file_num)
        if not candidate.is_file():
            raise FileNotFoundError(candidate)
        return candidate

    def get_test_md_file_path(self, file_ext, file_num, folder_name=None):
        return self.get_scratch_md_test_file(file_ext, file_num, folder_name=folder_name)

    def get_scratch_md_test_file(self, file_ext, file_num, folder_name=None):
        src = self._find_md_source(file_ext, file_num)
        dst_root = self._new_scratch_folder()
        if folder_name is not None:
            dst_root = dst_root / folder_name
            ensure_folder(str(dst_root))
        dst = dst_root / src.name
        ensured_copy(file_in=str(src), file_out=str(dst))
        return str(dst)

    def get_rand_md_test_file_path(self):
        file_ext, file_num = self.get_rand_md_test_file()
        return self.get_test_md_file_path(file_ext=file_ext, file_num=file_num)

    def get_rand_test_file(self, file_ext="txt"):
        dst_root = self._new_scratch_folder()
        dst = dst_root / "test_file_delete_me.{}".format(file_ext.lstrip("."))
        with dst.open("w+", encoding="utf-8") as open_file:
            open_file.write(str(uuid.uuid4()))
        return str(dst)

    @staticmethod
    def get_cover_file_name(book_id):
        return "book_id_{}.jpg".format(book_id)

    def _find_cover_source(self, book_id) -> Path:
        explicit = list(self._cover_source_dir.glob("book_id_{}.*".format(book_id)))
        if explicit:
            return sorted(explicit, key=lambda path: path.name.casefold())[0]
        raise FileNotFoundError("No cover file found for book_id={}".format(book_id))

    def get_test_cover_path(self, book_id):
        return self.get_scratch_test_cover_file(book_id)

    def get_scratch_test_cover_file(self, book_id):
        src = self._find_cover_source(book_id)
        dst_root = self._new_scratch_folder()
        dst = dst_root / src.name
        ensured_copy(file_in=str(src), file_out=str(dst))
        return str(dst)

    def get_rand_test_cover_path(self):
        src = self.rng.choice(self.available_test_book_covers)
        return self.get_scratch_test_cover_file(book_id=src.stem.split("_")[-1])


class _EphemeralScratchFolderManager:
    """Local fallback for tests that instantiate TestObjectsHandler without a manager."""

    def get_scratch_folder(self, filename=None):
        prefix = "liuxin_test_objects_"
        if filename:
            prefix += str(filename)
        return tempfile.mkdtemp(prefix=prefix)
