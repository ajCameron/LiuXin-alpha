"""
Support retained legacy database-fixture objects behavior.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise objects through a consuming regression::

        python -m pytest -q tests/databases/test_test_resources_manager.py
"""

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
    """
    Perform the find project root step with deterministic fixture inputs.

    Example:
        Exercise  find project root through a consuming regression::

            python -m pytest -q tests/databases/test_test_resources_manager.py


    :param start: Value supplied for start under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    start = start.resolve()
    for candidate in [start] + list(start.parents):
        if (candidate / "src" / "LiuXin_alpha").is_dir() and (candidate / "tests").is_dir():
            return candidate
    return start.parents[3] if len(start.parents) >= 4 else start


def _resolve_data_repo_root(project_root: Path) -> Path | None:
    """
    Resolve data repo root under the fixture contract.

    Example:
        Exercise  resolve data repo root through a consuming regression::

            python -m pytest -q tests/databases/test_test_resources_manager.py


    :param project_root: Repository root used to resolve checked-in fixture data.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
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
    """
    Resolve asset source under the fixture contract.

    Example:
        Exercise  resolve asset source through a consuming regression::

            python -m pytest -q tests/databases/test_test_resources_manager.py


    :param project_root: Repository root used to resolve checked-in fixture data.
    :param env_key: Value supplied for env key under the deterministic fixture contract.
    :param fallback_dirnames: Value supplied for fallback dirnames under the
        deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
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
    """
    Discover md files under the fixture contract.

    Example:
        Exercise  discover md files through a consuming regression::

            python -m pytest -q tests/databases/test_test_resources_manager.py


    :param root: Root directory containing the fixture corpus or generated tree.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return sorted(
        (
            path
            for path in root.iterdir()
            if path.is_file() and re.match(r"^[A-Za-z0-9]+_md_test_file_[0-9]+\.[A-Za-z0-9]+$", path.name)
        ),
        key=lambda path: path.name.casefold(),
    )


def _discover_cover_files(root: Path) -> list[Path]:
    """
    Discover cover files under the fixture contract.

    Example:
        Exercise  discover cover files through a consuming regression::

            python -m pytest -q tests/databases/test_test_resources_manager.py


    :param root: Root directory containing the fixture corpus or generated tree.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return sorted(
        (
            path
            for path in root.iterdir()
            if path.is_file() and re.match(r"^book_id_[0-9]+\.[A-Za-z0-9]+$", path.name)
        ),
        key=lambda path: path.name.casefold(),
    )


class TestObjectsHandler(object):
    """
    Small compatibility surface for the legacy DB builders.

    Example:
        Exercise TestObjectsHandler through a consuming regression::

            python -m pytest -q tests/databases/test_test_resources_manager.py
    """

    def __init__(self, scratch_file_handler=None, try_for_ramdisk_cache=True):
        """
        Initialize and validate the TestObjectsHandler test-support state.

        Example:
            Exercise TestObjectsHandler.  init   through a consuming regression::

                python -m pytest -q tests/databases/test_test_resources_manager.py


        :param scratch_file_handler: Value supplied for scratch file handler under the
            deterministic fixture contract.
        :param try_for_ramdisk_cache: Value supplied for try for ramdisk cache under the
            deterministic fixture contract.
        :return: None; completion is expressed through state changes or assertions.
        """
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
        """
        Perform the new scratch folder step with deterministic fixture inputs.

        Example:
            Exercise TestObjectsHandler. new scratch folder through a consuming regression::

                python -m pytest -q tests/databases/test_test_resources_manager.py


        :param filename: Archive member or fixture filename.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        return Path(self.sf_handler.get_scratch_folder(filename=filename))

    @staticmethod
    def get_md_file_name(file_ext, file_num):
        """
        Return md file name under the fixture contract.

        Example:
            Exercise TestObjectsHandler.get md file name through a consuming regression::

                python -m pytest -q tests/databases/test_test_resources_manager.py


        :param file_ext: Value supplied for file ext under the deterministic fixture
            contract.
        :param file_num: Value supplied for file num under the deterministic fixture
            contract.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        if file_ext.startswith("."):
            file_ext = file_ext[1:]
        return "{0}_md_test_file_{1}.{0}".format(file_ext, file_num)

    def get_rand_md_test_file(self):
        """
        Return rand md test file under the fixture contract.

        Example:
            Exercise TestObjectsHandler.get rand md test file through a consuming regression::

                python -m pytest -q tests/databases/test_test_resources_manager.py


        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        path = self.rng.choice(self.available_test_md_files)
        match = re.match(r"^([A-Za-z0-9]+)_md_test_file_([0-9]+)\.[A-Za-z0-9]+$", path.name)
        if match is None:
            raise ValueError("Unexpected metadata test file name: {}".format(path.name))
        return match.group(1), match.group(2)

    def _find_md_source(self, file_ext, file_num) -> Path:
        """
        Perform the find md source step with deterministic fixture inputs.

        Example:
            Exercise TestObjectsHandler. find md source through a consuming regression::

                python -m pytest -q tests/databases/test_test_resources_manager.py


        :param file_ext: Value supplied for file ext under the deterministic fixture
            contract.
        :param file_num: Value supplied for file num under the deterministic fixture
            contract.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        candidate = self._md_source_dir / self.get_md_file_name(file_ext, file_num)
        if not candidate.is_file():
            raise FileNotFoundError(candidate)
        return candidate

    def get_test_md_file_path(self, file_ext, file_num, folder_name=None):
        """
        Return test md file path under the fixture contract.

        Example:
            Exercise TestObjectsHandler.get test md file path through a consuming regression::

                python -m pytest -q tests/databases/test_test_resources_manager.py


        :param file_ext: Value supplied for file ext under the deterministic fixture
            contract.
        :param file_num: Value supplied for file num under the deterministic fixture
            contract.
        :param folder_name: Value supplied for folder name under the deterministic fixture
            contract.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        return self.get_scratch_md_test_file(file_ext, file_num, folder_name=folder_name)

    def get_scratch_md_test_file(self, file_ext, file_num, folder_name=None):
        """
        Return scratch md test file under the fixture contract.

        Example:
            Exercise TestObjectsHandler.get scratch md test file through a consuming regression::

                python -m pytest -q tests/databases/test_test_resources_manager.py


        :param file_ext: Value supplied for file ext under the deterministic fixture
            contract.
        :param file_num: Value supplied for file num under the deterministic fixture
            contract.
        :param folder_name: Value supplied for folder name under the deterministic fixture
            contract.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        src = self._find_md_source(file_ext, file_num)
        dst_root = self._new_scratch_folder()
        if folder_name is not None:
            dst_root = dst_root / folder_name
            ensure_folder(str(dst_root))
        dst = dst_root / src.name
        ensured_copy(file_in=str(src), file_out=str(dst))
        return str(dst)

    def get_rand_md_test_file_path(self):
        """
        Return rand md test file path under the fixture contract.

        Example:
            Exercise TestObjectsHandler.get rand md test file path through a consuming regression::

                python -m pytest -q tests/databases/test_test_resources_manager.py


        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        file_ext, file_num = self.get_rand_md_test_file()
        return self.get_test_md_file_path(file_ext=file_ext, file_num=file_num)

    def get_rand_test_file(self, file_ext="txt"):
        """
        Return rand test file under the fixture contract.

        Example:
            Exercise TestObjectsHandler.get rand test file through a consuming regression::

                python -m pytest -q tests/databases/test_test_resources_manager.py


        :param file_ext: Value supplied for file ext under the deterministic fixture
            contract.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        dst_root = self._new_scratch_folder()
        dst = dst_root / "test_file_delete_me.{}".format(file_ext.lstrip("."))
        with dst.open("w+", encoding="utf-8") as open_file:
            open_file.write(str(uuid.uuid4()))
        return str(dst)

    @staticmethod
    def get_cover_file_name(book_id):
        """
        Return cover file name under the fixture contract.

        Example:
            Exercise TestObjectsHandler.get cover file name through a consuming regression::

                python -m pytest -q tests/databases/test_test_resources_manager.py


        :param book_id: Value supplied for book id under the deterministic fixture contract.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        return "book_id_{}.jpg".format(book_id)

    def _find_cover_source(self, book_id) -> Path:
        """
        Perform the find cover source step with deterministic fixture inputs.

        Example:
            Exercise TestObjectsHandler. find cover source through a consuming regression::

                python -m pytest -q tests/databases/test_test_resources_manager.py


        :param book_id: Value supplied for book id under the deterministic fixture contract.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        explicit = list(self._cover_source_dir.glob("book_id_{}.*".format(book_id)))
        if explicit:
            return sorted(explicit, key=lambda path: path.name.casefold())[0]
        raise FileNotFoundError("No cover file found for book_id={}".format(book_id))

    def get_test_cover_path(self, book_id):
        """
        Return test cover path under the fixture contract.

        Example:
            Exercise TestObjectsHandler.get test cover path through a consuming regression::

                python -m pytest -q tests/databases/test_test_resources_manager.py


        :param book_id: Value supplied for book id under the deterministic fixture contract.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        return self.get_scratch_test_cover_file(book_id)

    def get_scratch_test_cover_file(self, book_id):
        """
        Return scratch test cover file under the fixture contract.

        Example:
            Exercise TestObjectsHandler.get scratch test cover file through a consuming regression::

                python -m pytest -q tests/databases/test_test_resources_manager.py


        :param book_id: Value supplied for book id under the deterministic fixture contract.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        src = self._find_cover_source(book_id)
        dst_root = self._new_scratch_folder()
        dst = dst_root / src.name
        ensured_copy(file_in=str(src), file_out=str(dst))
        return str(dst)

    def get_rand_test_cover_path(self):
        """
        Return rand test cover path under the fixture contract.

        Example:
            Exercise TestObjectsHandler.get rand test cover path through a consuming regression::

                python -m pytest -q tests/databases/test_test_resources_manager.py


        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        src = self.rng.choice(self.available_test_book_covers)
        return self.get_scratch_test_cover_file(book_id=src.stem.split("_")[-1])


class _EphemeralScratchFolderManager:
    """
    Local fallback for tests that instantiate TestObjectsHandler without a manager.

    Example:
        Exercise  EphemeralScratchFolderManager through a consuming regression::

            python -m pytest -q tests/databases/test_test_resources_manager.py
    """

    def get_scratch_folder(self, filename=None):
        """
        Return scratch folder under the fixture contract.

        Example:
            Exercise  EphemeralScratchFolderManager.get scratch folder through a consuming regression::

                python -m pytest -q tests/databases/test_test_resources_manager.py


        :param filename: Archive member or fixture filename.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        prefix = "liuxin_test_objects_"
        if filename:
            prefix += str(filename)
        return tempfile.mkdtemp(prefix=prefix)
