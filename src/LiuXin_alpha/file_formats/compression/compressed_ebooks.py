"""
Inspect compressed ebook containers and select their primary content member.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise compressed ebooks through a consuming regression::

        python -m pytest -q tests/file_formats/compression/test_compression_modernized.py
"""

from __future__ import annotations

import os
import zipfile
from collections import Counter
from collections.abc import Iterable

from LiuXin_alpha.constants.file_extensions import (
    BOOK_EXTENSIONS_DOTTED,
    RAR_BOOK_FILE_CONTENTS,
    RAR_BOOK_FILE_CONTENTS_DOTTED,
)
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.storage.local.file_properties import get_file_ext

try:
    import rarfile as _rarfile  # pyright: ignore[reportMissingImports]
except Exception:
    try:
        from LiuXin_alpha.utils.decompression.rarfile import rarfile as _rarfile  # type: ignore
    except Exception:
        _rarfile = None


__author__ = "Cameron"


def _count_file_types(file_names: Iterable[object]) -> Counter[str]:
    """
    Count lower-cased extensions (with leading dot).

    Example:
        Exercise  count file types through a consuming regression::

            python -m pytest -q tests/file_formats/compression/test_compression_modernized.py


    :param file_names: Value supplied for file names under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    counter = Counter()
    for name in file_names:
        _base, ext = os.path.splitext(str(name))
        if ext:
            counter[ext.lower()] += 1
    return counter


def is_ebook(list_of_names: Iterable[object]) -> bool:
    """
    Return True if filenames look like a single ebook archive.

    Example:
        Exercise is ebook through a consuming regression::

            python -m pytest -q tests/file_formats/compression/test_compression_modernized.py


    :param list_of_names: Value supplied for list of names under the utility contract.
    :return: True when the documented condition holds; otherwise False.
    """
    names = [str(x) for x in list_of_names]
    if not names:
        return False

    if is_comic(names):
        return True

    allowed = {x.lower() for x in RAR_BOOK_FILE_CONTENTS}
    for name in names:
        _base, ext = os.path.splitext(name)
        if not ext:
            continue
        if ext[1:].lower() not in allowed:
            return False
    return True


def is_comic(list_of_names: Iterable[object]) -> bool:
    """
    Return True when all relevant files are comic image types.

    Example:
        Exercise is comic through a consuming regression::

            python -m pytest -q tests/file_formats/compression/test_compression_modernized.py


    :param list_of_names: Value supplied for list of names under the utility contract.
    :return: True when the documented condition holds; otherwise False.
    """
    names = [str(x) for x in list_of_names]
    extensions = {
        x.rpartition(".")[-1].lower()
        for x in names
        if "." in x and x.lower().rpartition("/")[-1] != "thumbs.db"
    }
    comic_extensions = {"jpg", "jpeg", "png", "webp", "gif"}
    return bool(extensions) and len(extensions - comic_extensions) == 0


def is_file_book(file_path: str | os.PathLike[str]) -> bool:
    """
    Return True if the path is a book file or a book-like archive.

    Example:
        Exercise is file book through a consuming regression::

            python -m pytest -q tests/file_formats/compression/test_compression_modernized.py


    :param file_path: Value supplied for file path under the utility contract.
    :return: True when the documented condition holds; otherwise False.
    """
    ext = get_file_ext(os.fspath(file_path)).lower()

    if ext == ".zip":
        return is_zip_archive_book(file_path)
    if ext == ".rar":
        return is_rar_archive_book(file_path)

    return ext in {x.lower() for x in BOOK_EXTENSIONS_DOTTED}


def is_zip_archive_book(file_path: str | os.PathLike[str]) -> bool:
    """
    Heuristic: a zip archive is an ebook when all contents are ebook resources.

    Example:
        Exercise is zip archive book through a consuming regression::

            python -m pytest -q tests/file_formats/compression/test_compression_modernized.py


    :param file_path: Value supplied for file path under the utility contract.
    :return: True when the documented condition holds; otherwise False.
    """
    try:
        with zipfile.ZipFile(file_path, "r") as myzip:
            files = myzip.namelist()
    except Exception:
        return False

    if not files:
        return False

    extensions = _count_file_types(files)
    allowed = {x.lower() for x in RAR_BOOK_FILE_CONTENTS_DOTTED}
    return all(ext in allowed for ext in extensions)


def is_rar_archive_book(file_path: str | os.PathLike[str]) -> bool:
    """
    Heuristic: a rar archive is an ebook when all contents are ebook resources.

    Example:
        Exercise is rar archive book through a consuming regression::

            python -m pytest -q tests/file_formats/compression/test_compression_modernized.py


    :param file_path: Value supplied for file path under the utility contract.
    :return: True when the documented condition holds; otherwise False.
    """
    if _rarfile is None:
        return False

    try:
        with _rarfile.RarFile(file_path) as archive:  # type: ignore[attr-defined]
            files = [item.filename for item in archive.infolist()]
    except Exception as err:
        default_log.log_exception(
            f"Error parsing rar archive: {file_path}",
            err,
            "INFO",
        )
        return False

    if not files:
        return False

    extensions = _count_file_types(files)
    allowed = {x.lower() for x in RAR_BOOK_FILE_CONTENTS_DOTTED}
    return all(ext in allowed for ext in extensions)
