"""
Check that LiuXin's readers report a book's languages as calibre's readers do.

Before LiuXin had a language table, its readers reported one language as ``en``,
``fra`` or ``English`` depending on the format; calibre's report ``eng``. This
compares them on every book in ``LiuXin_alpha_data`` when it is checked out.

Example:
    Run this check::

        python -m pytest -q tests/metadata/file_sources/test_reader_languages.py
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from LiuXin_alpha.file_formats.plugin_runtime import get_file_type_metadata
from LiuXin_alpha.file_formats.providers.ported_calibre import (
    PORTED_READERS,
    ported_reader,
)

MODULES = {
    file_type: module
    for module, file_types in PORTED_READERS.values()
    for file_type in file_types
}


def _languages(raw: Any) -> list[str]:
    """
    List a record's languages, whichever container holds them.

    Example:
        >>> _languages(("eng",)), _languages(None), _languages("fra")
        (['eng'], [], ['fra'])


    :param raw: The field.
    :return: The codes.
    """
    if not raw:
        return []
    if isinstance(raw, str):
        return [raw]
    return [str(code) for code in raw]


def test_every_real_book_reads_the_same_languages_as_calibre(
    md_test_files_by_ext: dict[str, list[Path]],
) -> None:
    """
    For each book a ported calibre reader reads, LiuXin's reader gives the same languages.

    Example:
        >>> test_every_real_book_reads_the_same_languages_as_calibre({})  # doctest: +SKIP


    :param md_test_files_by_ext: The data repository's books, by extension.
    :return: None after every book is compared.
    """
    compared = 0
    for file_type, paths in md_test_files_by_ext.items():
        if file_type not in MODULES:
            continue
        for path in paths:
            with path.open("rb") as stream:
                ours = _languages(
                    getattr(
                        get_file_type_metadata(stream, file_type), "languages", None
                    )
                )
            with path.open("rb") as stream:
                theirs = _languages(ported_reader(MODULES[file_type])(stream).languages)
            assert ours == theirs, path.name
            compared += 1
    assert compared >= 30
