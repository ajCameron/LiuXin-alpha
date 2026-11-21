# Stands in for the metadata-reading part of calibre 9.18's src/calibre/customize/ui.py
# (get_file_type_metadata), whose plugins are here the ported reader modules.
# License: GPL v3. Copyright: Kovid Goyal.
# SPDX-License-Identifier: GPL-3.0-only
"""
Run the ported reader for a file type, as calibre's ``customize.ui.get_file_type_metadata`` does.

calibre's ZIP, RAR and comic readers read the books they find inside through
calibre's plugin system. Here each type goes to the ported module whose
``get_metadata`` calibre's plugin for that type runs, called as the plugin calls it.

Example:
    >>> import io
    >>> from LiuXin_alpha.file_formats.providers.ported_calibre import ported_reader
    >>> from LiuXin_alpha.metadata.upstream_calibre import _readers
    >>> _readers.lookup = ported_reader
    >>> get_file_type_metadata(io.BytesIO(b"Hello"), "TXT ").title
    'Unknown'
"""

from __future__ import annotations

import traceback
from collections.abc import Callable
from typing import IO, Any

#: calibre's metadata reader plugins that are ported, by file type: the module in
#: this package whose ``get_metadata`` the plugin runs.
READER_MODULES: dict[str, str] = {
    "txt": "txt",
    "imp": "imp",
    "rb": "rb",
    "lrx": "lrx",
    "snb": "snb",
    "rtf": "rtf",
    **dict.fromkeys(("tpz", "azw1"), "topaz"),
    **dict.fromkeys(("pml", "pmlz"), "pml"),
    "opf": "opf",
    **dict.fromkeys(("epub", "kepub"), "epub"),
    **dict.fromkeys(("htmlz", "txtz"), "extz"),
    "html": "html",
    **dict.fromkeys(("fb2", "fbz"), "fb2"),
    "odt": "odt",
    **dict.fromkeys(("mobi", "prc", "azw", "azw3", "azw4", "pobi"), "mobi"),
    **dict.fromkeys(("pdb", "updb"), "pdb"),
    "lrf": "lrf",
    "docx": "docx",
    "pdf": "pdf",
    **dict.fromkeys(("cbz", "cbr", "cb7", "cbc"), "comic"),
    **dict.fromkeys(("zip", "oebzip"), "zip"),
    "rar": "rar",
    "lit": "lit",
    "chm": "chm",
}

#: Modules whose ``get_metadata`` calibre's plugin calls with the file type.
PASSES_FORMAT: frozenset[str] = frozenset({"epub", "comic"})


#: Finds a ported reader by module name, returning its ``get_metadata`` as calibre's
#: plugin calls it: ``file_formats.providers.ported_calibre.ported_reader``, which sets
#: this when it runs a ported reader. The readers are not imported here: calibre's
#: archive readers reach this module through ``meta``, and several readers import
#: ``meta`` back, so importing them would make the readers one import cycle.
lookup: Callable[[str], Callable[..., Any]] | None = None


def run_reader(module: str, stream: IO[bytes], ftype: str) -> Any:
    """
    Run one ported reader on a stream as calibre's plugin for it does.

    Example:
        >>> import io
        >>> from LiuXin_alpha.file_formats.providers.ported_calibre import ported_reader
        >>> from LiuXin_alpha.metadata.upstream_calibre import _readers
        >>> _readers.lookup = ported_reader
        >>> run_reader("txt", io.BytesIO(b"Hello"), "txt").title
        'Unknown'


    :param module: The module's name in this package.
    :param stream: The book.
    :param ftype: Its file type.
    :return: calibre's metadata object.
    """
    if lookup is None:
        raise LookupError(
            "no ported reader lookup: run the ported readers through "
            "file_formats.providers.ported_calibre, which knows where they are"
        )
    get_metadata = lookup(module)
    if module in PASSES_FORMAT:
        return get_metadata(stream, ftype=ftype)
    return get_metadata(stream)


def get_file_type_metadata(stream: IO[bytes], ftype: str) -> Any:
    """
    Read a book with the ported reader for its type, as calibre's ``customize.ui`` does.

    As in calibre, a reader that fails is reported and empty metadata returned, and
    a type with no reader gives empty metadata.

    Example:
        >>> import io
        >>> get_file_type_metadata(io.BytesIO(b""), "lit").is_null("title")
        True


    :param stream: The book.
    :param ftype: Its file type.
    :return: calibre's metadata object.
    """
    from LiuXin_alpha.metadata.upstream_calibre.metadata_base import MetaInformation

    mi = MetaInformation(None, None)
    ftype = ftype.lower().strip()
    module = READER_MODULES.get(ftype)
    if module is None:
        return mi
    try:
        if hasattr(stream, "seek"):
            stream.seek(0)
        mi = run_reader(module, stream, ftype)
    except Exception:
        traceback.print_exc()
    return mi
