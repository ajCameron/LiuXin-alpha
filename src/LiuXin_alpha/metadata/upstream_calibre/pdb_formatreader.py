# License: GPLv3 Copyright: 2009, John Schember <john@nachtimwald.com>
# Ported from calibre (https://calibre-ebook.com) 9.18: src/calibre/ebooks/pdb/formatreader.py
# Changes from upstream: calibre imports point at upstream_calibre and its _compat.
# SPDX-License-Identifier: GPL-3.0-only

"""
Interface defining the necessary public functions for a pdb format reader.
"""

from typing import IO, TYPE_CHECKING

if TYPE_CHECKING:
    from optparse import Values

    from LiuXin_alpha.metadata.upstream_calibre._compat import OEBBook
    from LiuXin_alpha.metadata.upstream_calibre.pdb_header import PdbHeaderReader
    from LiuXin_alpha.metadata.upstream_calibre._compat import Log


class FormatReader:
    def __init__(self, header: PdbHeaderReader, stream: IO[bytes], log: Log, options: Values) -> None:
        raise NotImplementedError()

    def extract_content(self, output_dir: str) -> str | OEBBook | None:
        raise NotImplementedError()
