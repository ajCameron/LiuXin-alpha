# License: GPLv3 Copyright: 2009, John Schember <john@nachtimwald.com>
# Ported from calibre (https://calibre-ebook.com) 9.18: src/calibre/ebooks/txt/processor.py
# Changes from upstream: calibre imports point at upstream_calibre and its _compat, and
# 'from __future__ import annotations' is added.
# Only HTML_TEMPLATE and opf_writer are ported, with the imports they use;
# the rest is conversion code the metadata readers do not use.
# SPDX-License-Identifier: GPL-3.0-only

"""
Read content from txt file.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from LiuXin_alpha.metadata.upstream_calibre.opf2 import OPFCreator

if TYPE_CHECKING:
    from collections.abc import Iterable
    from LiuXin_alpha.metadata.upstream_calibre.book_base import Metadata


HTML_TEMPLATE = '<html><head><meta http-equiv="Content-Type" content="text/html; charset=utf-8"/><title>%s </title></head><body>\n%s\n</body></html>'


def opf_writer(path: str, opf_name: str, manifest: Iterable[tuple[str, str | None]], spine: Iterable[str], mi: Metadata) -> None:
    opf = OPFCreator(path, mi)
    opf.create_manifest(manifest)
    opf.create_spine(spine)
    with open(os.path.join(path, opf_name), 'wb') as opffile:
        opf.render(opffile)
