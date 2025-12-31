# -*- coding: utf-8 -*-
"""
Pure-python fallback for the compiled ``podofo`` extension.

The compiled extension exposes a PDF parser based on PoDoFo. This fallback is a stub
that keeps imports working and provides a clear error at runtime.

If you want PDF metadata/text extraction without compiling PoDoFo, consider wiring
this to an external tool (like `pdftotext`) or adding an optional dependency such
as `pypdf` in your application layer.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


class Error(Exception):
    pass


@dataclass
class PDFDoc:
    source: object

    def __post_init__(self) -> None:
        raise Error("podofo is not available; PDFDoc requires the compiled extension or an alternative backend")
