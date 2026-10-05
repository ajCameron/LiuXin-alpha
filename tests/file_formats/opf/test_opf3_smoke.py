"""
Provide test opf3 smoke utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test opf3 smoke through a consuming regression::

        python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py
"""

from __future__ import annotations

import importlib
import pytest

pytest.importorskip("lxml")
from lxml import etree

DC_NS = "http://purl.org/dc/elements/1.1/"

OPF3_MINIMAL = b"""<?xml version='1.0' encoding='utf-8'?>
<package xmlns="http://www.idpf.org/2007/opf"
         xmlns:dc="http://purl.org/dc/elements/1.1/"
         unique-identifier="BookId"
         version="3.0">
  <metadata>
    <dc:identifier id="BookId">urn:uuid:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee</dc:identifier>
    <dc:title>OPF3 Smoke</dc:title>
    <dc:creator id="creator">Bob Example</dc:creator>
    <dc:language>en</dc:language>
    <meta property="dcterms:modified">2020-01-01T00:00:00Z</meta>
  </metadata>
  <manifest>
    <item id="chap1" href="text/chap1.xhtml" media-type="application/xhtml+xml"/>
    <item id="nav" href="nav.xhtml" media-type="application/xhtml+xml" properties="nav"/>
  </manifest>
  <spine>
    <itemref idref="chap1"/>
  </spine>
</package>
"""


def _dc_text(root: etree._Element, tag: str) -> str | None:
    """
    Perform the dc text operation under explicit file-format and conversion rules.

    Example:
        Exercise  dc text through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param tag: Value supplied for tag under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    el = root.find(f".//{{{DC_NS}}}{tag}")
    return el.text if el is not None else None


def test_read_and_apply_metadata_smoke(legacy_liuxin_alias) -> None:
    """
    Perform the test read and apply metadata smoke operation under explicit file-format and conversion rules.

    Example:
        Exercise test read and apply metadata smoke through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param legacy_liuxin_alias: Value supplied for legacy liuxin alias under the utility
        contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    opf3 = importlib.import_module("LiuXin_alpha.file_formats.opf.opf3")
    read_metadata = getattr(opf3, "read_metadata")
    apply_metadata = getattr(opf3, "apply_metadata")

    root = etree.fromstring(OPF3_MINIMAL)
    mi = read_metadata(root)
    assert getattr(mi, "title", None) == "OPF3 Smoke"

    mi.title = "OPF3 Smoke (Updated)"
    apply_metadata(root, mi, apply_null=False, update_timestamp=False)

    assert _dc_text(root, "title") == "OPF3 Smoke (Updated)"
