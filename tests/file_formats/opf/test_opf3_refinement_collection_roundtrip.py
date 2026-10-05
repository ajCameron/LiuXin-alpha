"""
Provide test opf3 refinement collection roundtrip utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test opf3 refinement collection roundtrip through a consuming regression::

        python -m pytest -q tests/file_formats/opf/test_opf3_refinement_collection_roundtrip.py
"""
from __future__ import annotations

import importlib

import pytest

import LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.utils

pytest.importorskip("lxml")
from lxml import etree

OPF_NS = "http://www.idpf.org/2007/opf"
DC_NS = "http://purl.org/dc/elements/1.1/"

OPF3_WITH_REFINEMENTS_AND_COLLECTIONS = b"""<?xml version='1.0' encoding='utf-8'?>
<package xmlns="http://www.idpf.org/2007/opf"
         xmlns:dc="http://purl.org/dc/elements/1.1/"
         unique-identifier="BookId"
         version="3.0">
  <metadata>
    <dc:identifier id="BookId">urn:uuid:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee</dc:identifier>
    <dc:title id="title-main">Seed Title</dc:title>
    <meta refines="#title-main" property="title-type">main</meta>
    <meta refines="#title-main" property="file-as">Title, Seed</meta>
    <meta refines="#title-main" property="display-seq">1</meta>
    <dc:creator id="creator-main">Author Example</dc:creator>
    <dc:language>en</dc:language>
    <meta property="dcterms:modified">2020-01-01T00:00:00Z</meta>
    <meta id="series-col" property="belongs-to-collection">Seed Series</meta>
    <meta refines="#series-col" property="collection-type">series</meta>
    <meta refines="#series-col" property="group-position">4</meta>
    <meta id="set-col" property="belongs-to-collection">Library Universe</meta>
    <meta refines="#set-col" property="collection-type">set</meta>
    <meta refines="#set-col" property="group-position">2</meta>
    <meta refines="#set-col" property="display-seq">9</meta>
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


@pytest.fixture()
def opf_mod(legacy_liuxin_alias):
    """
    Perform the opf mod operation under explicit file-format and conversion rules.

    Example:
        Exercise opf mod through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_refinement_collection_roundtrip.py


    :param legacy_liuxin_alias: Value supplied for legacy liuxin alias under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return importlib.import_module("LiuXin_alpha.file_formats.opf.opf")


def _collection_nodes(root: etree._Element) -> list[etree._Element]:
    """
    Perform the collection nodes operation under explicit file-format and conversion rules.

    Example:
        Exercise  collection nodes through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_refinement_collection_roundtrip.py


    :param root: Root directory that bounds path resolution or traversal.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return root.xpath(".//opf:metadata/opf:meta[@property='belongs-to-collection']", namespaces={"opf": OPF_NS})


def _refines_for(root: etree._Element, elem_id: str) -> dict[str, str]:
    """
    Perform the refines for operation under explicit file-format and conversion rules.

    Example:
        Exercise  refines for through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_refinement_collection_roundtrip.py


    :param root: Root directory that bounds path resolution or traversal.
    :param elem_id: Value supplied for elem id under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    ans: dict[str, str] = {}
    for meta in root.xpath(".//opf:metadata/opf:meta[@refines=$rid]", namespaces={"opf": OPF_NS}, rid="#" + elem_id):
        prop = (meta.get("property") or "").strip()
        text = (meta.text or "").strip()
        if prop:
            ans[prop] = text
    return ans


def _find_collection(root: etree._Element, text: str) -> etree._Element | None:
    """
    Find collection under the format's safety and compatibility rules.

    Example:
        Exercise  find collection through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_refinement_collection_roundtrip.py


    :param root: Root directory that bounds path resolution or traversal.
    :param text: Text parsed, normalized or rendered.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    for node in _collection_nodes(root):
        if (node.text or "").strip() == text:
            return node
    return None


def test_opf3_set_metadata_preserves_non_series_collection_and_title_refines(opf_mod) -> None:
    """
    Perform the test opf3 set metadata preserves non series collection and title refines operation under explicit file-format and conversion rules.

    Example:
        Exercise test opf3 set metadata preserves non series collection and title refines through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_refinement_collection_roundtrip.py


    :param opf_mod: Value supplied for opf mod under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    mi, ver, *_ = opf_mod.get_metadata(OPF3_WITH_REFINEMENTS_AND_COLLECTIONS)
    assert ver.major == 3
    assert mi.series == "Seed Series"
    assert float(mi.series_index) == 4.0

    mi.title = "Updated Title"
    mi.title_sort = "Title, Updated"
    mi.series = "Updated Series"
    mi.series_index = 8.0

    out, ver2, _ = opf_mod.set_metadata(OPF3_WITH_REFINEMENTS_AND_COLLECTIONS, mi)
    assert ver2.major == 3
    root = etree.fromstring(bytes(out))

    # Series collection should be replaced with updated values.
    updated_series = _find_collection(root, "Updated Series")
    assert updated_series is not None
    updated_series_refines = _refines_for(root, updated_series.get("id") or "")
    assert updated_series_refines.get("collection-type") == "series"
    assert updated_series_refines.get("group-position") in {"8", "8.0"}

    # Non-series collection (and its custom refines) should survive unchanged.
    kept_set = _find_collection(root, "Library Universe")
    assert kept_set is not None
    kept_set_refines = _refines_for(root, kept_set.get("id") or "")
    assert kept_set_refines.get("collection-type") == "set"
    assert kept_set_refines.get("group-position") == "2"
    assert kept_set_refines.get("display-seq") == "9"

    # Main title should be updated and non-managed title refine preserved.
    title = root.find(f".//{{{DC_NS}}}title")
    assert title is not None
    assert (title.text or "").strip() == "Updated Title"
    title_id = title.get("id")
    assert title_id
    title_refines = _refines_for(root, title_id)
    assert title_refines.get("title-type") == "main"
    assert title_refines.get("file-as") == "Title, Updated"
    assert title_refines.get("display-seq") == "1"

    # Ensure no duplicate series collections were left behind.
    series_collections = []
    for node in _collection_nodes(root):
        rid = node.get("id") or ""
        props = _refines_for(root, rid)
        if props.get("collection-type") == "series":
            series_collections.append(node)
    assert len(series_collections) == 1
