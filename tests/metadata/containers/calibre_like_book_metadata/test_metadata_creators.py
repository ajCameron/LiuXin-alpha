# tests/metadata/containers/calibre_like_book_metadata/test_metadata_creators.py

from __future__ import annotations

from collections import OrderedDict

import pytest

from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import CalibreLikeLiuXinBookMetaData
from LiuXin_alpha.metadata.constants import CREATOR_CATEGORIES
from LiuXin_alpha.metadata.standardize import standardize_creator_category


def test_creators_property_and_get_authors_copy() -> None:
    md = CalibreLikeLiuXinBookMetaData(title="T", authors=["A", "B"])
    creators = md.creators
    assert "authors" in creators
    assert "A" in creators["authors"]
    assert "B" in creators["authors"]

    authors_copy = md.get_authors_copy()
    assert isinstance(authors_copy, list)
    assert set(authors_copy) >= {"A", "B"}


def test_add_creators_multiple_roles_and_dump_roundtrip() -> None:
    """
    Tests adding creators and a dump roundtrip.

    :return:
    """
    md = CalibreLikeLiuXinBookMetaData()

    md.add_creators(
        {
            "authors": ["Author One", "Author Two"],
            "editors": "Editor One",
        }
    )

    creators = md.creators
    assert "Author One" in creators.get("authors", [])
    assert "Author Two" in creators.get("authors", [])
    assert "Editor One" in creators.get("editors", [])

    dump = md.get_creators_dump()
    assert isinstance(dump, dict)
    for role, od in dump.items():
        assert isinstance(od, OrderedDict)

    md2 = CalibreLikeLiuXinBookMetaData()
    md2.update_creators(dump)
    creators2 = md2.creators
    assert "Author One" in creators2.get("authors", [])
    assert "Editor One" in creators2.get("editors", [])


def test_read_creators_accepts_duck_typed_input() -> None:
    md = CalibreLikeLiuXinBookMetaData()

    md.read_creators(
        {
            "authors": OrderedDict([("A", None)]),
            "illustrators": ["I1", "I2"],
        }
    )
    creators = md.creators
    assert "A" in creators.get("authors", [])
    assert "I1" in creators.get("illustrators", [])
    assert "I2" in creators.get("illustrators", [])


def test_creator_category_standardization_smoke() -> None:
    # Exercise standardizer used by creator setters
    for cat in list(CREATOR_CATEGORIES)[:5]:
        norm = standardize_creator_category(cat)
        assert norm is None or isinstance(norm, str)


def test_setting_creators_directly_is_blocked() -> None:
    md = CalibreLikeLiuXinBookMetaData()
    with pytest.raises(AttributeError):
        md.creators = {"authors": ["X"]}  # type: ignore[assignment]
