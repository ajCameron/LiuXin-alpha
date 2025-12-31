# tests/metadata/containers/calibre_like_book_metadata/test_metadata_identifiers.py

from __future__ import annotations

from collections import OrderedDict

import pytest

from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import CalibreLikeLiuXinBookMetaData
from LiuXin_alpha.metadata.constants import EXTERNAL_EBOOK_ID_SCHEMA, INTERNAL_EBOOK_ID_SCHEMA
from LiuXin_alpha.metadata.standardize import standardize_id_name, standardize_internal_id_name


def test_set_identifier_and_has_identifier_and_remove() -> None:
    """
    Tests the round trip of setting, having and then removing an identifier.

    :return:
    """
    md = CalibreLikeLiuXinBookMetaData()

    ext = next(iter(EXTERNAL_EBOOK_ID_SCHEMA))
    md.set_identifier(ext, "VAL1")
    assert md.has_identifier(ext) is True

    # calibre-like attr assignment also routes through id normalization
    setattr(md, ext, "VAL2")
    container = getattr(md, ext)
    assert isinstance(container, OrderedDict)
    assert "VAL1" in container or "VAL2" in container

    # remove
    md.set_identifier(ext, None)
    assert md.has_identifier(ext) is False


def test_set_identifiers_accepts_str_list_set_ordereddict() -> None:
    md = CalibreLikeLiuXinBookMetaData()
    ext = next(iter(EXTERNAL_EBOOK_ID_SCHEMA))

    md.set_identifiers({ext: "X"})
    md.set_identifiers({ext: ["Y", "Z"]})
    md.set_identifiers({ext: {"W"}})
    md.set_identifiers({ext: OrderedDict([("Q", None)])})

    container = getattr(md, ext)
    assert isinstance(container, OrderedDict)
    for v in ["X", "Y", "Z", "W", "Q"]:
        assert v in container


def test_internal_identifiers_add_and_get() -> None:
    md = CalibreLikeLiuXinBookMetaData()
    internal = next(iter(INTERNAL_EBOOK_ID_SCHEMA))

    # Many callers will not know internal naming; normalize is supported
    norm_internal = standardize_internal_id_name(internal) or internal
    md.add_internal_identifiers({norm_internal: "I1"})
    ids = md.get_internal_identifiers()
    assert norm_internal in ids
    assert "I1" in ids[norm_internal]


def test_read_identifiers_uses_cleaning_and_normalization() -> None:
    md = CalibreLikeLiuXinBookMetaData()

    # pick a normalized external id name candidate
    ext = next(iter(EXTERNAL_EBOOK_ID_SCHEMA))
    norm = standardize_id_name(ext, logging=False) or ext

    md.read_identifiers({norm: ["  V1  ", "V2"]})
    container = getattr(md, norm)

    assert isinstance(container, OrderedDict)

    assert "V1" in [cv for cv in container.keys()]
    assert "V2" in [cv for cv in container.keys()]


def test_identifier_type_unknown_is_ignored_or_raises_cleanly() -> None:
    md = CalibreLikeLiuXinBookMetaData()

    # If your implementation ignores unknown identifier keys, this should pass.
    # If it raises, ensure it raises a deterministic exception type.
    try:
        md.set_identifiers({"definitely_not_a_real_identifier": "X"})
    except Exception as e:  # pragma: no cover
        assert isinstance(e, (KeyError, ValueError, NotImplementedError, AttributeError))
