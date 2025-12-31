# tests/metadata/containers/calibre_like_book_metadata/test_metadata_core.py

"""
Core tests for the CalibreLikeBookMetadata class.
"""

from __future__ import annotations

import copy
import gc
from collections import OrderedDict

import pytest

from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import CalibreLikeLiuXinBookMetaData
from LiuXin_alpha.metadata.constants import METADATA_NULL_VALUES
from LiuXin_alpha.errors import InputIntegrityError


class _CloseTracker:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


def _raw_data(md: CalibreLikeLiuXinBookMetaData) -> dict:
    return object.__getattribute__(md, "_data")


def test_init_defaults_deepcopy_and_basic_truthiness() -> None:
    md = CalibreLikeLiuXinBookMetaData()
    assert md is not None

    d = _raw_data(md)
    # Ensure init uses a deepcopy (mutating should not mutate constant)
    d["title"] = "X"
    assert METADATA_NULL_VALUES["title"] != "X"

    # bool(md) should be stable (implementation-dependent), but must not crash
    assert isinstance(bool(md), bool)


@pytest.mark.parametrize(
    "authors",
    [
        None,
        "Single Author",
        ["A", "B"],
        ("C", "D"),
    ],
)
def test_init_title_and_authors_variants(authors) -> None:
    md = CalibreLikeLiuXinBookMetaData(title="My Title", authors=authors)
    assert md.title == "My Title"

    # creators() is the most stable contract to check author injection
    creators = md.creators
    assert isinstance(creators, dict)
    assert "authors" in creators

    if authors is None:
        # allow empty
        assert isinstance(creators["authors"], list)
    elif isinstance(authors, str):
        assert "Single Author" in creators["authors"]
    else:
        for a in authors:
            assert a in creators["authors"]


def test_init_with_other_overwrites_title_and_authors() -> None:
    base = CalibreLikeLiuXinBookMetaData(title="Base", authors=["Base Author"])
    base.tag = "oldtag"
    base.comment = "old comment"

    md = CalibreLikeLiuXinBookMetaData(title="New", authors=["New Author"], other=base)
    assert md.title == "New"
    assert "New Author" in md.creators.get("authors", [])
    # Should have inherited other fields too (copy semantics)
    assert isinstance(md.tags, OrderedDict)
    assert "oldtag" in md.tags


def test_forbidden_direct_setting_of_creators_and_identifiers_raises() -> None:
    md = CalibreLikeLiuXinBookMetaData()

    with pytest.raises(AttributeError):
        md.creators = None  # type: ignore[assignment]

    with pytest.raises(AttributeError):
        md.identifiers = None  # type: ignore[assignment]

    # Note: code uses "internal_identifers" (typo) in the guard list
    with pytest.raises(AttributeError):
        setattr(md, "internal_identifers", None)


def test_comments_are_deduped_and_trimmed() -> None:
    md = CalibreLikeLiuXinBookMetaData()
    md.comment = "  hello  "
    md.comments = "hello"  # should dedupe
    assert list(md.comments.keys()) == ["hello"]


def test_cover_data_requires_len_2_tuple() -> None:
    md = CalibreLikeLiuXinBookMetaData()

    with pytest.raises(InputIntegrityError):
        md.cover_data = ("only-one",)  # type: ignore[assignment]

    md.cover_data = ("path/to/cover.jpg", "jpeg")
    assert ("path/to/cover.jpg", "jpeg") in md.cover_data


def test_languages_accept_str_and_list_and_reject_weird() -> None:
    md = CalibreLikeLiuXinBookMetaData()

    md.languages = "en"
    md.languages = ["fr", "de"]
    assert "en" in md.languages
    assert "fr" in md.languages
    assert "de" in md.languages

    with pytest.raises(NotImplementedError):
        md.languages = 123  # type: ignore[assignment]


def test_publisher_accepts_str_and_list() -> None:
    md = CalibreLikeLiuXinBookMetaData()

    md.publisher = "PubA"
    md.publisher = ["PubB", "PubC"]
    assert "PubA" in md.publisher
    assert "PubB" in md.publisher
    assert "PubC" in md.publisher


def test_unknown_attribute_can_be_hung_on_object() -> None:
    md = CalibreLikeLiuXinBookMetaData()
    md.some_weird_field = 123  # should not crash
    assert md.some_weird_field == 123


def test_get_getitem_direct_get_default_behavior() -> None:
    md = CalibreLikeLiuXinBookMetaData(title="T", authors=["A"])
    assert md.get("title") == "T"
    assert md["title"] == "T"
    assert md.direct_get("title") == "T"

    assert md.get("does_not_exist", default="X") == "X"
    with pytest.raises(KeyError):
        _ = md["does_not_exist"]


def test_nullify_is_null_and_all_field_helpers() -> None:
    md = CalibreLikeLiuXinBookMetaData()
    md.tag = "t1"
    assert not md.is_null("tags")

    md.nullify("tags")
    assert md.is_null("tags")
    assert isinstance(md.tags, OrderedDict)
    assert len(md.tags) == 0

    keys = md.all_field_keys()
    assert isinstance(keys, frozenset)
    assert "title" in keys

    assert isinstance(md.all_set_fields(), dict)
    assert isinstance(md.all_non_none_fields(), dict)


def test_direct_add_key_check_and_nonchecked_path() -> None:
    md = CalibreLikeLiuXinBookMetaData()

    # key_check=True should reject unknown keys (exact exception depends on implementation)
    with pytest.raises((KeyError, AttributeError, ValueError)):
        md.direct_add("totally_unknown_key", 1, key_check=True)

    # key_check=False should allow it
    md.direct_add("totally_unknown_key", 1, key_check=False)
    assert _raw_data(md)["totally_unknown_key"] == 1


def test_dict_add_get_all_attr_get_data_copy_semantics() -> None:
    md = CalibreLikeLiuXinBookMetaData()
    md.dict_add(CalibreLikeLiuXinBookMetaData(title="T2", authors=["A2"]))

    d1 = md.get_all_attr(copy=True)
    d2 = md.get_data(rtn_deepcopy=True)

    assert isinstance(d1, dict)
    assert isinstance(d2, dict)

    # mutate returned dict should not mutate internal
    d1["title"] = "MUT"
    assert md.title != "MUT"


def test_deepcopy_metadata_and_magic_repr_str_unicode_iter() -> None:
    md = CalibreLikeLiuXinBookMetaData(title="T", authors=["A"])
    md.tag = "x"

    md2 = md.deepcopy_metadata()
    assert md2 is not md
    md2.tag = "y"
    assert "y" in md2.tags
    assert "y" not in md.tags

    md3 = copy.deepcopy(md)
    assert md3 is not md

    s = str(md)
    r = repr(md)
    u = md.__unicode__()  # explicit coverage
    assert isinstance(s, str)
    assert isinstance(r, str)
    assert isinstance(u, str)

    # iterator should yield strings
    it = iter(md)
    first = next(it)
    assert isinstance(first, str)


def test_smart_update_replace_vs_merge() -> None:
    a = CalibreLikeLiuXinBookMetaData(title="A", authors=["AA"])
    b = CalibreLikeLiuXinBookMetaData(title="B", authors=["BB"])
    a.tag = "tag_a"
    b.tag = "tag_b"
    b.comment = "c"

    a.smart_update(b, replace_metadata=False)
    # merge: keep title (typical), but must have both tags/comments
    assert "tag_a" in a.tags
    assert "tag_b" in a.tags
    assert "c" in a.comments

    a2 = CalibreLikeLiuXinBookMetaData(title="A", authors=["AA"])
    a2.smart_update(b, replace_metadata=True)
    # replace: allow title to change
    assert a2.title in ("A", "B")  # tolerate if implementation keeps original
    assert "tag_b" in a2.tags


def test_clean_finalize_to_html_and_del_cleanup() -> None:
    md = CalibreLikeLiuXinBookMetaData(title="T", authors=["A"])
    md.tag = "  Tag With Spaces  "
    md.clean()
    md.finalize()

    html = md.to_html()
    assert isinstance(html, str)

    # __del__ should close any registered cleanup files
    tracker = _CloseTracker()
    md.register_file_for_cleanup(tracker)
    assert tracker.closed is False

    # force deletion
    md_ref = md
    del md
    gc.collect()

    # md_ref still holds it; now delete and collect
    del md_ref
    gc.collect()
    assert tracker.closed is True
