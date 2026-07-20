"""Tests for the database-link update produced by catalog writers."""

from __future__ import annotations

import pytest

from LiuXin_alpha.catalog.write import LinkUpdate
from LiuXin_alpha.databases.macro_types import LINK_TYPE_UNSET, LinkValue
from LiuXin_alpha.databases.schema_specs import StorageLinkSpec


def _link_spec() -> StorageLinkSpec:
    return StorageLinkSpec(
        primary_table="titles",
        secondary_table="creators",
        link_table="creator_title_links",
        primary_link_col="creator_title_link_title_id",
        secondary_link_col="creator_title_link_creator_id",
        priority_link_col="creator_title_link_priority",
        type_link_col="creator_title_link_type",
        ordered=True,
        typed=True,
        type_part_of_identity=True,
    )


def test_link_update_materialises_complete_replacement_sets() -> None:
    supplied = [
        LinkValue(
            secondary_id=20,
            link_type="author",
            priority=2,
            extra={"credited_as": "A. Writer"},
        ),
        LinkValue(secondary_id=21, link_type="author", priority=1),
    ]

    update = LinkUpdate(
        link_spec=_link_spec(),
        replacements={
            10: (link for link in supplied),
            11: (),
        },
    )

    supplied.clear()

    assert update.replacements[10] == (
        LinkValue(
            secondary_id=20,
            link_type="author",
            priority=2,
            extra={"credited_as": "A. Writer"},
        ),
        LinkValue(secondary_id=21, link_type="author", priority=1),
    )
    assert update.replacements[11] == ()
    assert update.link_type is LINK_TYPE_UNSET


def test_link_update_can_scope_a_typed_replacement() -> None:
    update = LinkUpdate(
        link_spec=_link_spec(),
        replacements={10: [LinkValue(secondary_id=20, link_type="editor")]},
        link_type="editor",
    )

    assert update.link_type == "editor"


def test_link_update_rejects_ambiguous_or_untyped_payloads() -> None:
    with pytest.raises(TypeError, match="empty iterable"):
        LinkUpdate(link_spec=_link_spec(), replacements={10: None})  # type: ignore[dict-item]

    with pytest.raises(TypeError, match="LinkValue"):
        LinkUpdate(link_spec=_link_spec(), replacements={10: [20]})  # type: ignore[list-item]


def test_link_update_replacement_map_is_read_only() -> None:
    update = LinkUpdate(link_spec=_link_spec(), replacements={10: []})

    with pytest.raises(TypeError):
        update.replacements[10] = (LinkValue(secondary_id=20),)  # type: ignore[index]
