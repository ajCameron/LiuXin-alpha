"""Tests for the database-link update produced by catalog writers."""

from __future__ import annotations

from collections import UserDict, defaultdict, deque
from collections.abc import Callable, Iterable, Mapping
from dataclasses import FrozenInstanceError, replace
from types import MappingProxyType

import pytest

from LiuXin_alpha.caches.write.utils import UpdateDict
from LiuXin_alpha.catalog import Catalog
from LiuXin_alpha.catalog.write import LinkUpdate, LinkUpdateEntry, LinkUpdateLink
from LiuXin_alpha.databases.macro_types import LINK_TYPE_UNSET, LinkRow, LinkValue
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


def _plain_link_spec() -> StorageLinkSpec:
    return StorageLinkSpec(
        primary_table="titles",
        secondary_table="tags",
        link_table="tag_title_links",
        primary_link_col="tag_title_link_title_id",
        secondary_link_col="tag_title_link_tag_id",
    )


def _operation_payload(operation: str, payload: object) -> dict[str, object]:
    return {operation: payload}


def _ids(update: LinkUpdate, operation: str, primary_id: int = 10) -> tuple[object, ...]:
    links = getattr(update, operation)[primary_id]
    return tuple(link.secondary_id for link in links)


class _RecordingMacros:
    """Small portable-macro double used to inspect update composition."""

    def __init__(
        self,
        current: Mapping[int, tuple[LinkRow, ...]] | None = None,
    ) -> None:
        self.current = dict(current or {})
        self.reads: list[tuple[StorageLinkSpec, tuple[int, ...], object]] = []
        self.writes: list[
            tuple[StorageLinkSpec, Mapping[int, Iterable[LinkValue]], object]
        ] = []

    def get_link_rows_bulk(
        self,
        link_spec: StorageLinkSpec,
        primary_ids: Iterable[int],
        *,
        link_type: object = LINK_TYPE_UNSET,
    ) -> dict[int, tuple[LinkRow, ...]]:
        ids = tuple(primary_ids)
        self.reads.append((link_spec, ids, link_type))
        return {primary_id: self.current.get(primary_id, ()) for primary_id in ids}

    def replace_links_bulk(
        self,
        link_spec: StorageLinkSpec,
        replacements: Mapping[int, Iterable[LinkValue]],
        *,
        link_type: object = LINK_TYPE_UNSET,
    ) -> dict[int, tuple[LinkRow, ...]]:
        stable = {
            primary_id: tuple(links)
            for primary_id, links in replacements.items()
        }
        self.writes.append((link_spec, stable, link_type))
        return {
            primary_id: tuple(
                LinkRow(
                    primary_id=primary_id,
                    secondary_id=link.secondary_id,
                    link_type=link.link_type,
                    priority=link.priority,
                    extra=link.extra,
                )
                for link in links
            )
            for primary_id, links in stable.items()
        }


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


def test_link_update_materialises_incremental_operations_and_link_extras() -> None:
    extra = {"credited_as": "A. Writer"}
    additions = [LinkValue(secondary_id=20, extra=extra)]
    deletions = [LinkValue(secondary_id=21)]

    update = LinkUpdate(
        link_spec=_link_spec(),
        additions={10: (link for link in additions)},
        deletions={10: (link for link in deletions)},
    )

    additions.clear()
    deletions.clear()
    extra["credited_as"] = "Changed"

    assert update.replacements == {}
    assert update.additions[10] == (
        LinkValue(secondary_id=20, extra={"credited_as": "A. Writer"}),
    )
    assert update.deletions[10] == (LinkValue(secondary_id=21),)

    with pytest.raises(TypeError):
        update.additions[10] = ()  # type: ignore[index]
    with pytest.raises(TypeError):
        update.additions[10][0].extra["credited_as"] = "Changed"  # type: ignore[index]


def test_link_update_can_scope_a_typed_replacement() -> None:
    update = LinkUpdate(
        link_spec=_link_spec(),
        replacements={10: [LinkValue(secondary_id=20, link_type="editor")]},
        link_type="editor",
    )

    assert update.link_type == "editor"
    assert update.replacements[10][0].link_type == "editor"


def test_link_update_scope_is_inherited_by_all_operations() -> None:
    update = LinkUpdate(
        link_spec=_link_spec(),
        link_type="editor",
        additions={10: [LinkValue(secondary_id=20)]},
        deletions={10: [LinkValue(secondary_id=21)]},
    )

    assert update.additions[10][0].link_type == "editor"
    assert update.deletions[10][0].link_type == "editor"

    with pytest.raises(ValueError, match="does not match update scope"):
        LinkUpdate(
            link_spec=_link_spec(),
            link_type="editor",
            additions={10: [LinkValue(secondary_id=20, link_type="author")]},
        )


def test_link_update_rejects_types_outside_the_declared_allowed_set() -> None:
    spec = replace(_link_spec(), allowed_types=("author", "editor"))

    with pytest.raises(ValueError, match="not allowed by the link spec"):
        LinkUpdate(
            link_spec=spec,
            replacements={10: (LinkValue(20, link_type="reviewer"),)},
        )
    with pytest.raises(ValueError, match="not allowed by the link spec"):
        LinkUpdate(
            link_spec=spec,
            replacements={10: ()},
            link_type="reviewer",
        )


@pytest.mark.parametrize(
    ("link_type", "error", "message"),
    (
        ("", ValueError, "cannot be blank"),
        (object(), TypeError, "must be a string or None"),
    ),
)
def test_link_update_rejects_invalid_named_type_values(
    link_type: object,
    error: type[Exception],
    message: str,
) -> None:
    with pytest.raises(error, match=message):
        LinkUpdate(
            link_spec=_link_spec(),
            replacements={10: (LinkValue(20, link_type=link_type),)},  # type: ignore[arg-type]
        )


def test_link_update_allows_null_type_with_declared_allowed_types() -> None:
    spec = replace(_link_spec(), allowed_types=("author", "editor"))

    update = LinkUpdate(
        link_spec=spec,
        replacements={10: (LinkValue(20),)},
        link_type=None,
    )

    assert update.replacements == {10: (LinkValue(20),)}


def test_link_update_rejects_ambiguous_or_untyped_payloads() -> None:
    with pytest.raises(TypeError, match="empty iterable"):
        LinkUpdate(link_spec=_link_spec(), replacements={10: None})  # type: ignore[dict-item]

    with pytest.raises(TypeError, match="LinkValue"):
        LinkUpdate(link_spec=_link_spec(), replacements={10: [20]})  # type: ignore[list-item]

    with pytest.raises(TypeError, match="addition links cannot be None"):
        LinkUpdate(link_spec=_link_spec(), additions={10: None})  # type: ignore[dict-item]

    with pytest.raises(TypeError, match="deletion links must be LinkValue"):
        LinkUpdate(link_spec=_link_spec(), deletions={10: [20]})  # type: ignore[list-item]


def test_link_update_replacement_map_is_read_only() -> None:
    update = LinkUpdate(link_spec=_link_spec(), replacements={10: []})

    with pytest.raises(TypeError):
        update.replacements[10] = (LinkValue(secondary_id=20),)  # type: ignore[index]


def test_link_update_from_ids_accepts_cache_writer_shapes() -> None:
    update = LinkUpdate.from_ids(
        _link_spec(),
        {
            10: 20,
            11: [21, 22],
            12: None,
        },
        additions={13: (23, 24)},
        deletions={14: {25, 26}},
    )

    assert update.replacements == {
        10: (LinkValue(secondary_id=20),),
        11: (LinkValue(secondary_id=21), LinkValue(secondary_id=22)),
        12: (),
    }
    assert update.additions == {
        13: (LinkValue(secondary_id=23), LinkValue(secondary_id=24)),
    }
    assert {link.secondary_id for link in update.deletions[14]} == {25, 26}


def test_link_update_from_ids_accepts_typed_cache_writer_shapes() -> None:
    update = LinkUpdate.from_ids(
        _link_spec(),
        replacements={
            10: {
                "author": [20, 21],
                "editor": 22,
                "translator": None,
            },
        },
    )

    assert update.replacements[10] == (
        LinkValue(secondary_id=20, link_type="author"),
        LinkValue(secondary_id=21, link_type="author"),
        LinkValue(secondary_id=22, link_type="editor"),
    )


def test_link_update_from_values_resolves_secondary_values() -> None:
    ids = {"Ada": 20, "Grace": 21, "Edsger": 22}

    update = LinkUpdate.from_values(
        _link_spec(),
        replacements={10: ["Ada", "Grace"]},
        additions={11: "Edsger"},
        secondary_id_for=ids.__getitem__,
        link_type="author",
    )

    assert update.replacements[10] == (
        LinkValue(secondary_id=20, link_type="author"),
        LinkValue(secondary_id=21, link_type="author"),
    )
    assert update.additions[11] == (
        LinkValue(secondary_id=22, link_type="author"),
    )


def test_link_update_compact_factories_preserve_rich_link_values() -> None:
    rich = LinkValue(
        secondary_id=20,
        link_type="author",
        priority=4,
        extra={"credited_as": "A. Writer"},
    )

    update = LinkUpdate.from_values(
        _link_spec(),
        additions={10: rich},
        secondary_id_for=lambda value: pytest.fail(f"unexpected resolution: {value!r}"),
    )

    assert update.additions[10] == (rich,)


def test_link_update_compact_factories_validate_nested_and_none_values() -> None:
    untyped_spec = StorageLinkSpec(
        primary_table="titles",
        secondary_table="tags",
        link_table="tag_title_links",
        primary_link_col="tag_title_link_title_id",
        secondary_link_col="tag_title_link_tag_id",
    )

    with pytest.raises(TypeError, match="typed link spec"):
        LinkUpdate.from_ids(untyped_spec, replacements={10: {"subject": [20]}})

    with pytest.raises(TypeError, match="None is only valid"):
        LinkUpdate.from_ids(_link_spec(), additions={10: [20, None]})

    with pytest.raises(TypeError, match="secondary_id_for must be callable"):
        LinkUpdate.from_values(  # type: ignore[arg-type]
            _link_spec(),
            replacements={10: "Ada"},
            secondary_id_for=None,
        )


def test_link_update_from_legacy_matches_values_but_preserves_existing_ids() -> None:
    calls: list[str] = []
    ids = {"Ada": 21, "Grace": 22}

    def resolve(value: str) -> int:
        calls.append(value)
        return ids[value]

    update = LinkUpdate.from_legacy(
        _link_spec(),
        replacements={10: {"author": [20, "Ada"]}},
        additions={11: "Grace"},
        deletions={12: 23},
        secondary_id_for=resolve,
    )

    assert calls == ["Ada", "Grace"]
    assert update.replacements[10] == (
        LinkValue(20, link_type="author"),
        LinkValue(21, link_type="author"),
    )
    assert update.additions[11] == (LinkValue(22),)
    assert update.deletions[12] == (LinkValue(23),)


def test_compact_factories_remove_duplicate_legacy_link_identities() -> None:
    update = LinkUpdate.from_ids(
        _link_spec(),
        replacements={10: [20, 20, 21]},
        additions={11: {"author": [22, 22]}},
    )

    assert _ids(update, "replacements") == (20, 21)
    assert update.additions[11] == (LinkValue(22, link_type="author"),)


def test_value_factory_matches_each_repeated_metadata_value_once() -> None:
    calls: list[str] = []

    def resolve(value: str) -> int:
        calls.append(value)
        return 20

    update = LinkUpdate.from_values(
        _link_spec(),
        replacements={10: ["Ada", "Ada"]},
        additions={11: "Ada"},
        secondary_id_for=resolve,
    )

    assert calls == ["Ada"]
    assert update.replacements[10] == (LinkValue(20),)
    assert update.additions[11] == (LinkValue(20),)


def test_direct_construction_rejects_duplicate_logical_link_identities() -> None:
    with pytest.raises(ValueError, match="duplicate logical identity"):
        LinkUpdate(
            link_spec=_link_spec(),
            replacements={
                10: [
                    LinkValue(20, link_type="author"),
                    LinkValue(20, link_type="author", priority=2),
                ],
            },
        )


def test_link_update_rejects_database_incompatible_link_capabilities() -> None:
    with pytest.raises(ValueError, match="type on an untyped link spec"):
        LinkUpdate(
            link_spec=_plain_link_spec(),
            replacements={10: [LinkValue(20, link_type="subject")]},
        )

    with pytest.raises(ValueError, match="priority on an unordered link spec"):
        LinkUpdate(
            link_spec=_plain_link_spec(),
            replacements={10: [LinkValue(20, priority=1)]},
        )

    with pytest.raises(ValueError, match="type is part of its identity"):
        LinkUpdate.from_ids(
            _plain_link_spec(),
            replacements={10: 20},
            link_type="subject",
        )


def test_link_update_composes_incrementals_into_pure_replacements() -> None:
    macros = _RecordingMacros(
        {
            11: (
                LinkRow(
                    11,
                    20,
                    link_type="author",
                    priority=2,
                    extra={"credited_as": "Old"},
                ),
                LinkRow(
                    11,
                    21,
                    link_type="editor",
                    priority=1,
                    extra={"keep": True},
                ),
            ),
        }
    )
    update = LinkUpdate(
        link_spec=_link_spec(),
        replacements={10: [LinkValue(30, link_type="author")]},
        deletions={11: [LinkValue(20, link_type="author")]},
        additions={
            11: [
                LinkValue(21, link_type="editor", extra={"new": "value"}),
                LinkValue(22, link_type="author"),
            ],
        },
    )

    pure = update.as_replacement_update(macros)  # type: ignore[arg-type]

    assert macros.reads == [(_link_spec(), (11,), LINK_TYPE_UNSET)]
    assert pure.additions == pure.deletions == {}
    assert pure.replacements[10] == (LinkValue(30, link_type="author"),)
    assert pure.replacements[11] == (
        LinkValue(
            21,
            link_type="editor",
            priority=1,
            extra={"keep": True, "new": "value"},
        ),
        LinkValue(22, link_type="author"),
    )


def test_replacement_composition_deduplicates_rows_and_preserves_readded_order() -> None:
    macros = _RecordingMacros(
        {
            10: (
                LinkRow(10, 20, link_type="author", priority=1),
                LinkRow(10, 20, link_type="author", priority=2),
                LinkRow(10, 21, link_type="editor", priority=3),
            ),
        }
    )
    update = LinkUpdate(
        link_spec=_link_spec(),
        deletions={10: [LinkValue(20, link_type="author")]},
        additions={10: [LinkValue(20, link_type="author", priority=4)]},
    )

    pure = update.as_replacement_update(macros)  # type: ignore[arg-type]

    assert pure.replacements[10] == (
        LinkValue(20, link_type="author", priority=4),
        LinkValue(21, link_type="editor", priority=3),
    )


def test_link_update_write_uses_one_bulk_replacement_with_scope() -> None:
    macros = _RecordingMacros(
        {
            10: (
                LinkRow(10, 20, link_type="author", priority=1),
            ),
        }
    )
    update = LinkUpdate.from_ids(
        _link_spec(),
        additions={10: 21},
        deletions={10: 20},
        link_type="author",
    )

    rows = update.write(macros)  # type: ignore[arg-type]

    assert macros.reads == [(_link_spec(), (10,), "author")]
    assert len(macros.writes) == 1
    written_spec, replacements, written_scope = macros.writes[0]
    assert written_spec == _link_spec()
    assert written_scope == "author"
    assert replacements == {10: (LinkValue(21, link_type="author"),)}
    assert tuple(row.secondary_id for row in rows[10]) == (21,)


def test_catalog_writes_link_update_through_its_database_macros() -> None:
    macros = _RecordingMacros()
    catalog = Catalog(type("Database", (), {"macros": macros})())
    update = LinkUpdate.from_ids(
        _link_spec(),
        replacements={10: {"author": 20}},
        link_type="author",
    )

    rows = catalog.write_link_update(update)

    assert macros.reads == []
    assert macros.writes == [
        (
            _link_spec(),
            {10: (LinkValue(20, link_type="author"),)},
            "author",
        )
    ]
    assert rows[10] == (LinkRow(10, 20, link_type="author"),)


def test_catalog_link_update_boundary_rejects_other_values_and_preserves_noop() -> None:
    macros = _RecordingMacros()
    catalog = Catalog(type("Database", (), {"macros": macros})())

    with pytest.raises(TypeError, match="update must be a LinkUpdate"):
        catalog.write_link_update(object())  # type: ignore[arg-type]

    assert catalog.write_link_update(LinkUpdate.from_ids(_link_spec())) == {}
    assert macros.reads == []
    assert macros.writes == []


def test_empty_link_update_write_does_not_touch_the_database() -> None:
    macros = _RecordingMacros()

    assert LinkUpdate(link_spec=_link_spec()).write(macros) == {}  # type: ignore[arg-type]
    assert macros.reads == []
    assert macros.writes == []


def test_link_update_exposes_effective_primary_ids_and_mapping_access() -> None:
    update = LinkUpdate(
        link_spec=_link_spec(),
        replacements={
            10: [LinkValue(20, link_type="author")],
            11: (),
        },
        deletions={
            10: [LinkValue(20, link_type="author")],
            12: (),
        },
        additions={
            10: [LinkValue(21, link_type="editor")],
            13: (),
        },
    )

    assert update.mentioned_primary_ids == (10, 11, 12, 13)
    assert update.primary_ids == (10, 11)
    assert update.keys() == (10, 11)
    assert tuple(update) == (10, 11)
    assert len(update) == 2
    assert update
    assert 10 in update
    assert 12 not in update

    first = update[10]
    assert isinstance(first, LinkUpdateEntry)
    assert first.primary_id == 10
    assert first.has_replacement
    assert not first.clears_scope
    assert not first.is_incremental
    assert first.operation_names == ("replacements", "deletions", "additions")
    assert tuple(update.values()) == (update[10], update[11])
    assert tuple(update.items()) == ((10, update[10]), (11, update[11]))

    clear = update.for_primary_id(11)
    assert clear.has_replacement
    assert clear.clears_scope
    assert clear.operations == {"replacements": ()}

    assert not update.for_primary_id(12)
    assert not update.for_primary_id(99)
    assert update.get(12) is None
    marker = object()
    assert update.get(99, marker) is marker
    with pytest.raises(KeyError):
        update[99]


def test_per_id_view_is_read_only_and_identifies_incremental_updates() -> None:
    update = LinkUpdate.from_ids(
        _link_spec(),
        additions={10: [20, 21]},
        deletions={10: 22},
    )
    entry = update.for_primary_id(10)

    assert entry.is_incremental
    assert not entry.has_replacement
    assert entry.operation_names == ("deletions", "additions")
    assert entry.to_dict() == {
        "primary_id": 10,
        "operations": {
            "deletions": [{"secondary_id": 22}],
            "additions": [{"secondary_id": 20}, {"secondary_id": 21}],
        },
    }
    with pytest.raises(TypeError):
        entry.operations["additions"] = ()  # type: ignore[index]
    with pytest.raises(FrozenInstanceError):
        entry.primary_id = 11  # type: ignore[misc]
    assert not hasattr(entry, "__dict__")


def test_link_update_pretty_format_is_deterministic_and_operation_ordered() -> None:
    update = LinkUpdate(
        link_spec=_link_spec(),
        replacements={10: [LinkValue(20, link_type="author", priority=2)]},
        deletions={10: [LinkValue(21, link_type="editor")]},
        additions={
            10: [
                LinkValue(
                    22,
                    link_type="author",
                    priority=1,
                    extra={"credited_as": "A. Writer"},
                )
            ]
        },
    )

    display = update.pformat(width=72)

    assert display == update.pformat(width=72)
    assert str(update) == update.pformat()
    assert "'primary_table': 'titles'" in display
    assert "'secondary_table': 'creators'" in display
    assert "'link_table': 'creator_title_links'" in display
    assert "'link_type': LINK_TYPE_UNSET" in display
    assert "'updates':" in display
    assert "10:" in display
    assert display.index("'replacements'") < display.index("'deletions'")
    assert display.index("'deletions'") < display.index("'additions'")
    assert "credited_as" in display
    assert update[10].pformat() == str(update[10])

    inspection = update.to_dict()
    inspection["updates"][10]["additions"][0]["extra"]["credited_as"] = "Changed"
    assert update.additions[10][0].extra == {"credited_as": "A. Writer"}


def test_empty_incremental_entries_are_visible_but_do_not_write() -> None:
    macros = _RecordingMacros()
    update = LinkUpdate.from_ids(
        _link_spec(),
        additions={10: None},
        deletions={11: []},
    )

    assert update.mentioned_primary_ids == (11, 10)
    assert update.primary_ids == ()
    assert not update
    assert len(update) == 0
    assert update.to_dict()["updates"] == {}
    assert update.write(macros) == {}  # type: ignore[arg-type]
    assert macros.reads == []
    assert macros.writes == []


def test_link_update_returns_one_dataclass_per_link_in_operation_order() -> None:
    update = LinkUpdate(
        link_spec=_link_spec(),
        replacements={
            10: [
                LinkValue(
                    20,
                    link_type="author",
                    priority=2,
                    extra={"credited_as": "A. Writer"},
                )
            ],
        },
        deletions={10: [LinkValue(21, link_type="editor")]},
        additions={
            10: [LinkValue(22, link_type="author", priority=1)],
            11: [LinkValue(23, link_type="translator")],
        },
    )

    links = update.links()
    iterated_links = tuple(update.iter_links())

    assert all(isinstance(link, LinkUpdateLink) for link in links)
    assert iterated_links == links
    assert [
        (link.src_id, link.dst_id, link.operation)
        for link in links
    ] == [
        (10, 20, "replacements"),
        (10, 21, "deletions"),
        (10, 22, "additions"),
        (11, 23, "additions"),
    ]
    assert links[0].link_type == "author"
    assert links[0].priority == 2
    assert links[0].extra == {"credited_as": "A. Writer"}
    assert update[10].links() == links[:3]
    assert tuple(update[10].iter_links()) == links[:3]
    assert update.links_for_primary_id(10) == links[:3]
    assert update.links_for_primary_id(99) == ()
    with pytest.raises(TypeError):
        links[0].extra["credited_as"] = "Changed"  # type: ignore[index]


def test_link_dataclass_is_a_read_only_mapping_over_its_extras() -> None:
    link = LinkUpdateLink(
        src_id=10,
        dst_id=20,
        operation="additions",
        link_type="author",
        priority=2,
        extra={
            "credited_as": "A. Writer",
            "confidence": 0.9,
            "verified": True,
        },
    )

    assert isinstance(link, Mapping)
    assert link["credited_as"] == "A. Writer"
    assert link.get("confidence") == 0.9
    assert link.get("missing", "fallback") == "fallback"
    assert tuple(link) == ("credited_as", "confidence", "verified")
    assert tuple(link.keys()) == ("credited_as", "confidence", "verified")
    assert tuple(link.values()) == ("A. Writer", 0.9, True)
    assert tuple(link.items()) == (
        ("credited_as", "A. Writer"),
        ("confidence", 0.9),
        ("verified", True),
    )
    assert dict(link) == {
        "credited_as": "A. Writer",
        "confidence": 0.9,
        "verified": True,
    }
    assert len(link) == 3
    assert "credited_as" in link
    assert "src_id" not in link
    with pytest.raises(KeyError):
        _ = link["missing"]
    with pytest.raises(TypeError):
        link["credited_as"] = "Changed"  # type: ignore[index]


def test_iter_links_is_lazy_and_does_not_load_destination_values() -> None:
    calls: list[int] = []
    update = LinkUpdate.from_ids(
        _link_spec(),
        additions={10: {"author": (20, 21)}},
    )

    links = update.iter_links(
        dst_value_for=lambda dst_id: calls.append(dst_id),
    )

    assert isinstance(links, Iterable)
    first = next(links)
    assert first.dst_id == 20
    assert calls == []
    assert tuple(link.dst_id for link in links) == (21,)
    assert calls == []


def test_link_dataclass_resolves_and_caches_its_destination_value_lazily() -> None:
    calls: list[int] = []

    def load(dst_id: int) -> str:
        calls.append(dst_id)
        return {20: "Ada"}[dst_id]

    update = LinkUpdate.from_ids(
        _link_spec(),
        additions={10: {"author": 20}},
    )
    link = update.links(dst_value_for=load)[0]

    assert not link.dst_value_loaded
    assert "dst_value" not in link.to_dict()
    assert "dst_value" not in link.pformat()
    assert calls == []

    assert link.get_dst_value() == "Ada"
    assert link.dst_value_loaded
    assert calls == [20]
    assert link.get_dst_value() == "Ada"
    assert link.dst_value == "Ada"
    assert calls == [20]
    assert link.to_dict() == {
        "src_id": 10,
        "dst_id": 20,
        "operation": "additions",
        "extra": {},
        "link_type": "author",
        "dst_value": "Ada",
    }
    assert "'dst_value': 'Ada'" in str(link)


def test_link_destination_loader_handles_none_retries_failures_and_requires_loader() -> None:
    no_loader = LinkUpdateLink(src_id=10, dst_id=20, operation="additions")
    with pytest.raises(RuntimeError, match="no destination-value loader"):
        no_loader.get_dst_value()
    assert not no_loader.dst_value_loaded
    assert no_loader.get_dst_value(lambda dst_id: f"value {dst_id}") == "value 20"
    assert no_loader.dst_value == "value 20"

    none_calls: list[int] = []
    none_value = LinkUpdateLink(
        src_id=10,
        dst_id=20,
        operation="deletions",
        dst_value_for=lambda dst_id: none_calls.append(dst_id),
    )
    assert none_value.get_dst_value() is None
    assert none_value.get_dst_value() is None
    assert none_value.dst_value_loaded
    assert none_calls == [20]

    attempts: list[int] = []

    def flaky(dst_id: int) -> str:
        attempts.append(dst_id)
        if len(attempts) == 1:
            raise LookupError(dst_id)
        return "recovered"

    retry = LinkUpdateLink(
        src_id=10,
        dst_id=21,
        operation="replacements",
        dst_value_for=flaky,
    )
    with pytest.raises(LookupError):
        retry.get_dst_value()
    assert not retry.dst_value_loaded
    assert retry.get_dst_value() == "recovered"
    assert attempts == [21, 21]

    with pytest.raises(TypeError, match="dst_value_for must be callable"):
        LinkUpdateLink(
            src_id=10,
            dst_id=20,
            operation="additions",
            dst_value_for=object(),  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="operation must be"):
        LinkUpdateLink(src_id=10, dst_id=20, operation="unknown")


def test_link_dataclass_rejects_non_mapping_extras_and_non_callable_lazy_loader() -> None:
    with pytest.raises(TypeError, match="link extras must be a mapping"):
        LinkUpdateLink(
            src_id=10,
            dst_id=20,
            operation="additions",
            extra=[("credited_as", "A. Writer")],  # type: ignore[arg-type]
        )

    link = LinkUpdateLink(src_id=10, dst_id=20, operation="additions")
    with pytest.raises(TypeError, match="dst_value_for must be callable"):
        link.get_dst_value(object())  # type: ignore[arg-type]
    assert not link.dst_value_loaded


def test_link_dataclass_display_includes_priority_without_other_optional_fields() -> None:
    link = LinkUpdateLink(
        src_id=10,
        dst_id=20,
        operation="replacements",
        priority=0,
    )

    assert link.to_dict() == {
        "src_id": 10,
        "dst_id": 20,
        "operation": "replacements",
        "extra": {},
        "priority": 0,
    }


def test_link_dataclass_snapshots_extras_and_is_frozen_and_slotted() -> None:
    extra = {"credited_as": "Original"}
    link = LinkUpdateLink(
        src_id=10,
        dst_id=20,
        operation="replacements",
        extra=extra,
    )
    extra["credited_as"] = "Changed"

    assert link.extra == {"credited_as": "Original"}
    assert link.to_dict() == {
        "src_id": 10,
        "dst_id": 20,
        "operation": "replacements",
        "extra": {"credited_as": "Original"},
    }
    with pytest.raises(FrozenInstanceError):
        link.dst_id = 21  # type: ignore[misc]
    assert not hasattr(link, "__dict__")


def test_legacy_value_to_normalized_link_update_writes_through_real_db(db) -> None:
    db.driver_wrapper.executescript(
        """
        CREATE TABLE catalog_update_sources (
            catalog_update_source_id INTEGER PRIMARY KEY,
            catalog_update_source_name TEXT NOT NULL
        );
        CREATE TABLE catalog_update_values (
            catalog_update_value_id INTEGER PRIMARY KEY,
            catalog_update_value_name TEXT NOT NULL UNIQUE
        );
        CREATE TABLE catalog_update_links (
            catalog_update_source_id INTEGER NOT NULL,
            catalog_update_value_id INTEGER NOT NULL,
            UNIQUE(catalog_update_source_id, catalog_update_value_id),
            FOREIGN KEY(catalog_update_source_id)
                REFERENCES catalog_update_sources(catalog_update_source_id),
            FOREIGN KEY(catalog_update_value_id)
                REFERENCES catalog_update_values(catalog_update_value_id)
        );
        INSERT INTO catalog_update_sources VALUES (1, 'source');
        INSERT INTO catalog_update_values VALUES (10, 'existing');
        """
    )
    spec = StorageLinkSpec(
        primary_table="catalog_update_sources",
        secondary_table="catalog_update_values",
        link_table="catalog_update_links",
        primary_id_col="catalog_update_source_id",
        secondary_id_col="catalog_update_value_id",
        primary_link_col="catalog_update_source_id",
        secondary_link_col="catalog_update_value_id",
    )
    catalog = Catalog(db)

    def match_value(value: str) -> int:
        return db.macros.ensure_table_value(
            spec.secondary_table,
            "catalog_update_value_name",
            value,
            id_column=spec.secondary_id_col,
        )

    initial = LinkUpdate.from_legacy(
        spec,
        replacements={1: [10, "matched"]},
        secondary_id_for=match_value,
    )
    initial_rows = catalog.write_link_update(initial)
    matched_id = next(
        row.secondary_id
        for row in initial_rows[1]
        if row.secondary_id != 10
    )

    incremental = LinkUpdate.from_legacy(
        spec,
        additions={1: "added"},
        deletions={1: 10},
        secondary_id_for=match_value,
    )
    final_rows = catalog.write_link_update(incremental)

    assert {row.secondary_id for row in final_rows[1]} == {
        matched_id,
        match_value("added"),
    }
    assert {
        row[0]
        for row in db.driver_wrapper.execute(
            "SELECT catalog_update_value_name FROM catalog_update_values"
        )
    } == {"existing", "matched", "added"}


# Direct construction -------------------------------------------------------


def test_link_update_defaults_to_independent_empty_read_only_operations() -> None:
    first = LinkUpdate(link_spec=_link_spec())
    second = LinkUpdate(link_spec=_link_spec())

    assert first.replacements == first.additions == first.deletions == {}
    assert first.replacements is not first.additions
    assert first.additions is not first.deletions
    assert first.replacements is not second.replacements
    assert first.link_type is LINK_TYPE_UNSET

    for operation in (first.replacements, first.additions, first.deletions):
        with pytest.raises(TypeError):
            operation[10] = ()  # type: ignore[index]


def test_link_update_is_frozen_and_slotted() -> None:
    update = LinkUpdate(link_spec=_link_spec())

    with pytest.raises(FrozenInstanceError):
        update.link_type = "author"  # type: ignore[misc]
    assert not hasattr(update, "__dict__")


@pytest.mark.parametrize("operation", ("replacements", "additions", "deletions"))
@pytest.mark.parametrize(
    "collection_factory",
    (
        pytest.param(list, id="list"),
        pytest.param(tuple, id="tuple"),
        pytest.param(deque, id="deque"),
        pytest.param(lambda values: iter(values), id="iterator"),
        pytest.param(
            lambda values: (value for value in values),
            id="generator",
        ),
        pytest.param(
            lambda values: {index: value for index, value in enumerate(values)}.values(),
            id="dict-values-view",
        ),
    ),
)
def test_direct_construction_materialises_legacy_link_collections(
    operation: str,
    collection_factory: Callable[[tuple[LinkValue, ...]], Iterable[LinkValue]],
) -> None:
    links = (LinkValue(20), LinkValue(21))
    supplied = collection_factory(links)

    update = LinkUpdate(
        link_spec=_link_spec(),
        **_operation_payload(operation, {10: supplied}),  # type: ignore[arg-type]
    )

    assert _ids(update, operation) == (20, 21)
    assert isinstance(getattr(update, operation)[10], tuple)


def test_direct_construction_snapshots_all_caller_owned_containers() -> None:
    replacement_extra = UserDict({"credited_as": "Original"})
    replacement_links = [LinkValue(20, extra=replacement_extra)]
    addition_links = [LinkValue(21)]
    deletion_links = [LinkValue(22)]
    replacements = {10: replacement_links}
    additions = {10: addition_links}
    deletions = {10: deletion_links}

    update = LinkUpdate(
        link_spec=_link_spec(),
        replacements=replacements,
        additions=additions,
        deletions=deletions,
    )

    replacements.clear()
    additions.clear()
    deletions.clear()
    replacement_links.clear()
    addition_links.clear()
    deletion_links.clear()
    replacement_extra["credited_as"] = "Mutated"

    assert _ids(update, "replacements") == (20,)
    assert _ids(update, "additions") == (21,)
    assert _ids(update, "deletions") == (22,)
    assert update.replacements[10][0].extra == {"credited_as": "Original"}
    assert update.replacements[10][0].extra is not replacement_extra


def test_direct_construction_preserves_overlapping_operations() -> None:
    replacement = LinkValue(20, priority=1)
    deletion = LinkValue(20)
    addition = LinkValue(20, priority=2)

    update = LinkUpdate(
        link_spec=_link_spec(),
        replacements={10: [replacement]},
        deletions={10: [deletion]},
        additions={10: [addition]},
    )

    assert update.replacements[10] == (replacement,)
    assert update.deletions[10] == (deletion,)
    assert update.additions[10] == (addition,)


@pytest.mark.parametrize("operation", ("replacements", "additions", "deletions"))
def test_direct_construction_inherits_scope_without_losing_link_properties(
    operation: str,
) -> None:
    supplied = LinkValue(
        20,
        priority=3,
        extra={"credited_as": "A. Writer"},
    )

    update = LinkUpdate(
        link_spec=_link_spec(),
        link_type="author",
        **_operation_payload(operation, {10: [supplied]}),  # type: ignore[arg-type]
    )

    actual = getattr(update, operation)[10][0]
    assert actual == LinkValue(
        20,
        link_type="author",
        priority=3,
        extra={"credited_as": "A. Writer"},
    )
    assert actual is not supplied


@pytest.mark.parametrize("operation", ("replacements", "additions", "deletions"))
def test_direct_construction_accepts_an_explicit_matching_scope(operation: str) -> None:
    supplied = LinkValue(20, link_type="author")

    update = LinkUpdate(
        link_spec=_link_spec(),
        link_type="author",
        **_operation_payload(operation, {10: [supplied]}),  # type: ignore[arg-type]
    )

    assert getattr(update, operation)[10] == (supplied,)


# Legacy compact ID maps ----------------------------------------------------


@pytest.mark.parametrize("operation", ("replacements", "additions", "deletions"))
@pytest.mark.parametrize(
    ("raw_factory", "expected_ids"),
    (
        pytest.param(lambda: 20, (20,), id="scalar-id"),
        pytest.param(lambda: [20, 21], (20, 21), id="list"),
        pytest.param(lambda: (20, 21), (20, 21), id="tuple"),
        pytest.param(lambda: deque((20, 21)), (20, 21), id="deque"),
        pytest.param(lambda: range(20, 22), (20, 21), id="range"),
        pytest.param(lambda: iter((20, 21)), (20, 21), id="iterator"),
        pytest.param(
            lambda: (value for value in (20, 21)),
            (20, 21),
            id="generator",
        ),
        pytest.param(lambda: [], (), id="empty-list"),
        pytest.param(lambda: (), (), id="empty-tuple"),
        pytest.param(lambda: None, (), id="none-clear"),
    ),
)
def test_from_ids_normalises_ordered_legacy_shapes_for_every_operation(
    operation: str,
    raw_factory: Callable[[], object],
    expected_ids: tuple[int, ...],
) -> None:
    update = LinkUpdate.from_ids(
        _link_spec(),
        **_operation_payload(operation, {10: raw_factory()}),  # type: ignore[arg-type]
    )

    assert _ids(update, operation) == expected_ids


@pytest.mark.parametrize("operation", ("replacements", "additions", "deletions"))
@pytest.mark.parametrize(
    "raw_factory",
    (
        pytest.param(lambda: {20, 21}, id="set"),
        pytest.param(lambda: frozenset((20, 21)), id="frozenset"),
        pytest.param(lambda: {20: "first", 21: "second"}.keys(), id="dict-keys-view"),
    ),
)
def test_from_ids_normalises_unordered_legacy_shapes_for_every_operation(
    operation: str,
    raw_factory: Callable[[], Iterable[int]],
) -> None:
    update = LinkUpdate.from_ids(
        _link_spec(),
        **_operation_payload(operation, {10: raw_factory()}),  # type: ignore[arg-type]
    )

    assert set(_ids(update, operation)) == {20, 21}


@pytest.mark.parametrize(
    "mapping_factory",
    (
        pytest.param(lambda: {10: [20, 21]}, id="dict"),
        pytest.param(lambda: UserDict({10: [20, 21]}), id="user-dict"),
        pytest.param(lambda: UpdateDict({10: [20, 21]}), id="legacy-update-dict"),
        pytest.param(
            lambda: defaultdict(list, {10: [20, 21]}),
            id="legacy-default-dict",
        ),
        pytest.param(
            lambda: MappingProxyType({10: [20, 21]}),
            id="mapping-proxy",
        ),
    ),
)
def test_from_ids_accepts_legacy_mapping_implementations(
    mapping_factory: Callable[[], Mapping[int, object]],
) -> None:
    update = LinkUpdate.from_ids(_link_spec(), mapping_factory())  # type: ignore[arg-type]

    assert _ids(update, "replacements") == (20, 21)


def test_from_ids_snapshots_legacy_update_dict_and_inner_list() -> None:
    values = [20, 21]
    supplied = UpdateDict({10: values})

    update = LinkUpdate.from_ids(_link_spec(), supplied)
    supplied.clear()
    values.clear()

    assert _ids(update, "replacements") == (20, 21)


def test_from_ids_normalises_a_mixed_typed_legacy_update() -> None:
    typed_values: defaultdict[str, object] = defaultdict(list)
    typed_values["author"] = [20, 21]
    typed_values["editor"] = 22
    typed_values["reviewer"] = (23,)
    typed_values["illustrator"] = {24}
    typed_values["narrator"] = (value for value in (25, 26))
    typed_values["translator"] = None
    typed_values["empty-role"] = []
    typed_values["afterword"] = LinkValue(27, priority=8)
    supplied = UpdateDict({10: typed_values, 11: None, 12: {}})

    update = LinkUpdate.from_ids(_link_spec(), supplied)

    assert update.replacements[10] == (
        LinkValue(20, link_type="author"),
        LinkValue(21, link_type="author"),
        LinkValue(22, link_type="editor"),
        LinkValue(23, link_type="reviewer"),
        LinkValue(24, link_type="illustrator"),
        LinkValue(25, link_type="narrator"),
        LinkValue(26, link_type="narrator"),
        LinkValue(27, link_type="afterword", priority=8),
    )
    assert update.replacements[11] == ()
    assert update.replacements[12] == ()


@pytest.mark.parametrize("operation", ("replacements", "additions", "deletions"))
def test_from_ids_supports_typed_maps_for_every_operation(operation: str) -> None:
    update = LinkUpdate.from_ids(
        _link_spec(),
        **_operation_payload(
            operation,
            {10: {"author": [20, 21], "editor": 22}},
        ),  # type: ignore[arg-type]
    )

    assert getattr(update, operation)[10] == (
        LinkValue(20, link_type="author"),
        LinkValue(21, link_type="author"),
        LinkValue(22, link_type="editor"),
    )


def test_nested_typed_map_only_supplies_a_missing_rich_link_type() -> None:
    missing_type = LinkValue(20, priority=1)
    explicit_type = LinkValue(21, link_type="contributor", priority=2)

    update = LinkUpdate.from_ids(
        _link_spec(),
        {10: {"author": [missing_type, explicit_type]}},
    )

    assert update.replacements[10] == (
        LinkValue(20, link_type="author", priority=1),
        explicit_type,
    )


def test_factory_scope_is_applied_to_every_compact_operation() -> None:
    update = LinkUpdate.from_ids(
        _link_spec(),
        {10: [20]},
        additions={10: 21},
        deletions={10: (22,)},
        link_type="author",
    )

    assert update.replacements[10] == (LinkValue(20, link_type="author"),)
    assert update.additions[10] == (LinkValue(21, link_type="author"),)
    assert update.deletions[10] == (LinkValue(22, link_type="author"),)


def test_from_ids_preserves_rich_links_without_treating_them_as_ids() -> None:
    rich = LinkValue(
        20,
        link_type="author",
        priority=4,
        extra={"credited_as": "A. Writer"},
    )

    update = LinkUpdate.from_ids(
        _link_spec(),
        replacements={10: rich},
        additions={11: [21, rich]},
    )

    assert update.replacements[10] == (rich,)
    assert update.additions[11] == (LinkValue(21), rich)


@pytest.mark.parametrize("operation", ("replacements", "additions", "deletions"))
def test_none_is_a_distinct_sql_null_link_type_scope(operation: str) -> None:
    update = LinkUpdate.from_ids(
        _link_spec(),
        link_type=None,
        **_operation_payload(operation, {10: 20}),  # type: ignore[arg-type]
    )

    assert update.link_type is None
    assert getattr(update, operation)[10] == (LinkValue(20, link_type=None),)

    with pytest.raises(ValueError, match="link type 'author'.*scope None"):
        LinkUpdate(
            link_spec=_link_spec(),
            link_type=None,
            **_operation_payload(
                operation,
                {10: [LinkValue(20, link_type="author")]},
            ),  # type: ignore[arg-type]
        )


# Raw secondary values and resolver behavior --------------------------------


@pytest.mark.parametrize("operation", ("replacements", "additions", "deletions"))
@pytest.mark.parametrize(
    "raw_value",
    (
        pytest.param("Ada", id="string"),
        pytest.param(b"Ada", id="bytes"),
        pytest.param(bytearray(b"Ada"), id="bytearray"),
        pytest.param(3.5, id="float"),
        pytest.param(True, id="bool"),
    ),
)
def test_from_values_treats_legacy_scalar_types_as_one_value(
    operation: str,
    raw_value: object,
) -> None:
    resolved: list[object] = []

    def resolve(value: object) -> int:
        resolved.append(value)
        return 20

    update = LinkUpdate.from_values(
        _link_spec(),
        secondary_id_for=resolve,
        **_operation_payload(operation, {10: raw_value}),  # type: ignore[arg-type]
    )

    assert resolved == [raw_value]
    assert _ids(update, operation) == (20,)


@pytest.mark.parametrize(
    "raw_factory",
    (
        pytest.param(lambda: ["Ada", "Grace"], id="list"),
        pytest.param(lambda: ("Ada", "Grace"), id="tuple"),
        pytest.param(lambda: deque(("Ada", "Grace")), id="deque"),
        pytest.param(lambda: iter(("Ada", "Grace")), id="iterator"),
        pytest.param(
            lambda: (value for value in ("Ada", "Grace")),
            id="generator",
        ),
    ),
)
def test_from_values_resolves_ordered_legacy_collections_once(
    raw_factory: Callable[[], Iterable[str]],
) -> None:
    resolved: list[str] = []
    ids = {"Ada": 20, "Grace": 21}

    def resolve(value: str) -> int:
        resolved.append(value)
        return ids[value]

    update = LinkUpdate.from_values(
        _link_spec(),
        {10: raw_factory()},
        secondary_id_for=resolve,
    )

    assert resolved == ["Ada", "Grace"]
    assert _ids(update, "replacements") == (20, 21)


@pytest.mark.parametrize(
    "raw_factory",
    (
        pytest.param(lambda: {"Ada", "Grace"}, id="set"),
        pytest.param(lambda: frozenset(("Ada", "Grace")), id="frozenset"),
    ),
)
def test_from_values_resolves_unordered_legacy_collections(
    raw_factory: Callable[[], Iterable[str]],
) -> None:
    ids = {"Ada": 20, "Grace": 21}

    update = LinkUpdate.from_values(
        _link_spec(),
        {10: raw_factory()},
        secondary_id_for=ids.__getitem__,
    )

    assert set(_ids(update, "replacements")) == {20, 21}


def test_from_values_resolves_mixed_legacy_values_but_bypasses_rich_links() -> None:
    rich = LinkValue(
        99,
        link_type="author",
        priority=4,
        extra={"credited_as": "Existing"},
    )
    calls: list[object] = []

    def resolve(value: object) -> int:
        calls.append(value)
        return {"Ada": 20, 21: 21}[value]

    update = LinkUpdate.from_values(
        _link_spec(),
        additions={10: ["Ada", rich, 21]},
        secondary_id_for=resolve,
    )

    assert calls == ["Ada", 21]
    assert update.additions[10] == (
        LinkValue(20),
        rich,
        LinkValue(21),
    )


def test_from_values_does_not_resolve_none_clears_or_empty_roles() -> None:
    update = LinkUpdate.from_values(
        _link_spec(),
        replacements={10: None, 11: {"author": None, "editor": []}},
        secondary_id_for=lambda value: pytest.fail(f"unexpected value: {value!r}"),
    )

    assert update.replacements == {10: (), 11: ()}


def test_from_values_preserves_typed_role_order_and_resolution_order() -> None:
    calls: list[str] = []
    ids = {"Ada": 20, "Grace": 21, "Edsger": 22}

    def resolve(value: str) -> int:
        calls.append(value)
        return ids[value]

    update = LinkUpdate.from_values(
        _link_spec(),
        replacements={
            10: {
                "author": ["Ada", "Grace"],
                "editor": "Edsger",
            },
        },
        secondary_id_for=resolve,
    )

    assert calls == ["Ada", "Grace", "Edsger"]
    assert update.replacements[10] == (
        LinkValue(20, link_type="author"),
        LinkValue(21, link_type="author"),
        LinkValue(22, link_type="editor"),
    )


def test_from_values_propagates_resolver_exceptions_and_stops() -> None:
    calls: list[str] = []

    def resolve(value: str) -> int:
        calls.append(value)
        if value == "bad":
            raise KeyError(value)
        return 20

    with pytest.raises(KeyError, match="bad"):
        LinkUpdate.from_values(
            _link_spec(),
            {10: ["good", "bad", "never"]},
            secondary_id_for=resolve,
        )

    assert calls == ["good", "bad"]


# Validation ---------------------------------------------------------------


@pytest.mark.parametrize("bad_spec", (None, "titles-creators", object()))
def test_direct_construction_rejects_non_link_specs(bad_spec: object) -> None:
    with pytest.raises(TypeError, match="link_spec must be a StorageLinkSpec"):
        LinkUpdate(link_spec=bad_spec)  # type: ignore[arg-type]


@pytest.mark.parametrize("factory", ("from_ids", "from_values"))
@pytest.mark.parametrize("bad_spec", (None, "titles-creators", object()))
def test_compact_factories_reject_non_link_specs(factory: str, bad_spec: object) -> None:
    if factory == "from_ids":
        call = lambda: LinkUpdate.from_ids(bad_spec)  # type: ignore[arg-type]
    else:
        call = lambda: LinkUpdate.from_values(  # type: ignore[arg-type]
            bad_spec,
            secondary_id_for=lambda value: 20,
        )

    with pytest.raises(TypeError, match="link_spec must be a StorageLinkSpec"):
        call()


@pytest.mark.parametrize("operation", ("replacements", "additions", "deletions"))
@pytest.mark.parametrize(
    "bad_mapping",
    (
        pytest.param([], id="list"),
        pytest.param((10, 20), id="tuple"),
        pytest.param("not-a-map", id="string"),
        pytest.param(20, id="integer"),
    ),
)
def test_direct_construction_rejects_non_mapping_operations(
    operation: str,
    bad_mapping: object,
) -> None:
    with pytest.raises(TypeError, match=rf"{operation} must be a mapping"):
        LinkUpdate(
            link_spec=_link_spec(),
            **_operation_payload(operation, bad_mapping),  # type: ignore[arg-type]
        )


@pytest.mark.parametrize("operation", ("replacements", "additions", "deletions"))
@pytest.mark.parametrize(
    "bad_links",
    (
        pytest.param(20, id="scalar-id"),
        pytest.param(LinkValue(20), id="scalar-link-value"),
        pytest.param("not-links", id="string"),
        pytest.param([LinkValue(20), 21], id="mixed-list"),
    ),
)
def test_direct_construction_rejects_non_link_value_collections(
    operation: str,
    bad_links: object,
) -> None:
    singular = operation.removesuffix("s")
    with pytest.raises(TypeError, match=rf"{singular} links must be LinkValue"):
        LinkUpdate(
            link_spec=_link_spec(),
            **_operation_payload(operation, {10: bad_links}),  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    ("operation", "message"),
    (
        ("replacements", "replacement links cannot be None; use an empty iterable"),
        ("additions", "addition links cannot be None"),
        ("deletions", "deletion links cannot be None"),
    ),
)
def test_direct_construction_rejects_none_entries(
    operation: str,
    message: str,
) -> None:
    with pytest.raises(TypeError, match=message):
        LinkUpdate(
            link_spec=_link_spec(),
            **_operation_payload(operation, {10: None}),  # type: ignore[arg-type]
        )


@pytest.mark.parametrize("operation", ("replacements", "additions", "deletions"))
def test_direct_construction_rejects_non_mapping_extras(operation: str) -> None:
    bad_link = LinkValue(20, extra=[("credited_as", "A. Writer")])  # type: ignore[arg-type]
    singular = operation.removesuffix("s")

    with pytest.raises(TypeError, match=rf"{singular} link extras must be a mapping"):
        LinkUpdate(
            link_spec=_link_spec(),
            **_operation_payload(operation, {10: [bad_link]}),  # type: ignore[arg-type]
        )


@pytest.mark.parametrize("operation", ("replacements", "additions", "deletions"))
def test_direct_construction_rejects_scope_mismatches_for_every_operation(
    operation: str,
) -> None:
    with pytest.raises(
        ValueError,
        match=rf"{operation.removesuffix('s')} link type 'editor'.*scope 'author'",
    ):
        LinkUpdate(
            link_spec=_link_spec(),
            link_type="author",
            **_operation_payload(
                operation,
                {10: [LinkValue(20, link_type="editor")]},
            ),  # type: ignore[arg-type]
        )


@pytest.mark.parametrize("operation", ("replacements", "additions", "deletions"))
@pytest.mark.parametrize(
    "bad_mapping",
    (
        pytest.param([], id="list"),
        pytest.param((10, 20), id="tuple"),
        pytest.param("not-a-map", id="string"),
        pytest.param(20, id="integer"),
    ),
)
def test_from_ids_rejects_non_mapping_operations(
    operation: str,
    bad_mapping: object,
) -> None:
    with pytest.raises(TypeError, match=rf"{operation} must be a mapping"):
        LinkUpdate.from_ids(
            _link_spec(),
            **_operation_payload(operation, bad_mapping),  # type: ignore[arg-type]
        )


@pytest.mark.parametrize("operation", ("replacements", "additions", "deletions"))
def test_from_values_rejects_non_mapping_operations(operation: str) -> None:
    with pytest.raises(TypeError, match=rf"{operation} must be a mapping"):
        LinkUpdate.from_values(
            _link_spec(),
            secondary_id_for=lambda value: 20,
            **_operation_payload(operation, ["Ada"]),  # type: ignore[arg-type]
        )


@pytest.mark.parametrize("factory", ("from_ids", "from_values"))
@pytest.mark.parametrize("operation", ("replacements", "additions", "deletions"))
def test_compact_factories_reject_typed_maps_for_plain_links(
    factory: str,
    operation: str,
) -> None:
    kwargs = _operation_payload(operation, {10: {"subject": [20]}})

    with pytest.raises(TypeError, match="nested link-type mappings require a typed link spec"):
        if factory == "from_ids":
            LinkUpdate.from_ids(_plain_link_spec(), **kwargs)  # type: ignore[arg-type]
        else:
            LinkUpdate.from_values(
                _plain_link_spec(),
                secondary_id_for=lambda value: 20,
                **kwargs,  # type: ignore[arg-type]
            )


@pytest.mark.parametrize("factory", ("from_ids", "from_values"))
@pytest.mark.parametrize("operation", ("replacements", "additions", "deletions"))
@pytest.mark.parametrize(
    "bad_values_factory",
    (
        pytest.param(lambda: [20, None], id="list"),
        pytest.param(lambda: (20, None), id="tuple"),
        pytest.param(lambda: iter((20, None)), id="iterator"),
    ),
)
def test_compact_factories_reject_none_inside_value_collections(
    factory: str,
    operation: str,
    bad_values_factory: Callable[[], Iterable[int | None]],
) -> None:
    kwargs = _operation_payload(operation, {10: bad_values_factory()})

    with pytest.raises(TypeError, match="None is only valid as the complete value"):
        if factory == "from_ids":
            LinkUpdate.from_ids(_link_spec(), **kwargs)  # type: ignore[arg-type]
        else:
            LinkUpdate.from_values(
                _link_spec(),
                secondary_id_for=lambda value: int(value),
                **kwargs,  # type: ignore[arg-type]
            )


@pytest.mark.parametrize("bad_resolver", (None, 0, "resolver", object()))
def test_from_values_rejects_every_non_callable_resolver(bad_resolver: object) -> None:
    with pytest.raises(TypeError, match="secondary_id_for must be callable"):
        LinkUpdate.from_values(
            _link_spec(),
            replacements={10: "Ada"},
            secondary_id_for=bad_resolver,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize("bad_resolver", (None, 0, "resolver", object()))
def test_from_legacy_rejects_every_non_callable_resolver(bad_resolver: object) -> None:
    with pytest.raises(TypeError, match="secondary_id_for must be callable"):
        LinkUpdate.from_legacy(
            _link_spec(),
            replacements={10: "Ada"},
            secondary_id_for=bad_resolver,  # type: ignore[arg-type]
        )


def test_factory_scope_rejects_a_nested_type_that_conflicts_with_it() -> None:
    with pytest.raises(ValueError, match="link type 'editor'.*scope 'author'"):
        LinkUpdate.from_ids(
            _link_spec(),
            {10: {"editor": 20}},
            link_type="author",
        )
