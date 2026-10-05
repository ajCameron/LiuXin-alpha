"""
Verify test link update behavior against the public catalog contracts.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test link update through its owning regression module::

        python -m pytest -q tests/catalog/test_link_update.py
"""

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
    """
    Perform the link spec test-helper operation with deterministic inputs.

    Example:
        Exercise link spec through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: The deterministic value, row, identity or collection described above.
    """
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
    """
    Perform the plain link spec test-helper operation with deterministic inputs.

    Example:
        Exercise plain link spec through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: The deterministic value, row, identity or collection described above.
    """
    return StorageLinkSpec(
        primary_table="titles",
        secondary_table="tags",
        link_table="tag_title_links",
        primary_link_col="tag_title_link_title_id",
        secondary_link_col="tag_title_link_tag_id",
    )


def _operation_payload(operation: str, payload: object) -> dict[str, object]:
    """
    Perform the operation payload test-helper operation with deterministic inputs.

    Example:
        Exercise operation payload through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :param operation: Value supplied for operation under the catalog contract.
    :param payload: Value supplied for payload under the catalog contract.
    :return: The deterministic value, row, identity or collection described above.
    """
    return {operation: payload}


def _ids(update: LinkUpdate, operation: str, primary_id: int = 10) -> tuple[object, ...]:
    """
    Perform the ids test-helper operation with deterministic inputs.

    Example:
        Exercise ids through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :param update: Prepared catalog update to validate or apply.
    :param operation: Value supplied for operation under the catalog contract.
    :param primary_id: Primary catalog row identity owning the link operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    links = getattr(update, operation)[primary_id]
    return tuple(link.secondary_id for link in links)


class _RecordingMacros:
    """
    Small portable-macro double used to inspect update composition.

    Example:
        Exercise RecordingMacros through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py
    """

    def __init__(
        self,
        current: Mapping[int, tuple[LinkRow, ...]] | None = None,
    ) -> None:
        """
        Initialize the RecordingMacros test double.

        Example:
            Exercise RecordingMacros.init through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param current: Value supplied for current under the catalog contract.
        :return: None; the function records state or raises through its assertions.
        """
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
        """
        Return link rows bulk from deterministic test state.

        Example:
            Exercise RecordingMacros.get link rows bulk through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param link_spec: Value supplied for link spec under the catalog contract.
        :param primary_ids: Value supplied for primary ids under the catalog contract.
        :param link_type: Optional typed relation value carried by the link.
        :return: The deterministic value, row, identity or collection described above.
        """
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
        """
        Perform the replace links bulk test-helper operation with deterministic inputs.

        Example:
            Exercise RecordingMacros.replace links bulk through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param link_spec: Value supplied for link spec under the catalog contract.
        :param replacements: Value supplied for replacements under the catalog contract.
        :param link_type: Optional typed relation value carried by the link.
        :return: The deterministic value, row, identity or collection described above.
        """
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
    """
    Verify link update materialises complete replacement sets.

    Example:
        Exercise test link update materialises complete replacement sets through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify link update materialises incremental operations and link extras.

    Example:
        Exercise test link update materialises incremental operations and link extras through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify link update can scope a typed replacement.

    Example:
        Exercise test link update can scope a typed replacement through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
    update = LinkUpdate(
        link_spec=_link_spec(),
        replacements={10: [LinkValue(secondary_id=20, link_type="editor")]},
        link_type="editor",
    )

    assert update.link_type == "editor"
    assert update.replacements[10][0].link_type == "editor"


def test_link_update_scope_is_inherited_by_all_operations() -> None:
    """
    Verify link update scope remains inherited by all operations.

    Example:
        Exercise test link update scope is inherited by all operations through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify link update rejects types outside the declared allowed set.

    Example:
        Exercise test link update rejects types outside the declared allowed set through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify link update rejects invalid named type values.

    Example:
        Exercise test link update rejects invalid named type values through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :param link_type: Optional typed relation value carried by the link.
    :param error: Value supplied for error under the catalog contract.
    :param message: Value supplied for message under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
    with pytest.raises(error, match=message):
        LinkUpdate(
            link_spec=_link_spec(),
            replacements={10: (LinkValue(20, link_type=link_type),)},  # type: ignore[arg-type]
        )


def test_link_update_allows_null_type_with_declared_allowed_types() -> None:
    """
    Verify link update allows null type with declared allowed types.

    Example:
        Exercise test link update allows null type with declared allowed types through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
    spec = replace(_link_spec(), allowed_types=("author", "editor"))

    update = LinkUpdate(
        link_spec=spec,
        replacements={10: (LinkValue(20),)},
        link_type=None,
    )

    assert update.replacements == {10: (LinkValue(20),)}


def test_link_update_rejects_ambiguous_or_untyped_payloads() -> None:
    """
    Verify link update rejects ambiguous or untyped payloads.

    Example:
        Exercise test link update rejects ambiguous or untyped payloads through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
    with pytest.raises(TypeError, match="empty iterable"):
        LinkUpdate(link_spec=_link_spec(), replacements={10: None})  # type: ignore[dict-item]

    with pytest.raises(TypeError, match="LinkValue"):
        LinkUpdate(link_spec=_link_spec(), replacements={10: [20]})  # type: ignore[list-item]

    with pytest.raises(TypeError, match="addition links cannot be None"):
        LinkUpdate(link_spec=_link_spec(), additions={10: None})  # type: ignore[dict-item]

    with pytest.raises(TypeError, match="deletion links must be LinkValue"):
        LinkUpdate(link_spec=_link_spec(), deletions={10: [20]})  # type: ignore[list-item]


def test_link_update_replacement_map_is_read_only() -> None:
    """
    Verify link update replacement map remains read only.

    Example:
        Exercise test link update replacement map is read only through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
    update = LinkUpdate(link_spec=_link_spec(), replacements={10: []})

    with pytest.raises(TypeError):
        update.replacements[10] = (LinkValue(secondary_id=20),)  # type: ignore[index]


def test_link_update_from_ids_accepts_cache_writer_shapes() -> None:
    """
    Verify link update from ids accepts cache writer shapes.

    Example:
        Exercise test link update from ids accepts cache writer shapes through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify link update from ids accepts typed cache writer shapes.

    Example:
        Exercise test link update from ids accepts typed cache writer shapes through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify link update from values resolves secondary values.

    Example:
        Exercise test link update from values resolves secondary values through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify link update compact factories preserve rich link values.

    Example:
        Exercise test link update compact factories preserve rich link values through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify link update compact factories validate nested and none values.

    Example:
        Exercise test link update compact factories validate nested and none values through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify link update from legacy matches values but preserves existing ids.

    Example:
        Exercise test link update from legacy matches values but preserves existing ids through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
    calls: list[str] = []
    ids = {"Ada": 21, "Grace": 22}

    def resolve(value: str) -> int:
        """
        Perform the resolve test-helper operation with deterministic inputs.

        Example:
            Exercise test link update from legacy matches values but preserves existing ids.resolve through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param value: Public or stored value to normalize, compare or write.
        :return: The deterministic value, row, identity or collection described above.
        """
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
    """
    Verify compact factories remove duplicate legacy link identities.

    Example:
        Exercise test compact factories remove duplicate legacy link identities through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
    update = LinkUpdate.from_ids(
        _link_spec(),
        replacements={10: [20, 20, 21]},
        additions={11: {"author": [22, 22]}},
    )

    assert _ids(update, "replacements") == (20, 21)
    assert update.additions[11] == (LinkValue(22, link_type="author"),)


def test_value_factory_matches_each_repeated_metadata_value_once() -> None:
    """
    Verify value factory matches each repeated metadata value once.

    Example:
        Exercise test value factory matches each repeated metadata value once through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
    calls: list[str] = []

    def resolve(value: str) -> int:
        """
        Perform the resolve test-helper operation with deterministic inputs.

        Example:
            Exercise test value factory matches each repeated metadata value once.resolve through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param value: Public or stored value to normalize, compare or write.
        :return: The deterministic value, row, identity or collection described above.
        """
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
    """
    Verify direct construction rejects duplicate logical link identities.

    Example:
        Exercise test direct construction rejects duplicate logical link identities through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify link update rejects database incompatible link capabilities.

    Example:
        Exercise test link update rejects database incompatible link capabilities through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify link update composes incrementals into pure replacements.

    Example:
        Exercise test link update composes incrementals into pure replacements through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify replacement composition deduplicates rows and preserves readded order.

    Example:
        Exercise test replacement composition deduplicates rows and preserves readded order through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify link update write uses one bulk replacement with scope.

    Example:
        Exercise test link update write uses one bulk replacement with scope through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify catalog writes link update through its database macros.

    Example:
        Exercise test catalog writes link update through its database macros through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify catalog link update boundary rejects other values and preserves noop.

    Example:
        Exercise test catalog link update boundary rejects other values and preserves noop through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
    macros = _RecordingMacros()
    catalog = Catalog(type("Database", (), {"macros": macros})())

    with pytest.raises(TypeError, match="update must be a LinkUpdate"):
        catalog.write_link_update(object())  # type: ignore[arg-type]

    assert catalog.write_link_update(LinkUpdate.from_ids(_link_spec())) == {}
    assert macros.reads == []
    assert macros.writes == []


def test_empty_link_update_write_does_not_touch_the_database() -> None:
    """
    Verify empty link update write does not touch the database.

    Example:
        Exercise test empty link update write does not touch the database through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
    macros = _RecordingMacros()

    assert LinkUpdate(link_spec=_link_spec()).write(macros) == {}  # type: ignore[arg-type]
    assert macros.reads == []
    assert macros.writes == []


def test_link_update_exposes_effective_primary_ids_and_mapping_access() -> None:
    """
    Verify link update exposes effective primary ids and mapping access.

    Example:
        Exercise test link update exposes effective primary ids and mapping access through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify per id view remains read only and identifies incremental updates.

    Example:
        Exercise test per id view is read only and identifies incremental updates through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify link update pretty format remains deterministic and operation ordered.

    Example:
        Exercise test link update pretty format is deterministic and operation ordered through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify empty incremental entries remain visible but do not write.

    Example:
        Exercise test empty incremental entries are visible but do not write through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify link update returns one dataclass per link in operation order.

    Example:
        Exercise test link update returns one dataclass per link in operation order through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify link dataclass remains a read only mapping over its extras.

    Example:
        Exercise test link dataclass is a read only mapping over its extras through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify iter links remains lazy and does not load destination values.

    Example:
        Exercise test iter links is lazy and does not load destination values through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify link dataclass resolves and caches its destination value lazily.

    Example:
        Exercise test link dataclass resolves and caches its destination value lazily through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
    calls: list[int] = []

    def load(dst_id: int) -> str:
        """
        Load deterministic cache state for adapter tests.

        Example:
            Exercise test link dataclass resolves and caches its destination value lazily.load through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param dst_id: Value supplied for dst id under the catalog contract.
        :return: None; the function records state or raises through its assertions.
        """
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
    """
    Verify link destination loader handles none retries failures and requires loader.

    Example:
        Exercise test link destination loader handles none retries failures and requires loader through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
        """
        Perform the flaky test-helper operation with deterministic inputs.

        Example:
            Exercise test link destination loader handles none retries failures and requires loader.flaky through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param dst_id: Value supplied for dst id under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """
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
    """
    Verify link dataclass rejects non mapping extras and non callable lazy loader.

    Example:
        Exercise test link dataclass rejects non mapping extras and non callable lazy loader through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify link dataclass display includes priority without other optional fields.

    Example:
        Exercise test link dataclass display includes priority without other optional fields through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify link dataclass snapshots extras and remains frozen and slotted.

    Example:
        Exercise test link dataclass snapshots extras and is frozen and slotted through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify legacy value to normalized link update writes through real db.

    Example:
        Exercise test legacy value to normalized link update writes through real db through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :param db: Value supplied for db under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
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
        """
        Perform the match value test-helper operation with deterministic inputs.

        Example:
            Exercise test legacy value to normalized link update writes through real db.match value through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param value: Public or stored value to normalize, compare or write.
        :return: The deterministic value, row, identity or collection described above.
        """
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
    """
    Verify link update defaults to independent empty read only operations.

    Example:
        Exercise test link update defaults to independent empty read only operations through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify link update remains frozen and slotted.

    Example:
        Exercise test link update is frozen and slotted through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify direct construction materialises legacy link collections.

    Example:
        Exercise test direct construction materialises legacy link collections through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :param operation: Value supplied for operation under the catalog contract.
    :param collection_factory: Value supplied for collection factory under the catalog
        contract.
    :return: None; the function records state or raises through its assertions.
    """
    links = (LinkValue(20), LinkValue(21))
    supplied = collection_factory(links)

    update = LinkUpdate(
        link_spec=_link_spec(),
        **_operation_payload(operation, {10: supplied}),  # type: ignore[arg-type]
    )

    assert _ids(update, operation) == (20, 21)
    assert isinstance(getattr(update, operation)[10], tuple)


def test_direct_construction_snapshots_all_caller_owned_containers() -> None:
    """
    Verify direct construction snapshots all caller owned containers.

    Example:
        Exercise test direct construction snapshots all caller owned containers through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify direct construction preserves overlapping operations.

    Example:
        Exercise test direct construction preserves overlapping operations through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify direct construction inherits scope without losing link properties.

    Example:
        Exercise test direct construction inherits scope without losing link properties through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :param operation: Value supplied for operation under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify direct construction accepts an explicit matching scope.

    Example:
        Exercise test direct construction accepts an explicit matching scope through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :param operation: Value supplied for operation under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify from ids normalises ordered legacy shapes for every operation.

    Example:
        Exercise test from ids normalises ordered legacy shapes for every operation through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :param operation: Value supplied for operation under the catalog contract.
    :param raw_factory: Value supplied for raw factory under the catalog contract.
    :param expected_ids: Value supplied for expected ids under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify from ids normalises unordered legacy shapes for every operation.

    Example:
        Exercise test from ids normalises unordered legacy shapes for every operation through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :param operation: Value supplied for operation under the catalog contract.
    :param raw_factory: Value supplied for raw factory under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify from ids accepts legacy mapping implementations.

    Example:
        Exercise test from ids accepts legacy mapping implementations through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :param mapping_factory: Value supplied for mapping factory under the catalog
        contract.
    :return: None; the function records state or raises through its assertions.
    """
    update = LinkUpdate.from_ids(_link_spec(), mapping_factory())  # type: ignore[arg-type]

    assert _ids(update, "replacements") == (20, 21)


def test_from_ids_snapshots_legacy_update_dict_and_inner_list() -> None:
    """
    Verify from ids snapshots legacy update dict and inner list.

    Example:
        Exercise test from ids snapshots legacy update dict and inner list through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
    values = [20, 21]
    supplied = UpdateDict({10: values})

    update = LinkUpdate.from_ids(_link_spec(), supplied)
    supplied.clear()
    values.clear()

    assert _ids(update, "replacements") == (20, 21)


def test_from_ids_normalises_a_mixed_typed_legacy_update() -> None:
    """
    Verify from ids normalises a mixed typed legacy update.

    Example:
        Exercise test from ids normalises a mixed typed legacy update through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify from ids supports typed maps for every operation.

    Example:
        Exercise test from ids supports typed maps for every operation through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :param operation: Value supplied for operation under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify nested typed map only supplies a missing rich link type.

    Example:
        Exercise test nested typed map only supplies a missing rich link type through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify factory scope remains applied to every compact operation.

    Example:
        Exercise test factory scope is applied to every compact operation through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify from ids preserves rich links without treating them as ids.

    Example:
        Exercise test from ids preserves rich links without treating them as ids through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify none remains a distinct sql null link type scope.

    Example:
        Exercise test none is a distinct sql null link type scope through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :param operation: Value supplied for operation under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify from values treats legacy scalar types as one value.

    Example:
        Exercise test from values treats legacy scalar types as one value through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :param operation: Value supplied for operation under the catalog contract.
    :param raw_value: Value supplied for raw value under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
    resolved: list[object] = []

    def resolve(value: object) -> int:
        """
        Perform the resolve test-helper operation with deterministic inputs.

        Example:
            Exercise test from values treats legacy scalar types as one value.resolve through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param value: Public or stored value to normalize, compare or write.
        :return: The deterministic value, row, identity or collection described above.
        """
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
    """
    Verify from values resolves ordered legacy collections once.

    Example:
        Exercise test from values resolves ordered legacy collections once through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :param raw_factory: Value supplied for raw factory under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
    resolved: list[str] = []
    ids = {"Ada": 20, "Grace": 21}

    def resolve(value: str) -> int:
        """
        Perform the resolve test-helper operation with deterministic inputs.

        Example:
            Exercise test from values resolves ordered legacy collections once.resolve through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param value: Public or stored value to normalize, compare or write.
        :return: The deterministic value, row, identity or collection described above.
        """
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
    """
    Verify from values resolves unordered legacy collections.

    Example:
        Exercise test from values resolves unordered legacy collections through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :param raw_factory: Value supplied for raw factory under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
    ids = {"Ada": 20, "Grace": 21}

    update = LinkUpdate.from_values(
        _link_spec(),
        {10: raw_factory()},
        secondary_id_for=ids.__getitem__,
    )

    assert set(_ids(update, "replacements")) == {20, 21}


def test_from_values_resolves_mixed_legacy_values_but_bypasses_rich_links() -> None:
    """
    Verify from values resolves mixed legacy values but bypasses rich links.

    Example:
        Exercise test from values resolves mixed legacy values but bypasses rich links through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
    rich = LinkValue(
        99,
        link_type="author",
        priority=4,
        extra={"credited_as": "Existing"},
    )
    calls: list[object] = []

    def resolve(value: object) -> int:
        """
        Perform the resolve test-helper operation with deterministic inputs.

        Example:
            Exercise test from values resolves mixed legacy values but bypasses rich links.resolve through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param value: Public or stored value to normalize, compare or write.
        :return: The deterministic value, row, identity or collection described above.
        """
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
    """
    Verify from values does not resolve none clears or empty roles.

    Example:
        Exercise test from values does not resolve none clears or empty roles through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
    update = LinkUpdate.from_values(
        _link_spec(),
        replacements={10: None, 11: {"author": None, "editor": []}},
        secondary_id_for=lambda value: pytest.fail(f"unexpected value: {value!r}"),
    )

    assert update.replacements == {10: (), 11: ()}


def test_from_values_preserves_typed_role_order_and_resolution_order() -> None:
    """
    Verify from values preserves typed role order and resolution order.

    Example:
        Exercise test from values preserves typed role order and resolution order through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
    calls: list[str] = []
    ids = {"Ada": 20, "Grace": 21, "Edsger": 22}

    def resolve(value: str) -> int:
        """
        Perform the resolve test-helper operation with deterministic inputs.

        Example:
            Exercise test from values preserves typed role order and resolution order.resolve through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param value: Public or stored value to normalize, compare or write.
        :return: The deterministic value, row, identity or collection described above.
        """
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
    """
    Verify from values propagates resolver exceptions and stops.

    Example:
        Exercise test from values propagates resolver exceptions and stops through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
    calls: list[str] = []

    def resolve(value: str) -> int:
        """
        Perform the resolve test-helper operation with deterministic inputs.

        Example:
            Exercise test from values propagates resolver exceptions and stops.resolve through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param value: Public or stored value to normalize, compare or write.
        :return: The deterministic value, row, identity or collection described above.
        """
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
    """
    Verify direct construction rejects non link specs.

    Example:
        Exercise test direct construction rejects non link specs through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :param bad_spec: Value supplied for bad spec under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
    with pytest.raises(TypeError, match="link_spec must be a StorageLinkSpec"):
        LinkUpdate(link_spec=bad_spec)  # type: ignore[arg-type]


@pytest.mark.parametrize("factory", ("from_ids", "from_values"))
@pytest.mark.parametrize("bad_spec", (None, "titles-creators", object()))
def test_compact_factories_reject_non_link_specs(factory: str, bad_spec: object) -> None:
    """
    Verify compact factories reject non link specs.

    Example:
        Exercise test compact factories reject non link specs through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :param factory: Value supplied for factory under the catalog contract.
    :param bad_spec: Value supplied for bad spec under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify direct construction rejects non mapping operations.

    Example:
        Exercise test direct construction rejects non mapping operations through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :param operation: Value supplied for operation under the catalog contract.
    :param bad_mapping: Value supplied for bad mapping under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify direct construction rejects non link value collections.

    Example:
        Exercise test direct construction rejects non link value collections through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :param operation: Value supplied for operation under the catalog contract.
    :param bad_links: Value supplied for bad links under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify direct construction rejects none entries.

    Example:
        Exercise test direct construction rejects none entries through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :param operation: Value supplied for operation under the catalog contract.
    :param message: Value supplied for message under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
    with pytest.raises(TypeError, match=message):
        LinkUpdate(
            link_spec=_link_spec(),
            **_operation_payload(operation, {10: None}),  # type: ignore[arg-type]
        )


@pytest.mark.parametrize("operation", ("replacements", "additions", "deletions"))
def test_direct_construction_rejects_non_mapping_extras(operation: str) -> None:
    """
    Verify direct construction rejects non mapping extras.

    Example:
        Exercise test direct construction rejects non mapping extras through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :param operation: Value supplied for operation under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify direct construction rejects scope mismatches for every operation.

    Example:
        Exercise test direct construction rejects scope mismatches for every operation through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :param operation: Value supplied for operation under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify from ids rejects non mapping operations.

    Example:
        Exercise test from ids rejects non mapping operations through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :param operation: Value supplied for operation under the catalog contract.
    :param bad_mapping: Value supplied for bad mapping under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
    with pytest.raises(TypeError, match=rf"{operation} must be a mapping"):
        LinkUpdate.from_ids(
            _link_spec(),
            **_operation_payload(operation, bad_mapping),  # type: ignore[arg-type]
        )


@pytest.mark.parametrize("operation", ("replacements", "additions", "deletions"))
def test_from_values_rejects_non_mapping_operations(operation: str) -> None:
    """
    Verify from values rejects non mapping operations.

    Example:
        Exercise test from values rejects non mapping operations through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :param operation: Value supplied for operation under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify compact factories reject typed maps for plain links.

    Example:
        Exercise test compact factories reject typed maps for plain links through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :param factory: Value supplied for factory under the catalog contract.
    :param operation: Value supplied for operation under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify compact factories reject none inside value collections.

    Example:
        Exercise test compact factories reject none inside value collections through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :param factory: Value supplied for factory under the catalog contract.
    :param operation: Value supplied for operation under the catalog contract.
    :param bad_values_factory: Value supplied for bad values factory under the catalog
        contract.
    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify from values rejects every non callable resolver.

    Example:
        Exercise test from values rejects every non callable resolver through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :param bad_resolver: Value supplied for bad resolver under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
    with pytest.raises(TypeError, match="secondary_id_for must be callable"):
        LinkUpdate.from_values(
            _link_spec(),
            replacements={10: "Ada"},
            secondary_id_for=bad_resolver,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize("bad_resolver", (None, 0, "resolver", object()))
def test_from_legacy_rejects_every_non_callable_resolver(bad_resolver: object) -> None:
    """
    Verify from legacy rejects every non callable resolver.

    Example:
        Exercise test from legacy rejects every non callable resolver through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :param bad_resolver: Value supplied for bad resolver under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
    with pytest.raises(TypeError, match="secondary_id_for must be callable"):
        LinkUpdate.from_legacy(
            _link_spec(),
            replacements={10: "Ada"},
            secondary_id_for=bad_resolver,  # type: ignore[arg-type]
        )


def test_factory_scope_rejects_a_nested_type_that_conflicts_with_it() -> None:
    """
    Verify factory scope rejects a nested type that conflicts with it.

    Example:
        Exercise test factory scope rejects a nested type that conflicts with it through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: None; the function records state or raises through its assertions.
    """
    with pytest.raises(ValueError, match="link type 'editor'.*scope 'author'"):
        LinkUpdate.from_ids(
            _link_spec(),
            {10: {"editor": 20}},
            link_type="author",
        )
