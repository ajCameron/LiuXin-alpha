"""
Verify named metadata-command registration, item-centered writes, replacement semantics, and write events.

An in-memory fake database shared with hydrator tests exercises actual metadata
workflow/hydration code without opening a database or enabling a cache.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from LiuXin_alpha.core import CoreCommand, CoreQuery, CoreRuntime
from LiuXin_alpha.core.errors import CoreHandlerError
from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadataHydrator
from tests.metadata.containers.test_item_metadata_hydrator import _build_fake_database


@dataclass
class _MetadataLibrary:
    """
    Expose the fake database through the library attribute required by Core metadata handlers.

    Example:
        >>> database = object()
        >>> _MetadataLibrary(database).database is database
        True
    """

    database: object


def _metadata_tags(db) -> list[str]:
    """
    Rehydrate item 1's LiuXin metadata and return tag-map keys in their current iteration order.

    This reads the actual post-write metadata view rather than cached test expectations.

    Example:
        >>> _metadata_tags(_build_fake_database())
        ['Space Opera']


    :param db: Fake database containing the hydrator fixture's item-centered WEMI graph.
    :return: Tag names from the hydrated metadata's key order, without independent sorting.
    """
    metadata = LiuXinWEMIMetadataHydrator(db).hydrate_metadata("liuxin", item_id=1)
    return list(metadata.tags.keys())


def _identifier_values(metadata, scheme: str) -> list[str]:
    """
    Read one identifier scheme as a list, distinguishing a scalar string from an iterable of values.

    None means absent. Empty strings remain one-element lists, while iterable
    ordering follows the supplied value rather than an imposed sort.

    Example:
        >>> from unittest.mock import Mock
        >>> metadata = Mock()
        >>> metadata.get_identifiers.return_value = {"doi": "10.5555/example"}
        >>> _identifier_values(metadata, "doi"), _identifier_values(metadata, "missing")
        (['10.5555/example'], [])


    :param metadata: Hydrated metadata-like object exposing a scheme-keyed identifier mapping.
    :param scheme: Exact identifier scheme key to retrieve.
    :return: Empty list for None, a one-element list for a string, or list(raw) for other values.
    """
    raw = metadata.get_identifiers().get(scheme)
    if raw is None:
        return []
    if isinstance(raw, str):
        return [raw]
    return list(raw)


def test_core_runtime_describes_metadata_write_commands() -> None:
    """
    Require the generic metadata writer and five field-replacement routes in named command introspection.

    Example:
        >>> test_core_runtime_describes_metadata_write_commands()


    :return: None if all six expected metadata command names are advertised.
    """
    runtime = CoreRuntime(library=_MetadataLibrary(database=_build_fake_database()))

    described = runtime.execute_query(
        CoreQuery(name="api.describe", payload={"include_targets": False})
    ).result
    command_names = {entry["name"] for entry in described["commands"]}

    assert "metadata.write" in command_names
    assert "metadata.tags.replace" in command_names
    assert "metadata.labels.replace" in command_names
    assert "metadata.genre.replace" in command_names
    assert "metadata.series.replace" in command_names
    assert "metadata.identifiers.replace" in command_names


def test_core_runtime_metadata_write_appends_tags_and_emits_write_event() -> None:
    """
    Append a tag without removing the existing one and require one item-labeled write.completed event.

    Also checks the result's field selection and explicit no-cache reconciliation
    metadata, without treating event delivery as a separate persistence proof.

    Example:
        >>> test_core_runtime_metadata_write_appends_tags_and_emits_write_event()


    :return: None if the rehydrated tags, write receipt, and matching event payload agree.
    """
    db = _build_fake_database()
    runtime = CoreRuntime(library=_MetadataLibrary(database=db))
    events = []
    runtime.subscribe(events.append)

    result = runtime.execute_command(
        CoreCommand(
            name="metadata.write",
            payload={
                "item_id": 1,
                "values": {"tags": ["core-command-tag"]},
                "fields": ("tags",),
                "kind": "liuxin",
            },
        )
    ).result

    assert result["changed"] is True
    assert result["fields"] == ["tags"]
    assert result["cache"] == {
        "configured": False,
        "reconciled": False,
    }
    assert _metadata_tags(db) == ["Space Opera", "core-command-tag"]

    write_events = [event for event in events if event.event_type == "write.completed"]
    assert len(write_events) == 1
    assert write_events[0].payload["name"] == "metadata.write"
    assert write_events[0].payload["item_id"] == 1


def test_core_runtime_metadata_tags_replace_is_authoritative() -> None:
    """
    Replace the fixture's existing tag instead of appending and require removed links in the write report.

    Example:
        >>> test_core_runtime_metadata_tags_replace_is_authoritative()


    :return: None if replacement is reported and rehydration exposes only the replacement tag.
    """
    db = _build_fake_database()
    runtime = CoreRuntime(library=_MetadataLibrary(database=db))

    result = runtime.execute_command(
        CoreCommand(
            name="metadata.tags.replace",
            payload={
                "item_id": 1,
                "tags": ["replacement-tag"],
                "kind": "liuxin",
            },
        )
    ).result

    assert result["changed"] is True
    assert result["replace"] is True
    assert result["report"]["links_removed"]
    assert _metadata_tags(db) == ["replacement-tag"]


@pytest.mark.parametrize(
    ("command_name", "payload_field", "value", "field_name", "expected"),
    [
        (
            "metadata.labels.replace",
            "labels",
            ["replacement-label"],
            "labels",
            ["replacement-label"],
        ),
        (
            "metadata.genre.replace",
            "genre",
            ["Replacement Genre"],
            "genre",
            ["Replacement Genre"],
        ),
        (
            "metadata.series.replace",
            "series",
            ["Replacement Series"],
            "series",
            ["Replacement Series"],
        ),
        (
            "metadata.identifiers.replace",
            "identifiers",
            {"doi": {"10.5555/core-command"}},
            "identifiers",
            ["10.5555/core-command"],
        ),
    ],
)
def test_core_runtime_metadata_field_replace_commands_are_authoritative(
    command_name: str,
    payload_field: str,
    value,
    field_name: str,
    expected: list[str],
) -> None:
    """
    Exercise authoritative label/genre/series/identifier replacements and verify the rehydrated field values.

    Identifier replacement additionally requires the old Open Library scheme to
    disappear, not merely addition of the new DOI scheme.

    Example:
        >>> test_core_runtime_metadata_field_replace_commands_are_authoritative(
        ...     "metadata.labels.replace", "labels", ["replacement-label"], "labels", ["replacement-label"],
        ... )


    :param command_name: Parametrized named metadata replacement route.
    :param payload_field: Top-level request field carrying the replacement data.
    :param value: Replacement collection or identifier scheme mapping forwarded to the command.
    :param field_name: Expected write-field selection and rehydrated field to inspect.
    :param expected: Expected resulting values in the selected metadata field or DOI scheme.
    :return: None if the receipt and rehydrated metadata demonstrate the requested authoritative replacement.
    """
    db = _build_fake_database()
    runtime = CoreRuntime(library=_MetadataLibrary(database=db))

    result = runtime.execute_command(
        CoreCommand(
            name=command_name,
            payload={
                "item_id": 1,
                payload_field: value,
                "kind": "liuxin",
            },
        )
    ).result

    assert result["changed"] is True
    assert result["fields"] == [field_name]
    assert result["replace"] is True
    rehydrated = LiuXinWEMIMetadataHydrator(db).hydrate_metadata("liuxin", item_id=1)
    if field_name == "identifiers":
        assert _identifier_values(rehydrated, "doi") == expected
        assert _identifier_values(rehydrated, "openlibrary") == []
    else:
        assert list(getattr(rehydrated, field_name).keys()) == expected


def test_core_runtime_metadata_field_replace_requires_field_payload() -> None:
    """
    Reject a tags-replacement request that supplies item identity but omits replacement values.

    Example:
        >>> test_core_runtime_metadata_field_replace_requires_field_payload()


    :return: None if the wrapped handler error identifies the absent tags payload.
    """
    runtime = CoreRuntime(library=_MetadataLibrary(database=_build_fake_database()))

    with pytest.raises(CoreHandlerError) as exc_info:
        runtime.execute_command(
            CoreCommand(
                name="metadata.tags.replace",
                payload={"item_id": 1, "kind": "liuxin"},
            )
        )

    assert "payload missing `tags`" in str(exc_info.value)
