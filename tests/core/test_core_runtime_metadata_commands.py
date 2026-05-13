from __future__ import annotations

from dataclasses import dataclass

import pytest

from LiuXin_alpha.core import CoreCommand, CoreQuery, CoreRuntime
from LiuXin_alpha.core.errors import CoreHandlerError
from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadataHydrator
from tests.metadata.containers.test_item_metadata_hydrator import _build_fake_database


@dataclass
class _MetadataLibrary:
    database: object


def _metadata_tags(db) -> list[str]:
    metadata = LiuXinWEMIMetadataHydrator(db).hydrate_metadata("liuxin", item_id=1)
    return list(metadata.tags.keys())


def _identifier_values(metadata, scheme: str) -> list[str]:
    raw = metadata.get_identifiers().get(scheme)
    if raw is None:
        return []
    if isinstance(raw, str):
        return [raw]
    return list(raw)


def test_core_runtime_describes_metadata_write_commands() -> None:
    runtime = CoreRuntime(library=_MetadataLibrary(database=_build_fake_database()))

    described = runtime.execute_query(CoreQuery(name="api.describe", payload={"include_targets": False})).result
    command_names = {entry["name"] for entry in described["commands"]}

    assert "metadata.write" in command_names
    assert "metadata.tags.replace" in command_names
    assert "metadata.labels.replace" in command_names
    assert "metadata.genre.replace" in command_names
    assert "metadata.series.replace" in command_names
    assert "metadata.identifiers.replace" in command_names


def test_core_runtime_metadata_write_appends_tags_and_emits_write_event() -> None:
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
    assert _metadata_tags(db) == ["Space Opera", "core-command-tag"]

    write_events = [event for event in events if event.event_type == "write.completed"]
    assert len(write_events) == 1
    assert write_events[0].payload["name"] == "metadata.write"
    assert write_events[0].payload["item_id"] == 1


def test_core_runtime_metadata_tags_replace_is_authoritative() -> None:
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
        ("metadata.labels.replace", "labels", ["replacement-label"], "labels", ["replacement-label"]),
        ("metadata.genre.replace", "genre", ["Replacement Genre"], "genre", ["Replacement Genre"]),
        ("metadata.series.replace", "series", ["Replacement Series"], "series", ["Replacement Series"]),
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
    runtime = CoreRuntime(library=_MetadataLibrary(database=_build_fake_database()))

    with pytest.raises(CoreHandlerError) as exc_info:
        runtime.execute_command(
            CoreCommand(
                name="metadata.tags.replace",
                payload={"item_id": 1, "kind": "liuxin"},
            )
        )

    assert "payload missing `tags`" in str(exc_info.value)
