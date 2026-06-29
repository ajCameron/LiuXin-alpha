from __future__ import annotations

from typing import Any

import pytest

from LiuXin_alpha.metadata.api import (
    AgentIdentityAPI,
    AgentProfileAPI,
    ExpressionIdentityAPI,
    ItemIdentityAPI,
    ManifestationIdentityAPI,
    MetadataRecord,
    MutableMetadataRecord,
    WemiIdentityAPI,
    WorkIdentityAPI,
)


def _stored_property(name: str) -> property:
    def getter(self):
        return self._values.get(name)

    def setter(self, value) -> None:
        self._values[name] = value

    return property(getter, setter)


class _ConcreteWorkIdentity(WorkIdentityAPI):
    work_id = _stored_property("work_id")
    work_type = _stored_property("work_type")
    work_medium = _stored_property("work_medium")
    work_title = _stored_property("work_title")
    work_name = _stored_property("work_name")
    work_canonical_title = _stored_property("work_canonical_title")
    work_sort_title = _stored_property("work_sort_title")
    work_is_fiction = _stored_property("work_is_fiction")
    work_audience = _stored_property("work_audience")
    work_completion_status = _stored_property("work_completion_status")
    work_original_language_id = _stored_property("work_original_language_id")
    work_discovery_note = _stored_property("work_discovery_note")
    work_created_timestamp_ep_k = _stored_property("work_created_timestamp_ep_k")
    work_modified_timestamp_ep_k = _stored_property("work_modified_timestamp_ep_k")
    work_original_year = _stored_property("work_original_year")
    work_scratch = _stored_property("work_scratch")

    def __init__(self, **values: Any) -> None:
        self._values = dict(values)

    @classmethod
    def from_mapping(cls, row):
        return cls(**row)

    def to_mapping(self):
        return dict(self._values)


class _ConcreteItemIdentity(ItemIdentityAPI):
    item_id = _stored_property("item_id")
    item_manifestation_id = _stored_property("item_manifestation_id")
    item_flags = _stored_property("item_flags")
    item_type = _stored_property("item_type")
    item_location = _stored_property("item_location")
    item_inventory_code = _stored_property("item_inventory_code")
    item_source = _stored_property("item_source")
    item_source_detail = _stored_property("item_source_detail")
    item_source_path = _stored_property("item_source_path")
    item_source_name = _stored_property("item_source_name")
    item_acquired_date = _stored_property("item_acquired_date")
    item_acquired_price_minor = _stored_property("item_acquired_price_minor")
    item_lifecycle_status = _stored_property("item_lifecycle_status")
    item_condition = _stored_property("item_condition")

    def __init__(self, **values: Any) -> None:
        self._values = dict(values)

    @classmethod
    def from_mapping(cls, row):
        return cls(**row)

    def to_mapping(self):
        return dict(self._values)


class _ConcreteExpressionIdentity(ExpressionIdentityAPI):
    expression_id = _stored_property("expression_id")
    expression_type = _stored_property("expression_type")
    expression_language_id = _stored_property("expression_language_id")
    expression_label = _stored_property("expression_label")
    expression_title_override = _stored_property("expression_title_override")
    expression_subtitle = _stored_property("expression_subtitle")
    expression_flags = _stored_property("expression_flags")
    expression_status = _stored_property("expression_status")

    def __init__(self, **values: Any) -> None:
        self._values = dict(values)

    @classmethod
    def from_mapping(cls, row: MetadataRecord):
        return cls(**row)

    @property
    def to_mapping(self) -> MutableMetadataRecord:
        return dict(self._values)


class _ConcreteManifestationIdentity(ManifestationIdentityAPI):
    manifestation_id = _stored_property("manifestation_id")
    manifestation_expression_id = _stored_property("manifestation_expression_id")
    manifestation_format_detail = _stored_property("manifestation_format_detail")
    manifestation_carrier_type = _stored_property("manifestation_carrier_type")
    manifestation_edition_statement = _stored_property("manifestation_edition_statement")
    manifestation_pub_year = _stored_property("manifestation_pub_year")
    manifestation_status = _stored_property("manifestation_status")
    manifestation_flags = _stored_property("manifestation_flags")

    def __init__(self, **values: Any) -> None:
        self._values = dict(values)

    @classmethod
    def from_mapping(cls, row: MetadataRecord):
        return cls(**row)

    @property
    def to_mapping(self) -> MutableMetadataRecord:
        return dict(self._values)


class _ConcreteAgentIdentity(AgentIdentityAPI):
    agent_id = _stored_property("agent_id")
    agent_type = _stored_property("agent_type")
    display_name = _stored_property("display_name")

    def __init__(self, **values: Any) -> None:
        self._values = dict(values)


class _ConcreteAgentProfile(AgentProfileAPI):
    agent = _stored_property("agent")
    aliases = _stored_property("aliases")
    notes = _stored_property("notes")
    created_timestamp_ep_k = _stored_property("created_timestamp_ep_k")
    modified_timestamp_ep_k = _stored_property("modified_timestamp_ep_k")
    source_created_datestamp_ep_k = _stored_property("source_created_datestamp_ep_k")
    source_modified_datestamp_ep_k = _stored_property("source_modified_datestamp_ep_k")
    scratch = _stored_property("scratch")
    extra = _stored_property("extra")

    def __init__(self, **values: Any) -> None:
        self._values = dict(values)

    def to_mapping(self) -> MutableMetadataRecord:
        return dict(self._values)


class _ConcreteWemiIdentity(WemiIdentityAPI):
    id = _stored_property("id")

    def __init__(self, **values: Any) -> None:
        self._values = dict(values)

    @classmethod
    def from_mapping(cls, row: MetadataRecord):
        return cls(**row)

    def to_mapping(self) -> MutableMetadataRecord:
        return dict(self._values)


def test_work_identity_aliases_delegate_to_work_fields() -> None:
    identity = _ConcreteWorkIdentity.from_mapping({"work_id": 1})
    alias_pairs = (
        ("id", "work_id", 101),
        ("type", "work_type", "novel"),
        ("medium", "work_medium", "text"),
        ("title", "work_title", "Title"),
        ("name", "work_name", "Name"),
        ("canonical_title", "work_canonical_title", "Canonical"),
        ("sort_title", "work_sort_title", "Sort"),
        ("is_fiction", "work_is_fiction", 1),
        ("audience", "work_audience", "adult"),
        ("completion_status", "work_completion_status", "complete"),
        ("original_language_id", "work_original_language_id", 11),
        ("discovery_note", "work_discovery_note", "note"),
        ("created_timestamp_ep_k", "work_created_timestamp_ep_k", 123),
        ("modified_timestamp_ep_k", "work_modified_timestamp_ep_k", 456),
        ("original_year", "work_original_year", 2020),
        ("scratch", "work_scratch", "scratch"),
    )

    for alias, backing_field, value in alias_pairs:
        setattr(identity, alias, value)
        assert getattr(identity, alias) == value
        assert getattr(identity, backing_field) == value

    assert identity.to_mapping()["work_id"] == 101
    assert str(identity) == "_ConcreteWorkIdentity()"


def test_item_identity_aliases_delegate_to_item_fields() -> None:
    identity = _ConcreteItemIdentity()
    alias_pairs = (
        ("id", "item_id", 201),
        ("manifestation_id", "item_manifestation_id", 301),
        ("flags", "item_flags", "clean"),
        ("type", "item_type", "digital"),
        ("location", "item_location", "/books/book.epub"),
        ("inventory_code", "item_inventory_code", "INV-1"),
        ("source", "item_source", "store"),
        ("source_detail", "item_source_detail", "sale"),
        ("source_path", "item_source_path", "/incoming/book.epub"),
        ("source_name", "item_source_name", "Incoming Book"),
        ("acquired_date", "item_acquired_date", "2026-05-15"),
        ("acquired_price_minor", "item_acquired_price_minor", 999),
        ("lifecycle_status", "item_lifecycle_status", "active"),
        ("condition", "item_condition", "new"),
    )

    for alias, backing_field, value in alias_pairs:
        setattr(identity, alias, value)
        assert getattr(identity, alias) == value
        assert getattr(identity, backing_field) == value

    assert str(identity) == "_ConcreteItemIdentity()"


def test_expression_identity_alias_and_string_path() -> None:
    identity = _ConcreteExpressionIdentity.from_mapping({"expression_id": 1})

    identity.id = 55

    assert identity.id == 55
    assert identity.expression_id == 55
    assert identity.to_mapping["expression_id"] == 55
    assert str(identity) == "_ConcreteExpressionIdentity()"


def test_manifestation_identity_aliases_delegate_to_manifestation_fields() -> None:
    identity = _ConcreteManifestationIdentity.from_mapping({})

    identity.id = 65
    identity.expression_id = 75

    assert identity.id == 65
    assert identity.manifestation_id == 65
    assert identity.expression_id == 75
    assert identity.manifestation_expression_id == 75
    assert identity.to_mapping["manifestation_expression_id"] == 75
    assert str(identity) == "_ConcreteManifestationIdentity()"


def test_agent_identity_and_profile_defaults_and_passthroughs() -> None:
    unnamed = _ConcreteAgentIdentity(agent_id=1, agent_type="human", display_name=None)
    named = _ConcreteAgentIdentity(agent_id=2, agent_type="human", display_name="Agent Name")
    profile = _ConcreteAgentProfile(agent=named)
    empty_profile = _ConcreteAgentProfile(agent=None)

    assert unnamed.sort_name is None
    with pytest.raises(AttributeError, match="read-only"):
        unnamed.sort_name = "Name, Agent"

    assert unnamed.to_mapping() == {
        "agent_id": 1,
        "agent_type": "human",
        "agent_display_name": None,
        "agent_sort_name": None,
    }
    assert str(unnamed) == "<unnamed agent>"
    assert str(named) == "Agent Name"
    assert empty_profile.agent_id is None
    assert empty_profile.display_name is None
    assert empty_profile.sort_name is None
    assert profile.agent_id == 2
    assert profile.display_name == "Agent Name"
    assert profile.sort_name is None
    assert str(profile) == "_ConcreteAgentProfile()"


def test_base_wemi_identity_string_path() -> None:
    identity = _ConcreteWemiIdentity.from_mapping({"id": 88})

    assert identity.to_mapping() == {"id": 88}
    assert str(identity) == "_ConcreteWemiIdentity()"
