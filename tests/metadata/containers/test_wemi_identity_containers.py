from __future__ import annotations

import pytest

from LiuXin_alpha.metadata.containers import AgentIdentity
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_container import (
    ExpressionIdentity,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_container import (
    ManifestationIdentity,
)


def test_agent_identity_setters_and_id_write_guard() -> None:
    identity = AgentIdentity()

    identity.agent_id = 2
    identity.agent_type = "organisation"
    identity.display_name = "Display Name"
    identity.sort_name = "Name, Display"

    assert identity.to_mapping() == {
        "agent_id": 2,
        "agent_type": "organisation",
        "agent_display_name": "Display Name",
        "agent_sort_name": "Name, Display",
    }

    with pytest.raises(AttributeError, match="Agent id is already set"):
        identity.agent_id = 3


def test_agent_identity_from_mapping_uses_display_and_sort_fallbacks() -> None:
    identity = AgentIdentity.from_mapping(
        {
            "agent_id": 7,
            "agent_type": "person",
            "agent_canonical_name": "Canonical Name",
            "sort_name": "Name, Canonical",
        }
    )

    assert identity.display_name == "Canonical Name"
    assert identity.sort_name == "Name, Canonical"
    assert "Canonical Name" in str(identity)


def test_expression_identity_id_write_guard() -> None:
    identity = ExpressionIdentity()

    identity.expression_id = 10

    assert identity.expression_id == 10
    with pytest.raises(AttributeError, match="Expression id is already set"):
        identity.expression_id = 11


def test_expression_identity_mapping_flags_and_string_representation() -> None:
    empty = ExpressionIdentity()
    assert empty.to_mapping()["expression_flags"] is None

    identity = ExpressionIdentity.from_mapping(
        {
            "expression_id": 10,
            "expression_work_id": 2,
            "expression_type": "text",
            "expression_language_id": 1,
            "expression_label": "English text",
            "expression_flags": "draft, reviewed, draft, ",
            "expression_status": "active",
        }
    )

    assert identity.expression_flags == ("draft", "reviewed")
    assert identity.to_mapping()["expression_flags"] == "draft,reviewed"
    assert "English text" in str(identity)

    identity.expression_flags = ("reviewed", "", "proof", "reviewed")
    assert identity.expression_flags == ("reviewed", "proof")


def test_manifestation_identity_setters_and_id_write_guard() -> None:
    identity = ManifestationIdentity()

    identity.manifestation_id = 20
    identity.manifestation_format_detail = "EPUB"

    assert identity.manifestation_id == 20
    assert identity.manifestation_format_detail == "EPUB"
    with pytest.raises(AttributeError, match="Manifestation id is already set"):
        identity.manifestation_id = 21


def test_manifestation_identity_mapping_and_string_representation() -> None:
    identity = ManifestationIdentity.from_mapping(
        {
            "manifestation_id": 20,
            "manifestation_expression_id": 10,
            "manifestation_format_detail": "EPUB",
            "manifestation_carrier_type": "ebook",
            "manifestation_edition_statement": "First digital edition",
            "manifestation_pub_year": 2026,
            "manifestation_status": "active",
            "manifestation_flags": "clean",
            "manifestation_page_count": 320,
            "manifestation_runtime_minutes": None,
            "manifestation_note": "note",
        }
    )

    mapping = identity.to_mapping()

    assert mapping["manifestation_id"] == 20
    assert mapping["manifestation_expression_id"] == 10
    assert mapping["manifestation_format_detail"] == "EPUB"
    assert "EPUB" in str(identity)
