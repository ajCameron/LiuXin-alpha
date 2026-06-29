"""Database contract: agent/creator/organisation/publisher adders write FRBR agent tables."""

from __future__ import annotations

import pytest

from LiuXin_alpha.catalog.metadata_tools import Add


def test_add_creator_writes_agent_and_human_sidecar_with_links(open_db) -> None:
    tables = set(open_db.get_tables())
    if "agents" not in tables:
        pytest.skip("Schema does not expose FRBR agent tables")

    add = Add(open_db)
    creator_row = add.creator(
        creator="Ursula K. Le Guin",
        creator_short_name="Le Guin",
        creator_legal_name="Ursula Kroeber Le Guin",
        creator_birth_date="1929-10-21",
        creator_death_date="2018-01-22",
        creator_type="authors",
        creator_seminal_work="The Left Hand of Darkness",
        creator_wikipedia="https://en.wikipedia.org/wiki/Ursula_K._Le_Guin",
        creator_imdb="nm0494265",
        creator_link="https://www.ursulakleguin.com",
        creator_language="english",
        creator_bio="American author of speculative fiction.",
    )

    assert creator_row.table == "agents"
    assert creator_row["agent_type"] == "person"
    assert creator_row["agent_canonical_name"] == "Ursula K. Le Guin"

    human_rows = open_db.search("human_agents", "human_agent_agent_id", creator_row["agent_id"])
    assert len(human_rows) == 1
    human_row = human_rows[0]
    assert human_row["human_agent_birth_date"] == "1929-10-21"
    assert human_row["human_agent_death_date"] == "2018-01-22"
    assert human_row["human_agent_family_name"] == "Guin"

    language_rows = open_db.get_interlinked_rows(primary_row=creator_row, secondary_table="languages")
    assert language_rows

    note_rows = open_db.get_interlinked_rows(primary_row=creator_row, secondary_table="notes")
    assert any("speculative fiction" in (row["note"] or "") for row in note_rows)

    entity_rows = open_db.search("entity_identifiers", "entity_identifier_entity_id", creator_row["agent_id"])
    schemes = {row["entity_identifier_scheme"] for row in entity_rows}
    assert "wikipedia_url" in schemes
    assert "imdb_id" in schemes
    assert "url" in schemes


def test_add_organisation_and_publisher_write_org_sidecars_and_relations(open_db) -> None:
    tables = set(open_db.get_tables())
    if "agents" not in tables:
        pytest.skip("Schema does not expose FRBR agent tables")

    add = Add(open_db)

    parent_row = add.organisation(
        organisation="Tor Books",
        organisation_sort="Tor Books",
        organisation_website="https://www.tor.com",
        organisation_description="Primary Tor imprint.",
    )

    publisher_row = add.publisher(
        publisher="Tor UK",
        publisher_sort="Tor UK",
        publisher_description="UK Tor imprint.",
        publisher_wikipedia="https://en.wikipedia.org/wiki/Tor_Books",
        publisher_website="https://www.tor.co.uk",
        publisher_parent=parent_row,
    )

    assert parent_row.table == "agents"
    assert parent_row["agent_type"] == "organisation"
    assert publisher_row.table == "agents"
    assert publisher_row["agent_type"] == "organisation"

    parent_org_rows = open_db.search("org_agents", "org_agent_agent_id", parent_row["agent_id"])
    assert len(parent_org_rows) == 1
    assert parent_org_rows[0]["org_agent_website"] == "https://www.tor.com"

    publisher_org_rows = open_db.search("org_agents", "org_agent_agent_id", publisher_row["agent_id"])
    assert len(publisher_org_rows) == 1
    assert publisher_org_rows[0]["org_agent_website"] == "https://www.tor.co.uk"
    assert publisher_org_rows[0]["org_agent_description"] == "UK Tor imprint."

    relation_rows = open_db.search(
        "org_agent_relations",
        "org_agent_relation_child_agent_id",
        publisher_row["agent_id"],
    )
    assert relation_rows
    assert any(
        row["org_agent_relation_parent_agent_id"] == parent_row["agent_id"]
        and row["org_agent_relation_type"] == "imprint_of"
        for row in relation_rows
    )

    entity_rows = open_db.search("entity_identifiers", "entity_identifier_entity_id", publisher_row["agent_id"])
    schemes = {row["entity_identifier_scheme"] for row in entity_rows}
    assert "wikipedia_url" in schemes
    assert "url" in schemes


def test_add_organization_alias_works(open_db) -> None:
    tables = set(open_db.get_tables())
    if "agents" not in tables:
        pytest.skip("Schema does not expose FRBR agent tables")

    add = Add(open_db)
    row = add.organization(
        organization="Orbit Books",
        organization_sort="Orbit Books",
    )
    assert row.table == "agents"
    assert row["agent_type"] == "organisation"
    assert row["agent_canonical_name"] == "Orbit Books"
