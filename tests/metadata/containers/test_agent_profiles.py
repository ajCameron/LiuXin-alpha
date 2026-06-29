from __future__ import annotations

from LiuXin_alpha.metadata.api import (
    AgentProfileAPI,
    HumanAgentProfileAPI,
    OrganisationAgentProfileAPI,
)
from LiuXin_alpha.metadata.containers import (
    AgentIdentity,
    AgentProfile,
    HumanAgentProfile,
    OrganisationAgentProfile,
)


def test_human_agent_profile_uses_agents_and_human_sidecar_columns() -> None:
    profile = AgentProfile.from_mapping(
        {
            "agent_id": 7,
            "agent_type": "person",
            "agent_canonical_name": "Ursula K. Le Guin",
            "agent_sort_name": "Le Guin, Ursula K.",
            "agent_aliases": "UKLG(#BREAK#)Ursula Le Guin",
            "agent_note": "creator_type=author",
            "agent_created_timestamp_ep_k": 100,
            "agent_modified_timestamp_ep_k": 110,
            "human_agent_id": 3,
            "human_agent_agent_id": 7,
            "human_agent_given_name": "Ursula",
            "human_agent_middle_name": "K.",
            "human_agent_family_name": "Le Guin",
            "human_agent_preferred_name": "UKLG",
            "human_agent_birth_date": "1929-10-21",
            "human_agent_death_date": "2018-01-22",
            "human_agent_nationality": "US",
            "human_agent_biography": "Writer.",
        }
    )

    assert isinstance(profile, HumanAgentProfile)
    assert isinstance(profile, AgentProfileAPI)
    assert isinstance(profile, HumanAgentProfileAPI)
    assert profile.agent_id == 7
    assert profile.display_name == "Ursula K. Le Guin"
    assert profile.sort_name == "Le Guin, Ursula K."
    assert profile.aliases == ("UKLG", "Ursula Le Guin")
    assert profile.notes == "creator_type=author"
    assert profile.given_name == "Ursula"
    assert profile.family_name == "Le Guin"
    assert profile.birth_date == "1929-10-21"
    assert profile.biography == "Writer."

    mapping = profile.to_mapping()
    assert mapping["agent_canonical_name"] == "Ursula K. Le Guin"
    assert mapping["agent_aliases"] == "UKLG(#BREAK#)Ursula Le Guin"
    assert mapping["human_agent_family_name"] == "Le Guin"
    assert "org_agent_legal_name" not in mapping


def test_organisation_agent_profile_uses_agents_and_org_sidecar_columns() -> None:
    profile = AgentProfile.from_mapping(
        {
            "agent_id": 12,
            "agent_type": "organisation",
            "agent_canonical_name": "Ace Books",
            "agent_sort_name": "Ace Books",
            "agent_aliases": ["Ace", "Ace Books"],
            "agent_note": "publisher",
            "org_agent_id": 4,
            "org_agent_agent_id": 12,
            "org_agent_legal_name": "Ace Books Inc.",
            "org_agent_trading_name": "Ace",
            "org_agent_registration_id": "reg-1",
            "org_agent_jurisdiction": "US",
            "org_agent_founded_date": "1952-01-01",
            "org_agent_dissolved_date": None,
            "org_agent_website": "https://example.invalid/ace",
            "org_agent_contact_email": "rights@example.invalid",
            "org_agent_description": "Publisher.",
        }
    )

    assert isinstance(profile, OrganisationAgentProfile)
    assert isinstance(profile, AgentProfileAPI)
    assert isinstance(profile, OrganisationAgentProfileAPI)
    assert profile.agent_id == 12
    assert profile.display_name == "Ace Books"
    assert profile.aliases == ("Ace", "Ace Books")
    assert profile.legal_name == "Ace Books Inc."
    assert profile.trading_name == "Ace"
    assert profile.registration_id == "reg-1"
    assert profile.website == "https://example.invalid/ace"
    assert profile.description == "Publisher."

    mapping = profile.to_mapping()
    assert mapping["agent_canonical_name"] == "Ace Books"
    assert mapping["agent_aliases"] == "Ace(#BREAK#)Ace Books"
    assert mapping["org_agent_legal_name"] == "Ace Books Inc."
    assert "human_agent_family_name" not in mapping


def test_base_agent_profile_keeps_shared_agent_table_data() -> None:
    profile = AgentProfile(
        agent=AgentIdentity(
            agent_id=99,
            agent_type="group",
            agent_display_name="Collective Example",
            agent_sort_name="Collective Example",
        ),
        aliases=("Collective", "collective"),
        notes="No sidecar.",
        scratch="import-state",
    )

    assert not isinstance(profile, HumanAgentProfileAPI)
    assert not isinstance(profile, OrganisationAgentProfileAPI)
    assert profile.aliases == ("Collective",)

    mapping = profile.to_mapping()
    assert mapping["agent_id"] == 99
    assert mapping["agent_type"] == "group"
    assert mapping["agent_canonical_name"] == "Collective Example"
    assert mapping["agent_aliases"] == "Collective"
    assert mapping["agent_scratch"] == "import-state"
