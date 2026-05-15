"""Intrinsic profile implementation containers for canonical agents.

Category: agent-profile exception.
This module implements metadata about the agent itself. It is deliberately not a
WEMI `XMetadata` object and not a graph-spanning participation view.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.agent_containers.agent_identity_api import AgentIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.agent_containers.agent_profile_api import (
    AgentProfileAPI,
    HumanAgentProfileAPI,
    OrganisationAgentProfileAPI,
)
from LiuXin_alpha.metadata.containers.metadata_containers._string_formatting import (
    compact_mapping_string,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
from LiuXin_alpha.metadata.metadata_types import AgentID


AGENT_ALIAS_SEPARATOR = "(#BREAK#)"

_AGENT_BASE_KEYS = {
    "agent_id",
    "agent_type",
    "agent_canonical_name",
    "agent_display_name",
    "display_name",
    "agent_sort_name",
    "sort_name",
    "agent_aliases",
    "agent_note",
    "agent_created_timestamp_ep_k",
    "agent_modified_timestamp_ep_k",
    "agent_source_created_datestamp_ep_k",
    "agent_source_modified_datestamp_ep_k",
    "agent_scratch",
}

_HUMAN_SIDE_CAR_KEYS = {
    "human_agent_id",
    "human_agent_agent_id",
    "human_agent_given_name",
    "human_agent_middle_name",
    "human_agent_family_name",
    "human_agent_prefix",
    "human_agent_suffix",
    "human_agent_preferred_name",
    "human_agent_birth_date",
    "human_agent_death_date",
    "human_agent_nationality",
    "human_agent_biography",
    "human_agent_created_timestamp_ep_k",
    "human_agent_modified_timestamp_ep_k",
    "human_agent_scratch",
}

_ORG_SIDE_CAR_KEYS = {
    "org_agent_id",
    "org_agent_agent_id",
    "org_agent_legal_name",
    "org_agent_trading_name",
    "org_agent_registration_id",
    "org_agent_jurisdiction",
    "org_agent_founded_date",
    "org_agent_dissolved_date",
    "org_agent_website",
    "org_agent_contact_email",
    "org_agent_description",
    "org_agent_created_timestamp_ep_k",
    "org_agent_modified_timestamp_ep_k",
    "org_agent_scratch",
}

_ORG_AGENT_TYPES = {"organisation", "organization", "org", "company", "publisher"}
_HUMAN_AGENT_TYPES = {"person", "human", "author", "creator"}


def _row_keys(row: Mapping[str, Any]) -> set[str]:
    keys_method = getattr(row, "keys", None)
    if callable(keys_method):
        return {str(key) for key in keys_method()}
    return {str(key) for key in row}


def _has_any(row: Mapping[str, Any], keys: set[str]) -> bool:
    return bool(_row_keys(row) & keys)


def _first_present(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return None


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _text_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text != "" else None


def _aliases_from_value(value: Any) -> tuple[str, ...]:
    if value in (None, ""):
        return ()
    if isinstance(value, str):
        raw_values = value.split(AGENT_ALIAS_SEPARATOR)
    elif isinstance(value, Iterable):
        raw_values = list(value)
    else:
        raw_values = [value]

    aliases: list[str] = []
    seen: set[str] = set()
    for raw in raw_values:
        if raw is None:
            continue
        alias = str(raw).strip()
        if not alias:
            continue
        key = alias.casefold()
        if key in seen:
            continue
        seen.add(key)
        aliases.append(alias)
    return tuple(aliases)


def _aliases_to_storage(aliases: Iterable[str]) -> str | None:
    values = _aliases_from_value(tuple(aliases))
    return AGENT_ALIAS_SEPARATOR.join(values) if values else None


def _identity_from_mapping(row: Mapping[str, Any]) -> AgentIdentityAPI | None:
    agent_mapping = row.get("agent")
    if isinstance(agent_mapping, Mapping):
        return AgentIdentity.from_mapping(agent_mapping)
    if _has_any(row, _AGENT_BASE_KEYS):
        return AgentIdentity.from_mapping(row)
    return None


def _base_kwargs_from_mapping(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "agent": _identity_from_mapping(row),
        "aliases": _aliases_from_value(row.get("agent_aliases") or row.get("aliases")),
        "notes": _text_or_none(row.get("agent_note") or row.get("notes")),
        "created_timestamp_ep_k": _int_or_none(row.get("agent_created_timestamp_ep_k")),
        "modified_timestamp_ep_k": _int_or_none(row.get("agent_modified_timestamp_ep_k")),
        "source_created_datestamp_ep_k": _int_or_none(row.get("agent_source_created_datestamp_ep_k")),
        "source_modified_datestamp_ep_k": _int_or_none(row.get("agent_source_modified_datestamp_ep_k")),
        "scratch": _text_or_none(row.get("agent_scratch")),
        "extra": row.get("extra") if isinstance(row.get("extra"), Mapping) else {},
    }


def _normalised_agent_type(row: Mapping[str, Any]) -> str:
    return str(_first_present(row, "agent_type") or "").strip().casefold()


class AgentProfile(AgentProfileAPI):
    """
    Concrete shared implementation for rows from ``agents``.

    ``from_mapping()`` acts as a small factory when sidecar columns are present,
    returning :class:`HumanAgentProfile` or :class:`OrganisationAgentProfile`.
    """

    def __init__(
        self,
        *,
        agent: AgentIdentityAPI | None = None,
        aliases: Iterable[str] = (),
        notes: str | None = None,
        created_timestamp_ep_k: int | None = None,
        modified_timestamp_ep_k: int | None = None,
        source_created_datestamp_ep_k: int | None = None,
        source_modified_datestamp_ep_k: int | None = None,
        scratch: str | None = None,
        extra: Mapping[str, Any] | None = None,
    ) -> None:
        self._agent = agent
        self._aliases = _aliases_from_value(tuple(aliases))
        self._notes = notes
        self._created_timestamp_ep_k = created_timestamp_ep_k
        self._modified_timestamp_ep_k = modified_timestamp_ep_k
        self._source_created_datestamp_ep_k = source_created_datestamp_ep_k
        self._source_modified_datestamp_ep_k = source_modified_datestamp_ep_k
        self._scratch = scratch
        self._extra = dict(extra or {})

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any]) -> "AgentProfile":
        if cls is AgentProfile:
            agent_type = _normalised_agent_type(row)
            if _has_any(row, _ORG_SIDE_CAR_KEYS) or agent_type in _ORG_AGENT_TYPES:
                return OrganisationAgentProfile.from_mapping(row)
            if _has_any(row, _HUMAN_SIDE_CAR_KEYS) or agent_type in _HUMAN_AGENT_TYPES:
                return HumanAgentProfile.from_mapping(row)
        return cls(**_base_kwargs_from_mapping(row))

    @property
    def agent(self) -> AgentIdentityAPI | None:
        return self._agent

    @agent.setter
    def agent(self, value: AgentIdentityAPI | None) -> None:
        self._agent = value

    @property
    def aliases(self) -> tuple[str, ...]:
        return self._aliases

    @aliases.setter
    def aliases(self, value: tuple[str, ...] | list[str]) -> None:
        self._aliases = _aliases_from_value(value)

    @property
    def notes(self) -> str | None:
        return self._notes

    @notes.setter
    def notes(self, value: str | None) -> None:
        self._notes = value

    @property
    def created_timestamp_ep_k(self) -> int | None:
        return self._created_timestamp_ep_k

    @created_timestamp_ep_k.setter
    def created_timestamp_ep_k(self, value: int | None) -> None:
        self._created_timestamp_ep_k = value

    @property
    def modified_timestamp_ep_k(self) -> int | None:
        return self._modified_timestamp_ep_k

    @modified_timestamp_ep_k.setter
    def modified_timestamp_ep_k(self, value: int | None) -> None:
        self._modified_timestamp_ep_k = value

    @property
    def source_created_datestamp_ep_k(self) -> int | None:
        return self._source_created_datestamp_ep_k

    @source_created_datestamp_ep_k.setter
    def source_created_datestamp_ep_k(self, value: int | None) -> None:
        self._source_created_datestamp_ep_k = value

    @property
    def source_modified_datestamp_ep_k(self) -> int | None:
        return self._source_modified_datestamp_ep_k

    @source_modified_datestamp_ep_k.setter
    def source_modified_datestamp_ep_k(self, value: int | None) -> None:
        self._source_modified_datestamp_ep_k = value

    @property
    def scratch(self) -> str | None:
        return self._scratch

    @scratch.setter
    def scratch(self, value: str | None) -> None:
        self._scratch = value

    @property
    def extra(self) -> Mapping[str, Any]:
        return self._extra

    def add_alias(self, value: str) -> None:
        self._aliases = _aliases_from_value((*self._aliases, value))

    def _base_mapping(self) -> dict[str, Any]:
        agent_type = self.agent.agent_type if self.agent is not None else None
        display_name = self.agent.display_name if self.agent is not None else None
        sort_name = self.agent.sort_name if self.agent is not None else None
        return {
            "agent_id": self.agent_id,
            "agent_type": agent_type,
            "agent_canonical_name": display_name,
            "agent_sort_name": sort_name,
            "agent_aliases": _aliases_to_storage(self.aliases),
            "agent_note": self.notes,
            "agent_created_timestamp_ep_k": self.created_timestamp_ep_k,
            "agent_modified_timestamp_ep_k": self.modified_timestamp_ep_k,
            "agent_source_created_datestamp_ep_k": self.source_created_datestamp_ep_k,
            "agent_source_modified_datestamp_ep_k": self.source_modified_datestamp_ep_k,
            "agent_scratch": self.scratch,
            "extra": dict(self.extra),
        }

    def to_mapping(self) -> dict[str, Any]:
        return self._base_mapping()

    def __str__(self) -> str:
        return compact_mapping_string(
            self,
            self.to_mapping(),
            id_keys=("agent_id",),
            display_keys=("agent_canonical_name", "agent_type", "agent_sort_name"),
        )


class HumanAgentProfile(AgentProfile, HumanAgentProfileAPI):
    """Concrete profile for an ``agents`` row plus a ``human_agents`` sidecar."""

    def __init__(
        self,
        *,
        agent: AgentIdentityAPI | None = None,
        aliases: Iterable[str] = (),
        notes: str | None = None,
        created_timestamp_ep_k: int | None = None,
        modified_timestamp_ep_k: int | None = None,
        source_created_datestamp_ep_k: int | None = None,
        source_modified_datestamp_ep_k: int | None = None,
        scratch: str | None = None,
        extra: Mapping[str, Any] | None = None,
        human_agent_id: int | None = None,
        human_agent_agent_id: AgentID | None = None,
        given_name: str | None = None,
        middle_name: str | None = None,
        family_name: str | None = None,
        prefix: str | None = None,
        suffix: str | None = None,
        preferred_name: str | None = None,
        birth_date: str | None = None,
        death_date: str | None = None,
        nationality: str | None = None,
        biography: str | None = None,
        human_agent_created_timestamp_ep_k: int | None = None,
        human_agent_modified_timestamp_ep_k: int | None = None,
        human_agent_scratch: str | None = None,
    ) -> None:
        super().__init__(
            agent=agent,
            aliases=aliases,
            notes=notes,
            created_timestamp_ep_k=created_timestamp_ep_k,
            modified_timestamp_ep_k=modified_timestamp_ep_k,
            source_created_datestamp_ep_k=source_created_datestamp_ep_k,
            source_modified_datestamp_ep_k=source_modified_datestamp_ep_k,
            scratch=scratch,
            extra=extra,
        )
        self._human_agent_id = human_agent_id
        self._human_agent_agent_id = human_agent_agent_id
        self._given_name = given_name
        self._middle_name = middle_name
        self._family_name = family_name
        self._prefix = prefix
        self._suffix = suffix
        self._preferred_name = preferred_name
        self._birth_date = birth_date
        self._death_date = death_date
        self._nationality = nationality
        self._biography = biography
        self._human_agent_created_timestamp_ep_k = human_agent_created_timestamp_ep_k
        self._human_agent_modified_timestamp_ep_k = human_agent_modified_timestamp_ep_k
        self._human_agent_scratch = human_agent_scratch

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any]) -> "HumanAgentProfile":
        return cls(
            **_base_kwargs_from_mapping(row),
            human_agent_id=_int_or_none(row.get("human_agent_id")),
            human_agent_agent_id=_int_or_none(row.get("human_agent_agent_id")),
            given_name=_text_or_none(row.get("human_agent_given_name")),
            middle_name=_text_or_none(row.get("human_agent_middle_name")),
            family_name=_text_or_none(row.get("human_agent_family_name")),
            prefix=_text_or_none(row.get("human_agent_prefix")),
            suffix=_text_or_none(row.get("human_agent_suffix")),
            preferred_name=_text_or_none(row.get("human_agent_preferred_name")),
            birth_date=_text_or_none(row.get("human_agent_birth_date")),
            death_date=_text_or_none(row.get("human_agent_death_date")),
            nationality=_text_or_none(row.get("human_agent_nationality")),
            biography=_text_or_none(row.get("human_agent_biography")),
            human_agent_created_timestamp_ep_k=_int_or_none(row.get("human_agent_created_timestamp_ep_k")),
            human_agent_modified_timestamp_ep_k=_int_or_none(row.get("human_agent_modified_timestamp_ep_k")),
            human_agent_scratch=_text_or_none(row.get("human_agent_scratch")),
        )

    @property
    def human_agent_id(self) -> int | None:
        return self._human_agent_id

    @human_agent_id.setter
    def human_agent_id(self, value: int | None) -> None:
        self._human_agent_id = value

    @property
    def human_agent_agent_id(self) -> AgentID | None:
        return self._human_agent_agent_id

    @human_agent_agent_id.setter
    def human_agent_agent_id(self, value: AgentID | None) -> None:
        self._human_agent_agent_id = value

    @property
    def given_name(self) -> str | None:
        return self._given_name

    @given_name.setter
    def given_name(self, value: str | None) -> None:
        self._given_name = value

    @property
    def middle_name(self) -> str | None:
        return self._middle_name

    @middle_name.setter
    def middle_name(self, value: str | None) -> None:
        self._middle_name = value

    @property
    def family_name(self) -> str | None:
        return self._family_name

    @family_name.setter
    def family_name(self, value: str | None) -> None:
        self._family_name = value

    @property
    def prefix(self) -> str | None:
        return self._prefix

    @prefix.setter
    def prefix(self, value: str | None) -> None:
        self._prefix = value

    @property
    def suffix(self) -> str | None:
        return self._suffix

    @suffix.setter
    def suffix(self, value: str | None) -> None:
        self._suffix = value

    @property
    def preferred_name(self) -> str | None:
        return self._preferred_name

    @preferred_name.setter
    def preferred_name(self, value: str | None) -> None:
        self._preferred_name = value

    @property
    def birth_date(self) -> str | None:
        return self._birth_date

    @birth_date.setter
    def birth_date(self, value: str | None) -> None:
        self._birth_date = value

    @property
    def death_date(self) -> str | None:
        return self._death_date

    @death_date.setter
    def death_date(self, value: str | None) -> None:
        self._death_date = value

    @property
    def nationality(self) -> str | None:
        return self._nationality

    @nationality.setter
    def nationality(self, value: str | None) -> None:
        self._nationality = value

    @property
    def biography(self) -> str | None:
        return self._biography

    @biography.setter
    def biography(self, value: str | None) -> None:
        self._biography = value

    @property
    def human_agent_created_timestamp_ep_k(self) -> int | None:
        return self._human_agent_created_timestamp_ep_k

    @human_agent_created_timestamp_ep_k.setter
    def human_agent_created_timestamp_ep_k(self, value: int | None) -> None:
        self._human_agent_created_timestamp_ep_k = value

    @property
    def human_agent_modified_timestamp_ep_k(self) -> int | None:
        return self._human_agent_modified_timestamp_ep_k

    @human_agent_modified_timestamp_ep_k.setter
    def human_agent_modified_timestamp_ep_k(self, value: int | None) -> None:
        self._human_agent_modified_timestamp_ep_k = value

    @property
    def human_agent_scratch(self) -> str | None:
        return self._human_agent_scratch

    @human_agent_scratch.setter
    def human_agent_scratch(self, value: str | None) -> None:
        self._human_agent_scratch = value

    def to_mapping(self) -> dict[str, Any]:
        return {
            **self._base_mapping(),
            "human_agent_id": self.human_agent_id,
            "human_agent_agent_id": self.human_agent_agent_id,
            "human_agent_given_name": self.given_name,
            "human_agent_middle_name": self.middle_name,
            "human_agent_family_name": self.family_name,
            "human_agent_prefix": self.prefix,
            "human_agent_suffix": self.suffix,
            "human_agent_preferred_name": self.preferred_name,
            "human_agent_birth_date": self.birth_date,
            "human_agent_death_date": self.death_date,
            "human_agent_nationality": self.nationality,
            "human_agent_biography": self.biography,
            "human_agent_created_timestamp_ep_k": self.human_agent_created_timestamp_ep_k,
            "human_agent_modified_timestamp_ep_k": self.human_agent_modified_timestamp_ep_k,
            "human_agent_scratch": self.human_agent_scratch,
        }


class OrganisationAgentProfile(AgentProfile, OrganisationAgentProfileAPI):
    """Concrete profile for an ``agents`` row plus an ``org_agents`` sidecar."""

    def __init__(
        self,
        *,
        agent: AgentIdentityAPI | None = None,
        aliases: Iterable[str] = (),
        notes: str | None = None,
        created_timestamp_ep_k: int | None = None,
        modified_timestamp_ep_k: int | None = None,
        source_created_datestamp_ep_k: int | None = None,
        source_modified_datestamp_ep_k: int | None = None,
        scratch: str | None = None,
        extra: Mapping[str, Any] | None = None,
        org_agent_id: int | None = None,
        org_agent_agent_id: AgentID | None = None,
        legal_name: str | None = None,
        trading_name: str | None = None,
        registration_id: str | None = None,
        jurisdiction: str | None = None,
        founded_date: str | None = None,
        dissolved_date: str | None = None,
        website: str | None = None,
        contact_email: str | None = None,
        description: str | None = None,
        org_agent_created_timestamp_ep_k: int | None = None,
        org_agent_modified_timestamp_ep_k: int | None = None,
        org_agent_scratch: str | None = None,
    ) -> None:
        super().__init__(
            agent=agent,
            aliases=aliases,
            notes=notes,
            created_timestamp_ep_k=created_timestamp_ep_k,
            modified_timestamp_ep_k=modified_timestamp_ep_k,
            source_created_datestamp_ep_k=source_created_datestamp_ep_k,
            source_modified_datestamp_ep_k=source_modified_datestamp_ep_k,
            scratch=scratch,
            extra=extra,
        )
        self._org_agent_id = org_agent_id
        self._org_agent_agent_id = org_agent_agent_id
        self._legal_name = legal_name
        self._trading_name = trading_name
        self._registration_id = registration_id
        self._jurisdiction = jurisdiction
        self._founded_date = founded_date
        self._dissolved_date = dissolved_date
        self._website = website
        self._contact_email = contact_email
        self._description = description
        self._org_agent_created_timestamp_ep_k = org_agent_created_timestamp_ep_k
        self._org_agent_modified_timestamp_ep_k = org_agent_modified_timestamp_ep_k
        self._org_agent_scratch = org_agent_scratch

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any]) -> "OrganisationAgentProfile":
        return cls(
            **_base_kwargs_from_mapping(row),
            org_agent_id=_int_or_none(row.get("org_agent_id")),
            org_agent_agent_id=_int_or_none(row.get("org_agent_agent_id")),
            legal_name=_text_or_none(row.get("org_agent_legal_name")),
            trading_name=_text_or_none(row.get("org_agent_trading_name")),
            registration_id=_text_or_none(row.get("org_agent_registration_id")),
            jurisdiction=_text_or_none(row.get("org_agent_jurisdiction")),
            founded_date=_text_or_none(row.get("org_agent_founded_date")),
            dissolved_date=_text_or_none(row.get("org_agent_dissolved_date")),
            website=_text_or_none(row.get("org_agent_website")),
            contact_email=_text_or_none(row.get("org_agent_contact_email")),
            description=_text_or_none(row.get("org_agent_description")),
            org_agent_created_timestamp_ep_k=_int_or_none(row.get("org_agent_created_timestamp_ep_k")),
            org_agent_modified_timestamp_ep_k=_int_or_none(row.get("org_agent_modified_timestamp_ep_k")),
            org_agent_scratch=_text_or_none(row.get("org_agent_scratch")),
        )

    @property
    def org_agent_id(self) -> int | None:
        return self._org_agent_id

    @org_agent_id.setter
    def org_agent_id(self, value: int | None) -> None:
        self._org_agent_id = value

    @property
    def org_agent_agent_id(self) -> AgentID | None:
        return self._org_agent_agent_id

    @org_agent_agent_id.setter
    def org_agent_agent_id(self, value: AgentID | None) -> None:
        self._org_agent_agent_id = value

    @property
    def legal_name(self) -> str | None:
        return self._legal_name

    @legal_name.setter
    def legal_name(self, value: str | None) -> None:
        self._legal_name = value

    @property
    def trading_name(self) -> str | None:
        return self._trading_name

    @trading_name.setter
    def trading_name(self, value: str | None) -> None:
        self._trading_name = value

    @property
    def registration_id(self) -> str | None:
        return self._registration_id

    @registration_id.setter
    def registration_id(self, value: str | None) -> None:
        self._registration_id = value

    @property
    def jurisdiction(self) -> str | None:
        return self._jurisdiction

    @jurisdiction.setter
    def jurisdiction(self, value: str | None) -> None:
        self._jurisdiction = value

    @property
    def founded_date(self) -> str | None:
        return self._founded_date

    @founded_date.setter
    def founded_date(self, value: str | None) -> None:
        self._founded_date = value

    @property
    def dissolved_date(self) -> str | None:
        return self._dissolved_date

    @dissolved_date.setter
    def dissolved_date(self, value: str | None) -> None:
        self._dissolved_date = value

    @property
    def website(self) -> str | None:
        return self._website

    @website.setter
    def website(self, value: str | None) -> None:
        self._website = value

    @property
    def contact_email(self) -> str | None:
        return self._contact_email

    @contact_email.setter
    def contact_email(self, value: str | None) -> None:
        self._contact_email = value

    @property
    def description(self) -> str | None:
        return self._description

    @description.setter
    def description(self, value: str | None) -> None:
        self._description = value

    @property
    def org_agent_created_timestamp_ep_k(self) -> int | None:
        return self._org_agent_created_timestamp_ep_k

    @org_agent_created_timestamp_ep_k.setter
    def org_agent_created_timestamp_ep_k(self, value: int | None) -> None:
        self._org_agent_created_timestamp_ep_k = value

    @property
    def org_agent_modified_timestamp_ep_k(self) -> int | None:
        return self._org_agent_modified_timestamp_ep_k

    @org_agent_modified_timestamp_ep_k.setter
    def org_agent_modified_timestamp_ep_k(self, value: int | None) -> None:
        self._org_agent_modified_timestamp_ep_k = value

    @property
    def org_agent_scratch(self) -> str | None:
        return self._org_agent_scratch

    @org_agent_scratch.setter
    def org_agent_scratch(self, value: str | None) -> None:
        self._org_agent_scratch = value

    def to_mapping(self) -> dict[str, Any]:
        return {
            **self._base_mapping(),
            "org_agent_id": self.org_agent_id,
            "org_agent_agent_id": self.org_agent_agent_id,
            "org_agent_legal_name": self.legal_name,
            "org_agent_trading_name": self.trading_name,
            "org_agent_registration_id": self.registration_id,
            "org_agent_jurisdiction": self.jurisdiction,
            "org_agent_founded_date": self.founded_date,
            "org_agent_dissolved_date": self.dissolved_date,
            "org_agent_website": self.website,
            "org_agent_contact_email": self.contact_email,
            "org_agent_description": self.description,
            "org_agent_created_timestamp_ep_k": self.org_agent_created_timestamp_ep_k,
            "org_agent_modified_timestamp_ep_k": self.org_agent_modified_timestamp_ep_k,
            "org_agent_scratch": self.org_agent_scratch,
        }


__all__ = [
    "AgentProfile",
    "HumanAgentProfile",
    "OrganisationAgentProfile",
]
