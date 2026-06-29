"""Intrinsic profile API contracts for canonical agents.

Category: agent-profile exception.
These contracts model metadata about the agent itself. They are deliberately
not WEMI ``XMetadataAPI`` objects and not graph-spanning participation views.
"""
from __future__ import annotations

import abc

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.agent_containers.agent_identity_api import AgentIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_target_api import (
    MetadataRecord,
    MutableMetadataRecord,
)
from LiuXin_alpha.metadata.metadata_types import AgentID


class AgentProfileAPI(abc.ABC):
    """
    Shared intrinsic profile data backed by the ``agents`` table.

    Human and organisation-specific facts live on the subtype profile APIs
    below. Graph-spanning query results belong in dedicated read-side
    snapshot/view containers.
    """

    @property
    @abc.abstractmethod
    def agent(self) -> AgentIdentityAPI | None:
        """Primary agent identity for this profile."""

    @agent.setter
    @abc.abstractmethod
    def agent(self, value: AgentIdentityAPI | None) -> None:
        """Set primary agent identity."""

    @property
    def agent_id(self) -> AgentID | None:
        """Convenience pass-through to the attached identity, if any."""
        return self.agent.agent_id if self.agent is not None else None

    @property
    def display_name(self) -> str | None:
        """Convenience pass-through to the attached identity, if any."""
        return self.agent.display_name if self.agent is not None else None

    @property
    def sort_name(self) -> str | None:
        """Convenience pass-through to the attached identity, if any."""
        return self.agent.sort_name if self.agent is not None else None

    @property
    @abc.abstractmethod
    def aliases(self) -> tuple[str, ...]:
        """Alternative display names from ``agents.agent_aliases``."""

    @aliases.setter
    @abc.abstractmethod
    def aliases(self, value: tuple[str, ...] | list[str]) -> None:
        """Replace aliases."""

    @property
    @abc.abstractmethod
    def notes(self) -> str | None:
        """Free-form notes from ``agents.agent_note``."""

    @notes.setter
    @abc.abstractmethod
    def notes(self, value: str | None) -> None:
        """Set notes."""

    @property
    @abc.abstractmethod
    def created_timestamp_ep_k(self) -> int | None:
        """Creation timestamp from ``agents.agent_created_timestamp_ep_k``."""

    @created_timestamp_ep_k.setter
    @abc.abstractmethod
    def created_timestamp_ep_k(self, value: int | None) -> None:
        """Set creation timestamp."""

    @property
    @abc.abstractmethod
    def modified_timestamp_ep_k(self) -> int | None:
        """Modified timestamp from ``agents.agent_modified_timestamp_ep_k``."""

    @modified_timestamp_ep_k.setter
    @abc.abstractmethod
    def modified_timestamp_ep_k(self, value: int | None) -> None:
        """Set modified timestamp."""

    @property
    @abc.abstractmethod
    def source_created_datestamp_ep_k(self) -> int | None:
        """Source-created timestamp from ``agents.agent_source_created_datestamp_ep_k``."""

    @source_created_datestamp_ep_k.setter
    @abc.abstractmethod
    def source_created_datestamp_ep_k(self, value: int | None) -> None:
        """Set source-created timestamp."""

    @property
    @abc.abstractmethod
    def source_modified_datestamp_ep_k(self) -> int | None:
        """Source-modified timestamp from ``agents.agent_source_modified_datestamp_ep_k``."""

    @source_modified_datestamp_ep_k.setter
    @abc.abstractmethod
    def source_modified_datestamp_ep_k(self, value: int | None) -> None:
        """Set source-modified timestamp."""

    @property
    @abc.abstractmethod
    def scratch(self) -> str | None:
        """Scratch/import text from ``agents.agent_scratch``."""

    @scratch.setter
    @abc.abstractmethod
    def scratch(self, value: str | None) -> None:
        """Set scratch/import text."""

    @property
    @abc.abstractmethod
    def extra(self) -> MetadataRecord:
        """Extension fields for profile metadata not yet promoted to first-class properties."""

    @abc.abstractmethod
    def to_mapping(self) -> MutableMetadataRecord:
        """Convert to a plain mapping for storage / serialisation."""

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"


class HumanAgentProfileAPI(AgentProfileAPI):
    """Intrinsic profile data backed by ``agents`` plus ``human_agents``."""

    @property
    @abc.abstractmethod
    def human_agent_id(self) -> int | None:
        """Primary key from ``human_agents.human_agent_id``."""

    @human_agent_id.setter
    @abc.abstractmethod
    def human_agent_id(self, value: int | None) -> None:
        """Set human sidecar primary key."""

    @property
    @abc.abstractmethod
    def human_agent_agent_id(self) -> AgentID | None:
        """Foreign key from ``human_agents.human_agent_agent_id``."""

    @human_agent_agent_id.setter
    @abc.abstractmethod
    def human_agent_agent_id(self, value: AgentID | None) -> None:
        """Set human sidecar agent foreign key."""

    @property
    @abc.abstractmethod
    def given_name(self) -> str | None:
        """Given name from ``human_agents.human_agent_given_name``."""

    @given_name.setter
    @abc.abstractmethod
    def given_name(self, value: str | None) -> None:
        """Set given name."""

    @property
    @abc.abstractmethod
    def middle_name(self) -> str | None:
        """Middle name from ``human_agents.human_agent_middle_name``."""

    @middle_name.setter
    @abc.abstractmethod
    def middle_name(self, value: str | None) -> None:
        """Set middle name."""

    @property
    @abc.abstractmethod
    def family_name(self) -> str | None:
        """Family name from ``human_agents.human_agent_family_name``."""

    @family_name.setter
    @abc.abstractmethod
    def family_name(self, value: str | None) -> None:
        """Set family name."""

    @property
    @abc.abstractmethod
    def prefix(self) -> str | None:
        """Name prefix from ``human_agents.human_agent_prefix``."""

    @prefix.setter
    @abc.abstractmethod
    def prefix(self, value: str | None) -> None:
        """Set name prefix."""

    @property
    @abc.abstractmethod
    def suffix(self) -> str | None:
        """Name suffix from ``human_agents.human_agent_suffix``."""

    @suffix.setter
    @abc.abstractmethod
    def suffix(self, value: str | None) -> None:
        """Set name suffix."""

    @property
    @abc.abstractmethod
    def preferred_name(self) -> str | None:
        """Preferred personal name from ``human_agents.human_agent_preferred_name``."""

    @preferred_name.setter
    @abc.abstractmethod
    def preferred_name(self, value: str | None) -> None:
        """Set preferred personal name."""

    @property
    @abc.abstractmethod
    def birth_date(self) -> str | None:
        """Birth date from ``human_agents.human_agent_birth_date``."""

    @birth_date.setter
    @abc.abstractmethod
    def birth_date(self, value: str | None) -> None:
        """Set birth date."""

    @property
    @abc.abstractmethod
    def death_date(self) -> str | None:
        """Death date from ``human_agents.human_agent_death_date``."""

    @death_date.setter
    @abc.abstractmethod
    def death_date(self, value: str | None) -> None:
        """Set death date."""

    @property
    @abc.abstractmethod
    def nationality(self) -> str | None:
        """Nationality from ``human_agents.human_agent_nationality``."""

    @nationality.setter
    @abc.abstractmethod
    def nationality(self, value: str | None) -> None:
        """Set nationality."""

    @property
    @abc.abstractmethod
    def biography(self) -> str | None:
        """Biography from ``human_agents.human_agent_biography``."""

    @biography.setter
    @abc.abstractmethod
    def biography(self, value: str | None) -> None:
        """Set biography."""

    @property
    @abc.abstractmethod
    def human_agent_created_timestamp_ep_k(self) -> int | None:
        """Creation timestamp from ``human_agents``."""

    @human_agent_created_timestamp_ep_k.setter
    @abc.abstractmethod
    def human_agent_created_timestamp_ep_k(self, value: int | None) -> None:
        """Set human sidecar creation timestamp."""

    @property
    @abc.abstractmethod
    def human_agent_modified_timestamp_ep_k(self) -> int | None:
        """Modified timestamp from ``human_agents``."""

    @human_agent_modified_timestamp_ep_k.setter
    @abc.abstractmethod
    def human_agent_modified_timestamp_ep_k(self, value: int | None) -> None:
        """Set human sidecar modified timestamp."""

    @property
    @abc.abstractmethod
    def human_agent_scratch(self) -> str | None:
        """Scratch/import text from ``human_agents.human_agent_scratch``."""

    @human_agent_scratch.setter
    @abc.abstractmethod
    def human_agent_scratch(self, value: str | None) -> None:
        """Set human sidecar scratch/import text."""


class OrganisationAgentProfileAPI(AgentProfileAPI):
    """Intrinsic profile data backed by ``agents`` plus ``org_agents``."""

    @property
    @abc.abstractmethod
    def org_agent_id(self) -> int | None:
        """Primary key from ``org_agents.org_agent_id``."""

    @org_agent_id.setter
    @abc.abstractmethod
    def org_agent_id(self, value: int | None) -> None:
        """Set organisation sidecar primary key."""

    @property
    @abc.abstractmethod
    def org_agent_agent_id(self) -> AgentID | None:
        """Foreign key from ``org_agents.org_agent_agent_id``."""

    @org_agent_agent_id.setter
    @abc.abstractmethod
    def org_agent_agent_id(self, value: AgentID | None) -> None:
        """Set organisation sidecar agent foreign key."""

    @property
    @abc.abstractmethod
    def legal_name(self) -> str | None:
        """Legal name from ``org_agents.org_agent_legal_name``."""

    @legal_name.setter
    @abc.abstractmethod
    def legal_name(self, value: str | None) -> None:
        """Set legal name."""

    @property
    @abc.abstractmethod
    def trading_name(self) -> str | None:
        """Trading name from ``org_agents.org_agent_trading_name``."""

    @trading_name.setter
    @abc.abstractmethod
    def trading_name(self, value: str | None) -> None:
        """Set trading name."""

    @property
    @abc.abstractmethod
    def registration_id(self) -> str | None:
        """Registration ID from ``org_agents.org_agent_registration_id``."""

    @registration_id.setter
    @abc.abstractmethod
    def registration_id(self, value: str | None) -> None:
        """Set registration ID."""

    @property
    @abc.abstractmethod
    def jurisdiction(self) -> str | None:
        """Jurisdiction from ``org_agents.org_agent_jurisdiction``."""

    @jurisdiction.setter
    @abc.abstractmethod
    def jurisdiction(self, value: str | None) -> None:
        """Set jurisdiction."""

    @property
    @abc.abstractmethod
    def founded_date(self) -> str | None:
        """Founded date from ``org_agents.org_agent_founded_date``."""

    @founded_date.setter
    @abc.abstractmethod
    def founded_date(self, value: str | None) -> None:
        """Set founded date."""

    @property
    @abc.abstractmethod
    def dissolved_date(self) -> str | None:
        """Dissolved date from ``org_agents.org_agent_dissolved_date``."""

    @dissolved_date.setter
    @abc.abstractmethod
    def dissolved_date(self, value: str | None) -> None:
        """Set dissolved date."""

    @property
    @abc.abstractmethod
    def website(self) -> str | None:
        """Website from ``org_agents.org_agent_website``."""

    @website.setter
    @abc.abstractmethod
    def website(self, value: str | None) -> None:
        """Set website."""

    @property
    @abc.abstractmethod
    def contact_email(self) -> str | None:
        """Contact email from ``org_agents.org_agent_contact_email``."""

    @contact_email.setter
    @abc.abstractmethod
    def contact_email(self, value: str | None) -> None:
        """Set contact email."""

    @property
    @abc.abstractmethod
    def description(self) -> str | None:
        """Description from ``org_agents.org_agent_description``."""

    @description.setter
    @abc.abstractmethod
    def description(self, value: str | None) -> None:
        """Set description."""

    @property
    @abc.abstractmethod
    def org_agent_created_timestamp_ep_k(self) -> int | None:
        """Creation timestamp from ``org_agents``."""

    @org_agent_created_timestamp_ep_k.setter
    @abc.abstractmethod
    def org_agent_created_timestamp_ep_k(self, value: int | None) -> None:
        """Set organisation sidecar creation timestamp."""

    @property
    @abc.abstractmethod
    def org_agent_modified_timestamp_ep_k(self) -> int | None:
        """Modified timestamp from ``org_agents``."""

    @org_agent_modified_timestamp_ep_k.setter
    @abc.abstractmethod
    def org_agent_modified_timestamp_ep_k(self, value: int | None) -> None:
        """Set organisation sidecar modified timestamp."""

    @property
    @abc.abstractmethod
    def org_agent_scratch(self) -> str | None:
        """Scratch/import text from ``org_agents.org_agent_scratch``."""

    @org_agent_scratch.setter
    @abc.abstractmethod
    def org_agent_scratch(self, value: str | None) -> None:
        """Set organisation sidecar scratch/import text."""


__all__ = [
    "AgentProfileAPI",
    "HumanAgentProfileAPI",
    "OrganisationAgentProfileAPI",
]
