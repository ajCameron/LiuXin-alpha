"""
Define intrinsic profile contracts for canonical agents and their human or organisation sidecars.

These APIs model fields stored with the agent itself. Participation in works or
other graph-spanning query results belongs to read-side views.

Example:
    >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
    >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
    >>> identity = AgentIdentity(agent_id=7, agent_display_name='Ada')
    >>> profile = AgentProfile(agent=identity, aliases=('A. Lovelace',), notes='Writer')
    >>> profile.display_name
    'Ada'
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
    Require shared agent-profile fields and mapping conversion around an optional identity.

    The abstract setters define the editable surface; concrete implementations decide
    normalization and copying. Convenience identity properties return None when no
    identity is attached.

    Example:
        >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
        >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
        >>> identity = AgentIdentity(agent_id=7, agent_display_name='Ada')
        >>> profile = AgentProfile(agent=identity, aliases=('A. Lovelace',), notes='Writer')
        >>> profile.agent_id, profile.aliases
        (7, ('A. Lovelace',))
    """

    @property
    @abc.abstractmethod
    def agent(self) -> AgentIdentityAPI | None:
        """
        Require access to the attached identity object.

        Concrete profiles control normalization and ownership; identity and mapping values
        may remain shared according to that implementation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> identity = AgentIdentity(agent_id=7, agent_display_name='Ada')
            >>> profile = AgentProfile(agent=identity, aliases=('A. Lovelace',), notes='Writer')
            >>> profile.agent is identity
            True


        :return: Current attached identity object, or None where optional.
        """

    @agent.setter
    @abc.abstractmethod
    def agent(self, value: AgentIdentityAPI | None) -> None:
        """
        Require replacement of the attached identity object under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = AgentProfile()
            >>> identity = AgentIdentity(agent_id=7, agent_display_name='Ada')
            >>> profile.agent = identity
            >>> profile.agent is identity
            True


        :param value: New attached identity object.
        :return: None.
        """

    @property
    def agent_id(self) -> AgentID | None:
        """
        Return the attached identity id, or None when no identity is attached.

        This is a read-only pass-through and performs no synchronization or fallback beyond
        the linked identity.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> identity = AgentIdentity(agent_id=7, agent_display_name='Ada')
            >>> profile = AgentProfile(agent=identity, aliases=('A. Lovelace',), notes='Writer')
            >>> profile.agent_id
            7


        :return: Current attached identity id, or None.
        """
        return self.agent.agent_id if self.agent is not None else None

    @property
    def display_name(self) -> str | None:
        """
        Return the attached identity display name, or None when no identity is attached.

        This is a read-only pass-through and performs no synchronization or fallback beyond
        the linked identity.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> identity = AgentIdentity(agent_id=7, agent_display_name='Ada')
            >>> profile = AgentProfile(agent=identity, aliases=('A. Lovelace',), notes='Writer')
            >>> profile.display_name
            'Ada'


        :return: Current attached identity display name, or None.
        """
        return self.agent.display_name if self.agent is not None else None

    @property
    def sort_name(self) -> str | None:
        """
        Return the attached identity sort name, or None when no identity is attached.

        This is a read-only pass-through and performs no synchronization or fallback beyond
        the linked identity.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> identity = AgentIdentity(agent_id=7, agent_display_name='Ada')
            >>> profile = AgentProfile(agent=identity, aliases=('A. Lovelace',), notes='Writer')
            >>> profile.sort_name is None
            True


        :return: Current attached identity sort name, or None.
        """
        return self.agent.sort_name if self.agent is not None else None

    @property
    @abc.abstractmethod
    def aliases(self) -> tuple[str, ...]:
        """
        Require access to the alternative display names.

        Concrete profiles control normalization and ownership; identity and mapping values
        may remain shared according to that implementation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> identity = AgentIdentity(agent_id=7, agent_display_name='Ada')
            >>> profile = AgentProfile(agent=identity, aliases=('A. Lovelace',), notes='Writer')
            >>> profile.aliases
            ('A. Lovelace',)


        :return: Current alternative display names, or None where optional.
        """

    @aliases.setter
    @abc.abstractmethod
    def aliases(self, value: tuple[str, ...] | list[str]) -> None:
        """
        Require replacement of the alternative display names under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = AgentProfile()
            >>> profile.aliases = ('A. Lovelace',)
            >>> profile.aliases
            ('A. Lovelace',)


        :param value: New alternative display names.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def notes(self) -> str | None:
        """
        Require access to the free-form agent notes.

        Concrete profiles control normalization and ownership; identity and mapping values
        may remain shared according to that implementation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> identity = AgentIdentity(agent_id=7, agent_display_name='Ada')
            >>> profile = AgentProfile(agent=identity, aliases=('A. Lovelace',), notes='Writer')
            >>> profile.notes
            'Writer'


        :return: Current free-form agent notes, or None where optional.
        """

    @notes.setter
    @abc.abstractmethod
    def notes(self, value: str | None) -> None:
        """
        Require replacement of the free-form agent notes under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = AgentProfile()
            >>> profile.notes = 'Writer'
            >>> profile.notes
            'Writer'


        :param value: New free-form agent notes.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def created_timestamp_ep_k(self) -> int | None:
        """
        Require access to the agent-row creation timestamp.

        Concrete profiles control normalization and ownership; identity and mapping values
        may remain shared according to that implementation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> identity = AgentIdentity(agent_id=7, agent_display_name='Ada')
            >>> profile = AgentProfile(agent=identity, aliases=('A. Lovelace',), notes='Writer', created_timestamp_ep_k=10)
            >>> profile.created_timestamp_ep_k
            10


        :return: Current agent-row creation timestamp, or None where optional.
        """

    @created_timestamp_ep_k.setter
    @abc.abstractmethod
    def created_timestamp_ep_k(self, value: int | None) -> None:
        """
        Require replacement of the agent-row creation timestamp under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = AgentProfile()
            >>> profile.created_timestamp_ep_k = 10
            >>> profile.created_timestamp_ep_k
            10


        :param value: New agent-row creation timestamp.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def modified_timestamp_ep_k(self) -> int | None:
        """
        Require access to the agent-row modification timestamp.

        Concrete profiles control normalization and ownership; identity and mapping values
        may remain shared according to that implementation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> identity = AgentIdentity(agent_id=7, agent_display_name='Ada')
            >>> profile = AgentProfile(agent=identity, aliases=('A. Lovelace',), notes='Writer', modified_timestamp_ep_k=11)
            >>> profile.modified_timestamp_ep_k
            11


        :return: Current agent-row modification timestamp, or None where optional.
        """

    @modified_timestamp_ep_k.setter
    @abc.abstractmethod
    def modified_timestamp_ep_k(self, value: int | None) -> None:
        """
        Require replacement of the agent-row modification timestamp under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = AgentProfile()
            >>> profile.modified_timestamp_ep_k = 11
            >>> profile.modified_timestamp_ep_k
            11


        :param value: New agent-row modification timestamp.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def source_created_datestamp_ep_k(self) -> int | None:
        """
        Require access to the source creation datestamp.

        Concrete profiles control normalization and ownership; identity and mapping values
        may remain shared according to that implementation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> identity = AgentIdentity(agent_id=7, agent_display_name='Ada')
            >>> profile = AgentProfile(agent=identity, aliases=('A. Lovelace',), notes='Writer', source_created_datestamp_ep_k=12)
            >>> profile.source_created_datestamp_ep_k
            12


        :return: Current source creation datestamp, or None where optional.
        """

    @source_created_datestamp_ep_k.setter
    @abc.abstractmethod
    def source_created_datestamp_ep_k(self, value: int | None) -> None:
        """
        Require replacement of the source creation datestamp under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = AgentProfile()
            >>> profile.source_created_datestamp_ep_k = 12
            >>> profile.source_created_datestamp_ep_k
            12


        :param value: New source creation datestamp.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def source_modified_datestamp_ep_k(self) -> int | None:
        """
        Require access to the source modification datestamp.

        Concrete profiles control normalization and ownership; identity and mapping values
        may remain shared according to that implementation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> identity = AgentIdentity(agent_id=7, agent_display_name='Ada')
            >>> profile = AgentProfile(agent=identity, aliases=('A. Lovelace',), notes='Writer', source_modified_datestamp_ep_k=13)
            >>> profile.source_modified_datestamp_ep_k
            13


        :return: Current source modification datestamp, or None where optional.
        """

    @source_modified_datestamp_ep_k.setter
    @abc.abstractmethod
    def source_modified_datestamp_ep_k(self, value: int | None) -> None:
        """
        Require replacement of the source modification datestamp under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = AgentProfile()
            >>> profile.source_modified_datestamp_ep_k = 13
            >>> profile.source_modified_datestamp_ep_k
            13


        :param value: New source modification datestamp.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def scratch(self) -> str | None:
        """
        Require access to the scratch/import text.

        Concrete profiles control normalization and ownership; identity and mapping values
        may remain shared according to that implementation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> identity = AgentIdentity(agent_id=7, agent_display_name='Ada')
            >>> profile = AgentProfile(agent=identity, aliases=('A. Lovelace',), notes='Writer', scratch='raw')
            >>> profile.scratch
            'raw'


        :return: Current scratch/import text, or None where optional.
        """

    @scratch.setter
    @abc.abstractmethod
    def scratch(self, value: str | None) -> None:
        """
        Require replacement of the scratch/import text under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = AgentProfile()
            >>> profile.scratch = 'raw'
            >>> profile.scratch
            'raw'


        :param value: New scratch/import text.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def extra(self) -> MetadataRecord:
        """
        Require access to extension fields not promoted to first-class profile properties.

        Concrete implementations define whether this mapping is live, copied or otherwise
        normalized.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> identity = AgentIdentity(agent_id=7, agent_display_name='Ada')
            >>> profile = AgentProfile(agent=identity, aliases=('A. Lovelace',), notes='Writer')
            >>> dict(profile.extra)
            {}


        :return: Metadata record of extension fields.
        """

    @abc.abstractmethod
    def to_mapping(self) -> MutableMetadataRecord:
        """
        Require serialization of identity and intrinsic profile fields to a mutable metadata record.

        Field inclusion and copy depth belong to the concrete implementation; this contract
        performs no persistence.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> identity = AgentIdentity(agent_id=7, agent_display_name='Ada')
            >>> profile = AgentProfile(agent=identity, aliases=('A. Lovelace',), notes='Writer')
            >>> profile.to_mapping()['agent_id']
            7


        :return: Mutable metadata record for storage or interchange.
        """

    def __str__(self) -> str:
        """
        Return a minimal class-name diagnostic when a concrete profile does not override it.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> identity = AgentIdentity(agent_id=7, agent_display_name='Ada')
            >>> profile = AgentProfile(agent=identity, aliases=('A. Lovelace',), notes='Writer')
            >>> AgentProfileAPI.__str__(profile)
            'AgentProfile()'


        :return: Concrete class name followed by empty parentheses.
        """
        return f"{self.__class__.__name__}()"


class HumanAgentProfileAPI(AgentProfileAPI):
    """
    Require intrinsic person fields stored beside the shared agents row. Sidecar ids and foreign keys are separate from the attached identity; concrete implementations decide normalization and synchronization.

    Example:
        >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
        >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
        >>> profile = HumanAgentProfile(human_agent_id=3)
        >>> profile.human_agent_id
        3
    """

    @property
    @abc.abstractmethod
    def human_agent_id(self) -> int | None:
        """
        Require access to the human sidecar row id.

        The API does not coerce, validate or synchronize this sidecar value.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = HumanAgentProfile(human_agent_id=3)
            >>> profile.human_agent_id
            3


        :return: Stored human sidecar row id, or None.
        """

    @human_agent_id.setter
    @abc.abstractmethod
    def human_agent_id(self, value: int | None) -> None:
        """
        Require replacement of the human sidecar row id under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = HumanAgentProfile()
            >>> profile.human_agent_id = 3
            >>> profile.human_agent_id
            3


        :param value: New human sidecar row id, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def human_agent_agent_id(self) -> AgentID | None:
        """
        Require access to the human sidecar agent foreign key.

        The API does not coerce, validate or synchronize this sidecar value.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = HumanAgentProfile(human_agent_agent_id=7)
            >>> profile.human_agent_agent_id
            7


        :return: Stored human sidecar agent foreign key, or None.
        """

    @human_agent_agent_id.setter
    @abc.abstractmethod
    def human_agent_agent_id(self, value: AgentID | None) -> None:
        """
        Require replacement of the human sidecar agent foreign key under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = HumanAgentProfile()
            >>> profile.human_agent_agent_id = 7
            >>> profile.human_agent_agent_id
            7


        :param value: New human sidecar agent foreign key, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def given_name(self) -> str | None:
        """
        Require access to the given name.

        The API does not coerce, validate or synchronize this sidecar value.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = HumanAgentProfile(given_name='Ada')
            >>> profile.given_name
            'Ada'


        :return: Stored given name, or None.
        """

    @given_name.setter
    @abc.abstractmethod
    def given_name(self, value: str | None) -> None:
        """
        Require replacement of the given name under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = HumanAgentProfile()
            >>> profile.given_name = 'Ada'
            >>> profile.given_name
            'Ada'


        :param value: New given name, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def middle_name(self) -> str | None:
        """
        Require access to the middle name.

        The API does not coerce, validate or synchronize this sidecar value.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = HumanAgentProfile(middle_name='Augusta')
            >>> profile.middle_name
            'Augusta'


        :return: Stored middle name, or None.
        """

    @middle_name.setter
    @abc.abstractmethod
    def middle_name(self, value: str | None) -> None:
        """
        Require replacement of the middle name under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = HumanAgentProfile()
            >>> profile.middle_name = 'Augusta'
            >>> profile.middle_name
            'Augusta'


        :param value: New middle name, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def family_name(self) -> str | None:
        """
        Require access to the family name.

        The API does not coerce, validate or synchronize this sidecar value.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = HumanAgentProfile(family_name='Lovelace')
            >>> profile.family_name
            'Lovelace'


        :return: Stored family name, or None.
        """

    @family_name.setter
    @abc.abstractmethod
    def family_name(self, value: str | None) -> None:
        """
        Require replacement of the family name under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = HumanAgentProfile()
            >>> profile.family_name = 'Lovelace'
            >>> profile.family_name
            'Lovelace'


        :param value: New family name, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def prefix(self) -> str | None:
        """
        Require access to the name prefix.

        The API does not coerce, validate or synchronize this sidecar value.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = HumanAgentProfile(prefix='Countess')
            >>> profile.prefix
            'Countess'


        :return: Stored name prefix, or None.
        """

    @prefix.setter
    @abc.abstractmethod
    def prefix(self, value: str | None) -> None:
        """
        Require replacement of the name prefix under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = HumanAgentProfile()
            >>> profile.prefix = 'Countess'
            >>> profile.prefix
            'Countess'


        :param value: New name prefix, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def suffix(self) -> str | None:
        """
        Require access to the name suffix.

        The API does not coerce, validate or synchronize this sidecar value.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = HumanAgentProfile(suffix='I')
            >>> profile.suffix
            'I'


        :return: Stored name suffix, or None.
        """

    @suffix.setter
    @abc.abstractmethod
    def suffix(self, value: str | None) -> None:
        """
        Require replacement of the name suffix under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = HumanAgentProfile()
            >>> profile.suffix = 'I'
            >>> profile.suffix
            'I'


        :param value: New name suffix, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def preferred_name(self) -> str | None:
        """
        Require access to the preferred personal name.

        The API does not coerce, validate or synchronize this sidecar value.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = HumanAgentProfile(preferred_name='Ada')
            >>> profile.preferred_name
            'Ada'


        :return: Stored preferred personal name, or None.
        """

    @preferred_name.setter
    @abc.abstractmethod
    def preferred_name(self, value: str | None) -> None:
        """
        Require replacement of the preferred personal name under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = HumanAgentProfile()
            >>> profile.preferred_name = 'Ada'
            >>> profile.preferred_name
            'Ada'


        :param value: New preferred personal name, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def birth_date(self) -> str | None:
        """
        Require access to the birth date text.

        The API does not coerce, validate or synchronize this sidecar value.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = HumanAgentProfile(birth_date='1815-12-10')
            >>> profile.birth_date
            '1815-12-10'


        :return: Stored birth date text, or None.
        """

    @birth_date.setter
    @abc.abstractmethod
    def birth_date(self, value: str | None) -> None:
        """
        Require replacement of the birth date text under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = HumanAgentProfile()
            >>> profile.birth_date = '1815-12-10'
            >>> profile.birth_date
            '1815-12-10'


        :param value: New birth date text, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def death_date(self) -> str | None:
        """
        Require access to the death date text.

        The API does not coerce, validate or synchronize this sidecar value.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = HumanAgentProfile(death_date='1852-11-27')
            >>> profile.death_date
            '1852-11-27'


        :return: Stored death date text, or None.
        """

    @death_date.setter
    @abc.abstractmethod
    def death_date(self, value: str | None) -> None:
        """
        Require replacement of the death date text under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = HumanAgentProfile()
            >>> profile.death_date = '1852-11-27'
            >>> profile.death_date
            '1852-11-27'


        :param value: New death date text, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def nationality(self) -> str | None:
        """
        Require access to the nationality text.

        The API does not coerce, validate or synchronize this sidecar value.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = HumanAgentProfile(nationality='British')
            >>> profile.nationality
            'British'


        :return: Stored nationality text, or None.
        """

    @nationality.setter
    @abc.abstractmethod
    def nationality(self, value: str | None) -> None:
        """
        Require replacement of the nationality text under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = HumanAgentProfile()
            >>> profile.nationality = 'British'
            >>> profile.nationality
            'British'


        :param value: New nationality text, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def biography(self) -> str | None:
        """
        Require access to the biography text.

        The API does not coerce, validate or synchronize this sidecar value.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = HumanAgentProfile(biography='Mathematician')
            >>> profile.biography
            'Mathematician'


        :return: Stored biography text, or None.
        """

    @biography.setter
    @abc.abstractmethod
    def biography(self, value: str | None) -> None:
        """
        Require replacement of the biography text under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = HumanAgentProfile()
            >>> profile.biography = 'Mathematician'
            >>> profile.biography
            'Mathematician'


        :param value: New biography text, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def human_agent_created_timestamp_ep_k(self) -> int | None:
        """
        Require access to the human-sidecar creation timestamp.

        The API does not coerce, validate or synchronize this sidecar value.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = HumanAgentProfile(human_agent_created_timestamp_ep_k=20)
            >>> profile.human_agent_created_timestamp_ep_k
            20


        :return: Stored human-sidecar creation timestamp, or None.
        """

    @human_agent_created_timestamp_ep_k.setter
    @abc.abstractmethod
    def human_agent_created_timestamp_ep_k(self, value: int | None) -> None:
        """
        Require replacement of the human-sidecar creation timestamp under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = HumanAgentProfile()
            >>> profile.human_agent_created_timestamp_ep_k = 20
            >>> profile.human_agent_created_timestamp_ep_k
            20


        :param value: New human-sidecar creation timestamp, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def human_agent_modified_timestamp_ep_k(self) -> int | None:
        """
        Require access to the human-sidecar modification timestamp.

        The API does not coerce, validate or synchronize this sidecar value.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = HumanAgentProfile(human_agent_modified_timestamp_ep_k=21)
            >>> profile.human_agent_modified_timestamp_ep_k
            21


        :return: Stored human-sidecar modification timestamp, or None.
        """

    @human_agent_modified_timestamp_ep_k.setter
    @abc.abstractmethod
    def human_agent_modified_timestamp_ep_k(self, value: int | None) -> None:
        """
        Require replacement of the human-sidecar modification timestamp under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = HumanAgentProfile()
            >>> profile.human_agent_modified_timestamp_ep_k = 21
            >>> profile.human_agent_modified_timestamp_ep_k
            21


        :param value: New human-sidecar modification timestamp, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def human_agent_scratch(self) -> str | None:
        """
        Require access to the human-sidecar scratch text.

        The API does not coerce, validate or synchronize this sidecar value.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = HumanAgentProfile(human_agent_scratch='raw')
            >>> profile.human_agent_scratch
            'raw'


        :return: Stored human-sidecar scratch text, or None.
        """

    @human_agent_scratch.setter
    @abc.abstractmethod
    def human_agent_scratch(self, value: str | None) -> None:
        """
        Require replacement of the human-sidecar scratch text under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = HumanAgentProfile()
            >>> profile.human_agent_scratch = 'raw'
            >>> profile.human_agent_scratch
            'raw'


        :param value: New human-sidecar scratch text, or None.
        :return: None.
        """


class OrganisationAgentProfileAPI(AgentProfileAPI):
    """
    Require intrinsic organisation fields stored beside the shared agents row. Sidecar ids and foreign keys are separate from the attached identity; concrete implementations decide normalization and synchronization.

    Example:
        >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
        >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
        >>> profile = OrganisationAgentProfile(org_agent_id=3)
        >>> profile.org_agent_id
        3
    """

    @property
    @abc.abstractmethod
    def org_agent_id(self) -> int | None:
        """
        Require access to the organisation sidecar row id.

        The API does not coerce, validate or synchronize this sidecar value.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = OrganisationAgentProfile(org_agent_id=3)
            >>> profile.org_agent_id
            3


        :return: Stored organisation sidecar row id, or None.
        """

    @org_agent_id.setter
    @abc.abstractmethod
    def org_agent_id(self, value: int | None) -> None:
        """
        Require replacement of the organisation sidecar row id under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = OrganisationAgentProfile()
            >>> profile.org_agent_id = 3
            >>> profile.org_agent_id
            3


        :param value: New organisation sidecar row id, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def org_agent_agent_id(self) -> AgentID | None:
        """
        Require access to the organisation sidecar agent foreign key.

        The API does not coerce, validate or synchronize this sidecar value.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = OrganisationAgentProfile(org_agent_agent_id=7)
            >>> profile.org_agent_agent_id
            7


        :return: Stored organisation sidecar agent foreign key, or None.
        """

    @org_agent_agent_id.setter
    @abc.abstractmethod
    def org_agent_agent_id(self, value: AgentID | None) -> None:
        """
        Require replacement of the organisation sidecar agent foreign key under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = OrganisationAgentProfile()
            >>> profile.org_agent_agent_id = 7
            >>> profile.org_agent_agent_id
            7


        :param value: New organisation sidecar agent foreign key, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def legal_name(self) -> str | None:
        """
        Require access to the legal name.

        The API does not coerce, validate or synchronize this sidecar value.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = OrganisationAgentProfile(legal_name='Example Books Ltd')
            >>> profile.legal_name
            'Example Books Ltd'


        :return: Stored legal name, or None.
        """

    @legal_name.setter
    @abc.abstractmethod
    def legal_name(self, value: str | None) -> None:
        """
        Require replacement of the legal name under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = OrganisationAgentProfile()
            >>> profile.legal_name = 'Example Books Ltd'
            >>> profile.legal_name
            'Example Books Ltd'


        :param value: New legal name, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def trading_name(self) -> str | None:
        """
        Require access to the trading name.

        The API does not coerce, validate or synchronize this sidecar value.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = OrganisationAgentProfile(trading_name='Example Books')
            >>> profile.trading_name
            'Example Books'


        :return: Stored trading name, or None.
        """

    @trading_name.setter
    @abc.abstractmethod
    def trading_name(self, value: str | None) -> None:
        """
        Require replacement of the trading name under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = OrganisationAgentProfile()
            >>> profile.trading_name = 'Example Books'
            >>> profile.trading_name
            'Example Books'


        :param value: New trading name, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def registration_id(self) -> str | None:
        """
        Require access to the registration identifier.

        The API does not coerce, validate or synchronize this sidecar value.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = OrganisationAgentProfile(registration_id='ACME-7')
            >>> profile.registration_id
            'ACME-7'


        :return: Stored registration identifier, or None.
        """

    @registration_id.setter
    @abc.abstractmethod
    def registration_id(self, value: str | None) -> None:
        """
        Require replacement of the registration identifier under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = OrganisationAgentProfile()
            >>> profile.registration_id = 'ACME-7'
            >>> profile.registration_id
            'ACME-7'


        :param value: New registration identifier, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def jurisdiction(self) -> str | None:
        """
        Require access to the jurisdiction.

        The API does not coerce, validate or synchronize this sidecar value.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = OrganisationAgentProfile(jurisdiction='GB')
            >>> profile.jurisdiction
            'GB'


        :return: Stored jurisdiction, or None.
        """

    @jurisdiction.setter
    @abc.abstractmethod
    def jurisdiction(self, value: str | None) -> None:
        """
        Require replacement of the jurisdiction under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = OrganisationAgentProfile()
            >>> profile.jurisdiction = 'GB'
            >>> profile.jurisdiction
            'GB'


        :param value: New jurisdiction, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def founded_date(self) -> str | None:
        """
        Require access to the founded-date text.

        The API does not coerce, validate or synchronize this sidecar value.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = OrganisationAgentProfile(founded_date='2001-01-01')
            >>> profile.founded_date
            '2001-01-01'


        :return: Stored founded-date text, or None.
        """

    @founded_date.setter
    @abc.abstractmethod
    def founded_date(self, value: str | None) -> None:
        """
        Require replacement of the founded-date text under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = OrganisationAgentProfile()
            >>> profile.founded_date = '2001-01-01'
            >>> profile.founded_date
            '2001-01-01'


        :param value: New founded-date text, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def dissolved_date(self) -> str | None:
        """
        Require access to the dissolved-date text.

        The API does not coerce, validate or synchronize this sidecar value.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = OrganisationAgentProfile(dissolved_date='2020-01-01')
            >>> profile.dissolved_date
            '2020-01-01'


        :return: Stored dissolved-date text, or None.
        """

    @dissolved_date.setter
    @abc.abstractmethod
    def dissolved_date(self, value: str | None) -> None:
        """
        Require replacement of the dissolved-date text under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = OrganisationAgentProfile()
            >>> profile.dissolved_date = '2020-01-01'
            >>> profile.dissolved_date
            '2020-01-01'


        :param value: New dissolved-date text, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def website(self) -> str | None:
        """
        Require access to the website.

        The API does not coerce, validate or synchronize this sidecar value.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = OrganisationAgentProfile(website='https://example.invalid')
            >>> profile.website
            'https://example.invalid'


        :return: Stored website, or None.
        """

    @website.setter
    @abc.abstractmethod
    def website(self, value: str | None) -> None:
        """
        Require replacement of the website under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = OrganisationAgentProfile()
            >>> profile.website = 'https://example.invalid'
            >>> profile.website
            'https://example.invalid'


        :param value: New website, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def contact_email(self) -> str | None:
        """
        Require access to the contact email.

        The API does not coerce, validate or synchronize this sidecar value.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = OrganisationAgentProfile(contact_email='books@example.invalid')
            >>> profile.contact_email
            'books@example.invalid'


        :return: Stored contact email, or None.
        """

    @contact_email.setter
    @abc.abstractmethod
    def contact_email(self, value: str | None) -> None:
        """
        Require replacement of the contact email under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = OrganisationAgentProfile()
            >>> profile.contact_email = 'books@example.invalid'
            >>> profile.contact_email
            'books@example.invalid'


        :param value: New contact email, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def description(self) -> str | None:
        """
        Require access to the organisation description.

        The API does not coerce, validate or synchronize this sidecar value.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = OrganisationAgentProfile(description='Publisher')
            >>> profile.description
            'Publisher'


        :return: Stored organisation description, or None.
        """

    @description.setter
    @abc.abstractmethod
    def description(self, value: str | None) -> None:
        """
        Require replacement of the organisation description under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = OrganisationAgentProfile()
            >>> profile.description = 'Publisher'
            >>> profile.description
            'Publisher'


        :param value: New organisation description, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def org_agent_created_timestamp_ep_k(self) -> int | None:
        """
        Require access to the organisation-sidecar creation timestamp.

        The API does not coerce, validate or synchronize this sidecar value.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = OrganisationAgentProfile(org_agent_created_timestamp_ep_k=20)
            >>> profile.org_agent_created_timestamp_ep_k
            20


        :return: Stored organisation-sidecar creation timestamp, or None.
        """

    @org_agent_created_timestamp_ep_k.setter
    @abc.abstractmethod
    def org_agent_created_timestamp_ep_k(self, value: int | None) -> None:
        """
        Require replacement of the organisation-sidecar creation timestamp under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = OrganisationAgentProfile()
            >>> profile.org_agent_created_timestamp_ep_k = 20
            >>> profile.org_agent_created_timestamp_ep_k
            20


        :param value: New organisation-sidecar creation timestamp, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def org_agent_modified_timestamp_ep_k(self) -> int | None:
        """
        Require access to the organisation-sidecar modification timestamp.

        The API does not coerce, validate or synchronize this sidecar value.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = OrganisationAgentProfile(org_agent_modified_timestamp_ep_k=21)
            >>> profile.org_agent_modified_timestamp_ep_k
            21


        :return: Stored organisation-sidecar modification timestamp, or None.
        """

    @org_agent_modified_timestamp_ep_k.setter
    @abc.abstractmethod
    def org_agent_modified_timestamp_ep_k(self, value: int | None) -> None:
        """
        Require replacement of the organisation-sidecar modification timestamp under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = OrganisationAgentProfile()
            >>> profile.org_agent_modified_timestamp_ep_k = 21
            >>> profile.org_agent_modified_timestamp_ep_k
            21


        :param value: New organisation-sidecar modification timestamp, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def org_agent_scratch(self) -> str | None:
        """
        Require access to the organisation-sidecar scratch text.

        The API does not coerce, validate or synchronize this sidecar value.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = OrganisationAgentProfile(org_agent_scratch='raw')
            >>> profile.org_agent_scratch
            'raw'


        :return: Stored organisation-sidecar scratch text, or None.
        """

    @org_agent_scratch.setter
    @abc.abstractmethod
    def org_agent_scratch(self, value: str | None) -> None:
        """
        Require replacement of the organisation-sidecar scratch text under concrete profile policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_profile import AgentProfile, HumanAgentProfile, OrganisationAgentProfile
            >>> profile = OrganisationAgentProfile()
            >>> profile.org_agent_scratch = 'raw'
            >>> profile.org_agent_scratch
            'raw'


        :param value: New organisation-sidecar scratch text, or None.
        :return: None.
        """


__all__ = [
    "AgentProfileAPI",
    "HumanAgentProfileAPI",
    "OrganisationAgentProfileAPI",
]
