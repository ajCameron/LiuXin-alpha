"""
Implement intrinsic agent profiles using shared agent columns and optional human or organisation sidecars.

Profiles hold metadata about the agent itself. They are separate from WEMI metadata
bundles and graph-wide participation views. Mapping factories normalize aliases and
sidecar scalar fields without performing database lookups.

Example:
    >>> profile = AgentProfile.from_mapping({'agent_type': 'person', 'human_agent_given_name': 'Ada'})
    >>> isinstance(profile, HumanAgentProfile), profile.given_name
    (True, 'Ada')
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
    """
    Collect string forms of mapping keys, using keys() when available.

    Example:
        >>> _row_keys({1: 'one', 'agent_id': 7}) == {'1', 'agent_id'}
        True


    :param row: Mapping whose key names are inspected.
    :return: Set of stringified keys.
    """
    keys_method = getattr(row, "keys", None)
    if callable(keys_method):
        return {str(key) for key in keys_method()}
    return {str(key) for key in row}


def _has_any(row: Mapping[str, Any], keys: set[str]) -> bool:
    """
    Check whether the mapping contains any of the requested key names.

    Only key presence matters; values may be None or empty.

    Example:
        >>> _has_any({'org_agent_id': None}, {'org_agent_id'})
        True


    :param row: Mapping whose keys are inspected.
    :param keys: Set of candidate string key names.
    :return: True when the stringified row keys intersect the requested set.
    """
    return bool(_row_keys(row) & keys)


def _first_present(row: Mapping[str, Any], *keys: str) -> Any:
    """
    Read the first candidate key whose value is neither None nor an empty string.

    False and zero are retained.

    Example:
        >>> _first_present({'a': '', 'b': 0}, 'a', 'b')
        0


    :param row: Mapping to inspect in candidate order.
    :param keys: Candidate key names in descending precedence.
    :return: First eligible value, or None if every candidate is absent or empty.
    """
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return None


def _int_or_none(value: Any) -> int | None:
    """
    Convert a scalar to int, treating absent, empty or unconvertible values as None.

    TypeError, ValueError and OverflowError from int conversion are suppressed.

    Example:
        >>> _int_or_none('12'), _int_or_none('bad'), _int_or_none(float('inf'))
        (12, None, None)


    :param value: Value to convert with int; None and the empty string are treated as
        missing.
    :return: Converted integer, or None.
    """
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _text_or_none(value: Any) -> str | None:
    """
    Stringify a non-None value and replace the empty result with None.

    Whitespace is preserved.

    Example:
        >>> _text_or_none(0), _text_or_none(' '), _text_or_none('')
        ('0', ' ', None)


    :param value: Scalar to stringify without stripping.
    :return: String value, or None for a missing or empty value.
    """
    if value is None:
        return None
    text = str(value)
    return text if text != "" else None


def _aliases_from_value(value: Any) -> tuple[str, ...]:
    """
    Normalize alias input into an ordered, case-insensitively unique tuple.

    Strings split on AGENT_ALIAS_SEPARATOR; other iterables are consumed, and scalar
    values become a single candidate. None entries and blank stripped text are removed.
    The spelling of the first alias wins.

    Example:
        >>> _aliases_from_value('Ada(#BREAK#) ada (#BREAK#)Lovelace')
        ('Ada', 'Lovelace')


    :param value: Stored separator-delimited text, iterable of aliases, scalar, or None.
    :return: Tuple of stripped nonempty aliases.
    """
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
    """
    Normalize an alias iterable and join it using the storage separator.

    Example:
        >>> _aliases_to_storage(('Ada', 'ada', 'Lovelace'))
        'Ada(#BREAK#)Lovelace'
        >>> _aliases_to_storage(()) is None
        True


    :param aliases: Iterable of individual alias strings.
    :return: Separator-delimited text, or None when no aliases remain.
    """
    values = _aliases_from_value(tuple(aliases))
    return AGENT_ALIAS_SEPARATOR.join(values) if values else None


def _identity_from_mapping(row: Mapping[str, Any]) -> AgentIdentityAPI | None:
    """
    Build an identity from a nested agent mapping or recognized top-level agent columns.

    A nested mapping takes precedence even when empty. Without either source this helper
    returns None.

    Example:
        >>> _identity_from_mapping({'agent': {}, 'agent_id': 7}).agent_id is None
        True
        >>> _identity_from_mapping({'given_name': 'Ada'}) is None
        True


    :param row: Profile mapping with optional nested agent mapping and agent-prefixed
        columns.
    :return: New AgentIdentity, or None when no identity source is recognized.
    """
    agent_mapping = row.get("agent")
    if isinstance(agent_mapping, Mapping):
        return AgentIdentity.from_mapping(agent_mapping)
    if _has_any(row, _AGENT_BASE_KEYS):
        return AgentIdentity.from_mapping(row)
    return None


def _base_kwargs_from_mapping(row: Mapping[str, Any]) -> dict[str, Any]:
    """
    Extract shared profile constructor arguments from a row mapping.

    Aliases and notes use truthy prefixed-to-short-name fallbacks. Timestamps use
    forgiving integer conversion, text uses string conversion, and extra is accepted
    only when it is a mapping. Unknown top-level keys are ignored.

    Example:
        >>> values = _base_kwargs_from_mapping({'agent_created_timestamp_ep_k': '10', 'agent_aliases': 'Ada'})
        >>> values['created_timestamp_ep_k'], values['aliases']
        (10, ('Ada',))


    :param row: Mapping of shared agent columns, optional aliases/notes fallbacks and
        extension data.
    :return: Dictionary of shared constructor arguments; nested extra values are not
        copied here.
    """
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
    """
    Normalize the top-level agent type for factory dispatch.

    The nested agent mapping is not consulted.

    Example:
        >>> _normalised_agent_type({'agent_type': ' PERSON '})
        'person'


    :param row: Mapping containing the optional agent_type discriminator.
    :return: Stripped casefolded type text, or an empty string for a missing or falsey
        value.
    """
    return str(_first_present(row, "agent_type") or "").strip().casefold()


class AgentProfile(AgentProfileAPI):
    """
    Store shared agent profile metadata and an optional linked identity.

    The base from_mapping factory chooses an organisation or human subtype from
    top-level discriminator values or sidecar keys. Identity references are shared;
    aliases are normalized and extra is shallow-copied.

    Example:
        >>> profile = AgentProfile(aliases=('Ada', ' ada '), notes='Writer')
        >>> profile.aliases
        ('Ada',)
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
        """
        Initialize shared profile fields, normalize aliases and shallow-copy extension data.

        Identity and other scalar fields are retained as supplied. Pass an iterable of alias
        strings; a bare string is iterated as characters.

        Example:
            >>> profile = AgentProfile(aliases=('Ada', ' ada '), notes='Writer')
            >>> profile.notes, profile.aliases
            ('Writer', ('Ada',))


        :param agent: Optional shared agent identity object; retained by reference.
        :param aliases: Iterable of alias strings; copied, stripped and deduplicated
            case-insensitively in first-seen order.
        :param notes: Optional free-text agent note, retained as supplied.
        :param created_timestamp_ep_k: Optional agent-row creation timestamp in the stored
            ep_k representation.
        :param modified_timestamp_ep_k: Optional agent-row modification timestamp in the
            stored ep_k representation.
        :param source_created_datestamp_ep_k: Optional source creation datestamp in the
            stored ep_k representation.
        :param source_modified_datestamp_ep_k: Optional source modification datestamp in the
            stored ep_k representation.
        :param scratch: Optional agent-row scratch text, retained as supplied.
        :param extra: Optional extension mapping, copied into a new dictionary with shared
            nested values.
        :return: None.
        """
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
        """
        Build a profile, dispatching from the base class to organisation or human sidecars.

        Organisation sidecar keys or recognized organisation types take precedence, followed
        by human keys or types. Key presence counts even for empty values. Other subclasses
        are constructed from shared kwargs without this dispatch. Unknown top-level columns
        are not preserved in extra.

        Example:
            >>> profile = AgentProfile.from_mapping({'agent_type': 'person', 'org_agent_id': None})
            >>> isinstance(profile, OrganisationAgentProfile)
            True


        :param row: Mapping containing agent columns, optional sidecar columns and explicit
            extra data.
        :return: New profile instance chosen from the requested class and row contents.
        """
        if cls is AgentProfile:
            agent_type = _normalised_agent_type(row)
            if _has_any(row, _ORG_SIDE_CAR_KEYS) or agent_type in _ORG_AGENT_TYPES:
                return OrganisationAgentProfile.from_mapping(row)
            if _has_any(row, _HUMAN_SIDE_CAR_KEYS) or agent_type in _HUMAN_AGENT_TYPES:
                return HumanAgentProfile.from_mapping(row)
        return cls(**_base_kwargs_from_mapping(row))

    @property
    def agent(self) -> AgentIdentityAPI | None:
        """
        Return the linked identity object by reference.

        Example:
            >>> identity = AgentIdentity(agent_id=7)
            >>> AgentProfile(agent=identity).agent is identity
            True


        :return: Shared AgentIdentityAPI instance, or None.
        """
        return self._agent

    @agent.setter
    def agent(self, value: AgentIdentityAPI | None) -> None:
        """
        Replace the linked identity without copying it or synchronizing sidecar foreign keys.

        Example:
            >>> profile = AgentProfile()
            >>> profile.agent = AgentIdentity(agent_id=7)
            >>> profile.agent_id
            7


        :param value: New shared identity object, or None to unlink it.
        :return: None.
        """
        self._agent = value

    @property
    def aliases(self) -> tuple[str, ...]:
        """
        Return normalized aliases in first-seen order.

        Example:
            >>> profile = AgentProfile(aliases=('Ada', ' ada '), notes='Writer')
            >>> profile.aliases
            ('Ada',)


        :return: Tuple of unique aliases, compared case-insensitively during normalization.
        """
        return self._aliases

    @aliases.setter
    def aliases(self, value: tuple[str, ...] | list[str]) -> None:
        """
        Replace aliases after stripping blanks and removing case-insensitive duplicates.

        Example:
            >>> profile = AgentProfile()
            >>> profile.aliases = [' Ada ', 'ada', 'Lovelace']
            >>> profile.aliases
            ('Ada', 'Lovelace')


        :param value: List or tuple of individual alias strings.
        :return: None.
        """
        self._aliases = _aliases_from_value(value)

    @property
    def notes(self) -> str | None:
        """
        Return the stored agent note.

        Example:
            >>> profile = AgentProfile(notes='No sidecar.')
            >>> profile.notes
            'No sidecar.'


        :return: Agent note, or None when unset.
        """
        return self._notes

    @notes.setter
    def notes(self, value: str | None) -> None:
        """
        Replace the agent note without coercion or validation.

        Example:
            >>> profile = AgentProfile()
            >>> profile.notes = 'No sidecar.'
            >>> profile.notes
            'No sidecar.'


        :param value: New agent note, or None to clear it.
        :return: None.
        """
        self._notes = value

    @property
    def created_timestamp_ep_k(self) -> int | None:
        """
        Return the stored agent-row creation timestamp in ep_k form.

        Example:
            >>> profile = AgentProfile(created_timestamp_ep_k=100)
            >>> profile.created_timestamp_ep_k
            100


        :return: Agent-row creation timestamp in ep_k form, or None when unset.
        """
        return self._created_timestamp_ep_k

    @created_timestamp_ep_k.setter
    def created_timestamp_ep_k(self, value: int | None) -> None:
        """
        Replace the agent-row creation timestamp in ep_k form without coercion or validation.

        Example:
            >>> profile = AgentProfile()
            >>> profile.created_timestamp_ep_k = 100
            >>> profile.created_timestamp_ep_k
            100


        :param value: New agent-row creation timestamp in ep_k form, or None to clear it.
        :return: None.
        """
        self._created_timestamp_ep_k = value

    @property
    def modified_timestamp_ep_k(self) -> int | None:
        """
        Return the stored agent-row modification timestamp in ep_k form.

        Example:
            >>> profile = AgentProfile(modified_timestamp_ep_k=110)
            >>> profile.modified_timestamp_ep_k
            110


        :return: Agent-row modification timestamp in ep_k form, or None when unset.
        """
        return self._modified_timestamp_ep_k

    @modified_timestamp_ep_k.setter
    def modified_timestamp_ep_k(self, value: int | None) -> None:
        """
        Replace the agent-row modification timestamp in ep_k form without coercion or validation.

        Example:
            >>> profile = AgentProfile()
            >>> profile.modified_timestamp_ep_k = 110
            >>> profile.modified_timestamp_ep_k
            110


        :param value: New agent-row modification timestamp in ep_k form, or None to clear
            it.
        :return: None.
        """
        self._modified_timestamp_ep_k = value

    @property
    def source_created_datestamp_ep_k(self) -> int | None:
        """
        Return the stored source creation datestamp in ep_k form.

        Example:
            >>> profile = AgentProfile(source_created_datestamp_ep_k=80)
            >>> profile.source_created_datestamp_ep_k
            80


        :return: Source creation datestamp in ep_k form, or None when unset.
        """
        return self._source_created_datestamp_ep_k

    @source_created_datestamp_ep_k.setter
    def source_created_datestamp_ep_k(self, value: int | None) -> None:
        """
        Replace the source creation datestamp in ep_k form without coercion or validation.

        Example:
            >>> profile = AgentProfile()
            >>> profile.source_created_datestamp_ep_k = 80
            >>> profile.source_created_datestamp_ep_k
            80


        :param value: New source creation datestamp in ep_k form, or None to clear it.
        :return: None.
        """
        self._source_created_datestamp_ep_k = value

    @property
    def source_modified_datestamp_ep_k(self) -> int | None:
        """
        Return the stored source modification datestamp in ep_k form.

        Example:
            >>> profile = AgentProfile(source_modified_datestamp_ep_k=90)
            >>> profile.source_modified_datestamp_ep_k
            90


        :return: Source modification datestamp in ep_k form, or None when unset.
        """
        return self._source_modified_datestamp_ep_k

    @source_modified_datestamp_ep_k.setter
    def source_modified_datestamp_ep_k(self, value: int | None) -> None:
        """
        Replace the source modification datestamp in ep_k form without coercion or validation.

        Example:
            >>> profile = AgentProfile()
            >>> profile.source_modified_datestamp_ep_k = 90
            >>> profile.source_modified_datestamp_ep_k
            90


        :param value: New source modification datestamp in ep_k form, or None to clear it.
        :return: None.
        """
        self._source_modified_datestamp_ep_k = value

    @property
    def scratch(self) -> str | None:
        """
        Return the stored agent-row scratch text.

        Example:
            >>> profile = AgentProfile(scratch='import-state')
            >>> profile.scratch
            'import-state'


        :return: Agent-row scratch text, or None when unset.
        """
        return self._scratch

    @scratch.setter
    def scratch(self, value: str | None) -> None:
        """
        Replace the agent-row scratch text without coercion or validation.

        Example:
            >>> profile = AgentProfile()
            >>> profile.scratch = 'import-state'
            >>> profile.scratch
            'import-state'


        :param value: New agent-row scratch text, or None to clear it.
        :return: None.
        """
        self._scratch = value

    @property
    def extra(self) -> Mapping[str, Any]:
        """
        Expose the stored extension mapping without copying it.

        The returned object is the live dictionary, despite the read-only Mapping
        annotation.

        Example:
            >>> profile = AgentProfile(extra={'source': 'import'})
            >>> profile.extra is profile.extra
            True


        :return: Live extension mapping.
        """
        return self._extra

    def add_alias(self, value: str) -> None:
        """
        Append an alias through the same normalization and deduplication used by the setter.

        Example:
            >>> profile = AgentProfile(aliases=('Ada', ' ada '), notes='Writer')
            >>> profile.add_alias('Lovelace')
            >>> profile.add_alias('ADA')
            >>> profile.aliases
            ('Ada', 'Lovelace')


        :param value: Alias text to strip and add if nonblank and not already present
            ignoring case.
        :return: None.
        """
        self._aliases = _aliases_from_value((*self._aliases, value))

    def _base_mapping(self) -> dict[str, Any]:
        """
        Serialize shared agent columns using canonical-name and separator-delimited alias storage.

        Identity fields become None when unlinked. Extension data is shallow-copied; no
        sidecar fields are added.

        Example:
            >>> profile = AgentProfile(aliases=('Ada', ' ada '), notes='Writer')
            >>> profile._base_mapping()['agent_aliases']
            'Ada'


        :return: New mapping of shared fields, including None values and an extra
            dictionary.
        """
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
        """
        Serialize the shared profile fields without persisting them.

        Example:
            >>> profile = AgentProfile(aliases=('Ada', ' ada '), notes='Writer')
            >>> profile.to_mapping()['agent_note']
            'Writer'


        :return: New mapping of agent columns and a shallow copy of extra.
        """
        return self._base_mapping()

    def __str__(self) -> str:
        """
        Render a compact diagnostic summary from the profile mapping.

        Example:
            >>> profile = AgentProfile(agent=AgentIdentity(agent_id=7, agent_display_name='Ada'))
            >>> 'Ada' in str(profile)
            True


        :return: Human-readable summary emphasizing the agent id, name, type and sort name.
        """
        return compact_mapping_string(
            self,
            self.to_mapping(),
            id_keys=("agent_id",),
            display_keys=("agent_canonical_name", "agent_type", "agent_sort_name"),
        )


class HumanAgentProfile(AgentProfile, HumanAgentProfileAPI):
    """
    Combine shared agent metadata with optional human sidecar fields.

    Sidecar row ids and agent foreign keys remain independent of the linked identity.
    Direct construction and property assignment retain sidecar values as supplied;
    from_mapping normalizes scalar inputs.

    Example:
        >>> profile = HumanAgentProfile(given_name='Ada')
        >>> profile.given_name
        'Ada'
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
        """
        Initialize shared profile metadata and store the human sidecar fields.

        Shared aliases are normalized and extra is shallow-copied. Sidecar ids, foreign
        keys, dates and other fields are not validated or synchronized with the identity.

        Example:
            >>> profile = HumanAgentProfile(given_name='Ada')
            >>> profile.human_agent_agent_id is None
            True


        :param agent: Optional shared agent identity object; retained by reference.
        :param aliases: Iterable of alias strings; copied, stripped and deduplicated
            case-insensitively in first-seen order.
        :param notes: Optional free-text agent note, retained as supplied.
        :param created_timestamp_ep_k: Optional agent-row creation timestamp in the stored
            ep_k representation.
        :param modified_timestamp_ep_k: Optional agent-row modification timestamp in the
            stored ep_k representation.
        :param source_created_datestamp_ep_k: Optional source creation datestamp in the
            stored ep_k representation.
        :param source_modified_datestamp_ep_k: Optional source modification datestamp in the
            stored ep_k representation.
        :param scratch: Optional agent-row scratch text, retained as supplied.
        :param extra: Optional extension mapping, copied into a new dictionary with shared
            nested values.
        :param human_agent_id: Optional human sidecar row id; retained as supplied.
        :param human_agent_agent_id: Optional agent foreign key stored on the human sidecar;
            retained as supplied.
        :param given_name: Optional given name; retained as supplied.
        :param middle_name: Optional middle name; retained as supplied.
        :param family_name: Optional family name; retained as supplied.
        :param prefix: Optional name prefix; retained as supplied.
        :param suffix: Optional name suffix; retained as supplied.
        :param preferred_name: Optional preferred name; retained as supplied.
        :param birth_date: Optional birth date text; retained as supplied.
        :param death_date: Optional death date text; retained as supplied.
        :param nationality: Optional nationality text; retained as supplied.
        :param biography: Optional biography text; retained as supplied.
        :param human_agent_created_timestamp_ep_k: Optional human sidecar creation timestamp
            in ep_k form; retained as supplied.
        :param human_agent_modified_timestamp_ep_k: Optional human sidecar modification
            timestamp in ep_k form; retained as supplied.
        :param human_agent_scratch: Optional human sidecar scratch text; retained as
            supplied.
        :return: None.
        """
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
        """
        Build a human profile from shared agent columns and prefixed sidecar columns.

        Ids and timestamps use forgiving integer conversion. Text fields are stringified
        with empty strings becoming None; whitespace remains unchanged. No date parsing,
        foreign-key reconciliation or database lookup occurs.

        Example:
            >>> profile = HumanAgentProfile.from_mapping({'human_agent_id': '3', 'human_agent_given_name': 'Ada'})
            >>> profile.human_agent_id, profile.given_name
            (3, 'Ada')


        :param row: Mapping of shared agent fields and human_agent_prefixed sidecar columns.
        :return: New HumanAgentProfile instance, or the requested subclass.
        """
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
        """
        Return the stored human sidecar row id.

        Example:
            >>> profile = HumanAgentProfile(human_agent_id=3)
            >>> profile.human_agent_id
            3


        :return: Human sidecar row id, or None when unset.
        """
        return self._human_agent_id

    @human_agent_id.setter
    def human_agent_id(self, value: int | None) -> None:
        """
        Replace the human sidecar row id without coercion or validation.

        Example:
            >>> profile = HumanAgentProfile()
            >>> profile.human_agent_id = 3
            >>> profile.human_agent_id
            3


        :param value: New human sidecar row id, or None to clear it.
        :return: None.
        """
        self._human_agent_id = value

    @property
    def human_agent_agent_id(self) -> AgentID | None:
        """
        Return the stored agent foreign key stored on the human sidecar.

        Example:
            >>> profile = HumanAgentProfile(human_agent_agent_id=7)
            >>> profile.human_agent_agent_id
            7


        :return: Agent foreign key stored on the human sidecar, or None when unset.
        """
        return self._human_agent_agent_id

    @human_agent_agent_id.setter
    def human_agent_agent_id(self, value: AgentID | None) -> None:
        """
        Replace the agent foreign key stored on the human sidecar without coercion or validation.

        Example:
            >>> profile = HumanAgentProfile()
            >>> profile.human_agent_agent_id = 7
            >>> profile.human_agent_agent_id
            7


        :param value: New agent foreign key stored on the human sidecar, or None to clear
            it.
        :return: None.
        """
        self._human_agent_agent_id = value

    @property
    def given_name(self) -> str | None:
        """
        Return the stored given name.

        Example:
            >>> profile = HumanAgentProfile(given_name='Ada')
            >>> profile.given_name
            'Ada'


        :return: Given name, or None when unset.
        """
        return self._given_name

    @given_name.setter
    def given_name(self, value: str | None) -> None:
        """
        Replace the given name without coercion or validation.

        Example:
            >>> profile = HumanAgentProfile()
            >>> profile.given_name = 'Ada'
            >>> profile.given_name
            'Ada'


        :param value: New given name, or None to clear it.
        :return: None.
        """
        self._given_name = value

    @property
    def middle_name(self) -> str | None:
        """
        Return the stored middle name.

        Example:
            >>> profile = HumanAgentProfile(middle_name='Augusta')
            >>> profile.middle_name
            'Augusta'


        :return: Middle name, or None when unset.
        """
        return self._middle_name

    @middle_name.setter
    def middle_name(self, value: str | None) -> None:
        """
        Replace the middle name without coercion or validation.

        Example:
            >>> profile = HumanAgentProfile()
            >>> profile.middle_name = 'Augusta'
            >>> profile.middle_name
            'Augusta'


        :param value: New middle name, or None to clear it.
        :return: None.
        """
        self._middle_name = value

    @property
    def family_name(self) -> str | None:
        """
        Return the stored family name.

        Example:
            >>> profile = HumanAgentProfile(family_name='Lovelace')
            >>> profile.family_name
            'Lovelace'


        :return: Family name, or None when unset.
        """
        return self._family_name

    @family_name.setter
    def family_name(self, value: str | None) -> None:
        """
        Replace the family name without coercion or validation.

        Example:
            >>> profile = HumanAgentProfile()
            >>> profile.family_name = 'Lovelace'
            >>> profile.family_name
            'Lovelace'


        :param value: New family name, or None to clear it.
        :return: None.
        """
        self._family_name = value

    @property
    def prefix(self) -> str | None:
        """
        Return the stored name prefix.

        Example:
            >>> profile = HumanAgentProfile(prefix='Lady')
            >>> profile.prefix
            'Lady'


        :return: Name prefix, or None when unset.
        """
        return self._prefix

    @prefix.setter
    def prefix(self, value: str | None) -> None:
        """
        Replace the name prefix without coercion or validation.

        Example:
            >>> profile = HumanAgentProfile()
            >>> profile.prefix = 'Lady'
            >>> profile.prefix
            'Lady'


        :param value: New name prefix, or None to clear it.
        :return: None.
        """
        self._prefix = value

    @property
    def suffix(self) -> str | None:
        """
        Return the stored name suffix.

        Example:
            >>> profile = HumanAgentProfile(suffix='Example')
            >>> profile.suffix
            'Example'


        :return: Name suffix, or None when unset.
        """
        return self._suffix

    @suffix.setter
    def suffix(self, value: str | None) -> None:
        """
        Replace the name suffix without coercion or validation.

        Example:
            >>> profile = HumanAgentProfile()
            >>> profile.suffix = 'Example'
            >>> profile.suffix
            'Example'


        :param value: New name suffix, or None to clear it.
        :return: None.
        """
        self._suffix = value

    @property
    def preferred_name(self) -> str | None:
        """
        Return the stored preferred name.

        Example:
            >>> profile = HumanAgentProfile(preferred_name='Ada')
            >>> profile.preferred_name
            'Ada'


        :return: Preferred name, or None when unset.
        """
        return self._preferred_name

    @preferred_name.setter
    def preferred_name(self, value: str | None) -> None:
        """
        Replace the preferred name without coercion or validation.

        Example:
            >>> profile = HumanAgentProfile()
            >>> profile.preferred_name = 'Ada'
            >>> profile.preferred_name
            'Ada'


        :param value: New preferred name, or None to clear it.
        :return: None.
        """
        self._preferred_name = value

    @property
    def birth_date(self) -> str | None:
        """
        Return the stored birth date text.

        Example:
            >>> profile = HumanAgentProfile(birth_date='1815-12-10')
            >>> profile.birth_date
            '1815-12-10'


        :return: Birth date text, or None when unset.
        """
        return self._birth_date

    @birth_date.setter
    def birth_date(self, value: str | None) -> None:
        """
        Replace the birth date text without coercion or validation.

        Example:
            >>> profile = HumanAgentProfile()
            >>> profile.birth_date = '1815-12-10'
            >>> profile.birth_date
            '1815-12-10'


        :param value: New birth date text, or None to clear it.
        :return: None.
        """
        self._birth_date = value

    @property
    def death_date(self) -> str | None:
        """
        Return the stored death date text.

        Example:
            >>> profile = HumanAgentProfile(death_date='1852-11-27')
            >>> profile.death_date
            '1852-11-27'


        :return: Death date text, or None when unset.
        """
        return self._death_date

    @death_date.setter
    def death_date(self, value: str | None) -> None:
        """
        Replace the death date text without coercion or validation.

        Example:
            >>> profile = HumanAgentProfile()
            >>> profile.death_date = '1852-11-27'
            >>> profile.death_date
            '1852-11-27'


        :param value: New death date text, or None to clear it.
        :return: None.
        """
        self._death_date = value

    @property
    def nationality(self) -> str | None:
        """
        Return the stored nationality text.

        Example:
            >>> profile = HumanAgentProfile(nationality='British')
            >>> profile.nationality
            'British'


        :return: Nationality text, or None when unset.
        """
        return self._nationality

    @nationality.setter
    def nationality(self, value: str | None) -> None:
        """
        Replace the nationality text without coercion or validation.

        Example:
            >>> profile = HumanAgentProfile()
            >>> profile.nationality = 'British'
            >>> profile.nationality
            'British'


        :param value: New nationality text, or None to clear it.
        :return: None.
        """
        self._nationality = value

    @property
    def biography(self) -> str | None:
        """
        Return the stored biography text.

        Example:
            >>> profile = HumanAgentProfile(biography='Mathematician.')
            >>> profile.biography
            'Mathematician.'


        :return: Biography text, or None when unset.
        """
        return self._biography

    @biography.setter
    def biography(self, value: str | None) -> None:
        """
        Replace the biography text without coercion or validation.

        Example:
            >>> profile = HumanAgentProfile()
            >>> profile.biography = 'Mathematician.'
            >>> profile.biography
            'Mathematician.'


        :param value: New biography text, or None to clear it.
        :return: None.
        """
        self._biography = value

    @property
    def human_agent_created_timestamp_ep_k(self) -> int | None:
        """
        Return the stored human sidecar creation timestamp in ep_k form.

        Example:
            >>> profile = HumanAgentProfile(human_agent_created_timestamp_ep_k=100)
            >>> profile.human_agent_created_timestamp_ep_k
            100


        :return: Human sidecar creation timestamp in ep_k form, or None when unset.
        """
        return self._human_agent_created_timestamp_ep_k

    @human_agent_created_timestamp_ep_k.setter
    def human_agent_created_timestamp_ep_k(self, value: int | None) -> None:
        """
        Replace the human sidecar creation timestamp in ep_k form without coercion or validation.

        Example:
            >>> profile = HumanAgentProfile()
            >>> profile.human_agent_created_timestamp_ep_k = 100
            >>> profile.human_agent_created_timestamp_ep_k
            100


        :param value: New human sidecar creation timestamp in ep_k form, or None to clear
            it.
        :return: None.
        """
        self._human_agent_created_timestamp_ep_k = value

    @property
    def human_agent_modified_timestamp_ep_k(self) -> int | None:
        """
        Return the stored human sidecar modification timestamp in ep_k form.

        Example:
            >>> profile = HumanAgentProfile(human_agent_modified_timestamp_ep_k=110)
            >>> profile.human_agent_modified_timestamp_ep_k
            110


        :return: Human sidecar modification timestamp in ep_k form, or None when unset.
        """
        return self._human_agent_modified_timestamp_ep_k

    @human_agent_modified_timestamp_ep_k.setter
    def human_agent_modified_timestamp_ep_k(self, value: int | None) -> None:
        """
        Replace the human sidecar modification timestamp in ep_k form without coercion or validation.

        Example:
            >>> profile = HumanAgentProfile()
            >>> profile.human_agent_modified_timestamp_ep_k = 110
            >>> profile.human_agent_modified_timestamp_ep_k
            110


        :param value: New human sidecar modification timestamp in ep_k form, or None to
            clear it.
        :return: None.
        """
        self._human_agent_modified_timestamp_ep_k = value

    @property
    def human_agent_scratch(self) -> str | None:
        """
        Return the stored human sidecar scratch text.

        Example:
            >>> profile = HumanAgentProfile(human_agent_scratch='import-state')
            >>> profile.human_agent_scratch
            'import-state'


        :return: Human sidecar scratch text, or None when unset.
        """
        return self._human_agent_scratch

    @human_agent_scratch.setter
    def human_agent_scratch(self, value: str | None) -> None:
        """
        Replace the human sidecar scratch text without coercion or validation.

        Example:
            >>> profile = HumanAgentProfile()
            >>> profile.human_agent_scratch = 'import-state'
            >>> profile.human_agent_scratch
            'import-state'


        :param value: New human sidecar scratch text, or None to clear it.
        :return: None.
        """
        self._human_agent_scratch = value

    def to_mapping(self) -> dict[str, Any]:
        """
        Serialize shared agent fields and every human sidecar column.

        None values remain in the result; extra is shallow-copied by the shared serializer.
        This method does not persist data.

        Example:
            >>> profile = HumanAgentProfile(given_name='Ada')
            >>> profile.to_mapping()['human_agent_given_name']
            'Ada'


        :return: New dictionary with shared agent and human_agent_prefixed sidecar fields.
        """
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
    """
    Combine shared agent metadata with optional organisation sidecar fields.

    Sidecar row ids and agent foreign keys remain independent of the linked identity.
    Direct construction and property assignment retain sidecar values as supplied;
    from_mapping normalizes scalar inputs.

    Example:
        >>> profile = OrganisationAgentProfile(legal_name='Example Books Ltd')
        >>> profile.legal_name
        'Example Books Ltd'
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
        """
        Initialize shared profile metadata and store the organisation sidecar fields.

        Shared aliases are normalized and extra is shallow-copied. Sidecar ids, foreign
        keys, dates and other fields are not validated or synchronized with the identity.

        Example:
            >>> profile = OrganisationAgentProfile(legal_name='Example Books Ltd')
            >>> profile.org_agent_agent_id is None
            True


        :param agent: Optional shared agent identity object; retained by reference.
        :param aliases: Iterable of alias strings; copied, stripped and deduplicated
            case-insensitively in first-seen order.
        :param notes: Optional free-text agent note, retained as supplied.
        :param created_timestamp_ep_k: Optional agent-row creation timestamp in the stored
            ep_k representation.
        :param modified_timestamp_ep_k: Optional agent-row modification timestamp in the
            stored ep_k representation.
        :param source_created_datestamp_ep_k: Optional source creation datestamp in the
            stored ep_k representation.
        :param source_modified_datestamp_ep_k: Optional source modification datestamp in the
            stored ep_k representation.
        :param scratch: Optional agent-row scratch text, retained as supplied.
        :param extra: Optional extension mapping, copied into a new dictionary with shared
            nested values.
        :param org_agent_id: Optional organisation sidecar row id; retained as supplied.
        :param org_agent_agent_id: Optional agent foreign key stored on the organisation
            sidecar; retained as supplied.
        :param legal_name: Optional legal name; retained as supplied.
        :param trading_name: Optional trading name; retained as supplied.
        :param registration_id: Optional registration identifier text; retained as supplied.
        :param jurisdiction: Optional jurisdiction text; retained as supplied.
        :param founded_date: Optional founding date text; retained as supplied.
        :param dissolved_date: Optional dissolution date text; retained as supplied.
        :param website: Optional website text; retained as supplied.
        :param contact_email: Optional contact email text; retained as supplied.
        :param description: Optional organisation description; retained as supplied.
        :param org_agent_created_timestamp_ep_k: Optional organisation sidecar creation
            timestamp in ep_k form; retained as supplied.
        :param org_agent_modified_timestamp_ep_k: Optional organisation sidecar modification
            timestamp in ep_k form; retained as supplied.
        :param org_agent_scratch: Optional organisation sidecar scratch text; retained as
            supplied.
        :return: None.
        """
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
        """
        Build a organisation profile from shared agent columns and prefixed sidecar columns.

        Ids and timestamps use forgiving integer conversion. Text fields are stringified
        with empty strings becoming None; whitespace remains unchanged. No date parsing,
        foreign-key reconciliation or database lookup occurs.

        Example:
            >>> profile = OrganisationAgentProfile.from_mapping({'org_agent_id': '3', 'org_agent_legal_name': 'Example Books Ltd'})
            >>> profile.org_agent_id, profile.legal_name
            (3, 'Example Books Ltd')


        :param row: Mapping of shared agent fields and org_agent_prefixed sidecar columns.
        :return: New OrganisationAgentProfile instance, or the requested subclass.
        """
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
        """
        Return the stored organisation sidecar row id.

        Example:
            >>> profile = OrganisationAgentProfile(org_agent_id=4)
            >>> profile.org_agent_id
            4


        :return: Organisation sidecar row id, or None when unset.
        """
        return self._org_agent_id

    @org_agent_id.setter
    def org_agent_id(self, value: int | None) -> None:
        """
        Replace the organisation sidecar row id without coercion or validation.

        Example:
            >>> profile = OrganisationAgentProfile()
            >>> profile.org_agent_id = 4
            >>> profile.org_agent_id
            4


        :param value: New organisation sidecar row id, or None to clear it.
        :return: None.
        """
        self._org_agent_id = value

    @property
    def org_agent_agent_id(self) -> AgentID | None:
        """
        Return the stored agent foreign key stored on the organisation sidecar.

        Example:
            >>> profile = OrganisationAgentProfile(org_agent_agent_id=12)
            >>> profile.org_agent_agent_id
            12


        :return: Agent foreign key stored on the organisation sidecar, or None when unset.
        """
        return self._org_agent_agent_id

    @org_agent_agent_id.setter
    def org_agent_agent_id(self, value: AgentID | None) -> None:
        """
        Replace the agent foreign key stored on the organisation sidecar without coercion or validation.

        Example:
            >>> profile = OrganisationAgentProfile()
            >>> profile.org_agent_agent_id = 12
            >>> profile.org_agent_agent_id
            12


        :param value: New agent foreign key stored on the organisation sidecar, or None to
            clear it.
        :return: None.
        """
        self._org_agent_agent_id = value

    @property
    def legal_name(self) -> str | None:
        """
        Return the stored legal name.

        Example:
            >>> profile = OrganisationAgentProfile(legal_name='Example Books Ltd')
            >>> profile.legal_name
            'Example Books Ltd'


        :return: Legal name, or None when unset.
        """
        return self._legal_name

    @legal_name.setter
    def legal_name(self, value: str | None) -> None:
        """
        Replace the legal name without coercion or validation.

        Example:
            >>> profile = OrganisationAgentProfile()
            >>> profile.legal_name = 'Example Books Ltd'
            >>> profile.legal_name
            'Example Books Ltd'


        :param value: New legal name, or None to clear it.
        :return: None.
        """
        self._legal_name = value

    @property
    def trading_name(self) -> str | None:
        """
        Return the stored trading name.

        Example:
            >>> profile = OrganisationAgentProfile(trading_name='Example Books')
            >>> profile.trading_name
            'Example Books'


        :return: Trading name, or None when unset.
        """
        return self._trading_name

    @trading_name.setter
    def trading_name(self, value: str | None) -> None:
        """
        Replace the trading name without coercion or validation.

        Example:
            >>> profile = OrganisationAgentProfile()
            >>> profile.trading_name = 'Example Books'
            >>> profile.trading_name
            'Example Books'


        :param value: New trading name, or None to clear it.
        :return: None.
        """
        self._trading_name = value

    @property
    def registration_id(self) -> str | None:
        """
        Return the stored registration identifier text.

        Example:
            >>> profile = OrganisationAgentProfile(registration_id='reg-1')
            >>> profile.registration_id
            'reg-1'


        :return: Registration identifier text, or None when unset.
        """
        return self._registration_id

    @registration_id.setter
    def registration_id(self, value: str | None) -> None:
        """
        Replace the registration identifier text without coercion or validation.

        Example:
            >>> profile = OrganisationAgentProfile()
            >>> profile.registration_id = 'reg-1'
            >>> profile.registration_id
            'reg-1'


        :param value: New registration identifier text, or None to clear it.
        :return: None.
        """
        self._registration_id = value

    @property
    def jurisdiction(self) -> str | None:
        """
        Return the stored jurisdiction text.

        Example:
            >>> profile = OrganisationAgentProfile(jurisdiction='GB')
            >>> profile.jurisdiction
            'GB'


        :return: Jurisdiction text, or None when unset.
        """
        return self._jurisdiction

    @jurisdiction.setter
    def jurisdiction(self, value: str | None) -> None:
        """
        Replace the jurisdiction text without coercion or validation.

        Example:
            >>> profile = OrganisationAgentProfile()
            >>> profile.jurisdiction = 'GB'
            >>> profile.jurisdiction
            'GB'


        :param value: New jurisdiction text, or None to clear it.
        :return: None.
        """
        self._jurisdiction = value

    @property
    def founded_date(self) -> str | None:
        """
        Return the stored founding date text.

        Example:
            >>> profile = OrganisationAgentProfile(founded_date='1952-01-01')
            >>> profile.founded_date
            '1952-01-01'


        :return: Founding date text, or None when unset.
        """
        return self._founded_date

    @founded_date.setter
    def founded_date(self, value: str | None) -> None:
        """
        Replace the founding date text without coercion or validation.

        Example:
            >>> profile = OrganisationAgentProfile()
            >>> profile.founded_date = '1952-01-01'
            >>> profile.founded_date
            '1952-01-01'


        :param value: New founding date text, or None to clear it.
        :return: None.
        """
        self._founded_date = value

    @property
    def dissolved_date(self) -> str | None:
        """
        Return the stored dissolution date text.

        Example:
            >>> profile = OrganisationAgentProfile(dissolved_date='2000-01-01')
            >>> profile.dissolved_date
            '2000-01-01'


        :return: Dissolution date text, or None when unset.
        """
        return self._dissolved_date

    @dissolved_date.setter
    def dissolved_date(self, value: str | None) -> None:
        """
        Replace the dissolution date text without coercion or validation.

        Example:
            >>> profile = OrganisationAgentProfile()
            >>> profile.dissolved_date = '2000-01-01'
            >>> profile.dissolved_date
            '2000-01-01'


        :param value: New dissolution date text, or None to clear it.
        :return: None.
        """
        self._dissolved_date = value

    @property
    def website(self) -> str | None:
        """
        Return the stored website text.

        Example:
            >>> profile = OrganisationAgentProfile(website='https://example.invalid/books')
            >>> profile.website
            'https://example.invalid/books'


        :return: Website text, or None when unset.
        """
        return self._website

    @website.setter
    def website(self, value: str | None) -> None:
        """
        Replace the website text without coercion or validation.

        Example:
            >>> profile = OrganisationAgentProfile()
            >>> profile.website = 'https://example.invalid/books'
            >>> profile.website
            'https://example.invalid/books'


        :param value: New website text, or None to clear it.
        :return: None.
        """
        self._website = value

    @property
    def contact_email(self) -> str | None:
        """
        Return the stored contact email text.

        Example:
            >>> profile = OrganisationAgentProfile(contact_email='rights@example.invalid')
            >>> profile.contact_email
            'rights@example.invalid'


        :return: Contact email text, or None when unset.
        """
        return self._contact_email

    @contact_email.setter
    def contact_email(self, value: str | None) -> None:
        """
        Replace the contact email text without coercion or validation.

        Example:
            >>> profile = OrganisationAgentProfile()
            >>> profile.contact_email = 'rights@example.invalid'
            >>> profile.contact_email
            'rights@example.invalid'


        :param value: New contact email text, or None to clear it.
        :return: None.
        """
        self._contact_email = value

    @property
    def description(self) -> str | None:
        """
        Return the stored organisation description.

        Example:
            >>> profile = OrganisationAgentProfile(description='Publisher.')
            >>> profile.description
            'Publisher.'


        :return: Organisation description, or None when unset.
        """
        return self._description

    @description.setter
    def description(self, value: str | None) -> None:
        """
        Replace the organisation description without coercion or validation.

        Example:
            >>> profile = OrganisationAgentProfile()
            >>> profile.description = 'Publisher.'
            >>> profile.description
            'Publisher.'


        :param value: New organisation description, or None to clear it.
        :return: None.
        """
        self._description = value

    @property
    def org_agent_created_timestamp_ep_k(self) -> int | None:
        """
        Return the stored organisation sidecar creation timestamp in ep_k form.

        Example:
            >>> profile = OrganisationAgentProfile(org_agent_created_timestamp_ep_k=100)
            >>> profile.org_agent_created_timestamp_ep_k
            100


        :return: Organisation sidecar creation timestamp in ep_k form, or None when unset.
        """
        return self._org_agent_created_timestamp_ep_k

    @org_agent_created_timestamp_ep_k.setter
    def org_agent_created_timestamp_ep_k(self, value: int | None) -> None:
        """
        Replace the organisation sidecar creation timestamp in ep_k form without coercion or validation.

        Example:
            >>> profile = OrganisationAgentProfile()
            >>> profile.org_agent_created_timestamp_ep_k = 100
            >>> profile.org_agent_created_timestamp_ep_k
            100


        :param value: New organisation sidecar creation timestamp in ep_k form, or None to
            clear it.
        :return: None.
        """
        self._org_agent_created_timestamp_ep_k = value

    @property
    def org_agent_modified_timestamp_ep_k(self) -> int | None:
        """
        Return the stored organisation sidecar modification timestamp in ep_k form.

        Example:
            >>> profile = OrganisationAgentProfile(org_agent_modified_timestamp_ep_k=110)
            >>> profile.org_agent_modified_timestamp_ep_k
            110


        :return: Organisation sidecar modification timestamp in ep_k form, or None when
            unset.
        """
        return self._org_agent_modified_timestamp_ep_k

    @org_agent_modified_timestamp_ep_k.setter
    def org_agent_modified_timestamp_ep_k(self, value: int | None) -> None:
        """
        Replace the organisation sidecar modification timestamp in ep_k form without coercion or validation.

        Example:
            >>> profile = OrganisationAgentProfile()
            >>> profile.org_agent_modified_timestamp_ep_k = 110
            >>> profile.org_agent_modified_timestamp_ep_k
            110


        :param value: New organisation sidecar modification timestamp in ep_k form, or None
            to clear it.
        :return: None.
        """
        self._org_agent_modified_timestamp_ep_k = value

    @property
    def org_agent_scratch(self) -> str | None:
        """
        Return the stored organisation sidecar scratch text.

        Example:
            >>> profile = OrganisationAgentProfile(org_agent_scratch='import-state')
            >>> profile.org_agent_scratch
            'import-state'


        :return: Organisation sidecar scratch text, or None when unset.
        """
        return self._org_agent_scratch

    @org_agent_scratch.setter
    def org_agent_scratch(self, value: str | None) -> None:
        """
        Replace the organisation sidecar scratch text without coercion or validation.

        Example:
            >>> profile = OrganisationAgentProfile()
            >>> profile.org_agent_scratch = 'import-state'
            >>> profile.org_agent_scratch
            'import-state'


        :param value: New organisation sidecar scratch text, or None to clear it.
        :return: None.
        """
        self._org_agent_scratch = value

    def to_mapping(self) -> dict[str, Any]:
        """
        Serialize shared agent fields and every organisation sidecar column.

        None values remain in the result; extra is shallow-copied by the shared serializer.
        This method does not persist data.

        Example:
            >>> profile = OrganisationAgentProfile(legal_name='Example Books Ltd')
            >>> profile.to_mapping()['org_agent_legal_name']
            'Example Books Ltd'


        :return: New dictionary with shared agent and org_agent_prefixed sidecar fields.
        """
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
