"""Repository for people and organisations credited in the catalog."""

from __future__ import annotations

from collections.abc import Iterable
from typing import ClassVar, Mapping, Sequence

from LiuXin_alpha.databases.macro_types import LinkValue

from ..api.common import (
    CatalogMutationError,
    EntityId,
    IdentifierCandidate,
    MetadataCandidate,
    MatchResult,
    RowInput,
    RowMapping,
    WemiLevel,
)
from .base import BaseRepository, WEMI_TABLES, normalise_text


class AgentRepository(BaseRepository):
    """Store, resolve, match, and credit catalog Agents."""

    table_name = "agents"
    id_column = "agent_id"
    input_aliases: ClassVar[Mapping[str, str]] = {
        "id": "agent_id",
        "name": "agent_canonical_name",
        "canonical_name": "agent_canonical_name",
        "sort_name": "agent_sort_name",
        "type": "agent_type",
        "aliases": "agent_aliases",
        "note": "agent_note",
    }

    _ALIAS_SEPARATOR = "(#BREAK#)"

    @classmethod
    def normalise_aliases(cls, aliases: object) -> str | None:
        """Return the stable storage representation for Agent aliases.

        :param aliases: One alias, a sequence of aliases, or ``None``.
        :return: Case-insensitively deduplicated delimiter-joined aliases.
        """

        if aliases is None:
            return None
        values: Iterable[object]
        if isinstance(aliases, str):
            values = (aliases,)
        elif isinstance(aliases, Iterable):
            values = aliases
        else:
            raise TypeError("aliases must be a string, iterable, or None")
        result: list[str] = []
        seen: set[str] = set()
        for value in values:
            if value is None:
                continue
            alias = str(value).strip()
            key = alias.casefold()
            if not alias or key in seen:
                continue
            seen.add(key)
            result.append(alias)
        return cls._ALIAS_SEPARATOR.join(result) or None

    def create_person(
        self,
        data: RowInput,
        *,
        details: RowInput | None = None,
        identifiers: Sequence[Mapping[str, object]] = (),
        language_ids: Sequence[EntityId] = (),
        notes: Sequence[str | RowInput] = (),
    ) -> EntityId:
        """Atomically create a person Agent and its subtype metadata.

        :param data: Core Agent values using public aliases or storage columns.
        :param details: ``human_agents`` storage-column values.
        :param identifiers: Identifier mappings with scheme, value, source, and
            optional priority or ``is_primary``.
        :param language_ids: Existing native-language IDs to link.
        :param notes: Text or Note repository payloads to create and link.
        :return: New core Agent ID.
        """

        payload = self._agent_payload(data, required_type="person")
        with self._macros.transaction():
            agent_id = self.create(payload)
            sidecar = dict(details or {})
            sidecar["human_agent_agent_id"] = agent_id
            self._macros.insert_row(
                "human_agents",
                sidecar,
                id_column="human_agent_id",
            )
            self._attach_agent_metadata(
                agent_id,
                identifiers=identifiers,
                language_ids=language_ids,
                notes=notes,
            )
        return agent_id

    def create_organisation(
        self,
        data: RowInput,
        *,
        details: RowInput | None = None,
        parent_id: EntityId | None = None,
        relation_type: str = "imprint_of",
        relation_note: str | None = None,
        identifiers: Sequence[Mapping[str, object]] = (),
        language_ids: Sequence[EntityId] = (),
        notes: Sequence[str | RowInput] = (),
        synopses: Sequence[str | RowInput] = (),
    ) -> EntityId:
        """Atomically create an organisation Agent and related metadata.

        :param data: Core Agent values using public aliases or storage columns.
        :param details: ``org_agents`` storage-column values.
        :param parent_id: Optional existing parent organisation Agent ID.
        :param relation_type: Non-empty child-to-parent relationship type.
        :param relation_note: Optional relationship context.
        :param identifiers: Identifier mappings to assign to the Agent.
        :param language_ids: Existing language IDs to link.
        :param notes: Text or Note repository payloads to create and link.
        :param synopses: Text or Synopsis repository payloads to create and link.
        :return: New core Agent ID.
        """

        if parent_id is not None:
            self._require_organisation(parent_id)
            if not isinstance(relation_type, str) or not relation_type.strip():
                raise ValueError("relation_type must be a non-empty string")
        payload = self._agent_payload(data, required_type="organisation")
        with self._macros.transaction():
            agent_id = self.create(payload)
            sidecar = dict(details or {})
            sidecar["org_agent_agent_id"] = agent_id
            self._macros.insert_row(
                "org_agents",
                sidecar,
                id_column="org_agent_id",
            )
            if parent_id is not None:
                self._macros.insert_row(
                    "org_agent_relations",
                    {
                        "org_agent_relation_child_agent_id": agent_id,
                        "org_agent_relation_parent_agent_id": parent_id,
                        "org_agent_relation_type": relation_type.strip(),
                        "org_agent_relation_note": relation_note,
                    },
                    id_column="org_agent_relation_id",
                )
            self._attach_agent_metadata(
                agent_id,
                identifiers=identifiers,
                language_ids=language_ids,
                notes=notes,
                synopses=synopses,
            )
        return agent_id

    def _agent_payload(self, data: RowInput, *, required_type: str) -> dict[str, object]:
        payload = dict(data)
        supplied_type = payload.get("type", payload.get("agent_type"))
        type_aliases = {
            "human": "person",
            "author": "person",
            "creator": "person",
            "organization": "organisation",
            "org": "organisation",
            "company": "organisation",
            "publisher": "organisation",
        }
        if supplied_type is not None:
            normalized = str(supplied_type).strip().lower()
            normalized = type_aliases.get(normalized, normalized)
            if normalized != required_type:
                raise CatalogMutationError(
                    f"{required_type} aggregate cannot store Agent type {supplied_type!r}"
                )
        payload.pop("type", None)
        payload["agent_type"] = required_type
        aliases = payload.pop("aliases", payload.get("agent_aliases", None))
        if aliases is not None:
            payload["agent_aliases"] = self.normalise_aliases(aliases)
        return payload

    def _require_organisation(self, agent_id: EntityId) -> None:
        agent = self.require(agent_id)
        if agent.get("agent_type") != "organisation":
            raise CatalogMutationError(f"Agent {agent_id} is not an organisation")
        sidecars = self._macros.get_rows(
            "org_agents",
            where={"org_agent_agent_id": agent_id},
        )
        if not sidecars:
            raise CatalogMutationError(
                f"organisation Agent {agent_id} has no org_agents sidecar"
            )

    def _attach_agent_metadata(
        self,
        agent_id: EntityId,
        *,
        identifiers: Sequence[Mapping[str, object]],
        language_ids: Sequence[EntityId],
        notes: Sequence[str | RowInput],
        synopses: Sequence[str | RowInput] = (),
    ) -> None:
        if self.repositories is None:
            raise CatalogMutationError("Agent repository is not bound to repositories")
        for record in identifiers:
            scheme = record.get("scheme", record.get("identifier_type"))
            value = record.get("value")
            source = record.get("source", record.get("provenance"))
            if not isinstance(scheme, str) or not scheme.strip():
                raise CatalogMutationError("Agent identifier scheme must be non-empty")
            if not isinstance(value, str) or not value.strip():
                raise CatalogMutationError("Agent identifier value must be non-empty")
            if source is not None and not isinstance(source, str):
                raise TypeError("Agent identifier source must be a string or None")
            identifier_id = self.repositories.identifiers.match_or_create(
                IdentifierCandidate(scheme, value, source=source)
            )
            priority_value = record.get("priority")
            if priority_value is None and "is_primary" in record:
                priority_value = 0 if bool(record["is_primary"]) else 1
            if priority_value is not None and (
                not isinstance(priority_value, int) or isinstance(priority_value, bool)
            ):
                raise TypeError("Agent identifier priority must be an integer or None")
            self.repositories.identifiers.link_to_agent(
                identifier_id=identifier_id,
                agent_id=agent_id,
                priority=priority_value,
            )
        for language_id in language_ids:
            self._link(
                self.table_name,
                agent_id,
                "languages",
                language_id,
                link_type="native",
                priority=0,
            )
        for note in notes:
            note_id = self.repositories.notes.create(
                {"note": note} if isinstance(note, str) else note
            )
            self._link(self.table_name, agent_id, "notes", note_id, priority=0)
        for synopsis in synopses:
            synopsis_id = self.repositories.synopses.create(
                {"synopsis": synopsis} if isinstance(synopsis, str) else synopsis
            )
            self._link(
                self.table_name,
                agent_id,
                "synopses",
                synopsis_id,
                priority=0,
            )

    def resolve(self, *, name: str, role: str | None = None) -> RowMapping | None:
        """Return the first Agent with the same normalized canonical name.

        ``role`` is accepted for API compatibility but does not constrain Agent
        identity: roles belong to WEMI-to-Agent relationships.

        :param name: Canonical Agent name to resolve.
        :param role: Optional relationship role; validated when supplied.
        :return: Matching Agent, or ``None``.
        """

        if not isinstance(name, str):
            raise TypeError("name must be a string")
        if role is not None and (not isinstance(role, str) or not role.strip()):
            raise ValueError("role must be a non-empty string when supplied")
        wanted = normalise_text(name)
        if not wanted:
            return None
        for row in self._all_rows():
            canonical = row.get("agent_canonical_name")
            if isinstance(canonical, str) and normalise_text(canonical) == wanted:
                return row
        return None

    def match(self, candidate: MetadataCandidate) -> MatchResult:
        """Return the best existing Agent for ``candidate``.

        :param candidate: Candidate Agent metadata.
        :return: Explained match or non-match result.
        """

        from ..matching.agent_matcher import AgentMatcher

        return AgentMatcher(
            self.db,
            self.repositories,
            self.matching_policy,
        ).best(candidate)

    def match_or_create(self, candidate: MetadataCandidate) -> EntityId:
        """Return a unique Agent match or create on a genuine non-match.

        :param candidate: Candidate Agent metadata and structured hints.
        :return: Existing or newly created Agent ID.
        :raises CatalogAmbiguousMatchError: If several Agents remain plausible.
        :raises CatalogMatchConflictError: If decisive evidence conflicts.
        """

        match = self.match(candidate)
        if match.is_match:
            assert match.entity_id is not None
            return match.entity_id
        from ..matching.policy import raise_for_unresolved

        raise_for_unresolved(match)
        return self.create(candidate.data)

    def match_or_create_person(
        self,
        candidate: MetadataCandidate,
        *,
        details: RowInput | None = None,
    ) -> EntityId:
        """Return a matched person or create the complete person aggregate.

        :param candidate: Candidate Agent identity and core values.
        :param details: ``human_agents`` values used only when creating.
        :return: Existing person Agent ID or newly created aggregate ID.
        :raises CatalogMutationError: If a match selects a non-person Agent.
        """

        match = self.match(candidate)
        if match.is_match:
            assert match.entity_id is not None
            row = self.require(match.entity_id)
            if row.get("agent_type") != "person":
                raise CatalogMutationError(
                    f"person candidate matched non-person Agent {match.entity_id}"
                )
            return match.entity_id
        from ..matching.policy import raise_for_unresolved

        raise_for_unresolved(match)
        return self.create_person(candidate.data, details=details)

    def match_or_create_organisation(
        self,
        candidate: MetadataCandidate,
        *,
        details: RowInput | None = None,
    ) -> EntityId:
        """Return a matched organisation or create its complete aggregate.

        :param candidate: Candidate Agent identity and core values.
        :param details: ``org_agents`` values used only when creating.
        :return: Existing organisation Agent ID or new aggregate ID.
        :raises CatalogMutationError: If a match selects another Agent type.
        """

        match = self.match(candidate)
        if match.is_match:
            assert match.entity_id is not None
            row = self.require(match.entity_id)
            if row.get("agent_type") != "organisation":
                raise CatalogMutationError(
                    "organisation candidate matched non-organisation Agent "
                    f"{match.entity_id}"
                )
            return match.entity_id
        from ..matching.policy import raise_for_unresolved

        raise_for_unresolved(match)
        return self.create_organisation(candidate.data, details=details)

    def link_to_wemi(
        self,
        *,
        agent_id: EntityId,
        level: WemiLevel,
        entity_id: EntityId,
        role: str,
        priority: int | None = None,
    ) -> None:
        """Credit an Agent on a WEMI entity using a relationship role.

        :param agent_id: Existing Agent ID.
        :param level: WEMI level to credit.
        :param entity_id: Existing WEMI entity ID.
        :param role: Non-empty relationship role such as ``author``.
        :param priority: Optional stable credit order.
        :return: None.
        """

        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        if not isinstance(role, str) or not role.strip():
            raise ValueError("role must be a non-empty string")
        if priority is not None and (
            not isinstance(priority, int) or isinstance(priority, bool)
        ):
            raise TypeError("priority must be an integer or None")
        self._link(
            WEMI_TABLES[level],
            entity_id,
            self.table_name,
            agent_id,
            link_type=role.strip(),
            priority=priority,
        )

    def replace_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
        role: str,
        agent_ids: Sequence[EntityId],
    ) -> None:
        """Replace one role-scoped set of Agent credits on a WEMI entity.

        Credits with other roles are retained. Missing priorities are assigned
        from ``agent_ids`` order, highest first.

        :param level: WEMI level to credit.
        :param entity_id: Existing WEMI entity ID.
        :param role: Non-empty relationship role such as ``pbl``.
        :param agent_ids: Ordered existing Agent IDs for that role.
        :return: None.
        """

        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        if not isinstance(role, str) or not role.strip():
            raise ValueError("role must be a non-empty string")
        if not isinstance(agent_ids, Sequence) or isinstance(agent_ids, (str, bytes)):
            raise TypeError("agent_ids must be a sequence of integers")
        ordered_ids = tuple(agent_ids)
        for agent_id in ordered_ids:
            self.require(agent_id)
        table = WEMI_TABLES[level]
        self._require_table_row(table, entity_id)
        spec = self._link_spec(table, self.table_name)
        self._macros.replace_links(
            spec,
            entity_id,
            (LinkValue(agent_id, link_type=role.strip()) for agent_id in ordered_ids),
            link_type=role.strip(),
        )

    def list_for_wemi(self, *, level: WemiLevel, entity_id: EntityId) -> Sequence[RowMapping]:
        """Return Agents credited on one WEMI entity.

        :param level: WEMI level to inspect.
        :param entity_id: Existing WEMI entity ID.
        :return: Agent rows with relationship metadata.
        """

        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        return self._linked_rows(WEMI_TABLES[level], entity_id, self.table_name)


__all__ = ["AgentRepository"]
