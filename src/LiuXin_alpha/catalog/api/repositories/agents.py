"""
Describe the Agent repository's identity, aggregate, and WEMI-credit contract.

AgentRepositoryAPI extends generic CRUD with person/organisation aggregates,
canonical-name resolution, explained matching, and role-scoped credit operations.
The protocol contains declarations only; its documentation describes the concrete
Catalog repository and delegated portable-macro behavior.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol, runtime_checkable

from LiuXin_alpha.catalog.api.common import (
    EntityId,
    MetadataCandidate,
    MatchResult,
    RowInput,
    RowMapping,
    WemiLevel,
)
from LiuXin_alpha.catalog.api.repositories.base import BaseRepositoryAPI


@runtime_checkable
class AgentRepositoryAPI(BaseRepositoryAPI, Protocol):
    """
    Expose Agent storage, subtype creation, matching, and role-scoped WEMI credits.

    Generic create/update affect core Agent rows; explicit aggregate helpers add
    subtype metadata. Resolve returns the first normalized canonical-name match and
    only validates role, whereas match considers richer evidence and reports ambiguity
    or conflict. Matching helpers reuse successful matches without updating their data.
    Runtime protocol checks establish member presence, not signatures or these semantics.

    Example:
        >>> AgentRepositoryAPI.__name__
        'AgentRepositoryAPI'
    """

    def create_person(
        self,
        data: RowInput,
        *,
        details: RowInput | None = None,
        identifiers: Sequence[Mapping[str, object]] = (),
        language_ids: Sequence[EntityId] = (),
        notes: Sequence[str | RowInput] = (),
    ) -> EntityId:
        """
        Create a person Agent, human subtype row, and attached metadata atomically.

        Normalize the required person type before opening the macro transaction. Insert
        the core row, copy details and overwrite human_agent_agent_id with the new ID,
        then attach identifiers, native languages, and fresh Notes in that order. Errors
        inside this sequence roll back through the macro transaction. A bound repository
        group is required even for empty metadata. No matching or reuse occurs here.

        Example:
            >>> person_id = catalog.agents.create_person({'name': 'Ada'}, details={'human_agent_given_name': 'Ada'})  # doctest: +SKIP


        :param data: Core Agent fields; person/human/author/creator type spellings are accepted.
        :param details: Optional human_agents storage values; the Agent foreign key is overwritten.
        :param identifiers: Records with scheme (or identifier_type), value, source (or provenance), and optional priority/is_primary.
        :param language_ids: Existing language IDs linked as native with priority zero.
        :param notes: Text or Note mappings to create fresh and link at priority zero.
        :return: New core Agent ID after the aggregate transaction succeeds.
        :raises CatalogMutationError: If the supplied type conflicts, binding is absent, or identifier metadata is invalid.
        :raises Exception: Input, matching, schema, or database failures propagate with transaction rollback for writes inside it.
        """

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
        """
        Create an organisation, subtype row, optional parent relation, and metadata atomically.

        When parent_id is supplied, require an existing organisation with an org_agents
        sidecar, then validate nonblank relation_type before preparing the child payload.
        Without a parent, relation fields are unused. Inside one macro transaction,
        create the core Agent and subtype row, overwrite org_agent_agent_id, insert the
        trimmed child-to-parent relation, then attach identifiers, languages, Notes, and
        Synopses. A bound repository group is required even for empty metadata. Parent
        preflight reads occur before this transaction; no matching/reuse is performed.

        Example:
            >>> publisher_id = catalog.agents.create_organisation({'name': 'Example Press'}, parent_id=parent_id)  # doctest: +SKIP


        :param data: Core fields; organisation/organization/org/company/publisher types are accepted.
        :param details: Optional org_agents storage values; the Agent foreign key is overwritten.
        :param parent_id: Optional existing parent organisation ID with a subtype row.
        :param relation_type: Nonblank child-to-parent type, trimmed and used only with a parent.
        :param relation_note: Relationship context forwarded unchanged when a parent is supplied.
        :param identifiers: Identifier records with scheme/value, optional source, and priority/is_primary.
        :param language_ids: Existing language IDs linked as native with priority zero.
        :param notes: Text or Note mappings created fresh and linked with priority zero.
        :param synopses: Text or Synopsis mappings created fresh and linked with priority zero.
        :return: New core organisation Agent ID after the transaction succeeds.
        :raises CatalogNotFoundError: If a supplied parent does not exist.
        :raises CatalogMutationError: If parent/type/binding checks or identifier metadata fail.
        :raises ValueError: If relation_type is not a nonblank string when a parent is supplied.
        :raises Exception: Remaining validation and database failures propagate; writes inside the aggregate transaction roll back.
        """

    def resolve(self, *, name: str, role: str | None = None) -> RowMapping | None:
        """
        Return the first Agent whose canonical name matches normalized input.

        Require a string name and validate a supplied role as a nonblank string. Role is
        otherwise ignored: it does not filter credits, types, or identity. Compare only
        agent_canonical_name using NFKC/casefold/whitespace normalization; aliases are not
        searched. Blank normalized names return None without a database read. Other
        lookups materialize all rows in requested ascending Agent-ID order, then return
        the first match rather than detecting duplicate-name ambiguity.

        Example:
            >>> agent = catalog.agents.resolve(name='Ada', role='aut')  # doctest: +SKIP


        :param name: Canonical-name text to normalize and compare.
        :param role: Compatibility argument validated when supplied but not used as a filter.
        :return: First matching shallow Agent mapping, or None for a blank/unmatched name.
        :raises TypeError: If name is not a string.
        :raises ValueError: If a supplied role is not a nonblank string.
        """

    def match(self, candidate: MetadataCandidate) -> MatchResult:
        """
        Delegate an Agent identity decision to the configured AgentMatcher.

        Pass the borrowed database, repository group, and matching policy to a fresh
        matcher. Unlike resolve, matching considers aliases, type constraints, and
        supported evidence/hints, and reports ambiguity or conflict explicitly. This
        method makes no creation or metadata-update call.

        Example:
            >>> result = catalog.agents.match(MetadataCandidate({'name': 'Ada', 'type': 'person'}))  # doctest: +SKIP


        :param candidate: Agent core metadata and optional structured identity hints.
        :return: Explained match, no-match, ambiguous, or conflicting MatchResult.
        :raises Exception: Matcher validation, dependency, and database-read failures propagate.
        """

    def match_or_create(self, candidate: MetadataCandidate) -> EntityId:
        """
        Reuse a successful Agent match or create a core row on a genuine non-match.

        Raise for unresolved ambiguity/conflict before attempting creation. A match is
        returned without updating metadata; creation forwards candidate.data to generic
        create, which does not add subtype rows. Hints guide matching but are not attached
        as new metadata. Matching and insertion have no enclosing repository transaction
        or uniqueness guarantee against concurrent callers.

        Example:
            >>> agent_id = catalog.agents.match_or_create(MetadataCandidate({'name': 'Ada'}))  # doctest: +SKIP


        :param candidate: Core values to create when unmatched, plus hints used only for matching.
        :return: Existing matched Agent ID or newly inserted core Agent ID.
        :raises CatalogAmbiguousMatchError: If several Agents remain plausible.
        :raises CatalogMatchConflictError: If decisive evidence conflicts.
        :raises Exception: Matching, core-value validation, and insertion errors propagate.
        """

    def match_or_create_person(
        self,
        candidate: MetadataCandidate,
        *,
        details: RowInput | None = None,
    ) -> EntityId:
        """
        Reuse a matched person or create a person aggregate when there is no match.

        Require the matched row and check its exact person type before returning it.
        Existing subtype presence is not checked, and details do not update a match.
        Raise on ambiguity/conflict; otherwise pass candidate.data and details to
        create_person. Hints remain matching evidence rather than metadata to attach.
        Only creation has an aggregate transaction; matching is outside it.

        Example:
            >>> person_id = catalog.agents.match_or_create_person(MetadataCandidate({'name': 'Ada'}))  # doctest: +SKIP


        :param candidate: Core Agent values and identity hints used to find a person.
        :param details: human_agents storage values used only when creating a new aggregate.
        :return: Existing person Agent ID or newly created person aggregate ID.
        :raises CatalogMutationError: If the matched row is not a person or creation rejects its type.
        :raises CatalogAmbiguousMatchError: If multiple Agents remain plausible.
        :raises CatalogMatchConflictError: If decisive identity evidence conflicts.
        """

    def match_or_create_organisation(
        self,
        candidate: MetadataCandidate,
        *,
        details: RowInput | None = None,
    ) -> EntityId:
        """
        Reuse a matched organisation or create its aggregate on a genuine non-match.

        Require the matched row and its exact organisation type, without checking an
        org_agents sidecar or updating existing details. Ambiguity/conflict raises before
        creation. The creation path forwards only candidate.data and details; identity
        hints are not attached and no parent relation is requested. Matching remains
        outside the aggregate creation transaction.

        Example:
            >>> publisher_id = catalog.agents.match_or_create_organisation(MetadataCandidate({'name': 'Example Press'}))  # doctest: +SKIP


        :param candidate: Core Agent values and optional matching evidence for an organisation.
        :param details: org_agents storage values used only for a new aggregate.
        :return: Existing organisation Agent ID or newly created aggregate ID.
        :raises CatalogMutationError: If a match has another type or creation rejects the supplied type.
        :raises CatalogAmbiguousMatchError: If multiple Agents remain plausible.
        :raises CatalogMatchConflictError: If decisive identity evidence conflicts.
        """

    def link_to_wemi(
        self,
        *,
        agent_id: EntityId,
        level: WemiLevel,
        entity_id: EntityId,
        role: str,
        priority: int | None = None,
    ) -> None:
        """
        Upsert an Agent credit on a WEMI entity under a trimmed relationship role.

        Validate level, role, and optional integer priority before delegating endpoint
        checks and upsert to the transactional link helper. An existing identical ordered
        credit keeps its priority when none is supplied; a new one receives a derived
        priority. Explicit priorities pass through, including negative integers. Portable
        SQL traversal places larger priorities first. No Agent or WEMI row is created.

        Example:
            >>> catalog.agents.link_to_wemi(agent_id=agent_id, level='work', entity_id=work_id, role='aut', priority=1)  # doctest: +SKIP


        :param agent_id: Existing Agent ID to credit, validated after the WEMI endpoint.
        :param level: work, expression, manifestation, or item, selecting the source table.
        :param entity_id: Existing source entity ID at the selected WEMI level.
        :param role: Nonblank relationship type, trimmed before macro validation.
        :param priority: Integer ordering value excluding bool, or None to preserve/derive one.
        :return: None after the credit upsert succeeds.
        :raises ValueError: If level is unknown or role is not a nonblank string.
        :raises TypeError: If explicit priority is not a non-boolean integer, or an ID is invalid.
        :raises CatalogNotFoundError: If either endpoint is missing.
        """

    def list_for_wemi(self, *, level: WemiLevel, entity_id: EntityId) -> Sequence[RowMapping]:
        """
        Read all Agent credits for a WEMI entity with per-link metadata.

        Validate level, require the source row, and traverse links without a role filter.
        Return shallow Agent mappings with _catalog_link metadata for each relationship;
        an Agent credited in multiple roles may appear more than once. Portable SQL macros
        order by descending priority then Agent ID. Missing linked rows raise rather than
        being omitted, and traversal does not open a snapshot transaction.

        Example:
            >>> credits = catalog.agents.list_for_wemi(level='work', entity_id=work_id)  # doctest: +SKIP


        :param level: WEMI level whose Agent relationships should be read.
        :param entity_id: Existing WEMI entity ID required before link traversal.
        :return: Sequence of copied Agent rows with type/priority and other _catalog_link fields.
        :raises ValueError: If level is unknown or an ID is negative.
        :raises TypeError: If the source ID is not a non-boolean integer.
        :raises CatalogNotFoundError: If the source or any linked Agent is missing.
        """

    def replace_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
        role: str,
        agent_ids: Sequence[EntityId],
    ) -> None:
        """
        Replace one role's Agent credits while preserving credits under other roles.

        Validate level, role, and a non-text Sequence, then require every Agent in order
        before requiring the WEMI endpoint and resolving its link spec. Delegate replacement
        to the macro transaction; these preflight reads are outside it. Empty input clears
        the selected role. Portable SQL macros reject duplicate logical credits and assign
        missing ordered priorities from count down to one, so the first ID sorts first.
        No Agent rows are created or deleted.

        Example:
            >>> catalog.agents.replace_for_wemi(level='work', entity_id=work_id, role='aut', agent_ids=(first_id, second_id))  # doctest: +SKIP


        :param level: WEMI level selecting the source table.
        :param entity_id: Existing source entity ID, checked after all Agent IDs.
        :param role: Nonblank relationship role, trimmed for replacement and generated links.
        :param agent_ids: Ordered Sequence of existing Agent IDs; strings and bytes are rejected.
        :return: None after replacing the chosen role's links.
        :raises ValueError: If level/role or a nonnegative ID check fails.
        :raises TypeError: If agent_ids is not an accepted sequence or an ID has an invalid type.
        :raises CatalogNotFoundError: If any required Agent or the source entity is missing.
        :raises Exception: Link-spec, role, duplicate-credit, and database failures propagate.
        """
