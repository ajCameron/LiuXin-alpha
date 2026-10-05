"""
Store Agent identities, subtype aggregates, and role-scoped WEMI credits.

Generic CRUD normalizes aliases but creates only the core Agent row. Person and
organisation helpers add subtype rows and related metadata inside a macro
transaction. Resolution scans canonical names; matching delegates richer identity
decisions to AgentMatcher. Credit ordering and persistence follow portable macros.
"""

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
    """
    Manage Agent rows, person/organisation aggregates, and their Catalog relationships.

    Borrow the database through BaseRepository. Public field aliases map to Agent
    storage columns; create/update normalize alias collections without creating
    subtype rows. Aggregate creation additionally requires the bound repository group,
    even when no optional metadata is supplied. Matching policy is inherited from the
    binding, and relationship roles belong to credits rather than Agent identity.

    Example:
        >>> AgentRepository(None).table_name
        'agents'
    """

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
        """
        Join aliases in encounter order after trimming and normalized deduplication.

        A string is one alias, not a delimiter-separated collection. Other Iterable
        objects are consumed once; their non-None elements are converted with str. Skip
        empty trimmed values and duplicates under NFKC/casefold/whitespace normalization,
        while preserving the first retained spelling. The storage separator is (#BREAK#);
        embedded separators are not escaped or split here.

        Example:
            >>> AgentRepository.normalise_aliases([' Alice ', 'ＡＬＩＣＥ', None, '', 9])
            'Alice(#BREAK#)9'
            >>> AgentRepository.normalise_aliases([]) is None
            True


        :param aliases: One string, an Iterable of values, or None to store no aliases.
        :return: Delimiter-joined retained spellings, or None when none survive.
        :raises TypeError: If aliases is neither None, a string, nor an Iterable.
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
            key = normalise_text(alias)
            if not alias or key in seen:
                continue
            seen.add(key)
            result.append(alias)
        return cls._ALIAS_SEPARATOR.join(result) or None

    @classmethod
    def _normalise_alias_input(cls, data: RowInput) -> dict[str, object]:
        """
        Copy a mapping and normalize each supplied public or storage alias field.

        Process aliases and agent_aliases independently, leaving other keys and values
        unchanged. Duplicate-column/conflicting-value checks belong to later base input
        normalization. The input mapping is not modified, though iterable alias values
        such as generators are consumed.

        Example:
            >>> AgentRepository._normalise_alias_input({'aliases': [' Ada ', 'ada']})
            {'aliases': 'Ada'}


        :param data: Mapping containing core Agent fields and optional alias values.
        :return: Shallow dict with any aliases/agent_aliases values in storage form.
        :raises TypeError: If data is not a mapping or an alias value has an unsupported type.
        """

        if not isinstance(data, Mapping):
            raise TypeError("repository data must be a mapping")
        payload = dict(data)
        for key in ("aliases", "agent_aliases"):
            if key in payload:
                payload[key] = cls.normalise_aliases(payload[key])
        return payload

    def create(self, data: RowInput) -> EntityId:
        """
        Normalize aliases and insert one core Agent row through base CRUD.

        This does not create human_agents/org_agents subtype rows or attach metadata.
        The base layer validates writable columns and the returned ID; schema constraints
        and transaction behavior remain with the macro service.

        Example:
            >>> agent_id = catalog.agents.create({'name': 'Ada', 'aliases': ['A. Lovelace']})  # doctest: +SKIP


        :param data: Mapping of public Agent aliases or storage columns to insert.
        :return: Integer ID of the newly inserted core Agent row.
        :raises TypeError: If mapping, alias, or key validation fails.
        :raises CatalogMutationError: If base normalization or the inserted-ID check fails.
        """

        return super().create(self._normalise_alias_input(data))

    def update(self, entity_id: EntityId, data: RowInput) -> None:
        """
        Normalize alias input, require the Agent, and update only supplied fields.

        Mapping/alias normalization happens before the existence check. Remaining field
        validation follows that check in BaseRepository. Omitted fields are preserved;
        an empty mapping still requires an existing Agent but makes no update call. No
        subtype row is created or synchronized, and no outer transaction is added here.

        Example:
            >>> catalog.agents.update(agent_id, {'aliases': ['Ada', 'A. Lovelace']})  # doctest: +SKIP


        :param entity_id: Existing core Agent ID required after alias normalization.
        :param data: Mapping of replacement fields; an empty mapping is allowed.
        :return: None after an empty update or delegated write.
        :raises CatalogNotFoundError: If the Agent does not exist.
        :raises TypeError: If mapping, alias, key, or ID validation fails.
        :raises CatalogMutationError: If remaining field normalization rejects the update.
        """

        super().update(entity_id, self._normalise_alias_input(data))

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
        """
        Prepare core values for one required subtype without reading the database.

        Copy via dict, prefer a supplied type key over agent_type (including None), and
        normalize non-None type values with strip/lower and the supported subtype aliases.
        Reject a different type, remove type, and force agent_type to required_type.
        Pop the public aliases key in preference to agent_aliases; normalize its non-None
        value. An explicit public aliases=None is removed without overwriting a separately
        supplied storage alias value. Generic CRUD later normalizes any remaining field.

        Example:
            >>> AgentRepository(None)._agent_payload({'name': 'Ada', 'type': 'author'}, required_type='person')
            {'name': 'Ada', 'agent_type': 'person'}


        :param data: Dict-compatible core Agent values to copy and prepare.
        :param required_type: Exact subtype required by the caller, normally person or organisation.
        :return: Copied payload with forced storage type and any prepared aliases.
        :raises CatalogMutationError: If a non-None supplied type normalizes to another subtype.
        :raises Exception: Copying, string conversion, or alias-normalization errors propagate.
        """

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
        """
        Require an organisation core row and at least one linked subtype row.

        Check Agent existence and the exact stored organisation type before querying
        org_agents by org_agent_agent_id. Only presence is required; subtype uniqueness
        and parent relationships are not checked. This helper opens no transaction.

        Example:
            >>> catalog.agents._require_organisation(parent_id)  # doctest: +SKIP


        :param agent_id: Existing Agent ID to validate as an organisation parent.
        :return: None when the type and subtype-presence checks succeed.
        :raises CatalogNotFoundError: If the core Agent row is absent.
        :raises CatalogMutationError: If its type differs or no org_agents sidecar exists.
        """

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
        """
        Attach curated identifiers, native languages, fresh Notes, and fresh Synopses in order.

        Require a repository-group binding before inspecting even empty inputs. For each
        identifier, validate scheme/value as nonblank strings and source as string/None,
        then match or create the identifier before validating priority. Missing/None
        priority falls back to is_primary truthiness (zero for true, one for false);
        otherwise require an integer other than bool. Link the identifier with that
        priority. Language links use type native and priority zero; Note/Synopsis strings
        become repository payloads and each new row is linked with priority zero.

        This helper does not open an encompassing transaction or prevalidate all records.
        Aggregate callers provide rollback across these sequential writes. Direct callers
        can observe partial writes if a later record fails.

        Example:
            >>> catalog.agents._attach_agent_metadata(agent_id, identifiers=(), language_ids=(), notes=['Biography'])  # doctest: +SKIP


        :param agent_id: Agent ID receiving metadata; endpoint validation is delegated to link helpers.
        :param identifiers: Records using scheme/identifier_type, value, source/provenance, and optional priority/is_primary.
        :param language_ids: Existing languages to link with native type and priority zero.
        :param notes: Text or Note payloads to create and link in encounter order.
        :param synopses: Text or Synopsis payloads to create and link after Notes.
        :return: None after all requested metadata writes succeed.
        :raises CatalogMutationError: If repositories are unbound or identifier scheme/value is invalid.
        :raises TypeError: If identifier source or explicit priority has an unsupported type.
        :raises Exception: Identifier matching, row creation, and link failures propagate to the caller's transaction.
        """

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
        """
        Return the first Agent whose canonical name matches normalized input.

        Require a string name and validate a supplied role as a nonblank string. Role is
        otherwise ignored: it does not filter credits, types, or identity. Compare only
        agent_canonical_name using NFKC/casefold/whitespace normalization; aliases are not
        searched. Blank normalized names return None without a database read. Other
        lookups materialize all rows in requested ascending Agent-ID order, then return
        the first match rather than detecting duplicate-name ambiguity.

        Example:
            >>> AgentRepository(None).resolve(name='   ') is None
            True


        :param name: Canonical-name text to normalize and compare.
        :param role: Compatibility argument validated when supplied but not used as a filter.
        :return: First matching shallow Agent mapping, or None for a blank/unmatched name.
        :raises TypeError: If name is not a string.
        :raises ValueError: If a supplied role is not a nonblank string.
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

        from ..matching.agent_matcher import AgentMatcher

        return AgentMatcher(
            self.db,
            self.repositories,
            self.matching_policy,
        ).best(candidate)

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

        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        return self._linked_rows(WEMI_TABLES[level], entity_id, self.table_name)


__all__ = ["AgentRepository"]
