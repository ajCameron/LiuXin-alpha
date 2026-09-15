"""
Manage curated identifier values and their direct Catalog ownership fields.

Logical comparison normalizes scheme/value pairs across stored copies. Assignment
updates a compatible row or copies one owned elsewhere; ownership is stored on
entity_identifiers itself, not a relationship table. Primary flags are separate
from matching. Complete WEMI replacement has an encompassing macro transaction.
Raw Item observations belong to the separate item_identifiers repository.
"""

from __future__ import annotations

from typing import ClassVar, Mapping, Sequence

from ..api.common import (
    CatalogMutationError,
    EntityId,
    IdentifierCandidate,
    MatchResult,
    RowMapping,
    WemiLevel,
)
from .base import BaseRepository, WEMI_TABLES


class IdentifierRepository(BaseRepository):
    """
    Normalize, find, assign, and replace curated identifiers for WEMI entities and Agents.

    Borrow the database through BaseRepository. Generic CRUD translates public field
    aliases but does not itself apply scheme-specific normalization. Use normalise,
    find, match, or match_or_create for logical comparison. Matching requires a bound
    repository group and chooses a stored copy without deciding bibliographic-owner
    ambiguity. Assignment may return a different row ID, which callers must retain.

    Example:
        >>> repository = IdentifierRepository(None)
        >>> candidate = repository.normalise(IdentifierCandidate('ISBN-13', '978-0-306-40615-7'))
        >>> candidate.identifier_type, candidate.normalised_value
        ('isbn13', '9780306406157')
    """

    table_name = "entity_identifiers"
    id_column = "entity_identifier_id"
    input_aliases: ClassVar[Mapping[str, str]] = {
        "id": "entity_identifier_id",
        "identifier_type": "entity_identifier_scheme",
        "scheme": "entity_identifier_scheme",
        "value": "entity_identifier_value",
        "source": "entity_identifier_provenance",
        "provenance": "entity_identifier_provenance",
        "is_primary": "entity_identifier_is_primary",
        "level": "entity_identifier_entity_type",
        "entity_type": "entity_identifier_entity_type",
        "entity_id": "entity_identifier_entity_id",
    }

    def normalise(self, candidate: IdentifierCandidate) -> IdentifierCandidate:
        """
        Build a canonical comparison candidate without accessing storage.

        Delegate to the shared matching policy: canonicalize scheme aliases, validate
        ISBN checksum/length and UUID syntax, and apply DOI/OCLC prefix rules. Other
        schemes retain stripped, case-sensitive comparison text. A truthy normalised_value
        override takes precedence over the original value. Preserve the stripped original
        value separately from comparison text and retain source/hints references; neither
        provenance nor hints are validated here. The input candidate is not mutated.

        Example:
            >>> from LiuXin_alpha.catalog.repositories.identifiers import IdentifierRepository
            >>> candidate = IdentifierRepository(None).normalise(IdentifierCandidate('DOI', ' https://doi.org/10.1000/ABC '))
            >>> candidate.identifier_type, candidate.value, candidate.normalised_value
            ('doi', 'https://doi.org/10.1000/ABC', '10.1000/abc')


        :param candidate: IdentifierCandidate containing scheme, original value, and optional comparison override/provenance/hints.
        :return: New candidate with a canonical scheme and scheme-specific normalised_value.
        :raises TypeError: If the candidate or its scheme/original value has the wrong type.
        :raises ValueError: If scheme/value text is empty or scheme-specific validation fails.
        :raises Exception: Invalid normalized overrides propagate failures from shared normalization.
        """

        from ..matching.policy import normalise_identifier

        return normalise_identifier(candidate)

    def find(self, *, identifier_type: str, value: str) -> RowMapping | None:
        """
        Return the first stored row matching normalized scheme and value, regardless of owner.

        Validate both arguments as strings and normalize the incoming candidate before
        materializing all rows in requested ascending identifier-ID order. For each row,
        convert its raw scheme/value columns to strings (false-valued fields become empty)
        and normalize them, skipping TypeError/ValueError failures. Compare canonical
        scheme and normalised_value, ignoring provenance, primary flags, and ownership.
        Duplicate logical copies do not cause ambiguity; the first is returned unchanged.

        Example:
            >>> row = catalog.identifiers.find(identifier_type='doi', value='doi:10.1000/abc')  # doctest: +SKIP


        :param identifier_type: Scheme string or supported alias to normalize.
        :param value: Original identifier value string to normalize and compare.
        :return: First matching shallow row mapping, or None when no stored row matches.
        :raises TypeError: If either argument is not a string.
        :raises ValueError: If incoming normalization rejects the scheme/value.
        :raises Exception: Database reads and other row-processing errors propagate.
        """

        if not isinstance(identifier_type, str) or not isinstance(value, str):
            raise TypeError("identifier_type and value must be strings")
        wanted = self.normalise(IdentifierCandidate(identifier_type, value))
        for row in self._all_rows():
            try:
                existing = self.normalise(
                    IdentifierCandidate(
                        str(row.get("entity_identifier_scheme") or ""),
                        str(row.get("entity_identifier_value") or ""),
                    )
                )
            except (TypeError, ValueError):
                continue
            if existing.identifier_type == wanted.identifier_type and (
                existing.normalised_value == wanted.normalised_value
            ):
                return row
        return None

    def match(self, candidate: IdentifierCandidate) -> MatchResult:
        """
        Select one logical identifier storage copy with explained normalized evidence.

        Construct IdentifierMatcher with the borrowed database and bound repository group.
        It scans and sorts matching copies by storage ID, without owner/provenance filters
        or matching-policy thresholds. Multiple owners do not create ambiguity here;
        Work/Agent matchers resolve bibliographic ownership separately. The standard
        matcher reports a match or no-match and does not write to storage.

        Example:
            >>> result = catalog.identifiers.match(IdentifierCandidate('DOI', 'doi:10.1000/abc'))  # doctest: +SKIP


        :param candidate: Logical identifier with raw value or a normalized comparison override.
        :return: Explained first-copy MatchResult, or no_match when no normalized copy exists.
        :raises Exception: Incoming normalization, missing repository binding, and row-processing failures propagate.
        """

        from ..matching.identifier_matcher import IdentifierMatcher

        return IdentifierMatcher(self.db, self.repositories).best(candidate)

    def match_or_create(self, candidate: IdentifierCandidate) -> EntityId:
        """
        Reuse a matching storage copy or insert an initially unowned logical identifier.

        Normalize the candidate, then match across all owners. A successful match is
        returned without updating its spelling, provenance, primary flag, or ownership.
        Otherwise insert the canonical scheme, stripped original value, and source; the
        comparison override and hints are not stored. No encompassing transaction or
        concurrent uniqueness guarantee joins matching to insertion. The concrete matcher
        returns match/no-match; this wrapper does not separately reject other decisions.

        Example:
            >>> identifier_id = catalog.identifiers.match_or_create(IdentifierCandidate('doi', 'https://doi.org/10.1000/ABC', source='publisher'))  # doctest: +SKIP


        :param candidate: Identifier to normalize and match, with source used only for a new row.
        :return: Existing matching row ID, possibly already owned, or a new unowned row ID.
        :raises Exception: Normalization, binding, matching, field-validation, and insertion failures propagate.
        """

        normalised = self.normalise(candidate)
        match = self.match(normalised)
        if match.is_match:
            assert match.entity_id is not None
            return match.entity_id
        return self.create(
            {
                "identifier_type": normalised.identifier_type,
                "value": normalised.value,
                "source": normalised.source,
            }
        )

    def link_to_wemi(
        self,
        *,
        identifier_id: EntityId,
        level: WemiLevel,
        entity_id: EntityId,
        priority: int | None = None,
    ) -> EntityId:
        """
        Assign a curated identifier to a WEMI owner, copying an incompatible owned row.

        Validate level, require the owner and identifier, then update a compatible row or
        create a copy while leaving the original owner unchanged. Ownership lives directly
        on entity_identifiers, not a link table. Priority only controls the primary flag:
        zero sets it, any other integer clears it, and None preserves it even on a copy.
        No ranking value or cross-row primary uniqueness is enforced. There is no outer
        transaction across this method's reads and write; callers can supply one.

        Example:
            >>> assigned_id = catalog.identifiers.link_to_wemi(identifier_id=identifier_id, level='manifestation', entity_id=manifestation_id, priority=0)  # doctest: +SKIP


        :param identifier_id: Existing curated identifier row to assign or copy.
        :param level: work, expression, manifestation, or item, selecting the owner table/type.
        :param entity_id: Existing owner ID, validated before the identifier and priority.
        :param priority: Non-boolean integer controlling is_primary, or None to retain its stored value.
        :return: Assigned row ID; a copy has a new ID that callers must use.
        :raises ValueError: If level is unknown or an ID is negative.
        :raises TypeError: If an ID or supplied priority is not an accepted integer.
        :raises CatalogNotFoundError: If the owner or identifier does not exist.
        """

        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        return self._assign(
            identifier_id=identifier_id,
            entity_type=level,
            entity_table=WEMI_TABLES[level],
            entity_id=entity_id,
            priority=priority,
        )

    def link_to_agent(
        self,
        *,
        identifier_id: EntityId,
        agent_id: EntityId,
        priority: int | None = None,
    ) -> EntityId:
        """
        Assign a curated identifier to an Agent using direct ownership and copy semantics.

        Require the Agent before the identifier and priority checks. A row compatible
        with this owner is updated; one owned elsewhere is copied without changing the
        original. No existing equivalent row at the destination is sought. Priority sets
        only is_primary (zero true, other integers false); None retains the existing flag.
        The assignment does not add an encompassing transaction or enforce a unique primary.

        Example:
            >>> assigned_id = catalog.identifiers.link_to_agent(identifier_id=identifier_id, agent_id=agent_id, priority=0)  # doctest: +SKIP


        :param identifier_id: Existing identifier row to update or copy.
        :param agent_id: Existing Agent owner ID, checked first.
        :param priority: Non-boolean integer controlling the primary flag, or None to preserve it.
        :return: Assigned identifier row ID, which differs from the input when copied.
        :raises CatalogNotFoundError: If the Agent or identifier is missing.
        :raises TypeError: If an ID or explicit priority has an invalid type.
        :raises ValueError: If an ID is negative.
        """

        return self._assign(
            identifier_id=identifier_id,
            entity_type="agent",
            entity_table="agents",
            entity_id=agent_id,
            priority=priority,
        )

    def _assign(
        self,
        *,
        identifier_id: EntityId,
        entity_type: str,
        entity_table: str,
        entity_id: EntityId,
        priority: int | None,
    ) -> EntityId:
        """
        Update compatible ownership fields or copy an identifier for a different owner.

        Require the destination row, copy the required identifier mapping, then validate
        priority when supplied. Each existing ownership component must independently be
        None or equal to its requested value for in-place update; partially filled but
        compatible ownership is completed too. Otherwise copy all fields except the ID
        and names ending in _timestamp_ep_k, then overwrite ownership and any requested
        primary flag. Retain other fields, including provenance and a preserved primary
        flag when priority is None. No destination deduplication or outer transaction is
        performed; CRUD/schema constraints determine write behavior.

        Example:
            >>> assigned_id = catalog.identifiers._assign(identifier_id=identifier_id, entity_type='agent', entity_table='agents', entity_id=agent_id, priority=None)  # doctest: +SKIP


        :param identifier_id: Existing identifier ID to read before choosing update or copy.
        :param entity_type: Ownership discriminator stored on the assigned row.
        :param entity_table: Destination table checked independently of the supplied discriminator.
        :param entity_id: Existing destination row ID, required before identifier lookup.
        :param priority: None to preserve is_primary, zero to set it, or another non-boolean integer to clear it.
        :return: Original identifier ID after update, or newly inserted copy ID.
        :raises CatalogNotFoundError: If either required row is absent.
        :raises TypeError: If ID or priority validation fails.
        :raises Exception: Schema validation, update, or copy insertion errors propagate.
        """

        self._require_table_row(entity_table, entity_id)
        row = dict(self.require(identifier_id))
        current_level = row.get("entity_identifier_entity_type")
        current_id = row.get("entity_identifier_entity_id")
        changes = {
            "entity_identifier_entity_type": entity_type,
            "entity_identifier_entity_id": entity_id,
        }
        if priority is not None:
            if not isinstance(priority, int) or isinstance(priority, bool):
                raise TypeError("priority must be an integer or None")
            changes["entity_identifier_is_primary"] = int(priority == 0)
        if current_level in (None, entity_type) and current_id in (None, entity_id):
            self.update(identifier_id, changes)
            return identifier_id
        copied = {
            key: value
            for key, value in row.items()
            if key != self.id_column and not key.endswith("_timestamp_ep_k")
        }
        copied.update(changes)
        return self.create(copied)

    def list_for_wemi(self, *, level: WemiLevel, entity_id: EntityId) -> Sequence[RowMapping]:
        """
        Read curated identifier rows directly owned by one WEMI entity.

        Validate level and require the owner before scanning all identifier rows. Filter
        by exact ownership type and ID in Python, retaining requested ascending storage-ID
        order. Return all primary and non-primary rows, without value normalization,
        logical deduplication, or added _catalog_link metadata. Reads have no encompassing
        snapshot transaction.

        Example:
            >>> rows = catalog.identifiers.list_for_wemi(level='work', entity_id=work_id)  # doctest: +SKIP


        :param level: WEMI level selecting the ownership discriminator and source table.
        :param entity_id: Existing owner ID required before scanning identifiers.
        :return: Tuple of shallow identifier mappings with matching direct ownership fields.
        :raises ValueError: If level is unknown or the owner ID is negative.
        :raises TypeError: If the owner ID is not a non-boolean integer.
        :raises CatalogNotFoundError: If the WEMI owner is absent, even when it would have no identifiers.
        """

        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        return self._list_for_owner(
            entity_type=level,
            entity_table=WEMI_TABLES[level],
            entity_id=entity_id,
        )

    def primary_values_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
    ) -> Mapping[str, str]:
        """
        Project stored scheme/value strings from rows with a truthy primary flag.

        Read the owner's identifiers through list_for_wemi and skip false-valued primary
        flags. Both scheme and value must be strings, but this method does not normalize,
        trim, or validate their contents. Duplicate exact stored scheme keys raise; case
        variants remain distinct keys. It neither chooses one duplicate nor repairs data.
        Non-primary rows remain accessible through list_for_wemi.

        Example:
            >>> primary = catalog.identifiers.primary_values_for_wemi(level='work', entity_id=work_id)  # doctest: +SKIP


        :param level: WEMI level whose identifiers should be projected.
        :param entity_id: Existing owner ID required by the underlying listing call.
        :return: New dict of stored scheme strings to stored value strings, possibly empty.
        :raises CatalogMutationError: If a primary row lacks string scheme/value or repeats an exact scheme key.
        :raises Exception: Owner validation and identifier-read failures propagate.
        """

        result: dict[str, str] = {}
        for row in self.list_for_wemi(level=level, entity_id=entity_id):
            if not row.get("entity_identifier_is_primary"):
                continue
            scheme = row.get("entity_identifier_scheme")
            value = row.get("entity_identifier_value")
            if not isinstance(scheme, str) or not isinstance(value, str):
                raise CatalogMutationError(
                    "primary identifier rows require string scheme and value"
                )
            if scheme in result:
                raise CatalogMutationError(
                    f"multiple primary identifiers exist for scheme {scheme!r}"
                )
            result[scheme] = value
        return result

    def replace_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
        identifiers: Mapping[str, str],
    ) -> Mapping[str, EntityId]:
        """
        Atomically replace every curated identifier row owned by one WEMI entity.

        Validate level and the string mapping, normalize all candidates, and reject
        duplicate canonical schemes before opening a transaction or checking the owner.
        Inside the transaction, list the owner's existing IDs, match/create each logical
        value, assign it with priority zero, then delete old owned rows not selected.
        Copies protect other owners; reuse can retain earlier spelling/provenance. Every
        desired scheme is primary. An empty mapping still requires the owner and deletes
        all its identifiers, including non-primary rows. Failures roll back writes through
        the macro transaction. Returned IDs need not match earlier copies at this owner.

        Example:
            >>> assigned = catalog.identifiers.replace_for_wemi(level='work', entity_id=work_id, identifiers={'DOI': 'doi:10.1000/abc'})  # doctest: +SKIP


        :param level: WEMI level selecting the owner table and ownership discriminator.
        :param entity_id: Existing owner ID checked after complete mapping validation, inside the transaction.
        :param identifiers: Complete desired mapping of nonblank scheme strings to nonblank value strings.
        :return: New dict of canonical schemes to the assigned row IDs in mapping iteration order.
        :raises TypeError: If identifiers is not a Mapping or contains non-string/blank keys or values.
        :raises ValueError: If level, an ID, or scheme-specific normalization is invalid.
        :raises CatalogMutationError: If distinct mapping keys normalize to the same scheme.
        :raises CatalogNotFoundError: If the owner is absent.
        :raises Exception: Matching, assignment, deletion, and transaction failures propagate.
        """

        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        if not isinstance(identifiers, Mapping):
            raise TypeError("identifiers must be a string mapping")
        candidates: list[IdentifierCandidate] = []
        normalised_schemes: set[str] = set()
        for scheme, value in identifiers.items():
            if not isinstance(scheme, str) or not scheme.strip():
                raise TypeError("identifier schemes must be non-empty strings")
            if not isinstance(value, str) or not value.strip():
                raise TypeError("identifier values must be non-empty strings")
            candidate = self.normalise(IdentifierCandidate(scheme, value))
            if candidate.identifier_type in normalised_schemes:
                raise CatalogMutationError(
                    "identifiers contain a duplicate normalized scheme: "
                    f"{candidate.identifier_type!r}"
                )
            normalised_schemes.add(candidate.identifier_type)
            candidates.append(candidate)

        with self._macros.transaction():
            existing_ids = {
                row[self.id_column]
                for row in self.list_for_wemi(level=level, entity_id=entity_id)
            }
            assigned: dict[str, EntityId] = {}
            assigned_ids: set[EntityId] = set()
            for candidate in candidates:
                identifier_id = self.match_or_create(candidate)
                assigned_id = self.link_to_wemi(
                    identifier_id=identifier_id,
                    level=level,
                    entity_id=entity_id,
                    priority=0,
                )
                assigned[candidate.identifier_type] = assigned_id
                assigned_ids.add(assigned_id)
            for identifier_id in existing_ids - assigned_ids:
                self.delete(identifier_id)
        return assigned

    def list_for_agent(self, agent_id: EntityId) -> Sequence[RowMapping]:
        """
        Read all curated identifier rows directly owned by an existing Agent.

        Require the Agent, then scan all identifier rows and retain exact agent-type and
        owner-ID matches in requested ascending storage-ID order. Return both primary
        and non-primary rows without normalization, logical deduplication, or relationship
        metadata. No read transaction or owner existence cache is added.

        Example:
            >>> identifiers = catalog.identifiers.list_for_agent(agent_id)  # doctest: +SKIP


        :param agent_id: Existing Agent ID required before the identifier scan.
        :return: Tuple of shallow Agent-owned identifier mappings in repository order.
        :raises CatalogNotFoundError: If the Agent is missing.
        :raises TypeError: If agent_id is not a non-boolean integer.
        :raises ValueError: If agent_id is negative.
        """

        return self._list_for_owner(
            entity_type="agent",
            entity_table="agents",
            entity_id=agent_id,
        )

    def _list_for_owner(
        self,
        *,
        entity_type: str,
        entity_table: str,
        entity_id: EntityId,
    ) -> Sequence[RowMapping]:
        """
        Require an owner and select exact direct-ownership matches from a full row scan.

        The owner table is used only for existence validation; the discriminator is an
        independent equality filter. Read all identifiers through _all_rows and retain
        their order and shallow row mappings. Do not normalize values, inspect primary
        flags, deduplicate logical identifiers, or open an enclosing read transaction.

        Example:
            >>> rows = catalog.identifiers._list_for_owner(entity_type='agent', entity_table='agents', entity_id=agent_id)  # doctest: +SKIP


        :param entity_type: Exact ownership discriminator to compare on stored rows.
        :param entity_table: Table containing the required owner row.
        :param entity_id: Nonnegative non-boolean owner ID, also used in the equality filter.
        :return: Tuple of matching shallow mappings in requested ascending identifier-ID order.
        :raises CatalogNotFoundError: If the required owner row is absent.
        :raises Exception: ID validation and database reads propagate their failures.
        """

        self._require_table_row(entity_table, entity_id)
        return tuple(
            row
            for row in self._all_rows()
            if row.get("entity_identifier_entity_type") == entity_type
            and row.get("entity_identifier_entity_id") == entity_id
        )


__all__ = ["IdentifierRepository"]
