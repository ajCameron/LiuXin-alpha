"""
Specify normalization, lookup, assignment, and replacement of curated identifiers.

IdentifierRepositoryAPI extends generic CRUD for direct WEMI/Agent ownership on
entity_identifiers. Logical copies may share a value across owners; they differ
from raw Item observations in item_identifiers. These are protocol declarations;
the described behavior belongs to the concrete Catalog repository and its macros.
"""

from __future__ import annotations

from typing import Mapping, Protocol, Sequence, runtime_checkable

from ..common import EntityId, IdentifierCandidate, MatchResult, RowMapping, WemiLevel
from .base import BaseRepositoryAPI


@runtime_checkable
class IdentifierRepositoryAPI(BaseRepositoryAPI, Protocol):
    """
    Expose curated identifier comparison and direct owner-scoped storage operations.

    Matching chooses a logical storage copy without deciding owner ambiguity. Reuse
    can return an already-owned row; assignment may copy it and returns the assigned
    ID. Priority controls only the primary flag, while complete WEMI replacement is
    transactional. Generic CRUD does not promise scheme-specific normalization.
    Runtime protocol checks establish member presence, not signatures or semantics.

    Example:
        >>> from LiuXin_alpha.catalog.repositories.identifiers import IdentifierRepository
        >>> isinstance(IdentifierRepository(None), IdentifierRepositoryAPI)
        True
    """

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

    # Todo: Note - an identifier can be linked to multiple things
    # Todo: This might be better, semantically, as something like .find.from_string - this could be a callable class
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
