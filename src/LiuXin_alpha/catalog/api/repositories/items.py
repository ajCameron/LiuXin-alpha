"""
Declare the protocol for Item metadata storage and semantic repository operations.

The repository extends base CRUD with explicit parent-scoped matching and relationship reads.
Returned mappings are shallow row copies; changing them does not persist values.
The protocol adds no runtime implementation or transaction guarantee.
"""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from ..common import EntityId, MetadataCandidate, MatchResult, RowMapping, WemiBundle
from .base import BaseRepositoryAPI


@runtime_checkable
class ItemRepositoryAPI(BaseRepositoryAPI, Protocol):
    """
    Store individual copies and resolve their direct Manifestation ownership.

    Use items and item_id with aliases for manifestation_id, inventory_code, source, source_path,
    source_detail, source_name, acquisition details, location, and condition. Ownership lives in
    item_manifestation_id, so Item/Manifestation traversal does not attach _catalog_link metadata.
    Matching is scoped to an explicit Manifestation; only inventory/source identity fields and
    selected corroboration are compared. Bundle retrieval requires the bound repository group.
    Mutation guarantees remain with the base repository, portable macros, and caller transactions.

    This runtime-checkable protocol describes the concrete repository contract; method presence does
    not validate signatures, database capabilities, or policy.

    Example:
        >>> repository = catalog.items  # doctest: +SKIP
        >>> result = repository.match(manifestation_id, candidate)  # doctest: +SKIP
    """

    def list_for_manifestation(self, manifestation_id: EntityId) -> Sequence[RowMapping]:
        """
        Read Items whose direct parent column refers to an existing Manifestation.

        Require the Manifestation first, then ask macros for rows filtered by item_manifestation_id
        and ordered by item_id. Return shallow dictionaries without adding _catalog_link metadata:
        this ownership uses an Item column, not a separate traversed relationship row. There is no
        page limit or enclosing transaction across parent validation and the Item query.

        Example:
            >>> items = catalog.items.list_for_manifestation(manifestation_id)  # doctest: +SKIP


        :param manifestation_id: Nonnegative non-boolean integer ID of the required parent Manifestation.
        :return: Tuple of Item mappings in macro-provided ID order, empty for an existing parent with no Items.
        :raises CatalogNotFoundError: If the parent Manifestation is absent.
        :raises Exception: ID validation, database access, and row conversion failures propagate.
        """

    def manifestation_for_item(self, item_id: EntityId) -> RowMapping | None:
        """
        Require an Item and resolve its direct Manifestation reference when integer-valued.

        Missing, None, and other non-integer reference values return None. An int reference proceeds
        to strict ID validation, so bool passes the initial int check but then raises TypeError, and
        a negative integer raises ValueError. A valid integer referring to an absent Manifestation
        raises rather than being treated as an unassigned Item. No _catalog_link metadata is added.

        Example:
            >>> manifestation = catalog.items.manifestation_for_item(item_id)  # doctest: +SKIP


        :param item_id: Nonnegative non-boolean integer ID of the required Item.
        :return: Shallow Manifestation row mapping, or None for a non-integer/unassigned parent reference.
        :raises CatalogNotFoundError: If the Item or its integer-referenced Manifestation is missing.
        :raises Exception: ID validation, database reads, and row conversion failures propagate.
        """

    def get_metadata_bundle(self, item_id: EntityId) -> WemiBundle:
        """
        Build a BundleRetriever and return one upward WEMI path from an Item.

        Require a bound repository group; the fresh retriever reads that group's Item, then its
        direct Manifestation and the first linked Expression and Work in repository order. Missing
        parent relationships leave those levels absent; multiple parents select the first rather
        than raising matching ambiguity.

        Attach agents, curated identifiers, titles, notes, and available path-link metadata for the
        selected levels. Agents, identifiers, and notes are deduplicated by ID; titles retain
        occurrences. This is a delegated multi-read projection without an enclosing snapshot
        transaction, not an exhaustive graph or an immutable deep copy. No data is written.

        Example:
            >>> bundle = catalog.items.get_metadata_bundle(item_id)  # doctest: +SKIP


        :param item_id: ID of the Item required through the bound repository group.
        :return: WemiBundle containing the selected path and its collected attachments.
        :raises RuntimeError: If no repository group is bound.
        :raises CatalogNotFoundError: If the Item or a referenced row is missing.
        :raises Exception: Delegated ID validation, traversal, and attachment-read failures propagate.
        """

    def match(self, manifestation_id: EntityId, candidate: MetadataCandidate) -> MatchResult:
        """
        Match Item identity among rows belonging to one existing Manifestation.

        Fetch the scoped rows before validating the candidate, then normalize aliases while ignoring
        unknown columns. The explicit parent argument selects scope; it is not inferred from
        candidate.data. Repository ID-column restrictions and alias collision checks still apply.
        Candidate source/hints do not add evidence.

        Identity fields are item_inventory_code, item_source_path, and item_source_detail, each with
        weight five. Supporting fields are item_type, item_source, and item_source_name, each with
        weight one. Any exact identity comparison qualifies a row; approximate identity needs exact
        corroboration. Disagreements still reduce confidence, and the current bound/default policy
        decides final acceptance and ambiguity. Supporting fields alone cannot establish a match.

        The contextual helper returns match, no_match, or ambiguous; it does not construct a
        conflict decision. This method performs reads without persistence or an enclosing snapshot
        transaction.

        Example:
            >>> result = catalog.items.match(manifestation_id, MetadataCandidate({"inventory_code": "COPY-0042", "source": "manual"}))  # doctest: +SKIP


        :param manifestation_id: Existing Manifestation ID whose children form the entire match scope.
        :param candidate: MetadataCandidate containing Item identity and optional supporting fields.
        :return: An explained match, no_match, or ambiguous decision over the scoped rows.
        :raises CatalogNotFoundError: If the parent or a required linked row is missing.
        :raises Exception: ID/candidate validation, input normalization, policy access, and database/scoring failures propagate.
        """

    def match_or_create(self, manifestation_id: EntityId, candidate: MetadataCandidate) -> EntityId:
        """
        Reuse a scoped Item or create it under the explicit Manifestation.

        Return the matched ID without updating existing data or rewriting its relationship.
        Translate ambiguous/conflict results into Catalog exceptions before any creation. On
        no_match, shallow-copy candidate.data and overwrite its canonical item_manifestation_id with
        the explicit parent ID before calling create. A separate manifestation_id alias remains
        present: if it disagrees with the forced canonical value, normalization raises an alias
        conflict. No relationship-table link is created; ownership is stored directly on Item.

        Matching can ignore unknown fields that creation subsequently rejects. Candidate
        source/hints are not independently persisted or applied to attachments. Parent
        reads/matching and insertion have no common transaction or lock here. Backend constraints
        and caller transactions determine concurrent-write behavior.

        Example:
            >>> entity_id = catalog.items.match_or_create(manifestation_id, MetadataCandidate({"inventory_code": "COPY-0042", "source": "manual"}))  # doctest: +SKIP


        :param manifestation_id: Existing Manifestation defining both lookup scope and the new parent relationship.
        :param candidate: Item candidate whose data supplies fields for insertion only when no match is found.
        :return: Existing selected Item ID or newly created ID after the parent relationship is established.
        :raises CatalogAmbiguousMatchError: If scoped matching remains ambiguous.
        :raises CatalogMatchConflictError: If an overridden matcher supplies a conflict decision.
        :raises Exception: Parent lookup, candidate validation, matching, insertion, and relationship failures propagate.
        """
