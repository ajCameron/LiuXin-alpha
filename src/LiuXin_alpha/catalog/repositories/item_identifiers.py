"""
Store raw identifier observations directly owned by acquired Items.

These observations remain separate from curated entity identifiers. Generic CRUD
translates field aliases; matching normalizes values and can optionally filter
by Item. Match-or-create validates the Item before reusing or inserting a row.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import ClassVar

from ..api.common import EntityId, IdentifierCandidate, MatchResult, RowMapping
from ..matching.item_identifier_matcher import ItemIdentifierMatcher
from ..matching.policy import normalise_identifier
from .base import BaseRepository


class ItemIdentifierRepository(BaseRepository):
    """
    Expose Item observation storage and normalized exact matching.

    The borrowed database and generic CRUD come from BaseRepository. Matching uses
    this repository directly, without requiring a bound repository group. Global
    matching selects one observation rather than resolving which Item it identifies.

    Example:
        >>> repository = ItemIdentifierRepository(None)
        >>> repository.matcher().repository is repository
        True
    """

    table_name = "item_identifiers"
    id_column = "item_identifier_id"
    input_aliases: ClassVar[Mapping[str, str]] = {
        "id": "item_identifier_id",
        "item_id": "item_identifier_item_id",
        "identifier_type": "item_identifier_scheme",
        "scheme": "item_identifier_scheme",
        "value": "item_identifier_value",
        "source": "item_identifier_source",
    }

    def matcher(self) -> ItemIdentifierMatcher:
        """
        Create a fresh exact-observation matcher retaining this repository.

        Example:
            >>> repository = ItemIdentifierRepository(None)
            >>> repository.matcher().repository is repository
            True


        :return: ItemIdentifierMatcher bound by reference; construction performs no row reads.
        """

        return ItemIdentifierMatcher(self)

    def match(
        self,
        candidate: IdentifierCandidate,
        *,
        item_id: EntityId | None = None,
    ) -> MatchResult:
        """
        Choose the first normalized observation within an optional Item scope.

        Delegate to a fresh matcher, which scans, normalizes and sorts by observation
        ID. Stored normalization failures and non-integer observation IDs are skipped;
        source/provenance is not matching evidence. Incoming normalization errors propagate.

        Example:
            Matching without item_id can return a copy observed on a different Item.


        :param candidate: IdentifierCandidate whose scheme/value or normalized override should match.
        :param item_id: Optional equality filter; None searches all Items without an owner existence check.
        :return: Explained exact match or no_match; duplicate observations do not raise ambiguity.
        """

        return self.matcher().best(candidate, item_id=item_id)

    def exact(
        self,
        candidate_str: str,
        id_type: str,
        *,
        item_id: EntityId | None = None,
    ) -> MatchResult:
        """
        Build a value/scheme candidate and select its first exact observation.

        The matcher normalizes input and applies the same optional Item scope as match.
        It does not compare provenance or detect duplicate-observation ambiguity.

        Example:
            >>> result = catalog.item_identifiers.exact("record-42", "source-id", item_id=item_id)  # doctest: +SKIP


        :param candidate_str: Raw identifier value string.
        :param id_type: Scheme string or supported alias.
        :param item_id: Optional Item equality filter, not an existence-validated owner.
        :return: Exact MatchResult or no_match, selected by observation ID.
        """

        return self.matcher().exact(candidate_str, id_type, item_id=item_id)

    def match_or_create(
        self,
        item_id: EntityId,
        candidate: IdentifierCandidate,
    ) -> EntityId:
        """
        Reuse a normalized observation on an existing Item or insert one there.

        Validate the owner first, normalize, then match only within that Item. Reuse
        preserves stored spelling/source. Creation stores canonical scheme and original
        value, not normalized override or hints. No encompassing transaction or concurrent
        uniqueness guarantee joins the owner check, match, and insertion.

        Example:
            An identical value on another Item does not prevent creation on this Item.


        :param item_id: Existing Item ID required before candidate normalization.
        :param candidate: Observation to normalize, using its stripped original value and source on insertion.
        :return: Existing same-Item observation ID or newly inserted observation ID.
        """

        self._require_table_row("items", item_id)
        normalised = normalise_identifier(candidate)
        result = self.match(normalised, item_id=item_id)
        if result.is_match:
            assert result.entity_id is not None
            return result.entity_id
        return self.create(
            {
                "item_id": item_id,
                "scheme": normalised.identifier_type,
                "value": normalised.value,
                "source": normalised.source,
            }
        )

    def list_for_item(self, item_id: EntityId) -> Sequence[RowMapping]:
        """
        Require an Item and return its observation rows in repository-ID order.

        Read all observation rows and filter exact ownership in Python. No normalization,
        deduplication, provenance filtering, or enclosing snapshot transaction is added.
        A missing Item raises even if no observations would match.

        Example:
            >>> observations = catalog.item_identifiers.list_for_item(item_id)  # doctest: +SKIP


        :param item_id: Existing nonnegative, non-boolean Item ID.
        :return: Tuple of shallow matching observation mappings, including repeated logical values.
        """

        self._require_table_row("items", item_id)
        return tuple(
            row
            for row in self._all_rows()
            if row.get("item_identifier_item_id") == item_id
        )


__all__ = ["ItemIdentifierRepository"]
