"""
Coordinate detached shared metadata reads with the bound metadata owner.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from typing import TYPE_CHECKING, Literal, cast

from LiuXin_alpha.caches.api import CacheQuery, CacheSort
from LiuXin_alpha.caches.metadata.read_operations import complete_read, provenance
from LiuXin_alpha.caches.metadata.reads import MetadataReadSource
from LiuXin_alpha.catalog import record_queries
from LiuXin_alpha.catalog.api.common import CatalogNotFoundError
from LiuXin_alpha.catalog.api.metadata_shared import (
    SharedMetadataKind,
    SharedMetadataPage,
    SharedMetadataRecord,
)
from LiuXin_alpha.catalog.repositories.base import BaseRepository

if TYPE_CHECKING:
    from LiuXin_alpha.caches.metadata.service import MetadataAPI


class SharedMetadataReader:
    """
    Expose public aliases without leaking repository objects or live rows.

    Example:
        ``metadata.vocabulary("tags").require(tag_id)`` returns detached public aliases.
    """

    def __init__(
        self, owner: MetadataAPI, repository: BaseRepository, kind: SharedMetadataKind
    ) -> None:
        """
        Bind shared-record operations to one Catalog repository.

        Example:
            The tags service binds the Tag repository and labels returned records as tags.


        :param owner: Metadata service that owns attachment checks, transaction
            coordination and cache freshness.
        :param repository: Repository providing validation, aliases and deletion policy.
        :param kind: Public shared-metadata family label for returned records.
        :return: None; retains the bindings without reading rows.
        """
        self._owner = owner
        self._repository = repository
        self._kind: SharedMetadataKind = kind

    def _record(self, row: Mapping[str, object]) -> SharedMetadataRecord:
        """
        Copy public aliases from a complete shared-record row.

        Example:
            Changing record.fields after a read does not persist shared metadata.


        :param row: Physical row containing the repository's integer identity column.
        :return: Detached SharedMetadataRecord; a malformed identity raises TypeError.
        """
        repository = self._repository
        entity_id = row[repository.id_column]
        if type(entity_id) is not int:
            raise TypeError("Shared metadata identity must be an integer")
        fields = {
            alias: deepcopy(row[column])
            for alias, column in repository.input_aliases.items()
            if column != repository.id_column and column in row
        }
        return SharedMetadataRecord(self._kind, entity_id, fields)

    def get(self, entity_id: int) -> SharedMetadataRecord | None:
        """
        Read a complete shared record, preserving known-miss semantics.

        Example:
            A missing tag does not trigger database fallback.


        :param entity_id: Shared-record identity; booleans and negative IDs are invalid.
        :return: Detached shared record, or None for a confirmed miss.
        """
        BaseRepository._validate_entity_id(entity_id)
        with self._owner._reading():
            row = self._owner._reads().get_row_from_id(
                self._repository.table_name, entity_id
            )
            return (
                None
                if row is None
                else self._record(cast(Mapping[str, object], row.row_dict))
            )

    def require(self, entity_id: int) -> SharedMetadataRecord:
        """
        Require an existing shared identity.

        Example:
            ``metadata.agents.require(agent_id)`` requires an existing Agent.


        :param entity_id: Shared-record identity; booleans and negative IDs are invalid.
        :return: Detached shared record; a confirmed miss raises CatalogNotFoundError.
        """
        value = self.get(entity_id)
        if value is None:
            raise CatalogNotFoundError(f"Missing {self._kind} identity: {entity_id}")
        return value

    def list(
        self, *, limit: int = 100, offset: int = 0
    ) -> tuple[SharedMetadataRecord, ...]:
        """
        Read one complete ID-ordered page under the owner's cache lock.

        Example:
            ``metadata.vocabulary("tags").list(limit=20)`` reads a complete first page.


        :param limit: Maximum shared records; zero returns an empty page.
        :param offset: Number of ascending identities to skip.
        :return: Tuple of detached shared records in identity order.
        """
        BaseRepository._validate_page(limit, offset)
        with self._owner._reading():
            rows = self._owner._reads().rows(
                CacheQuery(
                    table=self._repository.table_name,
                    sort=(CacheSort(self._repository.id_column),),
                    limit=limit,
                    offset=offset,
                ),
                lambda: self._repository.list(limit=limit, offset=offset),
            )
            return tuple(self._record(row) for row in rows)

    def get_many(
        self, entity_ids: Sequence[int], *, missing: Literal["none", "raise"] = "none"
    ) -> tuple[SharedMetadataRecord | None, ...]:
        """Read once and detach every requested slot, including duplicate identities."""
        ids = tuple(entity_ids)
        if missing not in ("none", "raise"):
            raise ValueError("missing must be 'none' or 'raise'")
        for entity_id in ids:
            BaseRepository._validate_entity_id(entity_id)
        if not ids:
            with self._owner._lock():
                self._owner._assert_attached()
            return ()

        def read(source: MetadataReadSource) -> tuple[SharedMetadataRecord | None, ...]:
            """Read each unique root once while retaining independently detached slots."""
            rows = {
                entity_id: source.get_row_from_id(
                    self._repository.table_name, entity_id
                )
                for entity_id in dict.fromkeys(ids)
            }
            if missing == "raise" and any(row is None for row in rows.values()):
                raise CatalogNotFoundError(
                    "A requested shared metadata record is missing"
                )
            return tuple(
                None
                if row is None
                else self._record(cast(Mapping[str, object], row.row_dict))
                for entity_id in ids
                for row in (rows[entity_id],)
            )

        return complete_read(self._owner, read)

    def lookup(
        self, fields: Mapping[str, object], *, limit: int = 100, offset: int = 0
    ) -> SharedMetadataPage:
        """Return every exact candidate within a page, without identity selection."""
        return self._query(fields, limit=limit, offset=offset)

    def _query(
        self,
        fields: Mapping[str, object],
        *,
        limit: int,
        offset: int,
        text: str | None = None,
        mode: Literal["exact", "prefix", "contains"] = "prefix",
    ) -> SharedMetadataPage:
        """Validate public operands, then read only the rows that could match.

        An exact text operand narrows the read through the shared text key; other
        queries, including literal suggestions, read the whole shared population.
        """
        BaseRepository._validate_page(limit, offset)
        with self._owner._lock():
            self._owner._assert_attached()
            expected = record_queries.filters(
                self._repository, fields, allow_empty=text is not None
            )
        narrowing = record_queries.narrowing_text_match(expected)
        table = self._repository.table_name

        def read(source: MetadataReadSource) -> SharedMetadataPage:
            """Filter and page rows from the chosen source, retaining exact provenance."""
            rows = (
                source.get_all_row_dicts(table)
                if narrowing is None
                else source.find_text_rows(table, narrowing)
            )
            selected = record_queries.select(
                self._repository, rows, expected, text=text, mode=mode
            )
            origin, consistency, generation = provenance(self._owner, source)
            return SharedMetadataPage(
                tuple(self._record(row) for row in selected[offset : offset + limit]),
                len(selected),
                offset,
                limit,
                origin,
                consistency,
                generation,
            )

        return complete_read(self._owner, read)


class SuggestibleMetadataReader(SharedMetadataReader):
    """Offer literal autocomplete only for text-bearing public record families."""

    def suggest(
        self,
        text: str,
        *,
        mode: Literal["exact", "prefix", "contains"] = "prefix",
        limit: int = 20,
        offset: int = 0,
        filters: Mapping[str, object] | None = None,
    ) -> SharedMetadataPage:
        """Search literal text and aliases without fuzzy selection or creation."""
        if not isinstance(text, str):
            raise TypeError("suggestion text must be a string")
        if mode not in ("exact", "prefix", "contains"):
            raise ValueError("suggestion mode must be exact, prefix or contains")
        return self._query(
            {} if filters is None else filters,
            limit=limit,
            offset=offset,
            text=text,
            mode=mode,
        )
