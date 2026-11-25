"""Coordinate contextual metadata records while Catalog owns persistence policy.

Reads share one captured source, preserve known misses, and return detached public
aliases. Writes use the same transaction and reconciliation boundary as WEMI edits.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Literal, cast

from LiuXin_alpha.caches.api import (
    CacheFilterOperator,
    CachePredicate,
    CacheQuery,
    CacheSort,
)
from LiuXin_alpha.caches.metadata.shared import SharedMetadata
from LiuXin_alpha.catalog.api.common import CatalogNotFoundError, WemiLevel
from LiuXin_alpha.catalog.api.metadata_shared import SharedMetadataRecord
from LiuXin_alpha.catalog.repositories.base import WEMI_TABLES, BaseRepository
from LiuXin_alpha.catalog.repositories.entities import (
    AnnotationRepository,
    CommentRepository,
    SynopsisRepository,
)
from LiuXin_alpha.catalog.repositories.identifiers import IdentifierRepository
from LiuXin_alpha.catalog.repositories.item_identifiers import ItemIdentifierRepository
from LiuXin_alpha.catalog.repositories.notes import NoteRepository

if TYPE_CHECKING:
    from LiuXin_alpha.caches.metadata.service import MetadataAPI
    from LiuXin_alpha.catalog.api.metadata_results import MetadataWriteReceipt


class _OwnedMetadata(SharedMetadata):
    """Read directly owned records through one complete cache/database projection."""

    def _for_owner(
        self,
        table: str,
        entity_id: int,
        predicates: tuple[CachePredicate, ...],
        fallback: Callable[[], Sequence[Mapping[str, object]]],
    ) -> tuple[SharedMetadataRecord, ...]:
        """Require the owner and return an ID-ordered query with the same read policy."""
        BaseRepository._validate_entity_id(entity_id)
        with self._owner._reading():
            source = self._owner._reads(require_rows=True)
            source.get_row_from_id(table, entity_id)
            rows = source.rows(
                CacheQuery(
                    self._repository.table_name,
                    predicates=predicates,
                    sort=(CacheSort(self._repository.id_column),),
                ),
                fallback,
            )
            return tuple(self._record(row) for row in rows)


class TextMetadata(SharedMetadata):
    """Administer Notes, Comments and Synopses with fresh WEMI attachment writes."""

    def __init__(
        self,
        owner: MetadataAPI,
        repository: NoteRepository | CommentRepository | SynopsisRepository,
    ) -> None:
        """Bind the semantic text repository without reading or owning resources."""
        super().__init__(
            owner,
            repository,
            cast(Literal["notes", "comments", "synopses"], repository.table_name),
        )
        self._text = repository

    def add_for_wemi(
        self, *, level: WemiLevel, entity_id: int, text: str
    ) -> MetadataWriteReceipt[int]:
        """Create fresh text and attach it atomically under Catalog cardinality rules.

        :param level: WEMI owner level.
        :param entity_id: Existing owner identity.
        :param text: New text body, never implicitly reused.
        :return: Receipt containing the newly attached record identity.
        """
        return self._owner._write(
            lambda: (
                self._text.add_for_wemi(
                    level=level, entity_id=entity_id, data={"text": text}
                ),
                True,
            )
        )

    def list_for_wemi(
        self, *, level: WemiLevel, entity_id: int
    ) -> tuple[SharedMetadataRecord, ...]:
        """Read distinct attached records in ID order, requiring every linked target.

        This record view omits link attributes; WEMI hydration supplies those.
        An undeclared relationship has no attachments, while unavailable cached
        topology raises unless database fallback was explicitly enabled.

        :param level: WEMI owner level.
        :param entity_id: Required owner identity.
        :return: Detached text records, without implicit database fallback.
        """
        table = WEMI_TABLES[level]
        BaseRepository._validate_entity_id(entity_id)
        with self._owner._reading():
            source = self._owner._reads(require_rows=True)
            owner = source.get_row_from_id(table, entity_id)
            links = source.get_interlink_rows(owner, self._repository.table_name)
            if not links:
                return ()
            column = source.driver_wrapper.get_link_column(
                table, self._repository.table_name, self._repository.id_column
            )
            identities = set()
            for link in links:
                values = link if isinstance(link, Mapping) else link.row_dict
                target_id = values[column]
                if type(target_id) is not int:
                    raise TypeError("Text relationship has no integer target identity")
                identities.add(target_id)
            records = []
            for target_id in sorted(identities):
                row = source.get_row_from_id(self._repository.table_name, target_id)
                if row is None:
                    raise CatalogNotFoundError(f"Missing linked text: {target_id}")
                records.append(self._record(cast(Mapping[str, object], row.row_dict)))
            return tuple(records)


class IdentifierMetadata(_OwnedMetadata):
    """Coordinate curated identifier CRUD, owner queries and copy-on-assignment."""

    def __init__(self, owner: MetadataAPI, repository: IdentifierRepository) -> None:
        """Bind one curated identifier repository and the shared coordinator."""
        super().__init__(owner, repository, "identifiers")
        self._identifiers = repository

    def _owned(
        self,
        level: str,
        table: str,
        entity_id: int,
        fallback: Callable[[], Sequence[Mapping[str, object]]],
    ) -> tuple[SharedMetadataRecord, ...]:
        """Query both the direct owner discriminator and identity."""
        return self._for_owner(
            table,
            entity_id,
            (
                CachePredicate(
                    "entity_identifier_entity_type", CacheFilterOperator.EQ, level
                ),
                CachePredicate(
                    "entity_identifier_entity_id", CacheFilterOperator.EQ, entity_id
                ),
            ),
            fallback,
        )

    def list_for_wemi(
        self, *, level: WemiLevel, entity_id: int
    ) -> tuple[SharedMetadataRecord, ...]:
        """Require a WEMI owner and return its curated identifiers in ID order."""
        return self._owned(
            level,
            WEMI_TABLES[level],
            entity_id,
            lambda: self._identifiers.list_for_wemi(level=level, entity_id=entity_id),
        )

    def list_for_agent(self, agent_id: int) -> tuple[SharedMetadataRecord, ...]:
        """Require an Agent and return its curated identifiers in ID order."""
        return self._owned(
            "agent",
            "agents",
            agent_id,
            lambda: self._identifiers.list_for_agent(agent_id),
        )

    def link_to_wemi(
        self,
        *,
        identifier_id: int,
        level: WemiLevel,
        entity_id: int,
        priority: int | None = None,
    ) -> MetadataWriteReceipt[int]:
        """Assign or copy to a WEMI owner and return the actual assigned identity."""
        return self._owner._write(
            lambda: (
                self._identifiers.link_to_wemi(
                    identifier_id=identifier_id,
                    level=level,
                    entity_id=entity_id,
                    priority=priority,
                ),
                True,
            )
        )

    def link_to_agent(
        self, *, identifier_id: int, agent_id: int, priority: int | None = None
    ) -> MetadataWriteReceipt[int]:
        """Assign or copy to an Agent, preserving other owners' identifier rows."""
        return self._owner._write(
            lambda: (
                self._identifiers.link_to_agent(
                    identifier_id=identifier_id, agent_id=agent_id, priority=priority
                ),
                True,
            )
        )


class ItemIdentifierMetadata(_OwnedMetadata):
    """Administer Item observations without conflating them with curated identifiers."""

    def __init__(
        self, owner: MetadataAPI, repository: ItemIdentifierRepository
    ) -> None:
        """Bind one observation repository and the shared coordinator."""
        super().__init__(owner, repository, "item_identifiers")
        self._observations = repository

    def list_for_item(self, item_id: int) -> tuple[SharedMetadataRecord, ...]:
        """Require the Item and return its raw observations in identity order."""
        return self._for_owner(
            "items",
            item_id,
            (
                CachePredicate(
                    "item_identifier_item_id", CacheFilterOperator.EQ, item_id
                ),
            ),
            lambda: self._observations.list_for_item(item_id),
        )


class AnnotationMetadata(_OwnedMetadata):
    """Administer Item-scoped annotations without reusing equal anchor/text values."""

    def __init__(self, owner: MetadataAPI, repository: AnnotationRepository) -> None:
        """Bind one annotation repository and the shared coordinator."""
        super().__init__(owner, repository, "annotations")
        self._annotations = repository

    def list_for_item(
        self, item_id: int, *, user_id: int | None = None, kind: str | None = None
    ) -> tuple[SharedMetadataRecord, ...]:
        """Require an Item and select optional exact user/kind filters in ID order."""
        predicates = [
            CachePredicate("annotation_item_id", CacheFilterOperator.EQ, item_id)
        ]
        if user_id is not None:
            BaseRepository._validate_entity_id(user_id)
            predicates.append(
                CachePredicate("annotation_user_id", CacheFilterOperator.EQ, user_id)
            )
        if kind is not None:
            if not isinstance(kind, str) or not kind.strip():
                raise ValueError("kind must be a non-empty string or None")
            kind = kind.strip()
            predicates.append(
                CachePredicate("annotation_kind", CacheFilterOperator.EQ, kind)
            )
        return self._for_owner(
            "items",
            item_id,
            tuple(predicates),
            lambda: self._annotations.list_for_item(
                item_id, user_id=user_id, kind=kind
            ),
        )
