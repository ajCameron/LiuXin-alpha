"""Repositories for exact-default catalog value and vocabulary entities."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import ClassVar

from LiuXin_alpha.databases.macro_types import LinkValue

from ..api.common import EntityId, RowInput, RowMapping, WemiLevel
from ..matching.entity_specs import (
    ANNOTATION_SPEC,
    COMMENT_SPEC,
    GENRE_SPEC,
    LABEL_SPEC,
    LANGUAGE_SPEC,
    RATING_SPEC,
    SERIES_SPEC,
    SUBJECT_SPEC,
    SYNOPSIS_SPEC,
    TAG_SPEC,
)
from .base import WEMI_TABLES
from .exact import ExactEntityRepository


class TagRepository(ExactEntityRepository):
    """Store and exactly resolve reusable Tags."""

    table_name = TAG_SPEC.table_name
    id_column = TAG_SPEC.id_column
    input_aliases: ClassVar[Mapping[str, str]] = TAG_SPEC.input_aliases
    match_spec = TAG_SPEC


class LabelRepository(ExactEntityRepository):
    """Store and exactly resolve reusable operational Labels."""

    table_name = LABEL_SPEC.table_name
    id_column = LABEL_SPEC.id_column
    input_aliases: ClassVar[Mapping[str, str]] = LABEL_SPEC.input_aliases
    match_spec = LABEL_SPEC


class GenreRepository(ExactEntityRepository):
    """Store and exactly resolve hierarchical Genres."""

    table_name = GENRE_SPEC.table_name
    id_column = GENRE_SPEC.id_column
    input_aliases: ClassVar[Mapping[str, str]] = GENRE_SPEC.input_aliases
    match_spec = GENRE_SPEC


class SubjectRepository(ExactEntityRepository):
    """Store and exactly resolve hierarchical Subjects."""

    table_name = SUBJECT_SPEC.table_name
    id_column = SUBJECT_SPEC.id_column
    input_aliases: ClassVar[Mapping[str, str]] = SUBJECT_SPEC.input_aliases
    match_spec = SUBJECT_SPEC


class SeriesRepository(ExactEntityRepository):
    """Store and exactly resolve hierarchical Series."""

    table_name = SERIES_SPEC.table_name
    id_column = SERIES_SPEC.id_column
    input_aliases: ClassVar[Mapping[str, str]] = SERIES_SPEC.input_aliases
    match_spec = SERIES_SPEC


class LanguageRepository(ExactEntityRepository):
    """Exactly resolve immutable seeded Languages and code variants."""

    table_name = LANGUAGE_SPEC.table_name
    id_column = LANGUAGE_SPEC.id_column
    input_aliases: ClassVar[Mapping[str, str]] = LANGUAGE_SPEC.input_aliases
    match_spec = LANGUAGE_SPEC


class RatingRepository(ExactEntityRepository):
    """Store and exactly resolve reusable rating values and sources."""

    table_name = RATING_SPEC.table_name
    id_column = RATING_SPEC.id_column
    input_aliases: ClassVar[Mapping[str, str]] = RATING_SPEC.input_aliases
    match_spec = RATING_SPEC


class CommentRepository(ExactEntityRepository):
    """Exactly inspect comments without unsafe global reuse."""

    table_name = COMMENT_SPEC.table_name
    id_column = COMMENT_SPEC.id_column
    input_aliases: ClassVar[Mapping[str, str]] = COMMENT_SPEC.input_aliases
    match_spec = COMMENT_SPEC

    def add_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
        data: RowInput,
    ) -> EntityId:
        """Create and attach a fresh comment to one WEMI entity.

        :param level: WEMI level to comment on.
        :param entity_id: Existing WEMI entity ID.
        :param data: New Comment payload.
        :return: New Comment ID.
        """

        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        table = WEMI_TABLES[level]
        self._require_table_row(table, entity_id)
        with self._macros.transaction():
            comment_id = self.create(data)
            self._link(table, entity_id, self.table_name, comment_id, priority=0)
        return comment_id

    def replace_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
        data: RowInput | None,
    ) -> EntityId | None:
        """Replace the comments attached to one WEMI entity.

        A non-``None`` payload creates a fresh owned comment. ``None`` clears
        the relationship without deleting the now-unreferenced historical row.

        :param level: WEMI level whose comment should change.
        :param entity_id: Existing WEMI entity ID.
        :param data: New Comment payload, or ``None`` to clear it.
        :return: New Comment ID, or ``None`` after clearing.
        """

        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        table = WEMI_TABLES[level]
        self._require_table_row(table, entity_id)
        spec = self._link_spec(table, self.table_name)
        with self._macros.transaction():
            self._macros.replace_links(spec, entity_id, ())
            if data is None:
                return None
            comment_id = self.create(data)
            self._link(table, entity_id, self.table_name, comment_id, priority=0)
        return comment_id

    def list_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
    ) -> Sequence[RowMapping]:
        """Return comments attached to one WEMI entity.

        :param level: WEMI level to inspect.
        :param entity_id: Existing WEMI entity ID.
        :return: Priority-ordered Comment rows with link metadata.
        """

        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        return self._linked_rows(WEMI_TABLES[level], entity_id, self.table_name)


class SynopsisRepository(ExactEntityRepository):
    """Store and exactly resolve reusable Synopses."""

    table_name = SYNOPSIS_SPEC.table_name
    id_column = SYNOPSIS_SPEC.id_column
    input_aliases: ClassVar[Mapping[str, str]] = SYNOPSIS_SPEC.input_aliases
    match_spec = SYNOPSIS_SPEC

    def add_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
        data: RowInput,
    ) -> EntityId:
        """Create and attach a Synopsis to one WEMI entity.

        :param level: WEMI level described by the Synopsis.
        :param entity_id: Existing WEMI entity ID.
        :param data: Synopsis repository payload.
        :return: New Synopsis ID.
        """

        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        table = WEMI_TABLES[level]
        self._require_table_row(table, entity_id)
        with self._macros.transaction():
            synopsis_id = self.create(data)
            self._link(table, entity_id, self.table_name, synopsis_id, priority=0)
        return synopsis_id

    def list_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
    ) -> Sequence[RowMapping]:
        """Return Synopses attached to one WEMI entity."""

        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        return self._linked_rows(WEMI_TABLES[level], entity_id, self.table_name)

    def replace_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
        synopses: Sequence[str | RowInput],
    ) -> tuple[EntityId, ...]:
        """Replace all Synopses attached to one WEMI entity atomically."""

        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        if not isinstance(synopses, Sequence) or isinstance(
            synopses,
            (str, bytes),
        ):
            raise TypeError("synopses must be a sequence of strings or mappings")
        payloads: list[RowInput] = []
        for synopsis in synopses:
            if isinstance(synopsis, str):
                payloads.append({"synopsis": synopsis})
            elif isinstance(synopsis, Mapping):
                payloads.append(dict(synopsis))
            else:
                raise TypeError(
                    "synopses must contain only strings or mappings"
                )
        table = WEMI_TABLES[level]
        self._require_table_row(table, entity_id)
        spec = self._link_spec(table, self.table_name)
        with self._macros.transaction():
            synopsis_ids = tuple(self.create(payload) for payload in payloads)
            self._macros.replace_links(
                spec,
                entity_id,
                (LinkValue(synopsis_id) for synopsis_id in synopsis_ids),
            )
        return synopsis_ids


class AnnotationRepository(ExactEntityRepository):
    """Exactly inspect item-scoped Annotations without global reuse."""

    table_name = ANNOTATION_SPEC.table_name
    id_column = ANNOTATION_SPEC.id_column
    input_aliases: ClassVar[Mapping[str, str]] = ANNOTATION_SPEC.input_aliases
    match_spec = ANNOTATION_SPEC

    def list_for_item(
        self,
        item_id: EntityId,
        *,
        user_id: EntityId | None = None,
        kind: str | None = None,
    ) -> Sequence[RowMapping]:
        """Return stable Item-scoped Annotations with optional filters."""

        self._require_table_row("items", item_id)
        where: dict[str, object] = {"annotation_item_id": item_id}
        if user_id is not None:
            self._validate_entity_id(user_id)
            where["annotation_user_id"] = user_id
        if kind is not None:
            if not isinstance(kind, str) or not kind.strip():
                raise ValueError("kind must be a non-empty string or None")
            where["annotation_kind"] = kind.strip()
        rows = self._macros.get_rows(
            self.table_name,
            where=where,
            order_by=(self.id_column,),
        )
        return tuple(self._as_mapping(row) for row in rows)


__all__ = [
    "AnnotationRepository",
    "CommentRepository",
    "GenreRepository",
    "LabelRepository",
    "LanguageRepository",
    "RatingRepository",
    "SeriesRepository",
    "SubjectRepository",
    "SynopsisRepository",
    "TagRepository",
]
