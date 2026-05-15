"""Lazy item-centered LiuXin metadata slice."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable, Iterable, Mapping
from copy import deepcopy
from typing import Any

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.metadata.containers.metadata_containers.lazy_value_to_id import (
    LazyValueToID,
)
from LiuXin_alpha.metadata.containers.metadata_containers.liuxin_wemi_metadata import (
    LiuXinWEMIMetadata,
    WemiLevel,
    WemiRelationKey,
    WemiRelationLink,
)
from LiuXin_alpha.metadata.standardize import standardize_id_name


LegacyValueToIDLoader = Callable[[], Mapping[str, Any]]
WemiRelationLoader = Callable[[], Iterable[WemiRelationLink]]


class LazyLiuXinWEMIMetadata(LiuXinWEMIMetadata):
    """
    LiuXin/WEMI metadata slice with opt-in lazy relation-backed fields.

    This class is intentionally separate from ``LiuXinWEMIMetadata``. The eager
    hydrator keeps returning the existing concrete object; callers must ask for
    the lazy class through the lazy hydrator/from-database entry point.
    """

    _LEGACY_FIELD_ALIASES = {
        "genres": "genre",
        "genre": "genre",
        "subjects": "subject",
        "subject": "subject",
        "tags": "tags",
        "tag": "tags",
        "labels": "labels",
        "label": "labels",
        "series": "series",
        "notes": "notes",
        "note": "notes",
        "comments": "comments",
        "comment": "comments",
        "synopses": "synopses",
        "synopsis": "synopses",
        "rating": "ratings",
        "ratings": "ratings",
        "file": "files",
        "files": "files",
        "languages_available": "languages_available",
        "language_available": "languages_available",
        "identifier": "identifiers",
        "identifiers": "identifiers",
    }
    _PROJECTION_RELATION_ALIASES = {
        "agent": "agents",
        "author": "agents",
        "authors": "agents",
        "creator": "agents",
        "creators": "agents",
        "genre": "genres",
        "identifier": "identifiers",
        "label": "labels",
        "language": "languages",
        "rating": "ratings",
        "series_entry": "series",
        "subject": "subjects",
        "tag": "tags",
        "title": "titles",
    }

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        object.__setattr__(self, "_lazy_relation_loaders", {})
        object.__setattr__(self, "_lazy_identifiers_loaded", False)

    def install_lazy_value_to_id(
        self,
        field: str,
        loader: LegacyValueToIDLoader,
    ) -> None:
        field_key = self._normalize_lazy_legacy_field(field)
        data = object.__getattribute__(self, "_data")
        data[field_key] = LazyValueToID(loader, label=field_key)

    def install_lazy_relation_loader(
        self,
        level: str,
        relation_key: WemiRelationKey,
        loader: WemiRelationLoader,
    ) -> None:
        level_key = self.normalize_wemi_level(level)
        relation_key = self.get_wemi_metadata(level_key).validate_relation_name(relation_key)
        loaders = object.__getattribute__(self, "_lazy_relation_loaders")
        loaders[(level_key, relation_key)] = loader

    def get_wemi_relation_links(
        self,
        level: str,
        relation_key: WemiRelationKey,
    ) -> list[WemiRelationLink]:
        level_key = self.normalize_wemi_level(level)
        relation_key = self.get_wemi_metadata(level_key).validate_relation_name(relation_key)
        loaders = object.__getattribute__(self, "_lazy_relation_loaders")
        loader = loaders.pop((level_key, relation_key), None)
        if loader is not None:
            self.get_wemi_metadata(level_key).set_relation_links(
                relation_key,
                list(loader()),
            )
        return super().get_wemi_relation_links(level_key, relation_key)

    def get_wemi_related(
        self,
        level: str,
        relation_key: WemiRelationKey,
    ) -> list[Any]:
        return [link.target for link in self.get_wemi_relation_links(level, relation_key)]

    def load(self, *fields: str) -> "LazyLiuXinWEMIMetadata":
        if not fields:
            self.force_hydrate()
            self._hydrate_all_relation_loaders()
            return self

        for field in fields:
            self._hydrate_projection_dependencies(field)
        return self

    def hydrate_field(self, field: str) -> OrderedDict[str, Any] | Any:
        field_key = self._normalize_lazy_legacy_field(field)
        if field_key == "identifiers":
            self._hydrate_identifiers()
            return self.get_identifiers()
        data = object.__getattribute__(self, "_data")
        value = data.get(field_key)
        if isinstance(value, LazyValueToID):
            materialized = value.materialize()
            data[field_key] = materialized
            return materialized
        return value

    def force_hydrate(
        self,
        fields: Iterable[str] | None = None,
    ) -> "LazyLiuXinWEMIMetadata":
        if fields is None:
            data = object.__getattribute__(self, "_data")
            fields = [
                key
                for key, value in data.items()
                if isinstance(value, LazyValueToID)
            ]
            if not object.__getattribute__(self, "_lazy_identifiers_loaded"):
                fields.append("identifiers")
        for field in fields:
            self.hydrate_field(field)
        return self

    def lazy_fields(self) -> tuple[str, ...]:
        data = object.__getattribute__(self, "_data")
        fields = [
            key
            for key, value in data.items()
            if isinstance(value, LazyValueToID)
        ]
        if not object.__getattribute__(self, "_lazy_identifiers_loaded"):
            fields.append("identifiers")
        return tuple(fields)

    def is_lazy_field_loaded(self, field: str) -> bool:
        field_key = self._normalize_lazy_legacy_field(field)
        if field_key == "identifiers":
            return bool(object.__getattribute__(self, "_lazy_identifiers_loaded"))
        data = object.__getattribute__(self, "_data")
        value = data.get(field_key)
        if isinstance(value, LazyValueToID):
            return value.loaded
        return True

    def get_identifiers(self):
        self._hydrate_identifiers()
        return super().get_identifiers()

    def _hydrate_identifiers(self) -> None:
        if object.__getattribute__(self, "_lazy_identifiers_loaded"):
            return
        object.__setattr__(self, "_lazy_identifiers_loaded", True)
        self.sync_legacy_identifiers_from_wemi()

    def _hydrate_projection_dependencies(self, field: str) -> None:
        field_key = self._normalize_lazy_legacy_field(field)
        data = object.__getattribute__(self, "_data")
        if field_key == "identifiers" or isinstance(data.get(field_key), LazyValueToID):
            self.hydrate_field(field_key)

        relation_key = self._normalize_projection_relation_key(field)
        self._hydrate_relation_loaders_for_relation(relation_key)

    def _hydrate_all_relation_loaders(self) -> None:
        loaders = object.__getattribute__(self, "_lazy_relation_loaders")
        for level_key, relation_key in list(loaders):
            self.get_wemi_relation_links(level_key, relation_key)

    def _hydrate_relation_loaders_for_relation(self, relation_key: str) -> None:
        for level in self._LEVELS:
            metadata = self.get_wemi_metadata(level)
            try:
                normalized_relation_key = metadata.validate_relation_name(relation_key)
            except KeyError:
                continue
            self.get_wemi_relation_links(level, normalized_relation_key)

    @classmethod
    def _normalize_projection_relation_key(cls, field: str) -> str:
        normalized = str(field).strip().lower()
        return cls._PROJECTION_RELATION_ALIASES.get(normalized, normalized)

    def direct_get(self, item: str) -> Any:
        data = object.__getattribute__(self, "_data")
        field_key = self._LEGACY_FIELD_ALIASES.get(str(item).strip().lower())
        if field_key == "identifiers":
            return self.get_identifiers()
        if field_key is not None and isinstance(data.get(field_key), LazyValueToID):
            return self.hydrate_field(field_key)
        return super().direct_get(item)

    def __getattr__(self, item: str) -> Any:
        data = object.__getattribute__(self, "_data")
        field_key = self._LEGACY_FIELD_ALIASES.get(str(item).strip().lower())
        if field_key == "identifiers" or standardize_id_name(str(item)) is not None:
            self._hydrate_identifiers()
        if field_key is not None and isinstance(data.get(field_key), LazyValueToID):
            return self.hydrate_field(field_key)
        return super().__getattr__(item)

    def __getitem__(self, item: str) -> Any:
        data = object.__getattribute__(self, "_data")
        field_key = self._LEGACY_FIELD_ALIASES.get(str(item).strip().lower())
        if field_key == "identifiers":
            return self.get_identifiers()
        if field_key is not None and isinstance(data.get(field_key), LazyValueToID):
            return self.hydrate_field(field_key)
        return super().__getitem__(item)

    @classmethod
    def _is_empty_pretty_value(cls, value: Any) -> bool:
        if isinstance(value, LazyValueToID) and not value.loaded:
            return False
        return super()._is_empty_pretty_value(value)

    @classmethod
    def _format_pretty_value(
        cls,
        value: Any,
        *,
        max_items: int = 5,
        max_chars: int = 160,
    ) -> str:
        if isinstance(value, LazyValueToID) and not value.loaded:
            return repr(value)
        return super()._format_pretty_value(
            value,
            max_items=max_items,
            max_chars=max_chars,
        )

    @classmethod
    def _normalize_lazy_legacy_field(cls, field: str) -> str:
        field_key = cls._LEGACY_FIELD_ALIASES.get(str(field).strip().lower())
        return field_key if field_key is not None else str(field).strip().lower()

    @staticmethod
    def relation_target_text(target: Any, relation_key: WemiRelationKey) -> str | None:
        mapping: Mapping[str, Any]
        if isinstance(target, Row):
            mapping = target.row_dict
        elif isinstance(target, Mapping):
            mapping = target
        elif isinstance(target, str):
            text = target.strip()
            return text or None
        else:
            mapping = {}

        text_keys = {
            "tags": ("tag", "tag_name", "name", "text"),
            "labels": ("label_text", "label", "name", "text"),
            "genres": ("genre_full", "genre", "genre_name", "name", "text"),
            "subjects": ("subject_full", "subject", "subject_name", "name", "text"),
            "series": ("series_full", "series", "series_name", "name", "text"),
            "notes": ("note", "note_text", "text"),
            "comments": ("comment", "comment_text", "text"),
            "synopses": ("synopsis", "synopsis_text", "text"),
            "languages": ("language_code", "language", "language_name", "name", "text"),
            "files": (
                "file_storage_key",
                "file_source_path",
                "file_path",
                "file_url",
                "file_name",
                "file_extension",
                "name",
                "text",
            ),
        }.get(str(relation_key), ("name", "text"))

        for key in text_keys:
            value = mapping.get(key)
            if value is None and not mapping:
                value = getattr(target, key, None)
            if value is None:
                continue
            text = str(value).strip()
            if text:
                return text
        return None

    @staticmethod
    def relation_target_id(target: Any, relation_key: WemiRelationKey) -> int | None:
        mapping: Mapping[str, Any]
        if isinstance(target, Row):
            mapping = target.row_dict
        elif isinstance(target, Mapping):
            mapping = target
        else:
            mapping = {}

        id_keys = {
            "tags": ("tag_id", "id"),
            "labels": ("label_id", "id"),
            "genres": ("genre_id", "id"),
            "subjects": ("subject_id", "id"),
            "series": ("series_id", "id"),
            "notes": ("note_id", "id"),
            "comments": ("comment_id", "id"),
            "synopses": ("synopsis_id", "id"),
            "languages": ("language_id", "id"),
            "files": ("file_id", "id"),
        }.get(str(relation_key), ("id",))

        for key in id_keys:
            value = mapping.get(key)
            if value is None and not mapping:
                value = getattr(target, key, None)
            if value in (None, ""):
                continue
            try:
                return int(value)
            except (TypeError, ValueError, OverflowError):
                continue
        if isinstance(target, Row) and target.row_id is not None:
            return int(target.row_id)
        return None

    def lazy_legacy_terms_from_relation(
        self,
        *,
        field: str,
        relation_key: WemiRelationKey,
    ) -> OrderedDict[str, Any]:
        terms: OrderedDict[str, Any] = OrderedDict()
        seen: set[str] = set()
        for level in self._LEVELS:
            try:
                links = self.get_wemi_relation_links(level, relation_key)
            except KeyError:
                continue
            for link in links:
                if field == "ratings":
                    key, value = self._rating_key_value(link.target)
                    if key is None:
                        continue
                    terms[key] = value
                    continue
                text = self.relation_target_text(link.target, relation_key)
                if text is None:
                    continue
                text_key = text.casefold()
                if text_key in seen:
                    continue
                terms[text] = self.relation_target_id(link.target, relation_key)
                seen.add(text_key)
        return terms

    @staticmethod
    def _rating_key_value(target: Any) -> tuple[str | None, Any]:
        if isinstance(target, Row):
            mapping = target.row_dict
        elif isinstance(target, Mapping):
            mapping = target
        else:
            mapping = {}

        source = mapping.get("rating_source") or mapping.get("source")
        rating = mapping.get("rating_for_calibre_tag_viewer")
        if rating in (None, ""):
            rating = mapping.get("rating")
        if rating in (None, ""):
            return None, None

        key = "calibre" if mapping.get("rating_for_calibre_tag_viewer") not in (None, "") else None
        if key is None and source not in (None, ""):
            key = str(source)
        if key is None:
            key = "rating"
        return key, rating

    @classmethod
    def from_database(
        cls,
        database: Any,
        *,
        item_id: int | None = None,
        source_row: Mapping[str, Any] | Row | None = None,
    ) -> "LazyLiuXinWEMIMetadata":
        from LiuXin_alpha.metadata.containers.metadata_containers.liuxin_wemi_lazy_metadata_hydrator import (
            LazyLiuXinWEMIMetadataHydrator,
        )

        hydrator = LazyLiuXinWEMIMetadataHydrator(database)
        return hydrator.get_lazy_liuxin_wemi_metadata(
            item_id=item_id,
            source_row=source_row,
        )

    def __deepcopy__(self, memo: dict[int, Any]) -> "LazyLiuXinWEMIMetadata":
        existing = memo.get(id(self))
        if existing is not None:
            return existing

        self.force_hydrate()
        clone = type(self)(
            work_metadata=deepcopy(self.work_metadata, memo),
            expression_metadata=deepcopy(self.expression_metadata, memo),
            manifestation_metadata=deepcopy(self.manifestation_metadata, memo),
            item_metadata=deepcopy(self.item_metadata, memo),
        )
        memo[id(self)] = clone
        object.__setattr__(
            clone,
            "_data",
            deepcopy(object.__getattribute__(self, "_data"), memo),
        )
        return clone

    def deepcopy_metadata(self) -> "LazyLiuXinWEMIMetadata":
        return deepcopy(self)


LazyLiuXinWEMI = LazyLiuXinWEMIMetadata


__all__ = [
    "LazyLiuXinWEMI",
    "LazyLiuXinWEMIMetadata",
]
