"""Read-only projection views for WEMI metadata bundles."""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, Protocol

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api import (
    MetadataTextViewAPI,
    MetadataValuesViewAPI,
    ProjectionIdentifierMap,
    UnloadedMetadataProjectionError,
)


class _MetadataProjectionBundle(Protocol):
    def validate_relation_name(self, relation_key: str) -> str: ...

    def get_related(self, relation_key: str) -> list[Any]: ...

    def primary_related(self, relation_key: str) -> Any | None: ...


_TEXT_FIELD_CANDIDATES: dict[str, tuple[str, ...]] = {
    "tags": (
        "tag",
        "tag_name",
        "text",
        "name",
        "label_text",
        "genre_full",
        "genre",
        "subject_full",
        "subject",
        "series_full",
        "series",
    ),
    "labels": (
        "label_text",
        "label",
        "text",
        "name",
        "tag",
    ),
    "genres": (
        "genre_full",
        "genre",
        "genre_name",
        "text",
        "name",
    ),
    "subjects": (
        "subject_full",
        "subject",
        "subject_name",
        "text",
        "name",
    ),
    "series": (
        "series_full",
        "series",
        "series_name",
        "text",
        "name",
    ),
    "titles": (
        "text",
        "title",
        "work_canonical_title",
        "work_title",
        "expression_title_override",
        "expression_label",
        "manifestation_title",
        "item_title",
        "item_source_name",
        "name",
    ),
    "languages": (
        "display_name",
        "language",
        "language_code",
        "language_iso639_1",
        "language_iso639_2_t",
        "language_iso639_2_b",
        "language_bcp47_primary",
        "text",
        "name",
    ),
    "ratings": (
        "rating",
        "rating_value",
        "value",
        "text",
    ),
    "agents": (
        "agent_display_name",
        "display_name",
        "agent_canonical_name",
        "agent_name",
        "credited_as",
        "human_agent_preferred_name",
        "human_agent_family_name",
        "org_agent_trading_name",
        "org_agent_legal_name",
        "agent_sort_name",
        "sort_name",
        "name",
        "text",
    ),
}
_GENERIC_TEXT_FIELD_CANDIDATES: tuple[str, ...] = (
    "text",
    "name",
    "title",
    "value",
    "label",
)
_IDENTIFIER_SCHEME_KEYS: tuple[str, ...] = (
    "entity_identifier_scheme",
    "item_identifier_scheme",
    "identifier_scheme",
    "scheme",
    "type",
)
_IDENTIFIER_VALUE_KEYS: tuple[str, ...] = (
    "entity_identifier_value",
    "item_identifier_value",
    "identifier_value",
    "value",
    "identifier",
)
_IDENTITY_TITLE_FIELDS: tuple[str, ...] = (
    "work_canonical_title",
    "work_title",
    "expression_title_override",
    "expression_label",
    "expression_subtitle",
    "item_source_name",
)
_IDENTITY_ATTRIBUTES: tuple[str, ...] = (
    "work",
    "expression",
    "manifestation",
    "item",
)
_MISSING = object()


class MetadataValuesView(MetadataValuesViewAPI):
    """Structured read-only projections over one metadata bundle."""

    __slots__ = ("_metadata",)

    def __init__(self, metadata: _MetadataProjectionBundle) -> None:
        self._metadata = metadata

    def relation_values(self, relation_key: str) -> tuple[str, ...]:
        return self._relation_values(relation_key, require_known=True)

    @property
    def tags(self) -> tuple[str, ...]:
        return self._relation_values("tags", require_known=False)

    @property
    def labels(self) -> tuple[str, ...]:
        return self._relation_values("labels", require_known=False)

    @property
    def genres(self) -> tuple[str, ...]:
        return self._relation_values("genres", require_known=False)

    @property
    def subjects(self) -> tuple[str, ...]:
        return self._relation_values("subjects", require_known=False)

    @property
    def series(self) -> tuple[str, ...]:
        return self._relation_values("series", require_known=False)

    @property
    def titles(self) -> tuple[str, ...]:
        relation_titles = self._relation_values("titles", require_known=False)
        identity_title = self._identity_title()
        if identity_title is None:
            return relation_titles
        return _dedupe_text((*relation_titles, identity_title))

    @property
    def primary_title(self) -> str | None:
        try:
            primary = self._metadata.primary_related("titles")
        except KeyError:
            primary = None
        title = _target_text(primary, "titles")
        if title is not None:
            return title
        relation_titles = self._relation_values("titles", require_known=False)
        if relation_titles:
            return relation_titles[0]
        return self._identity_title()

    @property
    def identifiers(self) -> ProjectionIdentifierMap:
        identifiers: dict[str, list[str]] = {}
        for target in self._related_targets("identifiers", require_known=False):
            pair = _identifier_pair(target)
            if pair is None:
                continue
            scheme, value = pair
            values = identifiers.setdefault(scheme, [])
            if value not in values:
                values.append(value)
        return MappingProxyType(
            {scheme: tuple(values) for scheme, values in identifiers.items()}
        )

    @property
    def languages(self) -> tuple[str, ...]:
        return self._relation_values("languages", require_known=False)

    @property
    def ratings(self) -> tuple[str, ...]:
        return self._relation_values("ratings", require_known=False)

    @property
    def agents(self) -> tuple[str, ...]:
        return self._relation_values("agents", require_known=False)

    @property
    def agent_names(self) -> tuple[str, ...]:
        return self.agents

    def _relation_values(
        self,
        relation_key: str,
        *,
        require_known: bool,
    ) -> tuple[str, ...]:
        validated_relation_key = self._validated_relation_key(
            relation_key,
            require_known=require_known,
        )
        if validated_relation_key is None:
            return ()
        return _dedupe_text(
            _target_text(target, validated_relation_key)
            for target in self._metadata.get_related(validated_relation_key)
        )

    def _related_targets(
        self,
        relation_key: str,
        *,
        require_known: bool,
    ) -> list[Any]:
        validated_relation_key = self._validated_relation_key(
            relation_key,
            require_known=require_known,
        )
        if validated_relation_key is None:
            return []
        return self._metadata.get_related(validated_relation_key)

    def _validated_relation_key(
        self,
        relation_key: str,
        *,
        require_known: bool,
    ) -> str | None:
        try:
            return self._metadata.validate_relation_name(relation_key)
        except KeyError:
            if require_known:
                raise
            return None

    def _identity_title(self) -> str | None:
        for identity_attribute in _IDENTITY_ATTRIBUTES:
            identity = getattr(self._metadata, identity_attribute, None)
            title = _first_target_text(identity, _IDENTITY_TITLE_FIELDS)
            if title is not None:
                return title
        return None


class MetadataTextView(MetadataTextViewAPI):
    """Display/export text projections over one metadata bundle."""

    __slots__ = ("_values",)

    def __init__(self, values: MetadataValuesViewAPI) -> None:
        self._values = values

    def relation_text(self, relation_key: str, separator: str = ", ") -> str:
        return separator.join(self._values.relation_values(relation_key))

    @property
    def tags(self) -> str:
        return ", ".join(self._values.tags)

    @property
    def labels(self) -> str:
        return ", ".join(self._values.labels)

    @property
    def genres(self) -> str:
        return ", ".join(self._values.genres)

    @property
    def subjects(self) -> str:
        return ", ".join(self._values.subjects)

    @property
    def series(self) -> str:
        return ", ".join(self._values.series)

    @property
    def title(self) -> str | None:
        return self._values.primary_title

    @property
    def titles(self) -> str:
        return " ; ".join(self._values.titles)

    @property
    def languages(self) -> str:
        return ", ".join(self._values.languages)

    @property
    def ratings(self) -> str:
        return ", ".join(self._values.ratings)

    @property
    def agents(self) -> str:
        return ", ".join(self._values.agents)

    @property
    def agent_names(self) -> str:
        return self.agents


class LiuXinWEMIValuesView(MetadataValuesViewAPI):
    """Structured projections over a complete item-centred WEMI stack."""

    __slots__ = ("_metadata",)

    _LEVEL_ORDER = ("item", "manifestation", "expression", "work")
    _LEGACY_FIELD_BY_RELATION: dict[str, tuple[str, ...]] = {
        "tags": ("tags",),
        "labels": ("labels",),
        "genres": ("genre",),
        "subjects": ("subject",),
        "series": ("series",),
        "languages": ("languages", "language"),
        "ratings": ("ratings",),
        "agents": ("authors",),
    }
    _RELATION_ALIASES: dict[str, str] = {
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

    def __init__(self, metadata: Any) -> None:
        self._metadata = metadata

    def relation_values(self, relation_key: str) -> tuple[str, ...]:
        relation_key = self._normalize_relation_key(relation_key)
        if relation_key == "titles":
            return self.titles
        if relation_key == "identifiers":
            return _dedupe_text(
                identifier
                for values in self.identifiers.values()
                for identifier in values
            )

        if not self._stack_supports_relation(relation_key):
            raise KeyError(f"Unknown WEMI stack relation key {relation_key!r}.")
        self._raise_if_projection_unloaded(relation_key)
        wemi_values = self._wemi_relation_values(relation_key)
        legacy_values = self._legacy_values(
            relation_key,
            suppress_calibre_rating=bool(wemi_values),
        )
        values = [*legacy_values, *wemi_values]
        return _dedupe_text(values)

    @property
    def tags(self) -> tuple[str, ...]:
        return self.relation_values("tags")

    @property
    def labels(self) -> tuple[str, ...]:
        return self.relation_values("labels")

    @property
    def genres(self) -> tuple[str, ...]:
        return self.relation_values("genres")

    @property
    def subjects(self) -> tuple[str, ...]:
        return self.relation_values("subjects")

    @property
    def series(self) -> tuple[str, ...]:
        return self.relation_values("series")

    @property
    def titles(self) -> tuple[str, ...]:
        self._raise_if_projection_unloaded("titles")
        return _dedupe_text(
            (
                *tuple(getattr(self._metadata, "titles", ())),
                *self._wemi_relation_values("titles"),
            )
        )

    @property
    def primary_title(self) -> str | None:
        self._raise_if_projection_unloaded("titles")
        title = getattr(self._metadata, "display_title", None)
        return _clean_text(title)

    @property
    def identifiers(self) -> ProjectionIdentifierMap:
        self._raise_if_projection_unloaded("identifiers")
        identifiers: dict[str, list[str]] = {}
        get_identifiers = getattr(self._metadata, "get_identifiers", None)
        if callable(get_identifiers):
            for scheme, values in get_identifiers().items():
                for value in _iter_text_values(values):
                    _append_identifier(identifiers, str(scheme), value)

        for level in self._LEVEL_ORDER:
            bundle = self._stack_bundle(level)
            if bundle is None or not _bundle_supports_relation(bundle, "identifiers"):
                continue
            for scheme, values in bundle.values.identifiers.items():
                for value in values:
                    _append_identifier(identifiers, scheme, value)

        return MappingProxyType(
            {scheme: tuple(values) for scheme, values in identifiers.items()}
        )

    @property
    def languages(self) -> tuple[str, ...]:
        return self.relation_values("languages")

    @property
    def ratings(self) -> tuple[str, ...]:
        return self.relation_values("ratings")

    @property
    def agents(self) -> tuple[str, ...]:
        return self.relation_values("agents")

    @property
    def agent_names(self) -> tuple[str, ...]:
        return self.agents

    def _legacy_values(
        self,
        relation_key: str,
        *,
        suppress_calibre_rating: bool = False,
    ) -> tuple[str, ...]:
        fields = self._LEGACY_FIELD_BY_RELATION.get(relation_key, ())
        values: list[str] = []
        for field in fields:
            value = self._legacy_field_value(field)
            if relation_key == "ratings":
                values.extend(
                    _iter_rating_values(
                        value,
                        suppress_calibre=suppress_calibre_rating,
                    )
                )
            elif relation_key == "languages":
                values.extend(
                    value
                    for value in _iter_text_values(value)
                    if value.casefold() != "und"
                )
            else:
                values.extend(_iter_text_values(value))
        return tuple(values)

    def _wemi_relation_values(self, relation_key: str) -> tuple[str, ...]:
        values: list[str] = []
        for level in self._LEVEL_ORDER:
            bundle = self._stack_bundle(level)
            if bundle is None or not _bundle_supports_relation(bundle, relation_key):
                continue
            values.extend(bundle.values.relation_values(relation_key))
        return tuple(values)

    def _stack_supports_relation(self, relation_key: str) -> bool:
        if relation_key in self._LEGACY_FIELD_BY_RELATION or relation_key in {
            "identifiers",
            "titles",
        }:
            return True
        return any(
            _bundle_supports_relation(bundle, relation_key)
            for bundle in (
                self._stack_bundle(level)
                for level in self._LEVEL_ORDER
            )
            if bundle is not None
        )

    def _stack_bundle(self, level: str) -> Any | None:
        get_wemi_metadata = getattr(self._metadata, "get_wemi_metadata", None)
        if not callable(get_wemi_metadata):
            return None
        return get_wemi_metadata(level)

    @classmethod
    def _normalize_relation_key(cls, relation_key: str) -> str:
        normalized = str(relation_key).strip().lower()
        return cls._RELATION_ALIASES.get(normalized, normalized)

    def _raise_if_projection_unloaded(self, relation_key: str) -> None:
        dependencies = self._unloaded_projection_dependencies(relation_key)
        if dependencies:
            raise UnloadedMetadataProjectionError(relation_key, dependencies)

    def _unloaded_projection_dependencies(self, relation_key: str) -> tuple[str, ...]:
        dependencies: list[str] = []
        data = _metadata_data(self._metadata)

        for field in self._LEGACY_FIELD_BY_RELATION.get(relation_key, ()):
            if _is_unloaded_lazy_value(data.get(field)):
                dependencies.append(f"legacy:{field}")

        if (
            relation_key == "identifiers"
            and getattr(self._metadata, "_lazy_identifiers_loaded", True) is False
        ):
            dependencies.append("legacy:identifiers")

        loaders = _lazy_relation_loaders(self._metadata)
        if loaders:
            for level in self._LEVEL_ORDER:
                bundle = self._stack_bundle(level)
                if bundle is None:
                    continue
                try:
                    validated_relation_key = bundle.validate_relation_name(relation_key)
                except KeyError:
                    continue
                if (level, validated_relation_key) in loaders:
                    dependencies.append(f"{level}:{validated_relation_key}")

        return tuple(dependencies)

    def _legacy_field_value(self, field: str) -> Any:
        data = _metadata_data(self._metadata)
        if field in data:
            return data[field]
        get_value = getattr(self._metadata, "get", None)
        if callable(get_value):
            return get_value(field, None)
        return None


class LiuXinWEMITextView(MetadataTextViewAPI):
    """Display/export text projections over a complete WEMI stack."""

    __slots__ = ("_values",)

    def __init__(self, values: MetadataValuesViewAPI) -> None:
        self._values = values

    def relation_text(self, relation_key: str, separator: str = ", ") -> str:
        return separator.join(self._values.relation_values(relation_key))

    @property
    def tags(self) -> str:
        return ", ".join(self._values.tags)

    @property
    def labels(self) -> str:
        return ", ".join(self._values.labels)

    @property
    def genres(self) -> str:
        return ", ".join(self._values.genres)

    @property
    def subjects(self) -> str:
        return ", ".join(self._values.subjects)

    @property
    def series(self) -> str:
        return ", ".join(self._values.series)

    @property
    def title(self) -> str | None:
        return self._values.primary_title

    @property
    def titles(self) -> str:
        return " ; ".join(self._values.titles)

    @property
    def languages(self) -> str:
        return ", ".join(self._values.languages)

    @property
    def ratings(self) -> str:
        return ", ".join(self._values.ratings)

    @property
    def agents(self) -> str:
        return ", ".join(self._values.agents)

    @property
    def agent_names(self) -> str:
        return self.agents


def _dedupe_text(values: Any) -> tuple[str, ...]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if not text or text in seen:
            continue
        result.append(text)
        seen.add(text)
    return tuple(result)


def _target_text(target: Any, relation_key: str) -> str | None:
    if target is None:
        return None
    if isinstance(target, str):
        return _clean_text(target)
    if isinstance(target, (int, float, bool)):
        return _clean_text(str(target))

    candidates = _TEXT_FIELD_CANDIDATES.get(
        str(relation_key),
        _GENERIC_TEXT_FIELD_CANDIDATES,
    )
    text = _first_target_text(target, candidates)
    if text is not None:
        return text

    if _target_mapping(target):
        return None
    return _clean_text(str(target))


def _first_target_text(target: Any, candidates: tuple[str, ...]) -> str | None:
    if target is None:
        return None
    mapping = _target_mapping(target)
    for key in candidates:
        value = _target_value(target, mapping, key)
        text = _clean_text(value)
        if text is not None:
            return text
    return None


def _identifier_pair(target: Any) -> tuple[str, str] | None:
    mapping = _target_mapping(target)
    scheme = _first_target_text(target, _IDENTIFIER_SCHEME_KEYS)
    value = _first_target_text(target, _IDENTIFIER_VALUE_KEYS)
    if scheme is None or value is None:
        if isinstance(target, str) and ":" in target:
            scheme, value = target.split(":", 1)
        elif not mapping:
            return None
        else:
            return None
    scheme = scheme.strip()
    value = value.strip()
    if not scheme or not value:
        return None
    return scheme, value


def _target_mapping(target: Any) -> Mapping[str, Any]:
    if isinstance(target, Mapping):
        return target

    row_dict = getattr(target, "row_dict", None)
    if isinstance(row_dict, Mapping):
        return row_dict

    to_mapping = getattr(target, "to_mapping", None)
    if callable(to_mapping):
        payload = to_mapping()
        if isinstance(payload, Mapping):
            return payload

    return {}


def _target_value(
    target: Any,
    mapping: Mapping[str, Any],
    key: str,
) -> Any:
    if mapping:
        value = mapping.get(key, _MISSING)
        if value is not _MISSING:
            return value
    return getattr(target, key, None)


def _clean_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    return text or None


def _bundle_supports_relation(bundle: Any, relation_key: str) -> bool:
    try:
        bundle.validate_relation_name(relation_key)
    except KeyError:
        return False
    return True


def _iter_text_values(value: Any) -> tuple[str, ...]:
    if value in (None, ""):
        return ()
    if isinstance(value, Mapping):
        return _dedupe_text(value.keys())
    if isinstance(value, (list, tuple, set, frozenset)):
        return _dedupe_text(value)
    return _dedupe_text((value,))


def _iter_rating_values(
    value: Any,
    *,
    suppress_calibre: bool = False,
) -> tuple[str, ...]:
    if value in (None, ""):
        return ()
    if isinstance(value, Mapping):
        values = [
            rating
            for key, rating in value.items()
            if rating not in (None, "")
            and not (suppress_calibre and str(key).casefold() == "calibre")
        ]
        if values:
            return _dedupe_text(values)
        return _dedupe_text(
            key
            for key in value.keys()
            if not (suppress_calibre and str(key).casefold() == "calibre")
        )
    return _iter_text_values(value)


def _append_identifier(
    identifiers: dict[str, list[str]],
    scheme: str,
    value: str,
) -> None:
    scheme_text = str(scheme).strip()
    value_text = str(value).strip()
    if not scheme_text or not value_text:
        return
    values = identifiers.setdefault(scheme_text, [])
    if value_text not in values:
        values.append(value_text)


def _metadata_data(metadata: Any) -> Mapping[str, Any]:
    try:
        data = object.__getattribute__(metadata, "_data")
    except AttributeError:
        return {}
    if isinstance(data, Mapping):
        return data
    return {}


def _lazy_relation_loaders(metadata: Any) -> Mapping[tuple[str, str], Any]:
    try:
        loaders = object.__getattribute__(metadata, "_lazy_relation_loaders")
    except AttributeError:
        return {}
    if isinstance(loaders, Mapping):
        return loaders
    return {}


def _is_unloaded_lazy_value(value: Any) -> bool:
    if getattr(value, "loaded", True) is not False:
        return False
    return callable(getattr(value, "materialize", None))


__all__ = [
    "LiuXinWEMITextView",
    "LiuXinWEMIValuesView",
    "MetadataTextView",
    "MetadataValuesView",
]
