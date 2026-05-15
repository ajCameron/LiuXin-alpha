"""Read-only projection views for WEMI metadata bundles."""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, Protocol

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api import (
    MetadataTextViewAPI,
    MetadataValuesViewAPI,
    ProjectionIdentifierMap,
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


__all__ = [
    "MetadataTextView",
    "MetadataValuesView",
]
