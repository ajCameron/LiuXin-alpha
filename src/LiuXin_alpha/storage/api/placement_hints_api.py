"""Storage-owned placement hint models and metadata projection helpers."""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from typing import Protocol, TypeAlias, cast, runtime_checkable

StorageHintScalar: TypeAlias = str | int | float | bool | None
StorageHintValue: TypeAlias = (
    StorageHintScalar
    | list["StorageHintValue"]
    | tuple["StorageHintValue", ...]
    | Mapping[str, "StorageHintValue"]
)
StorageHintRecord: TypeAlias = Mapping[str, StorageHintValue]
MutableStorageHintRecord: TypeAlias = dict[str, StorageHintValue]


@dataclasses.dataclass(frozen=True, slots=True)
class WorkStorageHints:
    """
    Storage-facing projection of work metadata for placement decisions.

    Example:
        >>> WorkStorageHints(title="Permutation City").title
        'Permutation City'
    """

    work_id: int | None = None
    title: str | None = None
    canonical_title: str | None = None
    sort_title: str | None = None
    work_type: str | None = None
    medium: str | None = None
    primary_agents: tuple[str, ...] = ()
    series: tuple[str, ...] = ()
    genres: tuple[str, ...] = ()
    subjects: tuple[str, ...] = ()
    languages: tuple[str, ...] = ()
    labels: tuple[str, ...] = ()
    manifestation_types: tuple[str, ...] = ()
    file_formats: tuple[str, ...] = ()
    preferred_folder_tokens: tuple[str, ...] = ()
    preferred_filename_stem: str | None = None
    extra: StorageHintRecord = dataclasses.field(default_factory=dict)

    def to_mapping(self) -> MutableStorageHintRecord:
        """
        Return an ordinary mapping suitable for Store-specific inspection.

        Example:
            >>> WorkStorageHints(work_id=5).to_mapping()["work_id"]
            5
        """
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True, slots=True)
class ExpressionStorageHints:
    """
    Storage-facing projection of expression metadata for placement decisions.

    Example:
        >>> ExpressionStorageHints(language_code="en").language_code
        'en'
    """

    expression_id: int | None = None
    work_id: int | None = None
    title: str | None = None
    label: str | None = None
    expression_type: str | None = None
    language_code: str | None = None
    primary_agents: tuple[str, ...] = ()
    genres: tuple[str, ...] = ()
    labels: tuple[str, ...] = ()
    identifiers: tuple[str, ...] = ()
    extra: StorageHintRecord = dataclasses.field(default_factory=dict)

    def to_mapping(self) -> MutableStorageHintRecord:
        """
        Return an ordinary mapping suitable for Store-specific inspection.

        Example:
            >>> ExpressionStorageHints(work_id=5).to_mapping()["work_id"]
            5
        """
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True, slots=True)
class ManifestationStorageHints:
    """
    Storage-facing projection of manifestation metadata for placement decisions.

    Example:
        >>> ManifestationStorageHints(format_detail="EPUB").format_detail
        'EPUB'
    """

    manifestation_id: int | None = None
    expression_id: int | None = None
    title: str | None = None
    edition_statement: str | None = None
    format_detail: str | None = None
    carrier_type: str | None = None
    publication_year: int | None = None
    primary_agents: tuple[str, ...] = ()
    identifiers: tuple[str, ...] = ()
    file_formats: tuple[str, ...] = ()
    extra: StorageHintRecord = dataclasses.field(default_factory=dict)

    def to_mapping(self) -> MutableStorageHintRecord:
        """
        Return an ordinary mapping suitable for Store-specific inspection.

        Example:
            >>> ManifestationStorageHints(publication_year=1994).to_mapping()["publication_year"]
            1994
        """
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True, slots=True)
class ItemStorageHints:
    """
    Storage-facing projection of item metadata for placement decisions.

    Example:
        >>> ItemStorageHints(preferred_storage_key="books/5.epub").preferred_storage_key
        'books/5.epub'
    """

    item_id: int | None = None
    manifestation_id: int | None = None
    expression_id: int | None = None
    work_id: int | None = None
    title: str | None = None
    canonical_title: str | None = None
    sort_title: str | None = None
    subtitle: str | None = None
    item_type: str | None = None
    item_location: str | None = None
    inventory_code: str | None = None
    lifecycle_status: str | None = None
    condition: str | None = None
    source: str | None = None
    source_name: str | None = None
    source_path: str | None = None
    primary_agents: tuple[str, ...] = ()
    series: tuple[str, ...] = ()
    genres: tuple[str, ...] = ()
    subjects: tuple[str, ...] = ()
    languages: tuple[str, ...] = ()
    labels: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()
    attachment_roles: tuple[str, ...] = ()
    digital_asset_kinds: tuple[str, ...] = ()
    replica_modes: tuple[str, ...] = ()
    file_formats: tuple[str, ...] = ()
    preferred_folder_tokens: tuple[str, ...] = ()
    preferred_filename_stem: str | None = None
    preferred_storage_key: str | None = None
    extra: StorageHintRecord = dataclasses.field(default_factory=dict)

    def to_mapping(self) -> MutableStorageHintRecord:
        """
        Return an ordinary mapping suitable for Store-specific inspection.

        Example:
            >>> ItemStorageHints(item_id=44).to_mapping()["item_id"]
            44
        """
        return dataclasses.asdict(self)


StoragePlacementHints: TypeAlias = (
    WorkStorageHints
    | ExpressionStorageHints
    | ManifestationStorageHints
    | ItemStorageHints
    | StorageHintRecord
)


@runtime_checkable
class StorageHintProvider(Protocol):
    """
    Structural storage-side protocol for objects that already provide hints.

    Example:
        >>> isinstance(WorkStorageHints(), StorageHintProvider)
        False
    """

    def storage_hints(self) -> StoragePlacementHints:
        """
        Return storage placement hints.

        Example:
            >>> hints = provider.storage_hints()  # doctest: +SKIP
        """
        ...


@runtime_checkable
class StorageHintMetadataSource(Protocol):
    """
    Structural source that storage can project into placement hints.

    Example:
        >>> links = metadata.get_relation_links("works")  # doctest: +SKIP
    """

    def get_relation_links(self, relation: str) -> Iterable[object]:
        """
        Return relation links for a metadata relation name.

        Example:
            >>> list(metadata.get_relation_links("works"))  # doctest: +SKIP
        """
        ...


StorageHintSource: TypeAlias = (
    StoragePlacementHints | StorageHintProvider | StorageHintMetadataSource
)


def derive_storage_hints(
    metadata: StorageHintSource,
) -> StoragePlacementHints | None:
    """
    Derive storage placement hints from a metadata-like object.

    Existing hint values pass through unchanged. Metadata containers are read
    structurally, keeping the metadata package independent from storage.
    Broken optional providers produce ``None`` so hints remain advisory.

    Example:
        >>> hints = WorkStorageHints(work_id=5)
        >>> derive_storage_hints(hints) is hints
        True
    """

    if isinstance(
        metadata,
        (
            WorkStorageHints,
            ExpressionStorageHints,
            ManifestationStorageHints,
            ItemStorageHints,
        ),
    ):
        return metadata

    if isinstance(metadata, Mapping):
        return metadata

    hints_fn = getattr(metadata, "storage_hints", None)
    if callable(hints_fn):
        try:
            hints = hints_fn()
        except Exception:
            return None
        if isinstance(
            hints,
            (
                WorkStorageHints,
                ExpressionStorageHints,
                ManifestationStorageHints,
                ItemStorageHints,
            ),
        ):
            return hints
        if isinstance(hints, Mapping):
            return cast(StorageHintRecord, hints)

    if _has_relation_bundle(metadata, "item"):
        return _derive_item_storage_hints(metadata)
    if _has_relation_bundle(metadata, "work"):
        return _derive_work_storage_hints(metadata)
    if _has_relation_bundle(metadata, "manifestation"):
        return _derive_manifestation_storage_hints(metadata)
    if _has_relation_bundle(metadata, "expression"):
        return _derive_expression_storage_hints(metadata)
    return None


def _has_relation_bundle(metadata: object, entity_attr: str) -> bool:
    """
    Test whether a value exposes one WEMI entity and relation access.

    Example:
        >>> _has_relation_bundle(object(), "work")
        False
    """
    return hasattr(metadata, entity_attr) and callable(
        getattr(metadata, "get_relation_links", None)
    )


def _derive_work_storage_hints(metadata: object) -> WorkStorageHints:
    """
    Project a work metadata container into placement hints.

    Example:
        >>> hints = _derive_work_storage_hints(object())
        >>> isinstance(hints, WorkStorageHints)
        True
    """
    work_map = _rowish_to_mapping(getattr(metadata, "work", None))
    expression_links = _relation_links(metadata, "expressions")
    manifestation_links = _relation_links(metadata, "manifestations")
    file_links = _relation_links(metadata, "files")
    image_links = _relation_links(metadata, "images")
    agent_links = _relation_links(metadata, "agents")

    title = _value_from_mapping(work_map, ("work_canonical_title", "work_title"))
    if title in (None, "") and expression_links:
        title = _display_value(_target(expression_links[0]))

    canonical_title = _value_from_mapping(work_map, ("work_canonical_title", "work_title"))
    sort_title = _value_from_mapping(
        work_map,
        ("work_sort_title", "work_canonical_title", "work_title"),
    )
    primary_agents = _link_display_values(agent_links)
    series = _link_display_values(_relation_links(metadata, "series"))
    genres = _link_display_values(_relation_links(metadata, "genres"))
    subjects = _link_display_values(_relation_links(metadata, "subjects"))
    languages = _link_display_values(_relation_links(metadata, "languages"))
    labels = _link_display_values(_relation_links(metadata, "labels"))

    file_formats: list[str] = []
    for token in _format_candidates_from_links(
        manifestation_links + file_links + image_links,
        ("manifestation_format_detail", "file_extension", "image_extension"),
    ):
        if token not in file_formats:
            file_formats.append(token)

    preferred_folder_tokens: list[str] = []
    if primary_agents:
        preferred_folder_tokens.extend(primary_agents)
    elif series:
        preferred_folder_tokens.extend(series)
    if title not in (None, ""):
        preferred_folder_tokens.append(str(title))

    return WorkStorageHints(
        work_id=_optional_int(_value_from_mapping(work_map, ("work_id",))),
        title=_optional_str(title),
        canonical_title=_optional_str(canonical_title),
        sort_title=_optional_str(sort_title),
        work_type=_optional_str(_value_from_mapping(work_map, ("work_type",))),
        medium=_optional_str(_value_from_mapping(work_map, ("work_medium",))),
        primary_agents=primary_agents,
        series=series,
        genres=genres,
        subjects=subjects,
        languages=languages,
        labels=labels,
        manifestation_types=_manifestation_types_from_links(manifestation_links),
        file_formats=tuple(file_formats),
        preferred_folder_tokens=tuple(preferred_folder_tokens),
        preferred_filename_stem=_preferred_filename_stem(_optional_str(title), primary_agents),
        extra={
            "expression_count": len(expression_links),
            "manifestation_count": len(manifestation_links),
            "item_count": len(_relation_links(metadata, "items")),
            "file_count": len(file_links),
            "image_count": len(image_links),
            "identifier_count": len(_relation_links(metadata, "identifiers")),
        },
    )


def _derive_item_storage_hints(metadata: object) -> ItemStorageHints:
    """
    Project an item metadata container into placement hints.

    Example:
        >>> hints = _derive_item_storage_hints(object())
        >>> isinstance(hints, ItemStorageHints)
        True
    """
    work = _first_target(_relation_links(metadata, "works"))
    expression = _first_target(_relation_links(metadata, "expressions"))
    manifestation = _first_target(_relation_links(metadata, "manifestations"))

    work_map = _rowish_to_mapping(work)
    expression_map = _rowish_to_mapping(expression)
    manifestation_map = _rowish_to_mapping(manifestation)
    item = getattr(metadata, "item", None)
    item_map = _rowish_to_mapping(item)

    title = _value_from_mapping(expression_map, ("expression_title_override",))
    if title in (None, ""):
        title = _value_from_mapping(work_map, ("work_canonical_title", "work_title"))
    if title in (None, ""):
        title = _value_from_mapping(item_map, ("item_source_name",))
        if title not in (None, ""):
            title = Path(str(title)).stem

    canonical_title = _value_from_mapping(work_map, ("work_canonical_title", "work_title"))
    sort_title = _value_from_mapping(
        work_map,
        ("work_sort_title", "work_canonical_title", "work_title"),
    )
    subtitle = _value_from_mapping(manifestation_map, ("manifestation_subtitle",))
    if subtitle in (None, ""):
        subtitle = _value_from_mapping(expression_map, ("expression_subtitle",))

    agent_links = _relation_links(metadata, "agents")
    primary_agents = _link_display_values(agent_links, primary_only=True)
    if not primary_agents:
        primary_agents = _link_display_values(agent_links)

    file_links = _relation_links(metadata, "files")
    image_links = _relation_links(metadata, "images")
    digital_asset_links = _relation_links(metadata, "digital_assets")
    replica_links = _relation_links(metadata, "asset_replicas")

    file_formats: list[str] = []
    for token in _format_candidates_from_links(
        _relation_links(metadata, "manifestations")
        + file_links
        + image_links
        + digital_asset_links
        + replica_links,
        (
            "file_extension",
            "image_extension",
            "manifestation_format_detail",
            "digital_asset_extension",
            "asset_replica_extension",
        ),
    ):
        if token not in file_formats:
            file_formats.append(token)

    preferred_folder_tokens: list[str] = []
    if primary_agents:
        preferred_folder_tokens.extend(primary_agents)
    else:
        preferred_folder_tokens.extend(_link_display_values(_relation_links(metadata, "series")))
    if title not in (None, ""):
        preferred_folder_tokens.append(str(title))

    return ItemStorageHints(
        item_id=_optional_int(_value_from_mapping(item_map, ("item_id",))),
        manifestation_id=_optional_int(
            _value_from_mapping(manifestation_map, ("manifestation_id",))
            or _value_from_mapping(item_map, ("item_manifestation_id",))
        ),
        expression_id=_optional_int(_value_from_mapping(expression_map, ("expression_id",))),
        work_id=_optional_int(_value_from_mapping(work_map, ("work_id",))),
        title=_optional_str(title),
        canonical_title=_optional_str(canonical_title),
        sort_title=_optional_str(sort_title),
        subtitle=_optional_str(subtitle),
        item_type=_optional_str(_value_from_mapping(item_map, ("item_type",))),
        item_location=_optional_str(_value_from_mapping(item_map, ("item_location",))),
        inventory_code=_optional_str(_value_from_mapping(item_map, ("item_inventory_code",))),
        lifecycle_status=_optional_str(_value_from_mapping(item_map, ("item_lifecycle_status",))),
        condition=_optional_str(_value_from_mapping(item_map, ("item_condition",))),
        source=_optional_str(_value_from_mapping(item_map, ("item_source",))),
        source_name=_optional_str(_value_from_mapping(item_map, ("item_source_name",))),
        source_path=_optional_str(_value_from_mapping(item_map, ("item_source_path",))),
        primary_agents=primary_agents,
        series=_link_display_values(_relation_links(metadata, "series")),
        genres=_link_display_values(_relation_links(metadata, "genres")),
        subjects=_link_display_values(_relation_links(metadata, "subjects")),
        languages=_link_display_values(_relation_links(metadata, "languages")),
        labels=_link_display_values(_relation_links(metadata, "labels")),
        tags=_link_display_values(_relation_links(metadata, "tags")),
        attachment_roles=_item_attachment_roles(file_links, image_links),
        digital_asset_kinds=_item_digital_asset_kinds(digital_asset_links),
        replica_modes=_item_replica_modes(replica_links),
        file_formats=tuple(file_formats),
        preferred_folder_tokens=tuple(preferred_folder_tokens),
        preferred_filename_stem=_preferred_filename_stem(
            _optional_str(title),
            primary_agents,
            _optional_str(_value_from_mapping(item_map, ("item_source_name",))),
        ),
        preferred_storage_key=_preferred_storage_key(replica_links, file_links, image_links),
        extra={
            "work_count": len(_relation_links(metadata, "works")),
            "expression_count": len(_relation_links(metadata, "expressions")),
            "manifestation_count": len(_relation_links(metadata, "manifestations")),
            "file_count": len(file_links),
            "image_count": len(image_links),
            "digital_asset_count": len(digital_asset_links),
            "replica_count": len(replica_links),
        },
    )


def _derive_expression_storage_hints(metadata: object) -> ExpressionStorageHints:
    """
    Project an expression metadata container into placement hints.

    Example:
        >>> hints = _derive_expression_storage_hints(object())
        >>> isinstance(hints, ExpressionStorageHints)
        True
    """
    expression = getattr(metadata, "expression", None)
    language_links = _relation_links(metadata, "languages")
    return ExpressionStorageHints(
        expression_id=_optional_int(
            _value_from_mapping(
                _rowish_to_mapping(expression), ("expression_id",)
            )
        ),
        work_id=_optional_int(
            _value_from_mapping(
                _rowish_to_mapping(expression), ("expression_work_id",)
            )
        ),
        title=_optional_str(
            _value_from_mapping(
                _rowish_to_mapping(expression),
                ("expression_title_override",),
            )
        ),
        label=_optional_str(
            _value_from_mapping(
                _rowish_to_mapping(expression), ("expression_label",)
            )
        ),
        expression_type=_optional_str(
            _value_from_mapping(
                _rowish_to_mapping(expression), ("expression_type",)
            )
        ),
        language_code=_display_value(_target(language_links[0])) if language_links else None,
        primary_agents=_primary_agent_values(_relation_links(metadata, "agents")),
        genres=_link_display_values(_relation_links(metadata, "genres")),
        labels=_link_display_values(_relation_links(metadata, "labels")),
        identifiers=_link_display_values(_relation_links(metadata, "identifiers")),
    )


def _derive_manifestation_storage_hints(metadata: object) -> ManifestationStorageHints:
    """
    Project a manifestation metadata container into placement hints.

    Example:
        >>> hints = _derive_manifestation_storage_hints(object())
        >>> isinstance(hints, ManifestationStorageHints)
        True
    """
    manifestation = getattr(metadata, "manifestation", None)
    manifestation_map = _rowish_to_mapping(manifestation)
    title_links = _relation_links(metadata, "titles")
    return ManifestationStorageHints(
        manifestation_id=_optional_int(
            _value_from_mapping(manifestation_map, ("manifestation_id",))
        ),
        expression_id=_optional_int(
            _value_from_mapping(
                manifestation_map, ("manifestation_expression_id",)
            )
        ),
        title=_display_value(_target(title_links[0])) if title_links else None,
        edition_statement=_optional_str(
            _value_from_mapping(
                manifestation_map, ("manifestation_edition_statement",)
            )
        ),
        format_detail=_optional_str(
            _value_from_mapping(
                manifestation_map, ("manifestation_format_detail",)
            )
        ),
        carrier_type=_optional_str(
            _value_from_mapping(
                manifestation_map, ("manifestation_carrier_type",)
            )
        ),
        publication_year=_optional_int(
            _value_from_mapping(manifestation_map, ("manifestation_pub_year",))
        ),
        primary_agents=_primary_agent_values(_relation_links(metadata, "agents")),
        identifiers=_link_display_values(_relation_links(metadata, "identifiers")),
        file_formats=_link_display_values(_relation_links(metadata, "files")),
    )


def _relation_links(metadata: object, relation: str) -> list[object]:
    """
    Read one relation without making optional placement metadata mandatory.

    Example:
        >>> _relation_links(object(), "works")
        []
    """
    getter = getattr(metadata, "get_relation_links", None)
    if not callable(getter):
        return []
    try:
        relation_getter = cast(
            Callable[[str], Iterable[object]],
            getter,
        )
        return list(relation_getter(relation))
    except (KeyError, ValueError, TypeError):
        return []


def _target(link: object) -> object:
    """
    Return a relation link's target when present.

    Example:
        >>> _target(object()) is None
        True
    """
    return getattr(link, "target", None)


def _primary(link: object) -> bool:
    """
    Return whether a relation link identifies its primary target.

    Example:
        >>> _primary(object())
        False
    """
    return bool(getattr(link, "primary", False))


def _first_target(links: list[object]) -> object | None:
    """
    Prefer a primary target, otherwise return the first target.

    Example:
        >>> _first_target([]) is None
        True
    """
    if not links:
        return None
    for link in links:
        if _primary(link):
            return _target(link)
    return _target(links[0])


def _rowish_to_mapping(value: object) -> Mapping[str, object]:
    """
    Obtain a mapping from a row-like or mapping-like value.

    Example:
        >>> _rowish_to_mapping({"title": "Book"})["title"]
        'Book'
    """
    if value is None:
        return {}
    row_dict = getattr(value, "row_dict", None)
    if isinstance(row_dict, Mapping):
        return row_dict
    to_mapping = getattr(value, "to_mapping", None)
    if callable(to_mapping):
        mapping = to_mapping()
        if isinstance(mapping, Mapping):
            return mapping
    if isinstance(value, Mapping):
        return value
    return {}


def _value_from_mapping(mapping: Mapping[str, object], keys: tuple[str, ...]) -> object:
    """
    Return the first non-empty candidate from a mapping.

    Example:
        >>> _value_from_mapping({"title": "Book"}, ("name", "title"))
        'Book'
    """
    for key in keys:
        value = mapping.get(key)
        if value not in (None, ""):
            return value
    return None


def _display_value(value: object) -> str | None:
    """
    Derive a useful display token from a relation target.

    Example:
        >>> _display_value({"agent_canonical_name": "Greg Egan"})
        'Greg Egan'
    """
    mapping = _rowish_to_mapping(value)
    if mapping:
        for key in (
            "agent_canonical_name",
            "agent_sort_name",
            "work_canonical_title",
            "work_title",
            "expression_title_override",
            "expression_label",
            "manifestation_edition_statement",
            "manifestation_format_detail",
            "manifestation_carrier_type",
            "series",
            "series_name",
            "genre",
            "subject",
            "tag",
            "label",
            "language",
            "language_name",
            "language_code",
            "folder_name",
            "folder_relpath",
            "store_name",
            "store_root_uri",
            "file_name",
            "image_name",
            "digital_asset_name",
            "digital_asset_base_name",
            "composite_digital_asset_name",
            "identifier_value",
            "annotation_selected_text",
            "annotation_note_text",
            "note",
            "comment",
            "synopsis",
            "rating_label",
        ):
            found = mapping.get(key)
            if found not in (None, ""):
                return str(found)
        for key, item in mapping.items():
            if str(key).endswith("_id") or str(key).endswith("_timestamp_ep_k"):
                continue
            if item not in (None, ""):
                return str(item)
    if value in (None, ""):
        return None
    return str(value)


def _link_display_values(
    links: Iterable[object],
    *,
    primary_only: bool = False,
    unique: bool = True,
) -> tuple[str, ...]:
    """
    Collect stable display values from relation links.

    Example:
        >>> _link_display_values(())
        ()
    """
    values: list[str] = []
    seen: set[str] = set()
    for link in links:
        if primary_only and not _primary(link):
            continue
        display = _display_value(_target(link))
        if not display:
            continue
        if unique and display in seen:
            continue
        seen.add(display)
        values.append(display)
    return tuple(values)


def _primary_agent_values(links: list[object]) -> tuple[str, ...]:
    """
    Collect display values for primary agent links.

    Example:
        >>> _primary_agent_values([])
        ()
    """
    return tuple(
        filter(
            None,
            (_display_value(_target(link)) for link in links if _primary(link) or len(links) == 1),
        )
    )


def _manifestation_types_from_links(links: Iterable[object]) -> tuple[str, ...]:
    """
    Collect unique carrier or manifestation types from links.

    Example:
        >>> _manifestation_types_from_links(())
        ()
    """
    values: list[str] = []
    seen: set[str] = set()
    for link in links:
        mapping = _rowish_to_mapping(_target(link))
        raw = _value_from_mapping(
            mapping,
            (
                "manifestation_carrier_type",
                "manifestation_type",
                "manifestation_binding_type",
            ),
        )
        if raw in (None, ""):
            continue
        text = str(raw)
        if text in seen:
            continue
        seen.add(text)
        values.append(text)
    return tuple(values)


def _format_candidates_from_links(
    links: Iterable[object],
    keys: tuple[str, ...],
) -> tuple[str, ...]:
    """
    Collect normalized format candidates from relation targets.

    Example:
        >>> _format_candidates_from_links((), ("file_extension",))
        ()
    """
    values: list[str] = []
    seen: set[str] = set()
    for link in links:
        mapping = _rowish_to_mapping(_target(link))
        for key in keys:
            raw = mapping.get(key)
            if raw in (None, ""):
                continue
            token = str(raw).strip().lower()
            if token in seen:
                continue
            seen.add(token)
            values.append(token.upper())
    return tuple(values)


def _item_attachment_roles(file_links: list[object], image_links: list[object]) -> tuple[str, ...]:
    """
    Collect unique file and image attachment roles.

    Example:
        >>> _item_attachment_roles([], [])
        ()
    """
    roles: list[str] = []
    for link in file_links + image_links:
        mapping = _rowish_to_mapping(_target(link))
        role = _value_from_mapping(mapping, ("file_role", "image_role"))
        if role not in (None, "") and str(role) not in roles:
            roles.append(str(role))
    return tuple(roles)


def _item_digital_asset_kinds(links: list[object]) -> tuple[str, ...]:
    """
    Collect unique linked Digital Asset kind tokens.

    Example:
        >>> _item_digital_asset_kinds([])
        ()
    """
    kinds: list[str] = []
    for link in links:
        mapping = _rowish_to_mapping(_target(link))
        kind = _value_from_mapping(
            mapping,
            (
                "digital_asset_media_category",
                "digital_asset_mime_type",
                "digital_asset_extension",
            ),
        )
        if kind not in (None, "") and str(kind) not in kinds:
            kinds.append(str(kind))
    return tuple(kinds)


def _item_replica_modes(links: list[object]) -> tuple[str, ...]:
    """
    Collect unique Replica mode tokens.

    Example:
        >>> _item_replica_modes([])
        ()
    """
    modes: list[str] = []
    for link in links:
        mode = _value_from_mapping(_rowish_to_mapping(_target(link)), ("asset_replica_mode",))
        if mode not in (None, "") and str(mode) not in modes:
            modes.append(str(mode))
    return tuple(modes)


def _preferred_storage_key(*relations: list[object]) -> str | None:
    """
    Return the first existing storage key suggested by linked records.

    Example:
        >>> _preferred_storage_key([]) is None
        True
    """
    for links in relations:
        for link in links:
            mapping = _rowish_to_mapping(_target(link))
            for key in ("file_storage_key", "image_storage_key", "asset_replica_storage_key"):
                value = mapping.get(key)
                if value not in (None, ""):
                    return str(value)
    return None


def _preferred_filename_stem(
    title: str | None,
    primary_agents: tuple[str, ...],
    source_name: str | None = None,
) -> str | None:
    """
    Build a readable filename stem from title and authors.

    Example:
        >>> _preferred_filename_stem("Book", ("Author",))
        'Book - Author'
    """
    if title and primary_agents:
        return "{} - {}".format(title, " & ".join(primary_agents))
    if title:
        return title
    if source_name:
        return Path(str(source_name)).stem
    return None


def _optional_str(value: object) -> str | None:
    """
    Convert a non-empty value into an optional string.

    Example:
        >>> _optional_str(5)
        '5'
    """
    if value in (None, ""):
        return None
    return str(value)


def _optional_int(value: object) -> int | None:
    """
    Convert a simple scalar into an optional integer.

    Example:
        >>> _optional_int("5")
        5
    """
    if value in (None, ""):
        return None
    try:
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float, str)):
            return int(value)
    except (TypeError, ValueError):
        return None
    return None


__all__ = [
    "ExpressionStorageHints",
    "ItemStorageHints",
    "ManifestationStorageHints",
    "MutableStorageHintRecord",
    "StorageHintMetadataSource",
    "StorageHintProvider",
    "StorageHintRecord",
    "StorageHintScalar",
    "StorageHintSource",
    "StorageHintValue",
    "StoragePlacementHints",
    "WorkStorageHints",
    "derive_storage_hints",
]
