"""
Represent advisory placement hints and project structural WEMI metadata.

Hint values carry caller-supplied or derived suggestions without allocating storage.
Projection keeps metadata independent from storage imports and preserves explicit
selection/failure boundaries; it does not validate destinations or snapshot relations.
"""

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
    Carry Work-level titles, agents, facets, and suggested placement components. The Work projector
    includes all distinct agent displays in primary_agents and normalizes format candidates to
    uppercase.

    Construction performs no field validation, normalization, or recursive freezing. Frozen
    attributes prevent reassignment; nested values such as extra remain mutable, and annotations do
    not coerce supplied containers.

    Example:
        >>> WorkStorageHints(title="Permutation City").title
        'Permutation City'


    :ivar work_id: Optional catalogue Work identifier; no existence or positivity check is made.
    :ivar title: Optional display title selected for placement or filename suggestions.
    :ivar canonical_title: Optional preferred Work title, distinct from an Expression title override.
    :ivar sort_title: Optional Work sorting title; no sort-key normalization is applied here.
    :ivar work_type: Optional Work classification such as novel; vocabulary is not checked.
    :ivar medium: Optional Work medium such as text; vocabulary is not checked.
    :ivar primary_agents: Ordered agent display strings; projection-specific selection determines which agents appear.
    :ivar series: Ordered series display strings usable as a folder fallback.
    :ivar genres: Ordered genre display strings, without vocabulary validation.
    :ivar subjects: Ordered subject display strings, without vocabulary validation.
    :ivar languages: Ordered language displays, which need not be standardized language codes.
    :ivar labels: Ordered operational label displays.
    :ivar manifestation_types: Ordered carrier/type/binding suggestions from linked Manifestations.
    :ivar file_formats: Ordered format suggestions whose normalization depends on the producing projection.
    :ivar preferred_folder_tokens: Suggested path components in placement order; no filesystem sanitization is applied.
    :ivar preferred_filename_stem: Optional suggested basename without an added extension; no filename policy is enforced.
    :ivar extra: Additional producer-specific hints; the default is a fresh mutable dictionary.
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
        Convert every dataclass field to a new dictionary using dataclasses.asdict. Nested
        dataclasses and containers are recursively converted and other values are deep-copied; this
        is neither a shallow view nor a JSON serialization guarantee. Conversion/copy failures
        propagate.

        Example:
            >>> hints = WorkStorageHints(extra={"source": "catalogue"})
            >>> mapping = hints.to_mapping()
            >>> mapping["extra"] is hints.extra
            False
            >>> mapping["extra"]["source"]
            'catalogue'


        :return: New dictionary of all fields, retaining tuple/list container forms according to dataclasses.asdict.
        """
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True, slots=True)
class ExpressionStorageHints:
    """
    Carry Expression identity, title override, language, agents, and facets. Structural projection
    uses the first language display for language_code and retains primary agent links or a sole
    agent link.

    Construction performs no field validation, normalization, or recursive freezing. Frozen
    attributes prevent reassignment; nested values such as extra remain mutable, and annotations do
    not coerce supplied containers.

    Example:
        >>> ExpressionStorageHints(language_code="en").language_code
        'en'


    :ivar expression_id: Optional catalogue Expression identifier, without a referential-integrity check.
    :ivar work_id: Optional catalogue Work identifier; no existence or positivity check is made.
    :ivar title: Optional display title selected for placement or filename suggestions.
    :ivar label: Optional Expression display label, separate from its title override.
    :ivar expression_type: Optional Expression classification such as translation.
    :ivar language_code: Optional language hint; structural projection supplies a display that may be a name rather than a code.
    :ivar primary_agents: Ordered agent display strings; projection-specific selection determines which agents appear.
    :ivar genres: Ordered genre display strings, without vocabulary validation.
    :ivar labels: Ordered operational label displays.
    :ivar identifiers: Ordered identifier display strings, without scheme or uniqueness validation at construction.
    :ivar extra: Additional producer-specific hints; the default is a fresh mutable dictionary.
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
        Convert every dataclass field to a new dictionary using dataclasses.asdict. Nested
        dataclasses and containers are recursively converted and other values are deep-copied; this
        is neither a shallow view nor a JSON serialization guarantee. Conversion/copy failures
        propagate.

        Example:
            >>> hints = ExpressionStorageHints(extra={"source": "catalogue"})
            >>> mapping = hints.to_mapping()
            >>> mapping["extra"] is hints.extra
            False
            >>> mapping["extra"]["source"]
            'catalogue'


        :return: New dictionary of all fields, retaining tuple/list container forms according to dataclasses.asdict.
        """
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True, slots=True)
class ManifestationStorageHints:
    """
    Carry edition, carrier, publication, and linked-file suggestions. Structural projection derives
    title from the first title link and file_formats from file display values, which may be
    filenames or lowercase extensions.

    Construction performs no field validation, normalization, or recursive freezing. Frozen
    attributes prevent reassignment; nested values such as extra remain mutable, and annotations do
    not coerce supplied containers.

    Example:
        >>> ManifestationStorageHints(format_detail="EPUB").format_detail
        'EPUB'


    :ivar manifestation_id: Optional catalogue Manifestation identifier, without a referential-integrity check.
    :ivar expression_id: Optional catalogue Expression identifier, without a referential-integrity check.
    :ivar title: Optional display title selected for placement or filename suggestions.
    :ivar edition_statement: Optional human-readable edition description.
    :ivar format_detail: Optional detailed Manifestation format text, separate from linked file displays.
    :ivar carrier_type: Optional Manifestation carrier classification such as ebook.
    :ivar publication_year: Optional publication year; no calendar-range validation is applied.
    :ivar primary_agents: Ordered agent display strings; projection-specific selection determines which agents appear.
    :ivar identifiers: Ordered identifier display strings, without scheme or uniqueness validation at construction.
    :ivar file_formats: Ordered format suggestions whose normalization depends on the producing projection.
    :ivar extra: Additional producer-specific hints; the default is a fresh mutable dictionary.
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
        Convert every dataclass field to a new dictionary using dataclasses.asdict. Nested
        dataclasses and containers are recursively converted and other values are deep-copied; this
        is neither a shallow view nor a JSON serialization guarantee. Conversion/copy failures
        propagate.

        Example:
            >>> hints = ManifestationStorageHints(extra={"source": "catalogue"})
            >>> mapping = hints.to_mapping()
            >>> mapping["extra"] is hints.extra
            False
            >>> mapping["extra"]["source"]
            'catalogue'


        :return: New dictionary of all fields, retaining tuple/list container forms according to dataclasses.asdict.
        """
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True, slots=True)
class ItemStorageHints:
    """
    Carry Item provenance and related WEMI hints for physical placement. Suggested keys and
    components remain advisory; a Store owns destination interpretation and publication policy.

    Construction performs no field validation, normalization, or recursive freezing. Frozen
    attributes prevent reassignment; nested values such as extra remain mutable, and annotations do
    not coerce supplied containers.

    Example:
        >>> ItemStorageHints(preferred_storage_key="books/5.epub").preferred_storage_key
        'books/5.epub'


    :ivar item_id: Optional catalogue Item identifier, without a referential-integrity check.
    :ivar manifestation_id: Optional catalogue Manifestation identifier, without a referential-integrity check.
    :ivar expression_id: Optional catalogue Expression identifier, without a referential-integrity check.
    :ivar work_id: Optional catalogue Work identifier; no existence or positivity check is made.
    :ivar title: Optional display title selected for placement or filename suggestions.
    :ivar canonical_title: Optional preferred Work title, distinct from an Expression title override.
    :ivar sort_title: Optional Work sorting title; no sort-key normalization is applied here.
    :ivar subtitle: Optional subtitle selected from the Manifestation before the Expression.
    :ivar item_type: Optional Item classification such as digital.
    :ivar item_location: Optional descriptive Item location, without Store address validation.
    :ivar inventory_code: Optional library inventory label for the Item.
    :ivar lifecycle_status: Optional Item lifecycle label; no transition policy is enforced.
    :ivar condition: Optional descriptive Item condition.
    :ivar source: Optional Item source category or provenance label.
    :ivar source_name: Optional original source name, usable as a filename-stem fallback.
    :ivar source_path: Optional source-path text, without path resolution or an existence check.
    :ivar primary_agents: Ordered agent display strings; projection-specific selection determines which agents appear.
    :ivar series: Ordered series display strings usable as a folder fallback.
    :ivar genres: Ordered genre display strings, without vocabulary validation.
    :ivar subjects: Ordered subject display strings, without vocabulary validation.
    :ivar languages: Ordered language displays, which need not be standardized language codes.
    :ivar labels: Ordered operational label displays.
    :ivar tags: Ordered descriptive tag displays.
    :ivar attachment_roles: Ordered role suggestions from linked files and images.
    :ivar digital_asset_kinds: Ordered media-category, MIME-type, or extension suggestions from linked Assets.
    :ivar replica_modes: Ordered Replica mode strings, without policy validation.
    :ivar file_formats: Ordered format suggestions whose normalization depends on the producing projection.
    :ivar preferred_folder_tokens: Suggested path components in placement order; no filesystem sanitization is applied.
    :ivar preferred_filename_stem: Optional suggested basename without an added extension; no filename policy is enforced.
    :ivar preferred_storage_key: Optional opaque key suggestion; it is not validated, reserved, or checked for existence.
    :ivar extra: Additional producer-specific hints; the default is a fresh mutable dictionary.
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
        Convert every dataclass field to a new dictionary using dataclasses.asdict. Nested
        dataclasses and containers are recursively converted and other values are deep-copied; this
        is neither a shallow view nor a JSON serialization guarantee. Conversion/copy failures
        propagate.

        Example:
            >>> hints = ItemStorageHints(extra={"source": "catalogue"})
            >>> mapping = hints.to_mapping()
            >>> mapping["extra"] is hints.extra
            False
            >>> mapping["extra"]["source"]
            'catalogue'


        :return: New dictionary of all fields, retaining tuple/list container forms according to dataclasses.asdict.
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
    Describe an object that supplies an already projected placement value. Runtime protocol checks
    establish member presence, not return-type correctness or successful execution. Hint dataclasses
    expose to_mapping rather than this provider method.

    Example:
        >>> isinstance(WorkStorageHints(), StorageHintProvider)
        False
    """

    def storage_hints(self) -> StoragePlacementHints:
        """
        Supply a recognized hint dataclass or a storage hint mapping. Implementations own any
        metadata reads and errors; derive_storage_hints suppresses Exceptions from this call, but
        direct callers receive the implementation behavior.

        Example:
            >>> hints = provider.storage_hints()  # doctest: +SKIP


        :return: Advisory placement hints; no destination allocation or reservation is implied.
        """
        ...


@runtime_checkable
class StorageHintMetadataSource(Protocol):
    """
    Describe the relation-access seam used by structural WEMI projections. A matching object also
    needs the relevant item, work, manifestation, or expression attribute for dispatch. Runtime
    protocol checks do not validate relation contents or their lifetime.

    Example:
        >>> from types import SimpleNamespace
        >>> source = SimpleNamespace(get_relation_links=lambda relation: ())
        >>> isinstance(source, StorageHintMetadataSource)
        True
    """

    def get_relation_links(self, relation: str) -> Iterable[object]:
        """
        Expose ordered links for one named metadata relation. Projection expects each link to offer
        target and optionally primary attributes. Implementations choose whether reads are lazy,
        repeatable, or capable of raising; this protocol provides no snapshot.

        Example:
            >>> links = metadata.get_relation_links("works")  # doctest: +SKIP


        :param relation: Relation collection name understood by the source, such as works, agents, or files.
        :return: Iterable of relation-link objects; projection helpers consume it into a list.
        """
        ...


StorageHintSource: TypeAlias = (
    StoragePlacementHints | StorageHintProvider | StorageHintMetadataSource
)


def derive_storage_hints(
    metadata: StorageHintSource,
) -> StoragePlacementHints | None:
    """
    Obtain advisory placement hints by identity, provider call, or structural WEMI projection. Known
    hint dataclasses and Mapping values return unchanged without validating contents. Otherwise, a
    callable storage_hints provider runs first: an Exception from invocation returns None, while an
    unrecognized result falls through to structural projection. Provider attribute lookup occurs
    outside that guard.

    Structural dispatch prefers item, work, manifestation, then expression bundles. Relation access
    suppresses only its documented lookup/conversion exceptions; other property, mapping, and
    projection failures can escape. Reads can recur and observe changing metadata. No Store is
    opened, key reserved, or metadata package imported here.

    Example:
        >>> hints = WorkStorageHints(work_id=5)
        >>> derive_storage_hints(hints) is hints
        True
        >>> mapping = {"preferred_storage_key": "books/5.epub"}
        >>> derive_storage_hints(mapping) is mapping
        True


    :param metadata: Existing hint value, optional provider, or structurally readable metadata bundle.
    :return: Original supported hint value, a newly projected WEMI hint record, or None for unsupported input or a failed provider invocation.
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
    Check for an entity attribute and a callable relation getter without reading links. hasattr may
    evaluate a property; failures other than its ordinary missing-attribute case propagate. Entity
    value shape and relation support are not checked.

    Example:
        >>> from types import SimpleNamespace
        >>> _has_relation_bundle(SimpleNamespace(work=None, get_relation_links=lambda name: ()), "work")
        True


    :param metadata: Candidate object inspected structurally.
    :param entity_attr: Entity attribute name tested before looking for get_relation_links.
    :return: True when the entity attribute exists and get_relation_links is callable.
    """
    return hasattr(metadata, entity_attr) and callable(
        getattr(metadata, "get_relation_links", None)
    )


def _derive_work_storage_hints(metadata: object) -> WorkStorageHints:
    """
    Project Work fields and linked metadata into placement suggestions. Title prefers the canonical
    Work title, then the Work title, then the first Expression display; canonical and sorting titles
    are selected separately. All distinct agent displays are included regardless of primary flags.
    Manifestation, file, and image formats are normalized to uppercase and deduplicated.

    Folder tokens use agents, or series when agents are absent, followed by title. The filename stem
    joins title and agents. Extra fields count Expressions, Manifestations, Items, files, images,
    and identifiers. Relation reads and row conversion follow the shared helpers and do not
    establish a consistent snapshot.

    Example:
        >>> from types import SimpleNamespace
        >>> metadata = SimpleNamespace(work={"work_title": "Book"})
        >>> hints = _derive_work_storage_hints(metadata)
        >>> (hints.title, hints.preferred_folder_tokens)
        ('Book', ('Book',))


    :param metadata: Object with an optional work row/mapping and optional relation getter.
    :return: New Work hints, using None or empty tuples where supported metadata is absent.
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


# Todo: Perhaps a method on metadata? Though that does mix concerns perhaps too much
# Todo: Definitely SHOULD NOT be in the API tho...
def _derive_item_storage_hints(metadata: object) -> ItemStorageHints:
    """
    Combine Item provenance with selected Work, Expression, and Manifestation targets. Each WEMI
    target prefers the first primary link, even if its target is None, then the first link. Title
    prefers the Expression override, Work canonical/title fields, then the source-name stem.
    Subtitle prefers the Manifestation then Expression. Manifestation ID falls back to the Item
    field when the selected value is false before integer conversion.

    Agents use distinct primary displays, falling back to all displays when none remain. Formats
    combine Manifestation, file, image, Asset, and Replica links. Folder tokens use agents or series
    plus title; preferred keys scan Replicas, files, then images. Related counts and some selections
    read relations again, so changing sources need not yield a snapshot. Suggested names/keys are
    not sanitized or reserved.

    Example:
        >>> from types import SimpleNamespace
        >>> hints = _derive_item_storage_hints(SimpleNamespace(item={"item_source_name": "Book.epub"}))
        >>> (hints.title, hints.preferred_filename_stem)
        ('Book', 'Book')


    :param metadata: Object with optional item fields and relation access for WEMI, facets, and stored representations.
    :return: New Item hints with selected identifiers, display/provenance fields, placement suggestions, and relation counts.
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
    Project Expression fields, the first language display, and linked facets. language_code can
    contain a language name because display selection precedes code selection. Agent values retain
    primary links or a sole link, without general deduplication. Expression row conversion repeats
    for separate fields and can observe changes or propagate conversion errors.

    Example:
        >>> from types import SimpleNamespace
        >>> source = SimpleNamespace(expression={"expression_title_override": "Book"})
        >>> _derive_expression_storage_hints(source).title
        'Book'


    :param metadata: Object with an optional expression row/mapping and relation getter.
    :return: New Expression hints with genre, label, and identifier displays; extra retains its fresh empty default.
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
    Project Manifestation edition fields with the first title display and linked file displays.
    Title selection does not search for a primary link. Agent values include primary links or a sole
    link without general deduplication. file_formats uses display values directly, so it may contain
    filenames or lowercase extensions rather than normalized format tokens.

    Example:
        >>> from types import SimpleNamespace
        >>> source = SimpleNamespace(manifestation={"manifestation_format_detail": "EPUB"})
        >>> _derive_manifestation_storage_hints(source).format_detail
        'EPUB'


    :param metadata: Object with an optional manifestation row/mapping and relation getter.
    :return: New Manifestation hints with converted identifiers/year and display tuples; extra keeps its fresh empty default.
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
    Materialize one optional relation, suppressing KeyError, ValueError, and TypeError. Missing or
    noncallable getters also produce an empty list. The guard covers both invocation and iterable
    consumption, but getter attribute lookup is outside it and other failures propagate. Consumption
    is unbounded and has no snapshot guarantee.

    Example:
        >>> from types import SimpleNamespace
        >>> metadata = SimpleNamespace(get_relation_links=lambda name: iter((name,)))
        >>> _relation_links(metadata, "works")
        ['works']


    :param metadata: Object whose get_relation_links attribute is inspected.
    :param relation: Collection name forwarded unchanged to the getter.
    :return: Fresh list of yielded links, or an empty list for unavailable/invalid relations covered by the guard.
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


# Todo: These feel like utility objects which should be in utils somewhere
def _target(link: object) -> object:
    """
    Read a link target with None as the missing-attribute fallback. The target is neither copied nor
    converted; property access can propagate errors.

    Example:
        >>> from types import SimpleNamespace
        >>> _target(SimpleNamespace(target="Book"))
        'Book'


    :param link: Relation-link-like object inspected for a target attribute.
    :return: Original target value, or None when the attribute is absent.
    """
    return getattr(link, "target", None)


def _primary(link: object) -> bool:
    """
    Interpret a link primary attribute by ordinary truth conversion. Missing attributes are false;
    non-boolean values may be truthy, and attribute/truth-conversion errors propagate.

    Example:
        >>> from types import SimpleNamespace
        >>> _primary(SimpleNamespace(primary=1))
        True


    :param link: Relation-link-like object inspected for a primary attribute.
    :return: Boolean truth value of primary, defaulting to False.
    """
    return bool(getattr(link, "primary", False))


def _first_target(links: list[object]) -> object | None:
    """
    Select the target of the first primary link, otherwise the first link. A selected primary target
    of None is returned immediately rather than searching for another usable target. Links are
    inspected in supplied order.

    Example:
        >>> from types import SimpleNamespace
        >>> _first_target([SimpleNamespace(target="first"), SimpleNamespace(target="preferred", primary=True)])
        'preferred'


    :param links: Ordered relation links, possibly empty.
    :return: Selected target unchanged, or None for an empty list or missing selected target.
    """
    if not links:
        return None
    for link in links:
        if _primary(link):
            return _target(link)
    return _target(links[0])


def _rowish_to_mapping(value: object) -> Mapping[str, object]:
    """
    Prefer a Mapping-valued row_dict, then a callable to_mapping result, then the value itself if it
    is a Mapping. None and unsupported values yield a new empty dictionary. Returned mappings are
    not copied. Attribute lookup and conversion run without a general exception guard, including on
    custom Mapping objects.

    Example:
        >>> value = {"work_title": "Book"}
        >>> _rowish_to_mapping(value) is value
        True


    :param value: Optional row-like, converter-bearing, or mapping object.
    :return: First supported mapping representation, or a new empty dictionary.
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
    Select the first candidate unequal to None and the empty string. Zero, False, whitespace, and
    containers are retained without conversion. Custom mapping access or equality errors propagate.

    Example:
        >>> _value_from_mapping({"first": "", "second": 0}, ("first", "second"))
        0


    :param mapping: Mapping queried with get for each candidate field.
    :param keys: Candidate field names in priority order.
    :return: Original first qualifying value, or None when no candidate qualifies.
    """
    for key in keys:
        value = mapping.get(key)
        if value not in (None, ""):
            return value
    return None


def _display_value(value: object) -> str | None:
    """
    Choose display text from prioritized metadata fields, then mapping order, then str(value). Named
    priorities cover agents, WEMI titles/editions, facets, storage names, identifiers, and
    annotations; language_name precedes language_code. Mapping fallback skips keys ending in _id or
    _timestamp_ep_k. If no field qualifies, the original object is stringified, which can expose a
    mapping representation containing those keys.

    Only None and the empty string are absent. Whitespace and false numeric values can become
    display text. Row conversion, mapping operations, equality, and stringification failures
    propagate.

    Example:
        >>> _display_value({"language_code": "en", "language_name": "English"})
        'English'


    :param value: Relation target or scalar to represent for placement display.
    :return: Selected string without trimming, or None when the original value is None or empty text and no mapping field was selected.
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
    Collect nonempty target displays in link order, with optional primary filtering and exact-string
    deduplication. Whitespace survives display selection. Iterables are consumed once without a size
    bound; link access and conversion failures propagate.

    Example:
        >>> from types import SimpleNamespace
        >>> _link_display_values([SimpleNamespace(target="Book"), SimpleNamespace(target="Book")])
        ('Book',)


    :param links: Iterable of objects with target and optionally primary attributes.
    :param primary_only: When true, omit links whose primary attribute is false.
    :param unique: When true, retain only the first occurrence of each exact display string.
    :return: Tuple of surviving display strings in input order.
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
    Retain agent displays for primary links or for a sole link regardless of its primary flag.
    Missing/empty displays are removed. Duplicate displays remain, unlike the general unique display
    collector.

    Example:
        >>> from types import SimpleNamespace
        >>> _primary_agent_values([SimpleNamespace(target="Author")])
        ('Author',)


    :param links: Ordered agent links whose total count determines the sole-link fallback.
    :return: Tuple of qualifying agent displays, including duplicates.
    """
    return tuple(
        filter(
            None,
            (_display_value(_target(link)) for link in links if _primary(link) or len(links) == 1),
        )
    )


def _manifestation_types_from_links(links: Iterable[object]) -> tuple[str, ...]:
    """
    Collect distinct Manifestation carrier, type, or binding text in link order. Each row
    contributes its first non-None/nonempty candidate in that field order. Values are stringified
    without stripping, case normalization, or vocabulary validation.

    Example:
        >>> from types import SimpleNamespace
        >>> _manifestation_types_from_links([SimpleNamespace(target={"manifestation_carrier_type": "ebook"})])
        ('ebook',)


    :param links: Iterable of links targeting Manifestation-like rows or mappings.
    :return: Tuple of first-seen exact type strings.
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
    Collect format fields in link/key order, deduplicating stripped lowercase tokens before
    uppercasing output. Every listed field can contribute a candidate. None and initially empty
    strings are skipped, but whitespace-only input becomes an empty token. Values are not checked
    against a format registry; distinct lowercase Unicode tokens can share uppercase output.

    Example:
        >>> from types import SimpleNamespace
        >>> links = [SimpleNamespace(target={"file_extension": " epub "}), SimpleNamespace(target={"file_extension": "EPUB"})]
        >>> _format_candidates_from_links(links, ("file_extension",))
        ('EPUB',)


    :param links: Iterable of relation targets that can expose format-bearing mappings.
    :param keys: Field names inspected in order on every target.
    :return: Tuple of uppercase candidates whose lowercase normalized source tokens were distinct.
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
    Collect distinct attachment role strings from files followed by images. Each target prefers
    file_role over image_role; None/empty values are skipped, while whitespace and original case are
    retained.

    Example:
        >>> from types import SimpleNamespace
        >>> _item_attachment_roles([SimpleNamespace(target={"file_role": "primary"})], [])
        ('primary',)


    :param file_links: File links considered first.
    :param image_links: Image links considered after all file links.
    :return: Tuple of first-seen exact role strings.
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
    Collect distinct Asset kind suggestions by media category, MIME type, then extension. Each
    target supplies its first non-None/nonempty candidate. String values retain whitespace and case;
    no media registry is consulted.

    Example:
        >>> from types import SimpleNamespace
        >>> _item_digital_asset_kinds([SimpleNamespace(target={"digital_asset_mime_type": "application/epub+zip"})])
        ('application/epub+zip',)


    :param links: Ordered links targeting Digital Asset rows or mappings.
    :return: Tuple of first-seen exact kind strings.
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
    Collect distinct asset_replica_mode strings in link order. None and empty text are skipped;
    other values are stringified without policy, whitespace, or case validation.

    Example:
        >>> from types import SimpleNamespace
        >>> _item_replica_modes([SimpleNamespace(target={"asset_replica_mode": "managed"})])
        ('managed',)


    :param links: Ordered links targeting Replica rows or mappings.
    :return: Tuple of first-seen exact Replica mode strings.
    """
    modes: list[str] = []
    for link in links:
        mode = _value_from_mapping(_rowish_to_mapping(_target(link)), ("asset_replica_mode",))
        if mode not in (None, "") and str(mode) not in modes:
            modes.append(str(mode))
    return tuple(modes)


def _preferred_storage_key(*relations: list[object]) -> str | None:
    """
    Return the first supplied key hint across relation lists in argument order. Each link is
    inspected for file_storage_key, image_storage_key, then asset_replica_storage_key. None and
    empty text are skipped, but whitespace is retained. The result is not resolved, validated,
    reserved, or checked against storage.

    Example:
        >>> from types import SimpleNamespace
        >>> _preferred_storage_key([SimpleNamespace(target={"asset_replica_storage_key": "books/5.epub"})])
        'books/5.epub'


    :param relations: Ordered relation-link lists; earlier lists and earlier links have priority.
    :return: Stringified first qualifying key hint, or None when no key is supplied.
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
    Prefer title plus joined agent names, then title alone, then the source-name stem. Agents join
    with an ampersand separator after a title-and-hyphen prefix. The source fallback uses host
    pathlib semantics. No filename sanitization, extension policy, or collision check is performed.

    Example:
        >>> _preferred_filename_stem("Book", ("Author", "Coauthor"))
        'Book - Author & Coauthor'
        >>> _preferred_filename_stem(None, (), "Book.epub")
        'Book'


    :param title: Optional title; false values select the source-name fallback.
    :param primary_agents: Ordered agent names used only when title is truthy.
    :param source_name: Optional original name whose final extension is removed when title is absent.
    :return: Suggested filename stem, or None when neither title nor source name is truthy.
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
    Stringify a value unless it equals None or empty text. Whitespace, False, and zero are retained
    as strings. Conversion and equality errors are not suppressed.

    Example:
        >>> (_optional_str(0), _optional_str(" "), _optional_str(None))
        ('0', ' ', None)


    :param value: Value to convert without trimming or domain validation.
    :return: str(value), or None for absent/empty text.
    """
    if value in (None, ""):
        return None
    return str(value)


def _optional_int(value: object) -> int | None:
    """
    Convert booleans, integers, floats, or strings with int, suppressing TypeError and ValueError.
    None/empty text and other input types yield None. Booleans become zero/one, finite floats may
    truncate, and negative values remain valid. OverflowError, including conversion of infinity,
    propagates.

    Example:
        >>> (_optional_int("5"), _optional_int(True), _optional_int(3.9), _optional_int("bad"))
        (5, 1, 3, None)


    :param value: Optional simple scalar to interpret as an integer.
    :return: Converted integer, or None for absent, unsupported, or caught-invalid input.
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
