"""
Combine legacy LiuXin metadata with an item-centered WEMI bundle stack.

The four bundles retain identities, relation links and provenance alongside the
inherited flat fields. Views and diagnostic strings expose this state without
replacing it with a single flattened representation.

Example:
    >>> metadata = LiuXinWEMIMetadata("Example")
    >>> tuple(metadata.wemi_stack)
    ('work', 'expression', 'manifestation', 'item')
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterable, Mapping
from copy import deepcopy
from typing import Any, ClassVar, Literal, TypeAlias, cast

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api import (
    ExpressionIdentityAPI,
    ExpressionRelationKey,
    ExpressionRelationLink,
    ExpressionRelationTarget,
    ItemIdentityAPI,
    ItemRelationKey,
    ItemRelationLink,
    ItemRelationTarget,
    ManifestationIdentityAPI,
    ManifestationRelationKey,
    ManifestationRelationLink,
    ManifestationRelationTarget,
    RelationLinkID,
    WorkIdentityAPI,
    WorkRelationKey,
    WorkRelationLink,
    WorkRelationTarget,
)
from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import (
    CalibreLikeLiuXinBookMetaData,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import (
    ExpressionMetadata,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import (
    ItemMetadata,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import (
    ManifestationMetadata,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import (
    WorkMetadata,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.projection_views import (
    LiuXinWEMITextView,
    LiuXinWEMIValuesView,
)


WemiLevel: TypeAlias = Literal["work", "expression", "manifestation", "item"]
WemiMetadataBundle: TypeAlias = (
    WorkMetadata
    | ExpressionMetadata
    | ManifestationMetadata
    | ItemMetadata
)
WemiIdentity: TypeAlias = (
    WorkIdentityAPI
    | ExpressionIdentityAPI
    | ManifestationIdentityAPI
    | ItemIdentityAPI
)
WemiRelationLink: TypeAlias = (
    WorkRelationLink
    | ExpressionRelationLink
    | ManifestationRelationLink
    | ItemRelationLink
)
WemiRelationTarget: TypeAlias = (
    WorkRelationTarget
    | ExpressionRelationTarget
    | ManifestationRelationTarget
    | ItemRelationTarget
)
WemiRelationKey: TypeAlias = (
    WorkRelationKey
    | ExpressionRelationKey
    | ManifestationRelationKey
    | ItemRelationKey
)


class LiuXinWEMIMetadata(CalibreLikeLiuXinBookMetaData):
    """
    Represent one item slice with legacy fields and four separate WEMI bundles.

    Bundle and identity properties expose live objects. Sidecar mappings preserve the
    structured stack; Calibre conversion creates a flat compatibility copy and loses
    relation provenance.

    Example:
        >>> metadata = LiuXinWEMIMetadata("Example")
        >>> metadata.liuxin is metadata
        True
        >>> metadata.display_title
        'Example'
    """

    SIDECAR_SCHEMA_NAME: ClassVar[str] = "liuxin_wemi_item_metadata"
    SIDECAR_SCHEMA_VERSION: ClassVar[int] = 1

    _LEVELS: ClassVar[tuple[WemiLevel, ...]] = (
        "work",
        "expression",
        "manifestation",
        "item",
    )
    _LEVEL_ALIASES: ClassVar[dict[str, WemiLevel]] = {
        "w": "work",
        "e": "expression",
        "m": "manifestation",
        "i": "item",
    }
    _METADATA_STORAGE_BY_LEVEL: ClassVar[dict[WemiLevel, str]] = {
        "work": "_work_metadata",
        "expression": "_expression_metadata",
        "manifestation": "_manifestation_metadata",
        "item": "_item_metadata",
    }
    _METADATA_STORAGE_BY_ATTRIBUTE: ClassVar[dict[str, str]] = {
        "work_metadata": "_work_metadata",
        "expression_metadata": "_expression_metadata",
        "manifestation_metadata": "_manifestation_metadata",
        "item_metadata": "_item_metadata",
    }
    _IDENTITY_ATTRIBUTE_BY_LEVEL: ClassVar[dict[WemiLevel, str]] = {
        "work": "work",
        "expression": "expression",
        "manifestation": "manifestation",
        "item": "item",
    }
    _DATABASE_ID_ALIASES: ClassVar[dict[str, str]] = {
        "work": "work_id",
        "expression": "expression_id",
        "manifestation": "manifestation_id",
        "item": "item_id",
    }
    _PRETTY_IDENTITY_FIELDS: ClassVar[dict[WemiLevel, tuple[str, ...]]] = {
        "work": (
            "work_id",
            "work_canonical_title",
            "work_title",
            "work_sort_title",
            "work_type",
            "work_original_year",
        ),
        "expression": (
            "expression_id",
            "expression_work_id",
            "expression_title_override",
            "expression_subtitle",
            "expression_type",
            "expression_language_id",
            "expression_year",
            "expression_wordcount",
        ),
        "manifestation": (
            "manifestation_id",
            "manifestation_expression_id",
            "manifestation_format_detail",
            "manifestation_carrier_type",
            "manifestation_edition_statement",
            "manifestation_pub_year",
        ),
        "item": (
            "item_id",
            "item_manifestation_id",
            "item_type",
            "item_source",
            "item_source_name",
            "item_source_path",
            "item_lifecycle_status",
        ),
    }
    _PRETTY_LEGACY_FIELDS: ClassVar[tuple[str, ...]] = (
        "title",
        "title_sort",
        "authors",
        "creator_sort",
        "identifiers",
        "internal_identifiers",
        "tags",
        "labels",
        "languages",
        "publisher",
        "series",
        "ratings",
        "comments",
    )

    def __init__(
        self,
        title: str | None = None,
        authors: str | list[str] | tuple[str, ...] | None = None,
        other: CalibreLikeLiuXinBookMetaData | None = None,
        *,
        work_metadata: WorkMetadata | None = None,
        expression_metadata: ExpressionMetadata | None = None,
        manifestation_metadata: ManifestationMetadata | None = None,
        item_metadata: ItemMetadata | None = None,
    ) -> None:
        """
        Create empty bundles, initialize legacy metadata, then install explicit bundle objects.

        The inherited constructor merges other before applying explicit title/authors.
        Non-None bundle arguments are retained by reference and override bundles populated
        by that merge.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.work is None
            True


        :param title: Optional legacy title assigned after merging other metadata.
        :param authors: Optional creator text or sequence assigned after merging other
            metadata.
        :param other: Optional compatible source metadata merged during initialization.
        :param work_metadata: Optional work bundle retained by reference.
        :param expression_metadata: Optional expression bundle retained by reference.
        :param manifestation_metadata: Optional manifestation bundle retained by reference.
        :param item_metadata: Optional item bundle retained by reference.
        :return: None.
        """
        object.__setattr__(self, "_work_metadata", WorkMetadata())
        object.__setattr__(self, "_expression_metadata", ExpressionMetadata())
        object.__setattr__(self, "_manifestation_metadata", ManifestationMetadata())
        object.__setattr__(self, "_item_metadata", ItemMetadata())

        super().__init__(title=title, authors=authors, other=other)

        if work_metadata is not None:
            object.__setattr__(self, "_work_metadata", work_metadata)
        if expression_metadata is not None:
            object.__setattr__(self, "_expression_metadata", expression_metadata)
        if manifestation_metadata is not None:
            object.__setattr__(
                self,
                "_manifestation_metadata",
                manifestation_metadata,
            )
        if item_metadata is not None:
            object.__setattr__(self, "_item_metadata", item_metadata)

    def __setattr__(self, key: str, value: Any) -> None:
        """
        Route bundle and identity assignments before the legacy metadata setter.

        Names are stripped and lowercased for WEMI routing. values and text reject
        assignment with AttributeError; assigning None to a bundle creates a fresh empty
        bundle. Other fields use the inherited setter.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.work_metadata = None
            >>> metadata.work is None
            True


        :param key: Attribute name used for WEMI routing or legacy field assignment.
        :param value: Bundle, identity or legacy field value to store.
        :return: None.
        """
        normalized_key = key.lower().strip()
        if normalized_key in {"values", "text"}:
            raise AttributeError(f"{normalized_key!r} is a read-only projection view.")

        metadata_storage = self._METADATA_STORAGE_BY_ATTRIBUTE.get(normalized_key)
        if metadata_storage is not None:
            object.__setattr__(
                self,
                metadata_storage,
                self._coerce_metadata_bundle(normalized_key, value),
            )
            return

        if normalized_key in self._IDENTITY_ATTRIBUTE_BY_LEVEL:
            metadata = self.get_wemi_metadata(cast(WemiLevel, normalized_key))
            setattr(metadata, normalized_key, value)
            return

        super().__setattr__(key, value)

    @classmethod
    def _coerce_metadata_bundle(cls, attribute: str, value: Any) -> WemiMetadataBundle:
        """
        Return a supplied bundle value, or create the empty bundle for an attribute name.

        Non-None values pass through without runtime type validation. An unknown attribute
        raises KeyError only when an empty replacement is requested.

        Example:
            >>> isinstance(LiuXinWEMIMetadata._coerce_metadata_bundle('work_metadata', None), WorkMetadata)
            True


        :param attribute: Canonical work_metadata, expression_metadata,
            manifestation_metadata or item_metadata name.
        :param value: Bundle value to retain, or None to create an empty bundle.
        :return: Supplied value or a new empty metadata bundle.
        """
        if value is not None:
            return value

        if attribute == "work_metadata":
            return WorkMetadata()
        if attribute == "expression_metadata":
            return ExpressionMetadata()
        if attribute == "manifestation_metadata":
            return ManifestationMetadata()
        if attribute == "item_metadata":
            return ItemMetadata()
        raise KeyError(f"Unknown WEMI metadata attribute: {attribute!r}")

    @classmethod
    def normalize_wemi_level(cls, level: str) -> WemiLevel:
        """
        Normalize a WEMI level name, accepting the w/e/m/i abbreviations.

        Input is converted to a stripped lowercase string. Unknown levels raise KeyError.

        Example:
            >>> LiuXinWEMIMetadata.normalize_wemi_level(' W ')
            'work'


        :param level: Level name or single-letter abbreviation to normalize.
        :return: Canonical work, expression, manifestation or item level.
        """
        normalized = str(level).strip().lower()
        normalized = cls._LEVEL_ALIASES.get(normalized, normalized)
        if normalized not in cls._LEVELS:
            raise KeyError(
                "Unknown WEMI level {!r}. Expected one of {}.".format(
                    level,
                    ", ".join(cls._LEVELS),
                )
            )
        return cast(WemiLevel, normalized)

    @property
    def liuxin(self) -> "LiuXinWEMIMetadata":
        """
        Expose this object through its legacy LiuXin interface.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.liuxin is metadata
            True


        :return: This same metadata object.
        """
        return self

    @property
    def calibre(self) -> Any:
        """
        Create a Calibre-compatible copy with supported WEMI projections.

        The flat conversion preserves this container's legacy field storage and cannot
        retain all relation metadata.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.calibre.title
            'Example'


        :return: New Calibre metadata object.
        """
        return self.to_calibre()

    @property
    def work_metadata(self) -> WorkMetadata:
        """
        Read the live work metadata bundle.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> isinstance(metadata.work_metadata, WorkMetadata)
            True


        :return: Stored bundle object, without copying.
        """
        return object.__getattribute__(self, "_work_metadata")

    @work_metadata.setter
    def work_metadata(self, value: WorkMetadata | None) -> None:
        """
        Replace the work bundle, creating an empty bundle for None.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> previous = metadata.work_metadata
            >>> metadata.work_metadata = None
            >>> metadata.work_metadata is previous
            False


        :param value: Work bundle retained by reference, or None for a fresh empty bundle.
        :return: None.
        """
        object.__setattr__(
            self,
            "_work_metadata",
            self._coerce_metadata_bundle("work_metadata", value),
        )

    @property
    def expression_metadata(self) -> ExpressionMetadata:
        """
        Read the live expression metadata bundle.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> isinstance(metadata.expression_metadata, ExpressionMetadata)
            True


        :return: Stored bundle object, without copying.
        """
        return object.__getattribute__(self, "_expression_metadata")

    @expression_metadata.setter
    def expression_metadata(self, value: ExpressionMetadata | None) -> None:
        """
        Replace the expression bundle, creating an empty bundle for None.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> previous = metadata.expression_metadata
            >>> metadata.expression_metadata = None
            >>> metadata.expression_metadata is previous
            False


        :param value: Expression bundle retained by reference, or None for a fresh empty
            bundle.
        :return: None.
        """
        object.__setattr__(
            self,
            "_expression_metadata",
            self._coerce_metadata_bundle("expression_metadata", value),
        )

    @property
    def manifestation_metadata(self) -> ManifestationMetadata:
        """
        Read the live manifestation metadata bundle.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> isinstance(metadata.manifestation_metadata, ManifestationMetadata)
            True


        :return: Stored bundle object, without copying.
        """
        return object.__getattribute__(self, "_manifestation_metadata")

    @manifestation_metadata.setter
    def manifestation_metadata(self, value: ManifestationMetadata | None) -> None:
        """
        Replace the manifestation bundle, creating an empty bundle for None.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> previous = metadata.manifestation_metadata
            >>> metadata.manifestation_metadata = None
            >>> metadata.manifestation_metadata is previous
            False


        :param value: Manifestation bundle retained by reference, or None for a fresh empty
            bundle.
        :return: None.
        """
        object.__setattr__(
            self,
            "_manifestation_metadata",
            self._coerce_metadata_bundle("manifestation_metadata", value),
        )

    @property
    def item_metadata(self) -> ItemMetadata:
        """
        Read the live item metadata bundle.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> isinstance(metadata.item_metadata, ItemMetadata)
            True


        :return: Stored bundle object, without copying.
        """
        return object.__getattribute__(self, "_item_metadata")

    @item_metadata.setter
    def item_metadata(self, value: ItemMetadata | None) -> None:
        """
        Replace the item bundle, creating an empty bundle for None.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> previous = metadata.item_metadata
            >>> metadata.item_metadata = None
            >>> metadata.item_metadata is previous
            False


        :param value: Item bundle retained by reference, or None for a fresh empty bundle.
        :return: None.
        """
        object.__setattr__(
            self,
            "_item_metadata",
            self._coerce_metadata_bundle("item_metadata", value),
        )

    @property
    def work(self) -> WorkIdentityAPI | None:
        """
        Read the identity attached to the work bundle.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.work is None
            True


        :return: Current identity object, or None.
        """
        return self.work_metadata.work

    @work.setter
    def work(self, value: WorkIdentityAPI | None) -> None:
        """
        Replace only the identity on the existing work bundle.

        Relation buckets on that bundle remain intact.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.work = None
            >>> metadata.work is None
            True


        :param value: Work identity retained by the bundle, or None to clear it.
        :return: None.
        """
        self.work_metadata.work = value

    @property
    def expression(self) -> ExpressionIdentityAPI | None:
        """
        Read the identity attached to the expression bundle.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.expression is None
            True


        :return: Current identity object, or None.
        """
        return self.expression_metadata.expression

    @expression.setter
    def expression(self, value: ExpressionIdentityAPI | None) -> None:
        """
        Replace only the identity on the existing expression bundle.

        Relation buckets on that bundle remain intact.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.expression = None
            >>> metadata.expression is None
            True


        :param value: Expression identity retained by the bundle, or None to clear it.
        :return: None.
        """
        self.expression_metadata.expression = value

    @property
    def manifestation(self) -> ManifestationIdentityAPI | None:
        """
        Read the identity attached to the manifestation bundle.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.manifestation is None
            True


        :return: Current identity object, or None.
        """
        return self.manifestation_metadata.manifestation

    @manifestation.setter
    def manifestation(self, value: ManifestationIdentityAPI | None) -> None:
        """
        Replace only the identity on the existing manifestation bundle.

        Relation buckets on that bundle remain intact.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.manifestation = None
            >>> metadata.manifestation is None
            True


        :param value: Manifestation identity retained by the bundle, or None to clear it.
        :return: None.
        """
        self.manifestation_metadata.manifestation = value

    @property
    def item(self) -> ItemIdentityAPI | None:
        """
        Read the identity attached to the item bundle.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.item is None
            True


        :return: Current identity object, or None.
        """
        return self.item_metadata.item

    @item.setter
    def item(self, value: ItemIdentityAPI | None) -> None:
        """
        Replace only the identity on the existing item bundle.

        Relation buckets on that bundle remain intact.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.item = None
            >>> metadata.item is None
            True


        :param value: Item identity retained by the bundle, or None to clear it.
        :return: None.
        """
        self.item_metadata.item = value

    @property
    def wemi_stack(self) -> dict[WemiLevel, WemiMetadataBundle]:
        """
        Collect the four live bundles in WEMI order.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.wemi_stack["work"] is metadata.work_metadata
            True


        :return: New level-keyed dictionary referring to the existing bundles.
        """
        return {
            "work": self.work_metadata,
            "expression": self.expression_metadata,
            "manifestation": self.manifestation_metadata,
            "item": self.item_metadata,
        }

    @property
    def wemi_identities(self) -> dict[WemiLevel, WemiIdentity | None]:
        """
        Collect current identities in WEMI order.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.wemi_identities["item"] is None
            True


        :return: New level-keyed dictionary referring to identities, with None for absent
            levels.
        """
        return {
            "work": self.work,
            "expression": self.expression,
            "manifestation": self.manifestation,
            "item": self.item,
        }

    @property
    def database_ids(self) -> dict[str, int | None]:
        """
        Read identity ids and legacy parent-id hints without resolving the graph.

        These source-row hints may differ from the preferred links in relation buckets.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> len(metadata.database_ids), metadata.database_ids["work_id"]
            (7, None)


        :return: New dictionary of seven id fields, using None for missing values.
        """
        work = self.work
        expression = self.expression
        manifestation = self.manifestation
        item = self.item
        return {
            "work_id": getattr(work, "work_id", None),
            "expression_id": getattr(expression, "expression_id", None),
            "expression_work_id": getattr(expression, "expression_work_id", None),
            "manifestation_id": getattr(
                manifestation,
                "manifestation_id",
                None,
            ),
            "manifestation_expression_id": getattr(
                manifestation,
                "manifestation_expression_id",
                None,
            ),
            "item_id": getattr(item, "item_id", None),
            "item_manifestation_id": getattr(item, "item_manifestation_id", None),
        }

    @property
    def relation_link_ids(self) -> dict[WemiLevel, dict[str, tuple[RelationLinkID, ...]]]:
        """
        Collect non-None persisted link ids for every bundle relation bucket.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> tuple(metadata.relation_link_ids)
            ('work', 'expression', 'manifestation', 'item')


        :return: New nested level/relation dictionaries containing tuples of link ids.
        """
        return {
            level: {
                relation_key: tuple(
                    link.link_id
                    for link in metadata.get_relation_links(relation_key)
                    if link.link_id is not None
                )
                for relation_key in metadata.relation_names()
            }
            for level, metadata in self.wemi_stack.items()
        }

    @property
    def values(self) -> LiuXinWEMIValuesView:
        """
        Create a read-only structured projection view backed by this metadata slice.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.values.primary_title
            'Example'


        :return: New values view referring to this object.
        """
        return LiuXinWEMIValuesView(self)

    @property
    def text(self) -> LiuXinWEMITextView:
        """
        Create a display-text view backed by a new structured values view.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> isinstance(metadata.text, LiuXinWEMITextView)
            True


        :return: New text projection view.
        """
        return LiuXinWEMITextView(self.values)

    def load(self, *fields: str) -> "LiuXinWEMIMetadata":
        """
        Return this eager slice without performing hydration work.

        Field names are accepted for compatibility with lazy slices and are otherwise
        ignored.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.load("tags") is metadata
            True


        :param fields: Optional projection field names accepted for eager/lazy interface
            compatibility.
        :return: This metadata object.
        """
        return self

    @property
    def titles(self) -> tuple[str, ...]:
        """
        Collect unique nonblank title candidates in legacy/work/expression/item order.

        Candidates are stringified and trimmed. Deduplication is case-sensitive and
        preserves the first occurrence.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.titles
            ('Example',)


        :return: Ordered tuple of title strings.
        """
        seen: set[str] = set()
        titles: list[str] = []
        for value in self._iter_title_candidates():
            if value is None:
                continue
            title = str(value).strip()
            if not title or title in seen:
                continue
            titles.append(title)
            seen.add(title)
        return tuple(titles)

    @property
    def canonical_title(self) -> str | None:
        """
        Prefer work_canonical_title, then work_title, then the legacy title.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.canonical_title
            'Example'


        :return: First truthy, nonblank candidate after stripping, or None.
        """
        work = self.work
        for value in (
            getattr(work, "work_canonical_title", None),
            getattr(work, "work_title", None),
            self.get("title", None),
        ):
            if value and str(value).strip():
                return str(value).strip()
        return None

    @property
    def display_title(self) -> str | None:
        """
        Choose the first title candidate, falling back to the canonical title.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.display_title
            'Example'


        :return: Display title string, or None.
        """
        titles = self.titles
        return titles[0] if titles else self.canonical_title

    @property
    def sort_title(self) -> str | None:
        """
        Prefer work_sort_title, then legacy title_sort, then the display title.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.sort_title
            'Example'


        :return: First truthy, nonblank candidate after stripping, or None.
        """
        work = self.work
        for value in (
            getattr(work, "work_sort_title", None),
            self.get("title_sort", None),
            self.display_title,
        ):
            if value and str(value).strip():
                return str(value).strip()
        return None

    def pretty_string(
        self,
        *,
        include_empty: bool = False,
        include_relations: bool = True,
        include_legacy: bool = True,
    ) -> str:
        """
        Render titles, database ids, WEMI identities and optional relation/legacy summaries.

        Missing identities are labeled empty and skip relation summaries for that level.
        include_empty controls mapping filtering; blank top-level titles are still omitted.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.pretty_string().startswith("LiuXin WEMI Metadata")
            True


        :param include_empty: Include empty mapping entries in the diagnostic summary when
            True.
        :param include_relations: Include relation counts in the diagnostic summary when
            True.
        :param include_legacy: Include the legacy LiuXin field mapping when True.
        :return: Multiline diagnostic text without trailing whitespace.
        """
        lines = ["LiuXin WEMI Metadata"]
        self._append_pretty_value(lines, "Title", self.display_title)
        self._append_pretty_value(lines, "Canonical title", self.canonical_title)
        self._append_pretty_value(lines, "Sort title", self.sort_title)

        database_ids = self._filtered_mapping(self.database_ids, include_empty)
        if database_ids:
            lines.append("")
            lines.append("Database ids:")
            self._append_pretty_mapping(lines, database_ids, indent="  ")

        lines.append("")
        lines.append("WEMI stack:")
        for level in self._LEVELS:
            identity = self.get_wemi_identity(level)
            lines.append(f"  {level.title()}:")
            if identity is None:
                lines.append("    empty")
                continue

            identity_mapping = self._pretty_identity_mapping(level, identity, include_empty)
            if identity_mapping:
                self._append_pretty_mapping(lines, identity_mapping, indent="    ")

            if include_relations:
                relation_summary = self._pretty_relation_summary(
                    self.get_wemi_metadata(level),
                    include_empty,
                )
                if relation_summary:
                    lines.append("    relations: " + ", ".join(relation_summary))

        if include_legacy:
            legacy_mapping = self._pretty_legacy_mapping(include_empty)
            if legacy_mapping:
                lines.append("")
                lines.append("Legacy fields:")
                self._append_pretty_mapping(lines, legacy_mapping, indent="  ")

        return "\n".join(lines).rstrip()

    def to_pretty_string(
        self,
        *,
        include_empty: bool = False,
        include_relations: bool = True,
        include_legacy: bool = True,
    ) -> str:
        """
        Render the diagnostic summary using the same options as pretty_string.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.to_pretty_string() == metadata.pretty_string()
            True


        :param include_empty: Include empty mapping entries in the diagnostic summary when
            True.
        :param include_relations: Include relation counts in the diagnostic summary when
            True.
        :param include_legacy: Include the legacy LiuXin field mapping when True.
        :return: Multiline diagnostic description.
        """
        return self.pretty_string(
            include_empty=include_empty,
            include_relations=include_relations,
            include_legacy=include_legacy,
        )

    def __unicode__(self) -> str:
        """
        Render the default human-readable WEMI metadata description.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.__unicode__() == metadata.pretty_string()
            True


        :return: Multiline diagnostic description.
        """
        return self.pretty_string()

    def __str__(self) -> str:
        """
        Render the Unicode diagnostic description for normal string conversion.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> str(metadata) == metadata.pretty_string()
            True


        :return: Multiline diagnostic description.
        """
        return self.__unicode__()

    def __repr__(self) -> str:
        """
        Summarize the display title, item id and work id for debugging.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> repr(metadata)
            "LiuXinWEMIMetadata(title='Example', item_id=None, work_id=None)"


        :return: Compact LiuXinWEMIMetadata representation.
        """
        return (
            "LiuXinWEMIMetadata(title={!r}, item_id={!r}, work_id={!r})".format(
                self.display_title,
                self.get_database_id("item"),
                self.get_database_id("work"),
            )
        )

    def _iter_title_candidates(self) -> Iterable[str | None]:
        """
        Yield legacy, work, expression and item title candidates in preference order.

        Values are not stripped or deduplicated here; absent identity attributes produce
        None.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> next(iter(metadata._iter_title_candidates()))
            'Example'


        :return: Iterator yielding the seven raw candidates.
        """
        legacy_title = self.get("title", None)
        work = self.work
        expression = self.expression
        item = self.item

        yield legacy_title
        yield getattr(work, "work_canonical_title", None)
        yield getattr(work, "work_title", None)
        yield getattr(expression, "expression_title_override", None)
        yield getattr(expression, "expression_subtitle", None)
        yield getattr(expression, "expression_label", None)
        yield getattr(item, "item_source_name", None)

    def as_liuxin_metadata(self) -> "LiuXinWEMIMetadata":
        """
        Expose this slice through its inherited LiuXin interface.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.as_liuxin_metadata() is metadata
            True


        :return: This same metadata object.
        """
        return self

    def as_calibre_metadata(self) -> Any:
        """
        Convert this slice through the WEMI-aware Calibre conversion path.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.as_calibre_metadata().title
            'Example'


        :return: New flat Calibre metadata object.
        """
        return self.to_calibre()

    def to_calibre(self) -> Any:
        """
        Project supported WEMI values into a copied legacy container and convert it to Calibre.

        The source legacy fields stay unchanged. Flat Calibre output loses relation-link ids
        and provenance; conversion or copy failures propagate.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.to_calibre().title, metadata.title
            ('Example', 'Example')


        :return: New Calibre-shaped metadata object.
        """
        metadata = self.deepcopy_metadata()
        metadata._sync_projection_fields_for_calibre_conversion()
        return CalibreLikeLiuXinBookMetaData.to_calibre(metadata)

    def _sync_projection_fields_for_calibre_conversion(self) -> None:
        """
        Populate this container's legacy fields from structured WEMI projections.

        Fill an empty title, replace term mappings, update languages only when nonempty, and
        add supported identifiers. This mutates its receiver and is normally called on a
        conversion copy.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_liuxin_wemi_metadata.py


        :return: None.
        """
        data = object.__getattribute__(self, "_data")
        values = self.values

        title = values.primary_title or self.display_title or self.canonical_title
        if title and not str(data.get("title") or "").strip():
            data["title"] = title

        self._replace_conversion_terms("tags", values.tags)
        self._replace_conversion_terms("subject", values.subjects)
        self._replace_conversion_terms("genre", values.genres)
        self._replace_conversion_terms("labels", values.labels)
        self._replace_conversion_terms("series", values.series)

        languages = tuple(values.languages)
        if languages:
            data["languages"] = list(languages)

        for scheme, identifiers in values.identifiers.items():
            for identifier in identifiers:
                self.set_identifier(scheme, identifier)

    def _replace_conversion_terms(self, field: str, values: Iterable[str]) -> None:
        """
        Replace a legacy term field with ordered string keys and None row ids.

        Whitespace-only strings are omitted, but retained strings keep their original
        surrounding whitespace. Repeated keys collapse.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata._replace_conversion_terms("tags", ["one", "one", " "])
            >>> list(metadata.direct_get("tags").items())
            [('one', None)]


        :param field: Exact legacy storage key to replace; this helper performs no alias normalization.
        :param values: Iterable of values to stringify as legacy term keys.
        :return: None.
        """
        object.__getattribute__(self, "_data")[field] = OrderedDict(
            (str(value), None)
            for value in values
            if str(value).strip()
        )

    @classmethod
    def from_opf(
        cls,
        source: Any,
        *,
        database: Any = None,
        item_id: int | None = None,
        source_row: Mapping[str, Any] | Row | None = None,
        replace_metadata: bool = False,
    ) -> "LiuXinWEMIMetadata":
        """
        Import OPF fields and optionally merge them into a database-hydrated WEMI slice.

        The OPF helper can infer an item id. Its result is returned when already of the
        requested class; otherwise it is merged into a new cls instance.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_liuxin_wemi_metadata.py


        :param source: OPF source accepted by the OPF reader, such as bytes, a path or a
            readable stream.
        :param database: Optional caller-owned database/read source used when an item id or
            source row is available.
        :param item_id: Optional item row id; overrides an item id extracted from
            source_row.
        :param source_row: Database Row or source mapping supplying identity fields and
            row-id hints.
        :param replace_metadata: Replace supported existing metadata when True; otherwise
            use the family merge rules.
        :return: Imported WEMI metadata object.
        """
        from LiuXin_alpha.metadata.opf_tools import liuxin_wemi_metadata_from_opf

        metadata = liuxin_wemi_metadata_from_opf(
            source,
            database=database,
            item_id=item_id,
            source_row=source_row,
            replace_metadata=replace_metadata,
        )
        if isinstance(metadata, cls):
            return metadata
        return cls(other=metadata)

    def write_to_database(
        self,
        database: Any,
        *,
        fields: Iterable[str] | None = None,
        target_level: str | None = "work",
        item_id: int | None = None,
        target_row: Row | Mapping[str, Any] | None = None,
        replace: bool = False,
        mark_dirty: bool = True,
    ) -> Any:
        """
        Delegate supported relation-backed changes to a new WEMI writer.

        The returned report can contain both changes and errors; this facade adds no
        transaction or rollback boundary. Core identity fields and storage rows are outside
        the writer's scope.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_liuxin_wemi_metadata.py


        :param database: Caller-owned writable database passed to the writer.
        :param fields: Optional supported field names; None uses the writer defaults.
        :param target_level: Preferred WEMI level, default work; unresolved targets may fall
            back to another level.
        :param item_id: Optional fallback item id used when direct target and bundle resolution do not select a Row.
        :param target_row: Optional explicit database Row or mapping with the preferred
            level id.
        :param replace: Treat selected values as authoritative, unlinking stale terms and
            deleting stale entity identifiers when True.
        :param mark_dirty: Request best-effort dirty marking after reported changes when
            True.
        :return: Write report containing changes, skipped operations and errors.
        """
        from LiuXin_alpha.metadata.containers.metadata_containers.liuxin_wemi_metadata_writer import (
            LiuXinWEMIMetadataWriter,
        )

        return LiuXinWEMIMetadataWriter(database).write(
            self,
            fields=fields,
            target_level=target_level,
            item_id=item_id,
            target_row=target_row,
            replace=replace,
            mark_dirty=mark_dirty,
        )

    def get_wemi_metadata(self, level: str) -> WemiMetadataBundle:
        """
        Normalize the WEMI level and read its stored bundle.

        Full level names and w/e/m/i aliases are accepted; unknown levels raise KeyError.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.get_wemi_metadata("w") is metadata.work_metadata
            True


        :param level: WEMI level identifying the bundle to access.
        :return: Live metadata bundle.
        """
        normalized_level = self.normalize_wemi_level(level)
        return object.__getattribute__(
            self,
            self._METADATA_STORAGE_BY_LEVEL[normalized_level],
        )

    def get_wemi_identity(self, level: str) -> WemiIdentity | None:
        """
        Read the identity from the normalized WEMI level's bundle.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.get_wemi_identity("item") is None
            True


        :param level: WEMI level identifying the bundle to access.
        :return: Live identity object, or None when the bundle has no identity.
        """
        normalized_level = self.normalize_wemi_level(level)
        identity_attr = self._IDENTITY_ATTRIBUTE_BY_LEVEL[normalized_level]
        return getattr(self.get_wemi_metadata(normalized_level), identity_attr)

    def get_database_id(self, name: str) -> int | None:
        """
        Read a named id or parent-id hint after case/whitespace and alias normalization.

        Known but unset ids return None. Unknown names raise KeyError rather than returning
        a missing-value fallback.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.get_database_id(" WORK ") is None
            True
            >>> metadata.get_database_id("unknown")
            Traceback (most recent call last):
            ...
            KeyError: "Unknown WEMI database id name: 'unknown'"


        :param name: Database-id key or full level alias such as work or item.
        :return: Stored id, or None for a known but unset name.
        """
        normalized_name = self._DATABASE_ID_ALIASES.get(
            str(name).strip().lower(),
            str(name).strip().lower(),
        )
        try:
            return self.database_ids[normalized_name]
        except KeyError as exc:
            raise KeyError(f"Unknown WEMI database id name: {name!r}") from exc

    def get_wemi_relation_links(
        self,
        level: str,
        relation_key: WemiRelationKey,
    ) -> list[WemiRelationLink]:
        """
        Read the selected bundle's validated relation bucket.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.get_wemi_relation_links("work", "tags")
            []


        :param level: WEMI level identifying the bundle to access.
        :param relation_key: normalized relation bucket key from the selected bundle's
            ``RELATION_KEYS``.
        :return: Live list of relation links; invalid levels or bucket names raise KeyError.
        """
        return self.get_wemi_metadata(level).get_relation_links(relation_key)

    def set_wemi_relation_links(
        self,
        level: str,
        relation_key: WemiRelationKey,
        links: Iterable[WemiRelationLink],
    ) -> None:
        """
        Validate and replace all links in the selected bundle relation bucket.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_liuxin_wemi_metadata.py


        :param level: WEMI level identifying the bundle to access.
        :param relation_key: normalized relation bucket key from the selected bundle's
            ``RELATION_KEYS``.
        :param links: Iterable of relation links replacing the selected bucket.
        :return: None.
        """
        self.get_wemi_metadata(level).set_relation_links(relation_key, links)

    def add_wemi_relation_link(
        self,
        level: str,
        relation_key: WemiRelationKey,
        link: WemiRelationLink,
    ) -> None:
        """
        Validate and append one link through the selected bundle.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_liuxin_wemi_metadata.py


        :param level: WEMI level identifying the bundle to access.
        :param relation_key: normalized relation bucket key from the selected bundle's
            ``RELATION_KEYS``.
        :param link: Relation link to add or designate as preferred.
        :return: None.
        """
        self.get_wemi_metadata(level).add_relation_link(relation_key, link)

    def get_primary_wemi_relation_link(
        self,
        level: str,
        relation_key: WemiRelationKey,
    ) -> WemiRelationLink | None:
        """
        Select a preferred link by primary flag, priority, index and original order.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.get_primary_wemi_relation_link("work", "tags") is None
            True


        :param level: WEMI level identifying the bundle to access.
        :param relation_key: normalized relation bucket key from the selected bundle's
            ``RELATION_KEYS``.
        :return: Preferred link, or None for an empty bucket.
        """
        return self.get_wemi_metadata(level).primary_relation_link(relation_key)

    def set_primary_wemi_relation_link(
        self,
        level: str,
        relation_key: WemiRelationKey,
        link: WemiRelationLink,
    ) -> None:
        """
        Select or append a link and set primary flags within its bucket.

        The bundle matches object identity, non-None link id or an id-less target, then
        clears primary on the other links. Existing link objects can be mutated.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_liuxin_wemi_metadata.py


        :param level: WEMI level identifying the bundle to access.
        :param relation_key: normalized relation bucket key from the selected bundle's
            ``RELATION_KEYS``.
        :param link: Relation link to add or designate as preferred.
        :return: None.
        """
        self.get_wemi_metadata(level).set_primary_relation_link(relation_key, link)

    def get_wemi_related(
        self,
        level: str,
        relation_key: WemiRelationKey,
    ) -> list[WemiRelationTarget]:
        """
        Project the target objects from the selected relation bucket.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.get_wemi_related("work", "tags")
            []


        :param level: WEMI level identifying the bundle to access.
        :param relation_key: normalized relation bucket key from the selected bundle's
            ``RELATION_KEYS``.
        :return: New list retaining the current target objects.
        """
        return self.get_wemi_metadata(level).get_related(relation_key)

    def get_primary_wemi_related(
        self,
        level: str,
        relation_key: WemiRelationKey,
    ) -> WemiRelationTarget | None:
        """
        Read the target of the preferred link in the selected relation bucket.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.get_primary_wemi_related("work", "tags") is None
            True


        :param level: WEMI level identifying the bundle to access.
        :param relation_key: normalized relation bucket key from the selected bundle's
            ``RELATION_KEYS``.
        :return: Preferred target, or None for an empty bucket.
        """
        return self.get_wemi_metadata(level).primary_related(relation_key)

    def set_wemi_related(
        self,
        level: str,
        relation_key: WemiRelationKey,
        values: Iterable[WemiRelationTarget],
    ) -> None:
        """
        Replace a bucket with new links constructed around the supplied targets.

        The bundle supplies relation cardinality; previous link ids and provenance are not
        carried over.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_liuxin_wemi_metadata.py


        :param level: WEMI level identifying the bundle to access.
        :param relation_key: normalized relation bucket key from the selected bundle's
            ``RELATION_KEYS``.
        :param values: Iterable of relation targets replacing the selected bucket.
        :return: None.
        """
        self.get_wemi_metadata(level).set_related(relation_key, values)

    def add_wemi_related(
        self,
        level: str,
        relation_key: WemiRelationKey,
        value: WemiRelationTarget,
    ) -> None:
        """
        Wrap a target in the bundle's relation-link class and append it.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_liuxin_wemi_metadata.py


        :param level: WEMI level identifying the bundle to access.
        :param relation_key: normalized relation bucket key from the selected bundle's
            ``RELATION_KEYS``.
        :param value: Relation target retained by the newly constructed link.
        :return: None.
        """
        self.get_wemi_metadata(level).add_related(relation_key, value)

    def get_wemi_relation_link_ids(
        self,
        level: str,
        relation_key: WemiRelationKey,
    ) -> tuple[RelationLinkID, ...]:
        """
        Collect persisted ids from the current links in a relation bucket.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.get_wemi_relation_link_ids("work", "tags")
            ()


        :param level: WEMI level identifying the bundle to access.
        :param relation_key: normalized relation bucket key from the selected bundle's
            ``RELATION_KEYS``.
        :return: Tuple of non-None link ids in bucket order.
        """
        return tuple(
            link.link_id
            for link in self.get_wemi_relation_links(level, relation_key)
            if link.link_id is not None
        )

    def sync_legacy_title_from_wemi(self) -> str | None:
        """
        Assign the selected canonical title to the legacy title field when present.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.sync_legacy_title_from_wemi()
            'Example'


        :return: Selected canonical title, or None when no title is available.
        """
        title = self.canonical_title
        if title is not None:
            self.title = title
        return title

    def sync_legacy_tags_from_wemi(self) -> tuple[str, ...]:
        """
        Append distinct WEMI tags to the legacy tags mapping.

        Existing terms are preserved and case-insensitive duplicates are skipped across all
        levels.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.sync_legacy_tags_from_wemi()
            ()


        :return: Tuple of newly added term strings.
        """
        return self._sync_legacy_terms_from_wemi(field="tags", relation_key="tags")

    def sync_legacy_labels_from_wemi(self) -> tuple[str, ...]:
        """
        Append distinct WEMI labels to the legacy labels mapping.

        Existing terms are preserved and case-insensitive duplicates are skipped across all
        levels.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.sync_legacy_labels_from_wemi()
            ()


        :return: Tuple of newly added term strings.
        """
        return self._sync_legacy_terms_from_wemi(field="labels", relation_key="labels")

    def sync_legacy_genres_from_wemi(self) -> tuple[str, ...]:
        """
        Append distinct WEMI genres to the legacy genre mapping.

        Existing terms are preserved and case-insensitive duplicates are skipped across all
        levels.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.sync_legacy_genres_from_wemi()
            ()


        :return: Tuple of newly added term strings.
        """
        return self._sync_legacy_terms_from_wemi(field="genre", relation_key="genres")

    def sync_legacy_subjects_from_wemi(self) -> tuple[str, ...]:
        """
        Append distinct WEMI subjects to the legacy subject mapping.

        Existing terms are preserved and case-insensitive duplicates are skipped across all
        levels.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.sync_legacy_subjects_from_wemi()
            ()


        :return: Tuple of newly added term strings.
        """
        return self._sync_legacy_terms_from_wemi(field="subject", relation_key="subjects")

    def sync_legacy_series_from_wemi(self) -> tuple[str, ...]:
        """
        Append distinct WEMI series to the legacy series mapping.

        Existing terms are preserved and case-insensitive duplicates are skipped across all
        levels.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.sync_legacy_series_from_wemi()
            ()


        :return: Tuple of newly added term strings.
        """
        return self._sync_legacy_terms_from_wemi(field="series", relation_key="series")

    def sync_legacy_identifiers_from_wemi(self) -> tuple[tuple[str, str], ...]:
        """
        Add supported WEMI identifiers across levels to the legacy identifier stores.

        Case-insensitive scheme/value duplicates are skipped. A pair is reported only when
        the inherited setter changes the saved identifier set; unsupported schemes may
        therefore be omitted.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_liuxin_wemi_metadata.py


        :return: Tuple of newly saved scheme/value pairs.
        """
        synced: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = {
            (str(scheme).casefold(), str(value).casefold())
            for scheme, values in self.get_identifiers().items()
            for value in values
        }

        for level in self._LEVELS:
            try:
                links = self.get_wemi_relation_links(level, "identifiers")
            except KeyError:
                continue
            for link in links:
                pair = self._wemi_identifier_pair(link.target)
                if pair is None:
                    continue
                scheme, value = pair
                key = (scheme.casefold(), value.casefold())
                if key in seen:
                    continue
                before = set(seen)
                self.set_identifier(scheme, value)
                seen = {
                    (str(saved_scheme).casefold(), str(saved_value).casefold())
                    for saved_scheme, values in self.get_identifiers().items()
                    for saved_value in values
                }
                if seen == before:
                    continue
                synced.append((scheme, value))

        return tuple(synced)

    def _sync_legacy_terms_from_wemi(self, *, field: str, relation_key: WemiRelationKey) -> tuple[str, ...]:
        """
        Append distinct relation terms to an existing legacy field mapping.

        Traverse work through item, skip unsupported buckets and preserve the first
        case-insensitive spelling/id. The destination field must already hold a mutable
        mapping.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_liuxin_wemi_metadata.py


        :param field: Exact existing legacy storage key containing the mutable destination mapping.
        :param relation_key: normalized relation bucket key from the selected bundle's
            ``RELATION_KEYS``.
        :return: Tuple of newly added term strings.
        """
        data = object.__getattribute__(self, "_data")
        terms = data[field]
        synced: list[str] = []
        seen = {str(term).casefold() for term in terms}

        for level in self._LEVELS:
            try:
                links = self.get_wemi_relation_links(level, relation_key)
            except KeyError:
                continue
            for link in links:
                term = self._wemi_tag_text(link.target)
                if term is None:
                    continue
                term_key = term.casefold()
                if term_key in seen:
                    continue
                terms[term] = self._wemi_tag_id(link.target)
                synced.append(term)
                seen.add(term_key)

        return tuple(synced)

    @staticmethod
    def _wemi_tag_text(target: Any) -> str | None:
        """
        Choose the first nonblank supported term label from a relation target.

        Rows use row_dict, mappings use keys and strings are stripped directly. Attribute
        fallback applies only when the extracted mapping is empty.

        Example:
            >>> LiuXinWEMIMetadata._wemi_tag_text({'genre_full': ' Fiction / SF '})
            'Fiction / SF'


        :param target: Row, mapping, string or attribute-bearing relation target.
        :return: Stripped term text, or None.
        """
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

        for key in (
            "label_text",
            "genre_full",
            "genre",
            "genre_name",
            "series_full",
            "series",
            "series_name",
            "subject_full",
            "subject",
            "subject_name",
            "tag_name",
            "tag",
            "name",
            "text",
        ):
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
    def _wemi_tag_id(target: Any) -> int | None:
        """
        Try label, genre, series, subject, tag and generic id fields in order.

        Invalid integer conversions are skipped. A database Row's row_id is the final
        fallback.

        Example:
            >>> LiuXinWEMIMetadata._wemi_tag_id({'tag_id': '7'})
            7


        :param target: Row, mapping, string or attribute-bearing relation target.
        :return: First convertible id, or None.
        """
        mapping: Mapping[str, Any]
        if isinstance(target, Row):
            mapping = target.row_dict
        elif isinstance(target, Mapping):
            mapping = target
        else:
            mapping = {}

        for key in ("label_id", "genre_id", "series_id", "subject_id", "tag_id", "id"):
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

    @staticmethod
    def _wemi_identifier_pair(target: Any) -> tuple[str, str] | None:
        """
        Extract a stripped identifier scheme/value pair using supported field aliases.

        Rows and mappings are preferred; attribute fallback is used only for an empty
        mapping. Missing or blank components produce None.

        Example:
            >>> LiuXinWEMIMetadata._wemi_identifier_pair({'scheme': ' doi ', 'value': ' 10/example '})
            ('doi', '10/example')


        :param target: Row, mapping, string or attribute-bearing relation target.
        :return: Scheme/value tuple, or None.
        """
        mapping: Mapping[str, Any]
        if isinstance(target, Row):
            mapping = target.row_dict
        elif isinstance(target, Mapping):
            mapping = target
        else:
            mapping = {}

        scheme = None
        for key in (
            "entity_identifier_scheme",
            "item_identifier_scheme",
            "identifier_scheme",
            "scheme",
            "type",
        ):
            scheme = mapping.get(key)
            if scheme is None and not mapping:
                scheme = getattr(target, key, None)
            if scheme not in (None, ""):
                break

        value = None
        for key in (
            "entity_identifier_value",
            "item_identifier_value",
            "identifier_value",
            "value",
            "identifier",
        ):
            value = mapping.get(key)
            if value is None and not mapping:
                value = getattr(target, key, None)
            if value not in (None, ""):
                break

        if scheme in (None, "") or value in (None, ""):
            return None
        scheme_text = str(scheme).strip()
        value_text = str(value).strip()
        if not scheme_text or not value_text:
            return None
        return scheme_text, value_text

    @classmethod
    def _is_empty_pretty_value(cls, value: Any) -> bool:
        """
        Treat None, empty text and empty supported collections as absent display values.

        Zero and False remain meaningful values. Mapping length checks may materialize lazy
        wrappers.

        Example:
            >>> LiuXinWEMIMetadata._is_empty_pretty_value(0)
            False


        :param value: Value to inspect or render.
        :return: True for a recognized empty value.
        """
        if value is None:
            return True
        if value == "":
            return True
        if isinstance(value, (Mapping, list, tuple, set, frozenset)):
            return len(value) == 0
        return False

    @classmethod
    def _filtered_mapping(
        cls,
        mapping: Mapping[str, Any],
        include_empty: bool,
    ) -> dict[str, Any]:
        """
        Copy mapping entries to string keys, optionally omitting empty values.

        Values remain shared with the input mapping; key stringification can collapse
        distinct keys.

        Example:
            >>> LiuXinWEMIMetadata._filtered_mapping({'a': None, 'b': 0}, False)
            {'b': 0}


        :param mapping: Mapping whose current entries are read without modifying the input.
        :param include_empty: Retain empty values or zero relation counts when True.
        :return: New filtered dictionary.
        """
        return {
            str(key): value
            for key, value in mapping.items()
            if include_empty or not cls._is_empty_pretty_value(value)
        }

    @classmethod
    def _append_pretty_value(
        cls,
        lines: list[str],
        label: str,
        value: Any,
        *,
        indent: str = "",
    ) -> None:
        """
        Append one labeled diagnostic line when its value is nonempty.

        Example:
            >>> lines = []
            >>> LiuXinWEMIMetadata._append_pretty_value(lines, 'Count', 0)
            >>> lines
            ['Count: 0']


        :param lines: Output list extended in place with formatted lines.
        :param label: Label placed before the rendered value.
        :param value: Value to inspect or render.
        :param indent: Literal indentation prefix for appended lines.
        :return: None.
        """
        if cls._is_empty_pretty_value(value):
            return
        lines.append(f"{indent}{label}: {cls._format_pretty_value(value)}")

    @classmethod
    def _append_pretty_mapping(
        cls,
        lines: list[str],
        mapping: Mapping[str, Any],
        *,
        indent: str,
    ) -> None:
        """
        Append one diagnostic line per mapping entry in iteration order.

        Empty values are not filtered by this helper.

        Example:
            >>> lines = []
            >>> LiuXinWEMIMetadata._append_pretty_mapping(lines, {'x': 1}, indent='  ')
            >>> lines
            ['  x: 1']


        :param lines: Output list extended in place with formatted lines.
        :param mapping: Mapping whose current entries are read without modifying the input.
        :param indent: Literal indentation prefix for appended lines.
        :return: None.
        """
        for key, value in mapping.items():
            lines.append(f"{indent}{key}: {cls._format_pretty_value(value)}")

    @classmethod
    def _format_pretty_value(
        cls,
        value: Any,
        *,
        max_items: int = 5,
        max_chars: int = 160,
    ) -> str:
        """
        Format bounded diagnostic text for strings, mappings and collections.

        String whitespace is collapsed; mapping keys sort case-insensitively. Nested values
        use default limits, and sets retain their iteration order. Truncation adds a spaced
        ellipsis, so very small max_chars values are not a strict cap.

        Example:
            >>> LiuXinWEMIMetadata._format_pretty_value({'b': 2, 'a': 1}, max_items=1)
            '{a=1, ... (+1 more)}'


        :param value: Value to inspect or render.
        :param max_items: Maximum entries displayed at the current collection level.
        :param max_chars: Target character limit used to truncate the current rendered
            value.
        :return: Diagnostic string.
        """
        if value is None:
            return "None"
        if isinstance(value, str):
            text = " ".join(value.split())
        elif isinstance(value, Mapping):
            items = sorted(value.items(), key=lambda item: str(item[0]).casefold())
            rendered = [
                "{}={}".format(key, cls._format_pretty_value(one_value))
                for key, one_value in items[:max_items]
            ]
            if len(items) > max_items:
                rendered.append("... (+{} more)".format(len(items) - max_items))
            text = "{" + ", ".join(rendered) + "}"
        elif isinstance(value, (list, tuple, set, frozenset)):
            values = list(value)
            rendered = [
                cls._format_pretty_value(one_value)
                for one_value in values[:max_items]
            ]
            if len(values) > max_items:
                rendered.append("... (+{} more)".format(len(values) - max_items))
            text = "[" + ", ".join(rendered) + "]"
        else:
            text = str(value)

        if len(text) > max_chars:
            return text[: max(0, max_chars - 4)].rstrip() + " ..."
        return text

    @classmethod
    def _identity_to_mapping(cls, identity: WemiIdentity) -> dict[str, Any]:
        """
        Copy identity data through to_mapping, then to_dict when no callable to_mapping exists.

        Conversion failures propagate; unsupported objects yield an empty dictionary.

        Example:
            >>> LiuXinWEMIMetadata._identity_to_mapping(object())
            {}


        :param identity: Identity object exposing to_mapping or to_dict.
        :return: New identity dictionary.
        """
        to_mapping = getattr(identity, "to_mapping", None)
        if callable(to_mapping):
            return dict(to_mapping())
        to_dict = getattr(identity, "to_dict", None)
        if callable(to_dict):
            return dict(to_dict())
        return {}

    @classmethod
    def _pretty_identity_mapping(
        cls,
        level: WemiLevel,
        identity: WemiIdentity,
        include_empty: bool,
    ) -> dict[str, Any]:
        """
        Select the configured display fields from one identity mapping.

        Only keys present in the serialized identity are considered; empty values are
        optionally removed.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_liuxin_wemi_metadata.py


        :param level: WEMI level identifying the bundle to access.
        :param identity: Identity object exposing to_mapping or to_dict.
        :param include_empty: Retain empty values or zero relation counts when True.
        :return: New dictionary in configured display-field order.
        """
        identity_mapping = cls._identity_to_mapping(identity)
        fields = cls._PRETTY_IDENTITY_FIELDS[level]
        preferred = {
            field: identity_mapping.get(field)
            for field in fields
            if field in identity_mapping
        }
        return cls._filtered_mapping(preferred, include_empty)

    @classmethod
    def _pretty_relation_summary(
        cls,
        metadata: WemiMetadataBundle,
        include_empty: bool,
    ) -> list[str]:
        """
        Count links for each exposed relation bucket in order.

        Example:
            >>> LiuXinWEMIMetadata._pretty_relation_summary(WorkMetadata(), False)
            []


        :param metadata: WEMI bundle whose relation names and links are read.
        :param include_empty: Retain empty values or zero relation counts when True.
        :return: List of name: count fragments, omitting zero counts unless requested.
        """
        summaries: list[str] = []
        for relation_key in metadata.relation_names():
            count = len(metadata.get_relation_links(relation_key))
            if count or include_empty:
                summaries.append(f"{relation_key}: {count}")
        return summaries

    def _pretty_legacy_mapping(self, include_empty: bool) -> dict[str, Any]:
        """
        Collect configured legacy display fields and grouped identifier values.

        Empty values are optionally filtered. Ordinary field values remain shared with
        storage; identifier accessors may perform their own copying or hydration.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata._pretty_legacy_mapping(False)["title"]
            'Example'


        :param include_empty: Include empty mapping entries in the diagnostic summary when
            True.
        :return: New diagnostic mapping.
        """
        data = object.__getattribute__(self, "_data")
        preferred: dict[str, Any] = {}
        for key in self._PRETTY_LEGACY_FIELDS:
            if key == "identifiers":
                preferred[key] = self._filtered_mapping(
                    self.get_identifiers(),
                    include_empty,
                )
            elif key == "internal_identifiers":
                preferred[key] = self._filtered_mapping(
                    self.get_internal_identifiers(),
                    include_empty,
                )
            elif key in data:
                preferred[key] = data.get(key)
        return self._filtered_mapping(preferred, include_empty)

    def to_wemi_mapping(self, include_related: bool = True) -> dict[str, Any]:
        """
        Serialize each bundle under its canonical WEMI level name.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> tuple(metadata.to_wemi_mapping())
            ('work', 'expression', 'manifestation', 'item')


        :param include_related: Include relation targets and link payloads in each
            serialized bundle when True.
        :return: New level-to-bundle-payload dictionary.
        """
        return {
            "work": self.work_metadata.to_mapping(include_related=include_related),
            "expression": self.expression_metadata.to_mapping(
                include_related=include_related,
            ),
            "manifestation": self.manifestation_metadata.to_mapping(
                include_related=include_related,
            ),
            "item": self.item_metadata.to_mapping(include_related=include_related),
        }

    def to_sidecar_mapping(
        self,
        include_related: bool = True,
        include_legacy: bool = True,
    ) -> dict[str, Any]:
        """
        Build the standard sidecar mapping with schema, ids, titles and WEMI records.

        Legacy fields are deep-copied when included. include_related controls bundle
        serialization; the separate relation_link_ids summary is still emitted. No file is
        written.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> payload = metadata.to_sidecar_mapping(include_legacy=False)
            >>> payload["schema_version"], "liuxin" in payload
            (1, False)


        :param include_related: Include relation targets and link payloads in each
            serialized bundle when True.
        :param include_legacy: Include the legacy LiuXin field mapping when True.
        :return: Sidecar payload dictionary.
        """
        payload: dict[str, Any] = {
            "schema": self.SIDECAR_SCHEMA_NAME,
            "schema_version": self.SIDECAR_SCHEMA_VERSION,
            "database_ids": self.database_ids,
            "relation_link_ids": self.relation_link_ids,
            "titles": list(self.titles),
            "wemi": self.to_wemi_mapping(include_related=include_related),
        }
        if include_legacy:
            payload["liuxin"] = self.get_data(rtn_deepcopy=True)
        return payload

    def to_mapping(
        self,
        include_related: bool = True,
        include_legacy: bool = True,
    ) -> dict[str, Any]:
        """
        Build a sidecar payload through to_sidecar_mapping with the same options.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.to_mapping()["liuxin"]["title"]
            'Example'


        :param include_related: Include relation targets and link payloads in each
            serialized bundle when True.
        :param include_legacy: Include the legacy LiuXin field mapping when True.
        :return: Sidecar payload dictionary.
        """
        return self.to_sidecar_mapping(
            include_related=include_related,
            include_legacy=include_legacy,
        )

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "LiuXinWEMIMetadata":
        """
        Build a slice from nested wemi payloads or a direct mapping of levels.

        Existing bundle instances are retained, mapping bundles are deserialized, and
        unsupported or absent bundles become empty. A liuxin mapping updates legacy defaults
        by deep copy. Schema/version and summary keys are not validated or used to
        reconstruct identities.

        Example:
            >>> metadata = LiuXinWEMIMetadata.from_mapping({"liuxin": {"title": "Imported"}})
            >>> metadata.title
            'Imported'


        :param payload: Sidecar mapping with optional wemi and liuxin entries, or a mapping
            of WEMI levels.
        :return: New metadata slice.
        """
        wemi_payload = payload.get("wemi", payload)
        if not isinstance(wemi_payload, Mapping):
            wemi_payload = {}

        metadata = cls(
            work_metadata=cls._bundle_from_mapping(
                WorkMetadata,
                wemi_payload.get("work"),
            ),
            expression_metadata=cls._bundle_from_mapping(
                ExpressionMetadata,
                wemi_payload.get("expression"),
            ),
            manifestation_metadata=cls._bundle_from_mapping(
                ManifestationMetadata,
                wemi_payload.get("manifestation"),
            ),
            item_metadata=cls._bundle_from_mapping(
                ItemMetadata,
                wemi_payload.get("item"),
            ),
        )

        liuxin_payload = payload.get("liuxin")
        if isinstance(liuxin_payload, Mapping):
            object.__getattribute__(metadata, "_data").update(
                deepcopy(dict(liuxin_payload)),
            )

        return metadata

    @classmethod
    def from_sidecar_mapping(cls, payload: Mapping[str, Any]) -> "LiuXinWEMIMetadata":
        """
        Deserialize a sidecar through the standard from_mapping path.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> restored = LiuXinWEMIMetadata.from_sidecar_mapping(metadata.to_mapping())
            >>> restored.title
            'Example'


        :param payload: Sidecar mapping with optional wemi and liuxin entries, or a mapping
            of WEMI levels.
        :return: New metadata slice.
        """
        return cls.from_mapping(payload)

    @classmethod
    def from_database(
        cls,
        database: Any,
        *,
        item_id: int | None = None,
        source_row: Mapping[str, Any] | Row | None = None,
    ) -> "LiuXinWEMIMetadata":
        """
        Build an eager slice using the central database hydrator.

        Supply item_id or source_row. This delegates construction to the hydrator, which
        returns LiuXinWEMIMetadata rather than constructing cls itself. The caller owns the
        read-source lifetime.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_liuxin_wemi_metadata.py


        :param database: Caller-owned database/read source retained for metadata access; it
            is not closed here.
        :param item_id: Optional item row id; overrides an item id extracted from
            source_row.
        :param source_row: Database Row or source mapping supplying identity fields and
            row-id hints.
        :return: Eager WEMI metadata slice.
        """
        from LiuXin_alpha.metadata.containers.metadata_containers.liuxin_wemi_metadata_hydrator import (
            LiuXinWEMIMetadataHydrator,
        )

        return LiuXinWEMIMetadataHydrator(database).get_liuxin_wemi_metadata(
            item_id=item_id,
            source_row=source_row,
        )

    @staticmethod
    def _bundle_from_mapping(
        bundle_type: type[WemiMetadataBundle],
        payload: Any,
    ) -> WemiMetadataBundle:
        """
        Retain an existing bundle, deserialize a mapping, or create an empty bundle.

        Example:
            >>> bundle = WorkMetadata()
            >>> LiuXinWEMIMetadata._bundle_from_mapping(WorkMetadata, bundle) is bundle
            True


        :param bundle_type: Concrete bundle class with a no-argument constructor and
            from_mapping.
        :param payload: Existing bundle instance, mapping payload or unsupported value
            treated as empty.
        :return: Supplied bundle instance or a newly constructed bundle.
        """
        if isinstance(payload, bundle_type):
            return payload
        if isinstance(payload, Mapping):
            return bundle_type.from_mapping(payload)
        return bundle_type()

    def deepcopy_metadata(self) -> "LiuXinWEMIMetadata":
        """
        Copy each WEMI bundle and legacy storage into a new object of this concrete type.

        Separate deepcopy calls copy each bundle and the field mapping; the cleanup registry
        is freshly initialized. Use deepcopy when a shared memo across these components is
        required.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> copied = metadata.deepcopy_metadata()
            >>> copied is metadata, copied.work_metadata is metadata.work_metadata
            (False, False)


        :return: Independent metadata object.
        """
        metadata = type(self)(
            work_metadata=deepcopy(self.work_metadata),
            expression_metadata=deepcopy(self.expression_metadata),
            manifestation_metadata=deepcopy(self.manifestation_metadata),
            item_metadata=deepcopy(self.item_metadata),
        )
        object.__setattr__(
            metadata,
            "_data",
            deepcopy(object.__getattribute__(self, "_data")),
        )
        return metadata

    def __deepcopy__(self, memo: dict[int, Any]) -> "LiuXinWEMIMetadata":
        """
        Reuse a memoized clone or copy the bundles and legacy data with a shared memo.

        The clone is registered after its bundle copies are constructed and before legacy
        data is copied. The constructor initializes a fresh cleanup registry.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> copied = deepcopy(metadata)
            >>> copied.title, copied is metadata
            ('Example', False)


        :param memo: Deepcopy identity memo shared with nested bundle and field values.
        :return: Memoized or newly constructed metadata clone.
        """
        existing = memo.get(id(self))
        if existing is not None:
            return existing

        metadata = type(self)(
            work_metadata=deepcopy(self.work_metadata, memo),
            expression_metadata=deepcopy(self.expression_metadata, memo),
            manifestation_metadata=deepcopy(self.manifestation_metadata, memo),
            item_metadata=deepcopy(self.item_metadata, memo),
        )
        memo[id(self)] = metadata
        object.__setattr__(
            metadata,
            "_data",
            deepcopy(object.__getattribute__(self, "_data"), memo),
        )
        return metadata

    def smart_update(
        self,
        other: CalibreLikeLiuXinBookMetaData,
        replace_metadata: bool = False,
    ) -> None:
        """
        Merge legacy fields, then copy eligible incoming WEMI bundles as whole bundles.

        WEMI copying applies only when other is a LiuXinWEMIMetadata instance. Replacement
        overwrites every bundle, including empty ones; otherwise only bundles without
        identities or links are replaced.

        Example:
            >>> metadata = LiuXinWEMIMetadata("Example")
            >>> metadata.smart_update(LiuXinWEMIMetadata("Updated"))
            >>> metadata.title
            'Updated'


        :param other: Compatible source metadata object passed first to the inherited legacy
            merge.
        :param replace_metadata: Replace supported existing metadata when True; otherwise
            use the family merge rules.
        :return: None.
        """
        super().smart_update(other, replace_metadata=replace_metadata)

        if not isinstance(other, LiuXinWEMIMetadata):
            return

        for storage_name in self._METADATA_STORAGE_BY_LEVEL.values():
            current_bundle = object.__getattribute__(self, storage_name)
            incoming_bundle = object.__getattribute__(other, storage_name)
            if replace_metadata or not self._metadata_bundle_has_content(current_bundle):
                object.__setattr__(self, storage_name, deepcopy(incoming_bundle))

    @staticmethod
    def _metadata_bundle_has_content(bundle: WemiMetadataBundle) -> bool:
        """
        Check whether a bundle has any non-None identity or nonempty relation bucket.

        Example:
            >>> LiuXinWEMIMetadata._metadata_bundle_has_content(WorkMetadata())
            False


        :param bundle: Bundle exposing WEMI identities, relation_names and
            get_relation_links.
        :return: True for any identity or link; False for an empty bundle.
        """
        for identity_attr in ("work", "expression", "manifestation", "item"):
            if hasattr(bundle, identity_attr) and getattr(bundle, identity_attr) is not None:
                return True

        for relation_key in bundle.relation_names():
            if bundle.get_relation_links(relation_key):
                return True
        return False


LiuXinWEMI = LiuXinWEMIMetadata


__all__ = [
    "LiuXinWEMI",
    "LiuXinWEMIMetadata",
    "WemiIdentity",
    "WemiLevel",
    "WemiMetadataBundle",
    "WemiRelationKey",
    "WemiRelationLink",
    "WemiRelationTarget",
]
