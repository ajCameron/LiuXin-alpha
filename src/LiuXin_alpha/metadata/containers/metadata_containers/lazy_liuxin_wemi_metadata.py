"""
Provide opt-in lazy legacy fields and WEMI relation buckets for an item slice.

Projection views require explicit dependency loading, while supported legacy reads
may materialize fields automatically. Backing resources must remain available until
deferred access completes.

Example:
    >>> metadata = LazyLiuXinWEMIMetadata()
    >>> metadata.install_lazy_value_to_id("tags", lambda: {"deferred": 7})
    >>> metadata.is_lazy_field_loaded("tags")
    False
    >>> list(metadata.hydrate_field("tags"))
    ['deferred']
"""

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
    Extend an item metadata slice with deferred field and relation loaders.

    The identity stack stays available immediately. Relation loaders are popped before
    execution, and identifier hydration marks itself started before synchronizing;
    failures propagate without automatically restoring those pending states.

    Example:
        >>> metadata = LazyLiuXinWEMIMetadata()
        >>> metadata.install_lazy_value_to_id("tags", lambda: {"deferred": 7})
        >>> metadata.load("tags") is metadata
        True
        >>> metadata.values.tags
        ('deferred',)
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
        """
        Initialize the eager metadata state and empty lazy-loader bookkeeping.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_liuxin_wemi_metadata.py


        :param args: Positional title, authors and other metadata arguments forwarded to the
            eager constructor.
        :param kwargs: Keyword arguments, including WEMI bundles, forwarded to the eager
            constructor.
        :return: None.
        """
        super().__init__(*args, **kwargs)
        object.__setattr__(self, "_lazy_relation_loaders", {})
        object.__setattr__(self, "_lazy_identifiers_loaded", False)

    def install_lazy_value_to_id(
        self,
        field: str,
        loader: LegacyValueToIDLoader,
    ) -> None:
        """
        Replace a normalized legacy field with a lazy mapping wrapper.

        The previous field value is discarded; the loader runs on materialization rather
        than registration.

        Example:
            >>> metadata = LazyLiuXinWEMIMetadata()
            >>> metadata.install_lazy_value_to_id("tags", lambda: {"deferred": 7})
            >>> metadata.is_lazy_field_loaded("tags")
            False


        :param field: Legacy metadata field name; supported aliases are normalized by the
            implementation.
        :param loader: Zero-argument callable returning a value-to-id mapping.
        :return: None.
        """
        field_key = self._normalize_lazy_legacy_field(field)
        data = object.__getattribute__(self, "_data")
        data[field_key] = LazyValueToID(loader, label=field_key)

    def install_lazy_relation_loader(
        self,
        level: str,
        relation_key: WemiRelationKey,
        loader: WemiRelationLoader,
    ) -> None:
        """
        Register or replace a loader for a validated level/relation pair.

        Unknown levels or relation names raise KeyError before the loader is stored.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_liuxin_wemi_metadata.py


        :param level: WEMI level identifying the bundle to access.
        :param relation_key: normalized relation bucket key from the selected bundle's
            ``RELATION_KEYS``.
        :param loader: Zero-argument callable returning the relation links to install.
        :return: None.
        """
        level_key = self.normalize_wemi_level(level)
        relation_key = self.get_wemi_metadata(level_key).validate_relation_name(relation_key)
        loaders = object.__getattribute__(self, "_lazy_relation_loaders")
        loaders[(level_key, relation_key)] = loader

    def get_wemi_relation_links(
        self,
        level: str,
        relation_key: WemiRelationKey,
    ) -> list[WemiRelationLink]:
        """
        Materialize a pending bucket loader, then read its relation links.

        The loader is removed before invocation. Loader or assignment failures propagate and
        do not restore the pending loader automatically.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_liuxin_wemi_metadata.py


        :param level: WEMI level identifying the bundle to access.
        :param relation_key: normalized relation bucket key from the selected bundle's
            ``RELATION_KEYS``.
        :return: List of current links in the requested bucket.
        """
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
        """
        Materialize the selected bucket and project its target objects.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_liuxin_wemi_metadata.py


        :param level: WEMI level identifying the bundle to access.
        :param relation_key: normalized relation bucket key from the selected bundle's
            ``RELATION_KEYS``.
        :return: New list containing the link targets.
        """
        return [link.target for link in self.get_wemi_relation_links(level, relation_key)]

    def to_calibre(self) -> Any:
        """
        Load all pending dependencies before converting a copied slice to Calibre.

        Hydration changes this object's loaded state; the subsequent flat conversion cannot
        retain all WEMI provenance.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_liuxin_wemi_metadata.py


        :return: New Calibre-compatible metadata object.
        """
        self.load()
        return super().to_calibre()

    def load(self, *fields: str) -> "LazyLiuXinWEMIMetadata":
        """
        Load selected projection dependencies, or every pending dependency with no fields.

        The all-fields form hydrates legacy wrappers and identifiers before draining
        remaining relation loaders.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_liuxin_wemi_metadata.py


        :param fields: Projection field names whose legacy and relation dependencies should
            be loaded.
        :return: This metadata object.
        """
        if not fields:
            self.force_hydrate()
            self._hydrate_all_relation_loaders()
            return self

        for field in fields:
            self._hydrate_projection_dependencies(field)
        return self

    def hydrate_field(self, field: str) -> OrderedDict[str, Any] | Any:
        """
        Materialize a legacy field and replace its lazy wrapper with the live mapping.

        Identifier requests synchronize WEMI identifiers and return the identifier mapping.
        Other absent fields return None.

        Example:
            >>> metadata = LazyLiuXinWEMIMetadata()
            >>> metadata.install_lazy_value_to_id("tags", lambda: {"deferred": 7})
            >>> list(metadata.hydrate_field("tags").items())
            [('deferred', 7)]


        :param field: Legacy metadata field name; supported aliases are normalized by the
            implementation.
        :return: Hydrated field value; ordinary mappings are retained in legacy storage.
        """
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
        """
        Hydrate selected legacy fields, defaulting to wrappers and pending identifiers.

        Unrelated relation loaders may remain pending; use load without arguments to drain
        them all.

        Example:
            >>> metadata = LazyLiuXinWEMIMetadata()
            >>> metadata.install_lazy_value_to_id("tags", lambda: {"deferred": 7})
            >>> metadata.force_hydrate(["tags"]) is metadata
            True


        :param fields: Iterable of legacy field names; None selects every wrapper and
            pending identifiers.
        :return: This metadata object.
        """
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
        """
        List fields retaining lazy wrappers, plus pending identifier synchronization.

        A wrapper whose internal loaded flag is True still appears until hydrate_field
        replaces it.

        Example:
            >>> metadata = LazyLiuXinWEMIMetadata()
            >>> metadata.install_lazy_value_to_id("tags", lambda: {"deferred": 7})
            >>> "tags" in metadata.lazy_fields()
            True


        :return: Tuple of wrapper field names and, when pending, identifiers.
        """
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
        """
        Inspect a legacy wrapper's flag or the identifier synchronization flag.

        Fields without a wrapper, including absent names, count as loaded.

        Example:
            >>> metadata = LazyLiuXinWEMIMetadata()
            >>> metadata.install_lazy_value_to_id("tags", lambda: {"deferred": 7})
            >>> metadata.is_lazy_field_loaded("unregistered")
            True


        :param field: Legacy metadata field name; supported aliases are normalized by the
            implementation.
        :return: Whether the field has no pending legacy materialization.
        """
        field_key = self._normalize_lazy_legacy_field(field)
        if field_key == "identifiers":
            return bool(object.__getattribute__(self, "_lazy_identifiers_loaded"))
        data = object.__getattribute__(self, "_data")
        value = data.get(field_key)
        if isinstance(value, LazyValueToID):
            return value.loaded
        return True

    def get_identifiers(self):
        """
        Synchronize pending WEMI identifiers before reading the legacy identifier view.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_liuxin_wemi_metadata.py


        :return: Grouped identifier values returned by the inherited identifier accessor.
        """
        self._hydrate_identifiers()
        return super().get_identifiers()

    def _hydrate_identifiers(self) -> None:
        """
        Mark identifiers loaded, then synchronize supported WEMI identifier targets.

        Setting the flag first prevents recursive synchronization. A later failure leaves
        the flag set.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_liuxin_wemi_metadata.py


        :return: None.
        """
        if object.__getattribute__(self, "_lazy_identifiers_loaded"):
            return
        object.__setattr__(self, "_lazy_identifiers_loaded", True)
        self.sync_legacy_identifiers_from_wemi()

    def _hydrate_projection_dependencies(self, field: str) -> None:
        """
        Hydrate a field wrapper or identifiers, then matching relation buckets.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_liuxin_wemi_metadata.py


        :param field: Legacy metadata field name; supported aliases are normalized by the
            implementation.
        :return: None.
        """
        field_key = self._normalize_lazy_legacy_field(field)
        data = object.__getattribute__(self, "_data")
        if field_key == "identifiers" or isinstance(data.get(field_key), LazyValueToID):
            self.hydrate_field(field_key)

        relation_key = self._normalize_projection_relation_key(field)
        self._hydrate_relation_loaders_for_relation(relation_key)

    def _hydrate_all_relation_loaders(self) -> None:
        """
        Drain a snapshot of currently registered relation loader keys.

        Each bucket read removes its loader before running it; failures stop the loop.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_liuxin_wemi_metadata.py


        :return: None.
        """
        loaders = object.__getattribute__(self, "_lazy_relation_loaders")
        for level_key, relation_key in list(loaders):
            self.get_wemi_relation_links(level_key, relation_key)

    def _hydrate_relation_loaders_for_relation(self, relation_key: str) -> None:
        """
        Read a relation bucket on every WEMI level that supports its name.

        Unsupported relation names are skipped; loader failures propagate.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_liuxin_wemi_metadata.py


        :param relation_key: Relation name to validate against each level before loading.
        :return: None.
        """
        for level in self._LEVELS:
            metadata = self.get_wemi_metadata(level)
            try:
                normalized_relation_key = metadata.validate_relation_name(relation_key)
            except KeyError:
                continue
            self.get_wemi_relation_links(level, normalized_relation_key)

    @classmethod
    def _normalize_projection_relation_key(cls, field: str) -> str:
        """
        Normalize case/whitespace and expand supported projection aliases.

        Unrecognized names pass through after string normalization.

        Example:
            >>> LazyLiuXinWEMIMetadata._normalize_projection_relation_key(" Authors ")
            'agents'


        :param field: Legacy metadata field name; supported aliases are normalized by the
            implementation.
        :return: Normalized relation name.
        """
        normalized = str(field).strip().lower()
        return cls._PROJECTION_RELATION_ALIASES.get(normalized, normalized)

    def direct_get(self, item: str) -> Any:
        """
        Hydrate supported lazy aliases before the inherited direct field lookup.

        After a wrapper has been replaced, ordinary lookups use the original exact key.
        Identifier requests always use the grouped accessor.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_liuxin_wemi_metadata.py


        :param item: Legacy field lookup key; supported aliases apply while lazy.
        :return: Hydrated value or inherited direct field value.
        """
        data = object.__getattribute__(self, "_data")
        field_key = self._LEGACY_FIELD_ALIASES.get(str(item).strip().lower())
        if field_key == "identifiers":
            return self.get_identifiers()
        if field_key is not None and isinstance(data.get(field_key), LazyValueToID):
            return self.hydrate_field(field_key)
        return super().direct_get(item)

    def __getattr__(self, item: str) -> Any:
        """
        Hydrate supported legacy fields and recognized identifier schemes on attribute reads.

        A newly hydrated ordinary field returns its live mapping; later inherited attribute
        reads retain the base class copy behavior.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_liuxin_wemi_metadata.py


        :param item: Attribute name requested through the legacy metadata interface.
        :return: Hydrated value or the inherited attribute result.
        """
        data = object.__getattribute__(self, "_data")
        field_key = self._LEGACY_FIELD_ALIASES.get(str(item).strip().lower())
        if field_key == "identifiers" or standardize_id_name(str(item)) is not None:
            self._hydrate_identifiers()
        if field_key is not None and isinstance(data.get(field_key), LazyValueToID):
            return self.hydrate_field(field_key)
        return super().__getattr__(item)

    def __getitem__(self, item: str) -> Any:
        """
        Hydrate supported lazy aliases before an exact inherited mapping lookup.

        Identifier requests use the grouped accessor; other missing exact keys raise
        KeyError.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_liuxin_wemi_metadata.py


        :param item: Legacy field lookup key; supported aliases apply while lazy.
        :return: Hydrated value or stored field value.
        """
        data = object.__getattribute__(self, "_data")
        field_key = self._LEGACY_FIELD_ALIASES.get(str(item).strip().lower())
        if field_key == "identifiers":
            return self.get_identifiers()
        if field_key is not None and isinstance(data.get(field_key), LazyValueToID):
            return self.hydrate_field(field_key)
        return super().__getitem__(item)

    @classmethod
    def _is_empty_pretty_value(cls, value: Any) -> bool:
        """
        Keep an unloaded lazy mapping visible without materializing it.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_liuxin_wemi_metadata.py


        :param value: Value being considered for diagnostic omission.
        :return: False for an unloaded wrapper; otherwise the eager emptiness result.
        """
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
        """
        Render unloaded lazy mappings as placeholders without running their loaders.

        Loaded wrappers and ordinary values use the eager bounded formatter.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_liuxin_wemi_metadata.py


        :param value: Value to render.
        :param max_items: Maximum entries included by the eager formatter.
        :param max_chars: Maximum text length requested from the eager formatter.
        :return: Diagnostic value text.
        """
        if isinstance(value, LazyValueToID) and not value.loaded:
            return repr(value)
        return super()._format_pretty_value(
            value,
            max_items=max_items,
            max_chars=max_chars,
        )

    @classmethod
    def _normalize_lazy_legacy_field(cls, field: str) -> str:
        """
        Normalize whitespace/case and map recognized legacy field aliases.

        Example:
            >>> LazyLiuXinWEMIMetadata._normalize_lazy_legacy_field(" Genres ")
            'genre'


        :param field: Legacy metadata field name; supported aliases are normalized by the
            implementation.
        :return: Canonical legacy key, or the normalized original name.
        """
        field_key = cls._LEGACY_FIELD_ALIASES.get(str(field).strip().lower())
        return field_key if field_key is not None else str(field).strip().lower()

    @staticmethod
    def relation_target_text(target: Any, relation_key: WemiRelationKey) -> str | None:
        """
        Extract a nonblank display value using relation-specific column priorities.

        Rows use row_dict; mappings use keys; strings are stripped directly. Attribute
        fallback is used only when the extracted mapping is empty.

        Example:
            >>> LazyLiuXinWEMIMetadata.relation_target_text({"tag": " example "}, "tags")
            'example'


        :param target: Row, mapping, string or attribute-bearing relation target.
        :param relation_key: normalized relation bucket key from the selected bundle's
            ``RELATION_KEYS``.
        :return: First usable stripped text, or None.
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
        """
        Extract an integer target id using relation-specific id keys.

        Invalid candidate conversions are skipped; a database Row's own row_id is the final
        fallback.

        Example:
            >>> LazyLiuXinWEMIMetadata.relation_target_id({"tag_id": "7"}, "tags")
            7


        :param target: Row, mapping or attribute-bearing relation target.
        :param relation_key: normalized relation bucket key from the selected bundle's
            ``RELATION_KEYS``.
        :return: Converted id, or None.
        """
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
        """
        Read a relation across levels into an ordered legacy field mapping.

        Ordinary terms use first-wins case-insensitive deduplication with target row ids.
        Ratings map source labels to values and later entries overwrite the same label.
        Unsupported level buckets are skipped.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_liuxin_wemi_metadata.py


        :param field: Legacy metadata field name; supported aliases are normalized by the
            implementation.
        :param relation_key: normalized relation bucket key from the selected bundle's
            ``RELATION_KEYS``.
        :return: Ordered term-to-id or rating-source-to-value mapping.
        """
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
        """
        Extract a rating source key and value from a Row or mapping.

        A present Calibre-viewer rating wins, including zero. Otherwise use rating and the
        source label, falling back to rating as the key.

        Example:
            >>> LazyLiuXinWEMIMetadata._rating_key_value({"rating": 0})
            ('rating', 0)


        :param target: Rating Row or mapping; other objects provide no rating fields.
        :return: Source/value pair, or (None, None) when no rating is available.
        """
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
        """
        Build a lazy slice using the dedicated database hydrator.

        Provide item_id or source_row. The returned loaders retain the caller-owned
        database, which must remain usable for later reads.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_liuxin_wemi_metadata.py


        :param database: Caller-owned database/read source retained for metadata access; it
            is not closed here.
        :param item_id: Optional item row id; overrides an item id extracted from
            source_row.
        :param source_row: Database Row or source mapping supplying identity fields and
            row-id hints.
        :return: New lazy metadata slice.
        """
        from LiuXin_alpha.metadata.containers.metadata_containers.liuxin_wemi_lazy_metadata_hydrator import (
            LazyLiuXinWEMIMetadataHydrator,
        )

        hydrator = LazyLiuXinWEMIMetadataHydrator(database)
        return hydrator.get_lazy_liuxin_wemi_metadata(
            item_id=item_id,
            source_row=source_row,
        )

    def __deepcopy__(self, memo: dict[int, Any]) -> "LazyLiuXinWEMIMetadata":
        """
        Hydrate legacy fields before copying bundles, field data and pending loaders.

        This mutates the source's hydration state. Unrelated relation loader callables may
        remain deferred and retain their original captured resources.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_liuxin_wemi_metadata.py


        :param memo: Deepcopy identity memo reused for nested bundles and stored values.
        :return: Memoized or newly copied lazy metadata object.
        """
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
        object.__setattr__(
            clone,
            "_lazy_relation_loaders",
            deepcopy(object.__getattribute__(self, "_lazy_relation_loaders"), memo),
        )
        object.__setattr__(
            clone,
            "_lazy_identifiers_loaded",
            object.__getattribute__(self, "_lazy_identifiers_loaded"),
        )
        return clone

    def deepcopy_metadata(self) -> "LazyLiuXinWEMIMetadata":
        """
        Deep-copy this slice, including the hydration side effects of __deepcopy__.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_liuxin_wemi_metadata.py


        :return: Independent stored metadata with any still-pending loader callables
            retained.
        """
        return deepcopy(self)


LazyLiuXinWEMI = LazyLiuXinWEMIMetadata


__all__ = [
    "LazyLiuXinWEMI",
    "LazyLiuXinWEMIMetadata",
]
