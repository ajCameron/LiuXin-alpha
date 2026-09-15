"""
Define structural contracts for mutable Catalog field descriptors.

Records describe schema, storage and search rather than entity values.
Concrete containers store authoritative state in internal maps; this API
documents their supported mapping surface and compatibility limitations.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, KeysView, Mapping, MutableMapping, ValuesView
from typing import Literal, NotRequired, Protocol, TypeAlias, TypedDict, TypeVar, overload, runtime_checkable


KnownFieldMetadataDataType: TypeAlias = Literal[
    "rating",
    "text",
    "comments",
    "datetime",
    "int",
    "float",
    "bool",
    "series",
    "composite",
    "enumeration",
    None,
]
FieldMetadataDataType: TypeAlias = str | None
FieldMetadataKind: TypeAlias = Literal["field", "category", "user", "search"]
FieldMetadataMultiplicity: TypeAlias = Mapping[str, str | None]
FieldMetadataDisplay: TypeAlias = Mapping[str, object]
FieldMetadataSearchTarget: TypeAlias = str | list[str]
FieldMetadataRecord: TypeAlias = MutableMapping[str, object]
GroupedSearchTerms: TypeAlias = Mapping[str, FieldMetadataSearchTarget]
FieldRecordIndexMap: TypeAlias = Mapping[str, int]

_DefaultT = TypeVar("_DefaultT")


class FieldMetadataEntry(TypedDict, total=False):
    """
    Describe optional keys in a mutable field or category descriptor.

    Storage keys identify tables/columns; datatype and is_multiple describe
    values, search_terms lists aliases, and display holds presentation hints.
    The TypedDict permits omitted keys and performs no runtime validation or
    immutability enforcement.

    Example:
        A minimal annotation may use ``entry: FieldMetadataEntry = {"datatype": "text"}``.
    """

    table: NotRequired[str | None]
    table_id: NotRequired[str]
    column: NotRequired[str | None]
    link_column: NotRequired[str | None]
    category_sort: NotRequired[str | None]
    datatype: NotRequired[FieldMetadataDataType]
    is_multiple: NotRequired[FieldMetadataMultiplicity]
    kind: NotRequired[FieldMetadataKind]
    name: NotRequired[str | None]
    search_terms: NotRequired[list[str]]
    is_custom: NotRequired[bool]
    is_category: NotRequired[bool]
    is_csp: NotRequired[bool]
    main_table: NotRequired[str]
    auxiliary_table: NotRequired[str]
    in_table: NotRequired[str]
    liuxin_table_name: NotRequired[str]
    label: NotRequired[str]
    display: NotRequired[FieldMetadataDisplay]
    is_editable: NotRequired[bool]
    colnum: NotRequired[int | None]
    rec_index: NotRequired[int]
    link_attrs: NotRequired[list[str]]
    val_unique: NotRequired[bool]
    clear_unused: NotRequired[bool]


class SerializedFieldMetadataState(TypedDict):
    """
    Describe dynamic maps accepted by the field-metadata deserializer.

    Builtin records are regenerated. Custom/user/search records overlay them;
    the supplied search map replaces generated aliases. The concrete loader
    retains references and does not restore absent Series companions.

    Example:
        Keep builtin aliases in search_term_map when the reconstructed container
        must retain those searches.
    """

    custom_fields: MutableMapping[str, FieldMetadataRecord]
    user_categories: MutableMapping[str, FieldMetadataRecord]
    search_categories: MutableMapping[str, FieldMetadataRecord]
    search_term_map: MutableMapping[str, FieldMetadataSearchTarget]
    custom_label_to_key_map: MutableMapping[str, str]


class FieldMetadataGetterAPI(Protocol):
    """
    Describe exact-key lookup with an optional caller-supplied default.

    Concrete containers bind get to their internal map, so title_sort is not
    resolved to sort as it is by subscription.

    Example:
        An absent title_sort key returns None from get even though subscription
        can retrieve the sort descriptor.
    """

    @overload
    def __call__(self, key: str, /) -> FieldMetadataRecord | None:
        """
        Read a live descriptor by exact key, returning None for a miss.

        Example:
            ``metadata.get("unknown")`` returns None.


        :param key: Internal metadata key; most methods do not resolve search aliases.
        :return: Shared mutable descriptor, or None.
        """

    @overload
    def __call__(self, key: str, default: _DefaultT, /) -> FieldMetadataRecord | _DefaultT:
        """
        Read a live descriptor by exact key with an explicit fallback.

        Example:
            Pass a unique sentinel as default to distinguish an absent key from a stored value.


        :param key: Internal metadata key; most methods do not resolve search aliases.
        :param default: Object returned unchanged when the key is absent.
        :return: Shared mutable descriptor, or the supplied default object.
        """


@runtime_checkable
class FieldMetadataAPI(Protocol):
    """
    Manage mutable field descriptors in ordered internal registries.

    Descriptors describe schema, storage, display hints and search aliases, not
    values on a particular book. Standard keys are unprefixed; custom labels
    normally use #. Kind distinguishes fields, built-in categories, user
    categories and saved searches. Multiplicity mappings define cache/UI split
    and join separators; an empty mapping denotes a single value. Table, column,
    link_column and category_sort describe storage or specialized category reads;
    None may require a derived/cache value. Name is the display label, rec_index
    is the database result position and is_csp denotes colon-separated pairs.

    The dict superclass is retained for compatibility. Supported mapping methods
    use _tb_cats; unrelated inherited dict methods need not reflect those records.
    Direct assignment is forbidden but returned descriptors are live and mutable.
    Custom maps and aliases require the dedicated registration/removal methods.

    Example:
        Use ``metadata["title"]["datatype"]`` to inspect schema; use repositories
        to retrieve the title value belonging to a Work.
    """

    VALID_DATA_TYPES: frozenset[str | None]
    search_items: list[str]
    custom_field_prefix: str
    custom_label_to_key_map: MutableMapping[str, str]
    get: FieldMetadataGetterAPI

    def __getitem__(self, key: str) -> FieldMetadataRecord:
        """
        Read a live descriptor, with title_sort treated as an alias of sort.

        Example:
            Reading ``metadata["title_sort"]`` returns the same record as ``metadata["sort"]``.


        :param key: Internal metadata key; most methods do not resolve search aliases.
        :return: Mutable record stored under the resolved key.
        :raises KeyError: The resolved key is absent.
        """

    def __setitem__(self, key: str, val: FieldMetadataRecord) -> None:
        """
        Reject direct descriptor assignment.

        Example:
            Use add_custom_field or a category method to insert records.


        :param key: Requested key, ignored.
        :param val: Requested value, ignored.
        :return: Never returns normally.
        :raises AttributeError: Assignment is forbidden for these containers.
        """

    def __delitem__(self, key: str) -> None:
        """
        Delete a record from the main ordered map only.

        Search aliases, custom records, label maps and companion fields are not
        cleaned up. No title_sort alias resolution is performed.

        Example:
            Deleting a custom key can leave its record in custom_field_metadata().


        :param key: Internal metadata key; most methods do not resolve search aliases.
        :return: None; updates in-memory metadata only.
        :raises KeyError: The exact key is absent.
        """

    def __iter__(self) -> Iterator[str]:
        """
        Iterate the main map in insertion order.

        Example:
            The virtual title_sort alias is absent unless explicitly stored.


        :return: Iterator of current keys; mutation during iteration can invalidate it.
        """

    def __contains__(self, key: object) -> bool:
        """
        Recognize stored keys and the unconditional title_sort alias.

        Example:
            Even after sort is removed, title_sort still reports membership.


        :param key: Hashable candidate key.
        :return: True for a stored key or title_sort.
        """

    def has_key(self, key: str) -> bool:
        """
        Expose the legacy spelling of the membership operation.

        Example:
            ``metadata.has_key("title_sort")`` includes the virtual alias.


        :param key: Internal metadata key; most methods do not resolve search aliases.
        :return: Same boolean as the in operator.
        """

    def keys(self) -> KeysView[str]:
        """
        Expose a live view of stored metadata keys.

        Example:
            A previously obtained keys view includes a subsequently added category.


        :return: Keys view reflecting subsequent main-map changes.
        """

    def sortable_field_keys(self) -> list[str]:
        """
        Select field records with a non-None datatype.

        Display configuration and actual comparison support are not inspected.

        Example:
            Series index fields qualify; category records do not.


        :return: New list in main-map order.
        """

    def displayable_field_keys(self) -> list[str]:
        """
        Select typed fields while excluding internal display helpers and indexes.

        Example:
            au_map, marked, ondevice, cover, series_sort and recognized series indexes are excluded.


        :return: New list in main-map order.
        """

    def standard_field_keys(self) -> list[str]:
        """
        Select non-custom records whose kind is field.

        Example:
            A custom Series companion has is_custom=False and can appear in this list.


        :return: New list in main-map order.
        """

    def custom_field_keys(self, include_composites: bool = True) -> list[str]:
        """
        Select custom field records, optionally omitting composites.

        Example:
            With include_composites=False, a calculated composite field is omitted.


        :param include_composites: Whether datatype=composite records are included.
        :return: New list in main-map order.
        """

    def all_field_keys(self) -> list[str]:
        """
        Select records whose kind is field, including custom fields.

        Example:
            The news category and user categories do not appear in this field-only list.


        :return: New list excluding category, user and search records.
        """

    def iterkeys(self) -> Iterator[str]:
        """
        Expose the legacy iterator spelling for main-map keys.

        Example:
            Iterating keys includes dynamic categories but does not synthesize aliases.


        :return: Key iterator in insertion order.
        """

    def itervalues(self) -> Iterable[FieldMetadataRecord]:
        """
        Iterate live descriptor objects from the main map.

        Example:
            Mutating a yielded descriptor changes the container; no record copy is made.


        :return: Iterator over shared records in insertion order.
        """

    def values(self) -> ValuesView[FieldMetadataRecord]:
        """
        Expose a live view of the main-map descriptors.

        Example:
            New categories appear in an already obtained values view.


        :return: Values view holding shared mutable records.
        """

    def iteritems(self) -> Iterator[tuple[str, FieldMetadataRecord]]:
        """
        Yield stored key/descriptor pairs in insertion order.

        Example:
            A caller can inspect each record's kind without copying all descriptors.


        :return: Iterator of pairs sharing the live descriptor objects.
        """

    def custom_iteritems(self) -> Iterator[tuple[str, FieldMetadataRecord]]:
        """
        Yield pairs from the dedicated custom-field map.

        Example:
            Generated Series companions are absent because they are not stored in the custom map.


        :return: Iterator of custom keys and shared records.
        """

    def items(self) -> list[tuple[str, FieldMetadataRecord]]:
        """
        Snapshot the main map's pairs without copying descriptors.

        Example:
            Later additions do not extend this list, but existing record mutations remain visible.


        :return: New list of key/record tuples in insertion order.
        """

    def is_custom_field(self, key: str) -> bool:
        """
        Classify a key solely by its custom prefix.

        Example:
            An unknown ``#missing`` key still counts as custom by this predicate.


        :param key: String key to inspect; existence is not required.
        :return: Whether the key starts with custom_field_prefix.
        """

    def is_ignorable_field(self, key: str) -> bool:
        """
        Classify custom-prefixed and @-prefixed names as ignorable.

        Example:
            ``@Shelf`` is ignorable even if no such user category has been registered.


        :param key: String key to inspect; existence is not required.
        :return: True for either namespace prefix.
        """

    def ignorable_field_keys(self) -> list[str]:
        """
        Select stored keys in the custom or @ namespace.

        Example:
            A custom Series companion is included even though its is_custom flag is false.


        :return: New list in main-map order.
        """

    def is_series_index(self, key: str) -> bool:
        """
        Recognize float _index fields with an existing base key.

        Example:
            A float ``#cycle_index`` qualifies when ``#cycle`` exists, regardless of its datatype.


        :param key: Candidate key, including unknown or malformed values.
        :return: Boolean; common lookup/type/attribute errors produce False.
        """

    def key_to_label(self, key: str) -> str:
        """
        Read a stored label, falling back to the exact key.

        Example:
            A stored None label is returned as None; it does not trigger the key fallback.


        :param key: Internal metadata key; most methods do not resolve search aliases.
        :return: Stored label value, or key if no label entry exists.
        :raises KeyError: The exact key is absent; title_sort is not resolved here.
        """

    def label_to_key(self, label: str, prefer_custom: bool = False) -> str:
        """
        Resolve internal keys, non-custom labels and custom labels in precedence order.

        Without custom preference, exact keys win, followed by the first non-custom
        record with a matching label, then the custom label map.

        Example:
            If both title and #title exist, prefer_custom=True selects #title for label title.


        :param label: Internal key or stored label; not a translated display name.
        :param prefer_custom: Try the custom label map before exact keys and non-custom labels.
        :return: Resolved internal key.
        :raises ValueError: No candidate resolves the label.
        """

    def all_metadata(self) -> dict[str, FieldMetadataRecord]:
        """
        Snapshot the main map while sharing live record values.

        Example:
            This includes dynamic categories; it is not a serialized copy of auxiliary maps.


        :return: New plain dictionary of all stored descriptors.
        """

    def custom_field_metadata(self, include_composites: bool = True) -> Mapping[str, FieldMetadataRecord]:
        """
        Expose custom records, optionally filtering calculated composites.

        Example:
            Changing the unfiltered mapping itself changes the dedicated custom registry.


        :param include_composites: True returns the live custom map; false builds a filtered main-map selection.
        :return: Live custom map when unfiltered, otherwise a new dictionary sharing records.
        """

    def add_custom_field(
        self,
        label: str,
        table: str | None,
        column: str | None,
        datatype: FieldMetadataDataType,
        colnum: int | None,
        name: str | None,
        display: FieldMetadataDisplay,
        is_editable: bool,
        is_multiple: FieldMetadataMultiplicity,
        is_category: bool,
        is_csp: bool = False,
        in_table: str = "books",
    ) -> None:
        """
        Register or refresh an in-memory custom descriptor and optional Series companion.

        A same-key record is refreshed only when is_custom is exactly True and its
        label, colnum and effective in_table agree. Otherwise it is a duplicate error.
        Datatype validation precedes record updates. Series fields add a non-custom
        float _index companion and aliases. Refreshing an existing companion changes
        only in_table; changing away from Series does not remove the old companion.
        New records and alias registration are not atomic: a duplicate alias may fail
        after map insertion. This method creates no database columns.

        Example:
            Refreshing the same label, colnum and in_table preserves the descriptor object.


        :param label: Unprefixed custom label; custom_field_prefix is prepended.
        :param table: Storage table name or None.
        :param column: Storage column name or None.
        :param datatype: Logical datatype, validated against VALID_DATA_TYPES.
        :param colnum: Custom-column number used in refresh identity checks.
        :param name: Display name stored unchanged.
        :param display: Display mapping retained by reference, not made immutable.
        :param is_editable: Editability flag stored without coercion.
        :param is_multiple: Multiplicity/separator mapping retained by reference.
        :param is_category: Whether the field forms a browse category.
        :param is_csp: Colon-separated-pair flag; not a composite-datatype flag.
        :param in_table: Main table label; also part of refresh identity.
        :return: None; updates in-memory metadata only.
        :raises ValueError: The key conflicts, datatype is unsupported, or a new alias is duplicated.
        """

    def remove_dynamic_categories(self) -> None:
        """
        Remove category records of kind user or search and their declared aliases.

        Only records with a truthy is_category flag qualify. Other maps are retained.

        Example:
            Standard fields, custom fields and the news category remain.


        :return: None; updates in-memory metadata only.
        """

    def remove_user_categories(self) -> None:
        """
        Remove user-category records and their declared search aliases.

        Example:
            Saved-search category records remain; missing aliases are tolerated.


        :return: None; updates in-memory metadata only.
        """

    def add_grouped_search_terms(self, gst: GroupedSearchTerms) -> None:
        """
        Replace list-target groups, then register supplied aliases.

        Duplicate-term ValueErrors are printed as tracebacks and processing continues.
        Targets are retained by reference. Other failures propagate without rollback.

        Example:
            Calling this with {} removes list-target groups but preserves old string-target aliases.


        :param gst: Group name to string or list target mapping; targets are not validated.
        :return: None; updates in-memory metadata only.
        """

    def cc_series_index_column_for(self, key: str) -> int:
        """
        Compute the tuple position immediately after a field's rec_index.

        Example:
            A field at rec_index=8 yields 9 even if it is not a Series.


        :param key: Internal metadata key; most methods do not resolve search aliases.
        :return: Stored rec_index plus one; no datatype or companion validation is performed.
        :raises KeyError: The field or its rec_index is absent.
        """

    def add_user_category(self, label: str, name: str | None) -> None:
        """
        Insert a user category with exact and ICU-lowercase search aliases.

        Duplicate keys fail before insertion. Alias conflicts can fail after the
        record and earlier aliases are inserted.

        Example:
            ``@Shelf`` registers both @Shelf and @shelf aliases.


        :param label: Exact category key, commonly beginning with @.
        :param name: Display name stored unchanged.
        :return: None; updates in-memory metadata only.
        :raises ValueError: The key or one of its aliases already exists.
        """

    def add_search_category(self, label: str, name: str | None) -> None:
        """
        Insert a saved-search category without search aliases.

        Example:
            A saved-search category appears in keys() but not all_field_keys().


        :param label: Exact category key.
        :param name: Display name stored unchanged.
        :return: None; updates in-memory metadata only.
        :raises ValueError: The key already exists.
        """

    def set_field_record_index(self, label: str, index: int, prefer_custom: bool = False) -> None:
        """
        Set a record position using exact keys and prefixed custom keys.

        Resolution here does not use label_to_key or its stored-label search.

        Example:
            For label title, custom preference chooses #title if present, otherwise title.


        :param label: Field key or unprefixed custom label.
        :param index: Position stored without type or range validation.
        :param prefer_custom: Try the prefixed key before the exact key.
        :return: None; updates in-memory metadata only.
        :raises KeyError: Neither selected key exists.
        """

    def get_search_terms(self) -> list[str]:
        """
        List sorted registered aliases followed by special search items.

        Example:
            Defaults append all and search after sorting aliases; the result is not globally sorted.


        :return: New list ending in the current search_items order.
        """

    def search_term_to_field_key(self, term: str) -> FieldMetadataSearchTarget:
        """
        Resolve an exact search alias, preserving unknown terms.

        Example:
            An unknown ``unregistered`` alias returns ``unregistered`` rather than raising KeyError.


        :param term: Case-sensitive alias; lowercasing is not performed here.
        :return: Registered string/list target, or the original term when unknown.
        """

    def searchable_fields(self) -> list[str]:
        """
        Select field records declaring at least one search term.

        This inspects record declarations, not whether aliases still exist in the map.

        Example:
            Grouped aliases do not themselves add fields; news and dynamic categories are excluded.


        :return: New list in main-map order.
        """


@runtime_checkable
class CalibreFieldMetadataAPI(FieldMetadataAPI, Protocol):
    """
    Extend the descriptor contract with Calibre result-index assignment.

    Example:
        A result adapter can assign indexes with set_field_record_index_from_field_map
        before reading descriptor rec_index values.
    """

    def set_field_record_index_from_field_map(self, field_map: FieldRecordIndexMap) -> None:
        """
        Assign tuple positions sequentially with standard-key preference.

        Example:
            If a later key is unknown, positions already assigned to earlier keys remain.


        :param field_map: Mapping from field keys/custom labels to record positions.
        :return: None; updates in-memory metadata only.
        :raises KeyError: A key cannot be resolved by set_field_record_index.
        """


class FieldMetadataDeserializerAPI(Protocol):
    """
    Describe reconstruction from borrowed custom and dynamic registry maps.

    Example:
        The returned registry shares supplied descriptor objects; copy state first
        if the caller requires mutation isolation.
    """

    def __call__(self, src: SerializedFieldMetadataState) -> FieldMetadataAPI:
        """
        Overlay borrowed serialized dynamic state on fresh LiuXin built-ins.

        The supplied search map replaces builtin aliases wholesale. Overlay order is
        custom fields, user categories, then search categories; later keys win. No
        validation or reconstruction of absent Series companions occurs. The bound
        get method continues to reference the augmented main map.

        Example:
            Mutating a supplied custom record is visible in the reconstructed container.


        :param src: Mapping containing custom_fields, user_categories, search_categories,
            search_term_map and custom_label_to_key_map.
        :return: New FieldMetadata sharing supplied maps and descriptor objects.
        :raises KeyError: A required serialized-state key is missing.
        """


__all__ = [
    "CalibreFieldMetadataAPI",
    "FieldMetadataAPI",
    "FieldMetadataDataType",
    "FieldMetadataDeserializerAPI",
    "FieldMetadataDisplay",
    "FieldMetadataEntry",
    "FieldMetadataGetterAPI",
    "FieldMetadataKind",
    "FieldMetadataMultiplicity",
    "FieldMetadataRecord",
    "FieldMetadataSearchTarget",
    "FieldRecordIndexMap",
    "GroupedSearchTerms",
    "KnownFieldMetadataDataType",
    "SerializedFieldMetadataState",
]
