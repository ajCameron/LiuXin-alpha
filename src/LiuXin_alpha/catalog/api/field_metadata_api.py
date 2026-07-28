"""
Structural API for catalog field metadata objects.
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
    Known keys used by calibre-compatible field metadata records.
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
    Serialized state accepted by the field metadata deserializer.
    """

    custom_fields: MutableMapping[str, FieldMetadataRecord]
    user_categories: MutableMapping[str, FieldMetadataRecord]
    search_categories: MutableMapping[str, FieldMetadataRecord]
    search_term_map: MutableMapping[str, FieldMetadataSearchTarget]
    custom_label_to_key_map: MutableMapping[str, str]


class FieldMetadataGetterAPI(Protocol):
    """
    Typed shape of ``FieldMetadata.get``.
    """

    @overload
    def __call__(self, key: str, /) -> FieldMetadataRecord | None:
        """
        Return a field metadata record, or ``None`` when the key is unknown.

        :param key:
        :return:
        """

    @overload
    def __call__(self, key: str, default: _DefaultT, /) -> FieldMetadataRecord | _DefaultT:
        """
        Return a field metadata record, or the supplied default.

        :param key:
        :param default:
        :return:
        """


@runtime_checkable
class FieldMetadataAPI(Protocol):
    """
    Public API implemented by catalog field metadata containers.

    The concrete implementation is dict-backed for compatibility, but it stores
    live records in internal ordered mappings. This protocol describes the
    supported mapping-like surface instead of the inherited ``dict`` methods
    whose behaviour does not reflect those internal records.
    """

    VALID_DATA_TYPES: frozenset[str | None]
    search_items: list[str]
    custom_field_prefix: str
    custom_label_to_key_map: MutableMapping[str, str]
    get: FieldMetadataGetterAPI

    def __getitem__(self, key: str) -> FieldMetadataRecord:
        """
        Return the metadata record for a field key.

        :param key:
        :return:
        """

    def __setitem__(self, key: str, val: FieldMetadataRecord) -> None:
        """
        Preserve dict-style assignment compatibility.

        Implementations may reject assignment.

        :param key:
        :param val:
        :return:
        """

    def __delitem__(self, key: str) -> None:
        """
        Remove a metadata record by key.

        :param key:
        :return:
        """

    def __iter__(self) -> Iterator[str]:
        """
        Iterate field keys.

        :return:
        """

    def __contains__(self, key: object) -> bool:
        """
        Return whether a field key is present.

        :param key:
        :return:
        """

    def has_key(self, key: str) -> bool:
        """
        Compatibility spelling for membership tests.

        :param key:
        :return:
        """

    def keys(self) -> KeysView[str]:
        """
        Return all metadata keys.

        :return:
        """

    def sortable_field_keys(self) -> list[str]:
        """
        Return field keys with sortable datatypes.

        :return:
        """

    def displayable_field_keys(self) -> list[str]:
        """
        Return field keys that should be exposed in display contexts.

        :return:
        """

    def standard_field_keys(self) -> list[str]:
        """
        Return non-custom field keys.

        :return:
        """

    def custom_field_keys(self, include_composites: bool = True) -> list[str]:
        """
        Return custom field keys.

        :param include_composites:
        :return:
        """

    def all_field_keys(self) -> list[str]:
        """
        Return every field key.

        :return:
        """

    def iterkeys(self) -> Iterator[str]:
        """
        Compatibility iterator over metadata keys.

        :return:
        """

    def itervalues(self) -> Iterable[FieldMetadataRecord]:
        """
        Compatibility iterator over metadata records.

        :return:
        """

    def values(self) -> ValuesView[FieldMetadataRecord]:
        """
        Return all metadata records.

        :return:
        """

    def iteritems(self) -> Iterator[tuple[str, FieldMetadataRecord]]:
        """
        Compatibility iterator over metadata items.

        :return:
        """

    def custom_iteritems(self) -> Iterator[tuple[str, FieldMetadataRecord]]:
        """
        Iterate custom metadata records.

        :return:
        """

    def items(self) -> list[tuple[str, FieldMetadataRecord]]:
        """
        Return metadata items as a list.

        :return:
        """

    def is_custom_field(self, key: str) -> bool:
        """
        Return whether a key belongs to the custom field namespace.

        :param key:
        :return:
        """

    def is_ignorable_field(self, key: str) -> bool:
        """
        Return whether a field can be ignored in generic field walks.

        :param key:
        :return:
        """

    def ignorable_field_keys(self) -> list[str]:
        """
        Return ignorable field keys.

        :return:
        """

    def is_series_index(self, key: str) -> bool:
        """
        Return whether a key is a series index companion field.

        :param key:
        :return:
        """

    def key_to_label(self, key: str) -> str:
        """
        Convert an internal key to its label.

        :param key:
        :return:
        """

    def label_to_key(self, label: str, prefer_custom: bool = False) -> str:
        """
        Convert a field label to its internal key.

        :param label:
        :param prefer_custom:
        :return:
        """

    def all_metadata(self) -> dict[str, FieldMetadataRecord]:
        """
        Return all metadata records as a plain dictionary.

        :return:
        """

    def custom_field_metadata(self, include_composites: bool = True) -> Mapping[str, FieldMetadataRecord]:
        """
        Return custom field metadata records.

        :param include_composites:
        :return:
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
        Add or refresh a custom field metadata record.

        :param label:
        :param table:
        :param column:
        :param datatype:
        :param colnum:
        :param name:
        :param display:
        :param is_editable:
        :param is_multiple:
        :param is_category:
        :param is_csp:
        :param in_table:
        :return:
        """

    def remove_dynamic_categories(self) -> None:
        """
        Remove user and saved-search categories.

        :return:
        """

    def remove_user_categories(self) -> None:
        """
        Remove user categories.

        :return:
        """

    def add_grouped_search_terms(self, gst: GroupedSearchTerms) -> None:
        """
        Add grouped search-term aliases.

        :param gst:
        :return:
        """

    def cc_series_index_column_for(self, key: str) -> int:
        """
        Return the companion series-index custom column number.

        :param key:
        :return:
        """

    def add_user_category(self, label: str, name: str | None) -> None:
        """
        Add a user-defined category.

        :param label:
        :param name:
        :return:
        """

    def add_search_category(self, label: str, name: str | None) -> None:
        """
        Add a saved-search category.

        :param label:
        :param name:
        :return:
        """

    def set_field_record_index(self, label: str, index: int, prefer_custom: bool = False) -> None:
        """
        Set the database record index for a field label.

        :param label:
        :param index:
        :param prefer_custom:
        :return:
        """

    def get_search_terms(self) -> list[str]:
        """
        Return all supported search terms.

        :return:
        """

    def search_term_to_field_key(self, term: str) -> FieldMetadataSearchTarget:
        """
        Resolve a search term to a field key or grouped field-key list.

        :param term:
        :return:
        """

    def searchable_fields(self) -> list[str]:
        """
        Return fields addressable through search terms.

        :return:
        """


@runtime_checkable
class CalibreFieldMetadataAPI(FieldMetadataAPI, Protocol):
    """Additional public API exposed by the calibre-compatible variant."""

    def set_field_record_index_from_field_map(self, field_map: FieldRecordIndexMap) -> None:
        """
        Set field record indexes from a label-to-index mapping.

        :param field_map:
        :return:
        """


class FieldMetadataDeserializerAPI(Protocol):
    """Callable API implemented by ``fm_from_dict``."""

    def __call__(self, src: SerializedFieldMetadataState) -> FieldMetadataAPI:
        """
        Deserialize field metadata state.

        :param src:
        :return:
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
