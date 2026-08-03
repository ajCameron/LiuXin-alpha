"""Structural API for Calibre-compatible field metadata containers.

Field metadata describes how logical fields map to storage, display, sorting,
categories, and search terms. It is schema metadata, not the value of a field
on one book or WEMI entity.

Concrete containers behave like mappings, but their authoritative records live
in ordered internal maps; this protocol documents the supported mapping-shaped
surface rather than every inherited ``dict`` method.

Example::

    title = field_metadata["title"]
    assert title["datatype"] == "text"

    target = field_metadata.search_term_to_field_key("authors")
    custom = field_metadata.custom_field_metadata(include_composites=False)
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
    """Known keys in one Calibre-compatible field description.

    ``table``/``column`` describe storage, ``datatype`` and ``is_multiple``
    describe values, ``search_terms`` provides query aliases, and ``display``
    holds immutable presentation hints. Optionality reflects the several record
    kinds: standard fields, custom fields, categories, and saved searches.
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
    """Round-trippable dynamic state accepted by ``fm_from_dict``.

    Standard built-in field definitions come from the concrete container and
    are not repeated here; this state carries custom fields and dynamic
    category/search maps.
    """

    custom_fields: MutableMapping[str, FieldMetadataRecord]
    user_categories: MutableMapping[str, FieldMetadataRecord]
    search_categories: MutableMapping[str, FieldMetadataRecord]
    search_term_map: MutableMapping[str, FieldMetadataSearchTarget]
    custom_label_to_key_map: MutableMapping[str, str]


class FieldMetadataGetterAPI(Protocol):
    """Overloaded callable shape of ``FieldMetadata.get``.

    The overloads preserve the caller's explicit default type while returning a
    mutable metadata record for known keys.
    """

    @overload
    def __call__(self, key: str, /) -> FieldMetadataRecord | None:
        """
        Return a field metadata record, or ``None`` when the key is unknown.

        :param key: Internal field key, custom key, or category key.
        :return: Metadata record, or ``None`` when unknown.
        """

    @overload
    def __call__(self, key: str, default: _DefaultT, /) -> FieldMetadataRecord | _DefaultT:
        """
        Return a field metadata record, or the supplied default.

        :param key: Internal field key, custom key, or category key.
        :param default: Value returned when ``key`` is unknown.
        :return: Metadata record or the supplied default.
        """


@runtime_checkable
class FieldMetadataAPI(Protocol):
    """
    Public API implemented by catalog field metadata containers.

    The concrete implementation is dict-backed for compatibility, but it stores
    live records in internal ordered mappings. This protocol describes the
    supported mapping-like surface instead of the inherited ``dict`` methods
    whose behaviour does not reflect those internal records.

    Keys such as ``"title"`` identify standard fields. Custom keys normally use
    the ``"#label"`` form. Call :meth:`label_to_key` when input may be a
    user-facing label rather than an internal key.

    Example::

        if "title" in field_metadata:
            title_description = field_metadata["title"]
        custom_keys = field_metadata.custom_field_keys()
    """

    VALID_DATA_TYPES: frozenset[str | None]
    search_items: list[str]
    custom_field_prefix: str
    custom_label_to_key_map: MutableMapping[str, str]
    get: FieldMetadataGetterAPI

    def __getitem__(self, key: str) -> FieldMetadataRecord:
        """
        Return the metadata record for a field key.

        :param key: Existing internal field/category key.
        :return: Live metadata record for ``key``.
        :raises KeyError: If ``key`` is unknown.
        """

    def __setitem__(self, key: str, val: FieldMetadataRecord) -> None:
        """
        Preserve dict-style assignment compatibility.

        Implementations may reject assignment.

        :param key: Internal field key.
        :param val: Complete metadata record.
        :return: ``None``.
        :raises TypeError: When the concrete container is read-only.
        """

    def __delitem__(self, key: str) -> None:
        """
        Remove a metadata record by key.

        :param key: Existing dynamic or custom metadata key.
        :return: ``None``.
        """

    def __iter__(self) -> Iterator[str]:
        """
        Iterate field keys.

        :return: Iterator of internal metadata keys in stable order.
        """

    def __contains__(self, key: object) -> bool:
        """
        Return whether a field key is present.

        :param key: Candidate internal field/category key.
        :return: Whether a metadata record exists for ``key``.
        """

    def has_key(self, key: str) -> bool:
        """
        Compatibility spelling for membership tests.

        :param key: Candidate internal field/category key.
        :return: Same result as ``key in field_metadata``.
        """

    def keys(self) -> KeysView[str]:
        """
        Return all metadata keys.

        :return: Dynamic view over all internal metadata keys.
        """

    def sortable_field_keys(self) -> list[str]:
        """
        Return field keys with sortable datatypes.

        :return: Keys whose datatype and display policy permit sorting.
        """

    def displayable_field_keys(self) -> list[str]:
        """
        Return field keys that should be exposed in display contexts.

        :return: Keys eligible for generic display field selection.
        """

    def standard_field_keys(self) -> list[str]:
        """
        Return non-custom field keys.

        :return: Built-in, non-custom field keys.
        """

    def custom_field_keys(self, include_composites: bool = True) -> list[str]:
        """
        Return custom field keys.

        :param include_composites: Include calculated composite custom fields.
        :return: Custom keys, normally in ``"#label"`` form.
        """

    def all_field_keys(self) -> list[str]:
        """
        Return every field key.

        :return: Standard, custom, and dynamic category/search keys.
        """

    def iterkeys(self) -> Iterator[str]:
        """
        Compatibility iterator over metadata keys.

        :return: Compatibility iterator equivalent to ``iter(keys())``.
        """

    def itervalues(self) -> Iterable[FieldMetadataRecord]:
        """
        Compatibility iterator over metadata records.

        :return: Compatibility iterable over live metadata records.
        """

    def values(self) -> ValuesView[FieldMetadataRecord]:
        """
        Return all metadata records.

        :return: Dynamic view over all metadata records.
        """

    def iteritems(self) -> Iterator[tuple[str, FieldMetadataRecord]]:
        """
        Compatibility iterator over metadata items.

        :return: Compatibility iterator of ``(key, record)`` pairs.
        """

    def custom_iteritems(self) -> Iterator[tuple[str, FieldMetadataRecord]]:
        """
        Iterate custom metadata records.

        :return: Iterator of custom-field ``(key, record)`` pairs.
        """

    def items(self) -> list[tuple[str, FieldMetadataRecord]]:
        """
        Return metadata items as a list.

        :return: Snapshot list of all ``(key, record)`` pairs.
        """

    def is_custom_field(self, key: str) -> bool:
        """
        Return whether a key belongs to the custom field namespace.

        :param key: Internal metadata key.
        :return: Whether ``key`` belongs to the custom-field namespace.
        """

    def is_ignorable_field(self, key: str) -> bool:
        """
        Return whether a field can be ignored in generic field walks.

        :param key: Internal metadata key.
        :return: Whether generic metadata walks should skip this field.
        """

    def ignorable_field_keys(self) -> list[str]:
        """
        Return ignorable field keys.

        :return: All keys currently classified as ignorable.
        """

    def is_series_index(self, key: str) -> bool:
        """
        Return whether a key is a series index companion field.

        :param key: Internal metadata key.
        :return: Whether this is the numeric index companion of a series field.
        """

    def key_to_label(self, key: str) -> str:
        """
        Convert an internal key to its label.

        :param key: Internal standard/custom field key.
        :return: User-facing label associated with ``key``.
        :raises KeyError: If ``key`` is unknown.
        """

    def label_to_key(self, label: str, prefer_custom: bool = False) -> str:
        """
        Convert a field label to its internal key.

        :param label: User-facing label or already-internal key.
        :param prefer_custom: Prefer a custom field when standard/custom labels
            collide.
        :return: Resolved internal key.
        """

    def all_metadata(self) -> dict[str, FieldMetadataRecord]:
        """
        Return all metadata records as a plain dictionary.

        :return: Snapshot mapping of every key to its metadata record.
        """

    def custom_field_metadata(self, include_composites: bool = True) -> Mapping[str, FieldMetadataRecord]:
        """
        Return custom field metadata records.

        :param include_composites: Include calculated composite custom fields.
        :return: Mapping containing only custom-field records.
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

        Existing ``label`` records are refreshed rather than duplicated.

        :param label: Stable custom label without the ``#`` prefix.
        :param table: Auxiliary storage table, if any.
        :param column: Value column in ``table``, if any.
        :param datatype: Logical datatype such as ``"text"`` or ``"series"``.
        :param colnum: Database/custom-column ordinal.
        :param name: User-facing field name.
        :param display: Immutable display configuration.
        :param is_editable: Whether interfaces may edit values.
        :param is_multiple: Multiplicity/separator description.
        :param is_category: Whether values form a browse category.
        :param is_csp: Whether the field uses colon-separated pairs.
        :param in_table: Main record table containing direct values.
        :return: ``None``.
        """

    def remove_dynamic_categories(self) -> None:
        """
        Remove user and saved-search categories.

        :return: ``None``. Standard/custom records remain.
        """

    def remove_user_categories(self) -> None:
        """
        Remove user categories.

        :return: ``None``. Saved-search categories remain.
        """

    def add_grouped_search_terms(self, gst: GroupedSearchTerms) -> None:
        """
        Add grouped search-term aliases.

        :param gst: Group name to field-key or field-key-list mapping.
        :return: ``None``.
        """

    def cc_series_index_column_for(self, key: str) -> int:
        """
        Return the companion series-index custom column number.

        :param key: Custom series field key.
        :return: Database column number of its index companion.
        """

    def add_user_category(self, label: str, name: str | None) -> None:
        """
        Add a user-defined category.

        :param label: Stable category label/key.
        :param name: Optional user-facing category name.
        :return: ``None``.
        """

    def add_search_category(self, label: str, name: str | None) -> None:
        """
        Add a saved-search category.

        :param label: Stable saved-search label/key.
        :param name: Optional user-facing category name.
        :return: ``None``.
        """

    def set_field_record_index(self, label: str, index: int, prefer_custom: bool = False) -> None:
        """
        Set the database record index for a field label.

        :param label: Field label or internal key.
        :param index: Zero-based position in database result records.
        :param prefer_custom: Prefer a custom field on label collision.
        :return: ``None``.
        """

    def get_search_terms(self) -> list[str]:
        """
        Return all supported search terms.

        :return: Sorted or stable list of supported search aliases.
        """

    def search_term_to_field_key(self, term: str) -> FieldMetadataSearchTarget:
        """
        Resolve a search term to a field key or grouped field-key list.

        :param term: Search alias such as ``"authors"``.
        :return: One internal field key or a grouped list of keys.
        :raises KeyError: If the term is unknown.
        """

    def searchable_fields(self) -> list[str]:
        """
        Return fields addressable through search terms.

        :return: Internal field keys reachable from configured search terms.
        """


@runtime_checkable
class CalibreFieldMetadataAPI(FieldMetadataAPI, Protocol):
    """Field metadata plus Calibre result-record index assignment.

    Use this specialization where row tuples from a Calibre-compatible query
    need to be mapped back to logical field descriptions.
    """

    def set_field_record_index_from_field_map(self, field_map: FieldRecordIndexMap) -> None:
        """
        Set field record indexes from a label-to-index mapping.

        :param field_map: Field label/key to database result index mapping.
        :return: ``None``.
        """


class FieldMetadataDeserializerAPI(Protocol):
    """Callable contract implemented by ``fm_from_dict``.

    It reconstructs custom fields and dynamic categories over the concrete
    implementation's built-in standard field definitions.
    """

    def __call__(self, src: SerializedFieldMetadataState) -> FieldMetadataAPI:
        """
        Deserialize field metadata state.

        :param src: Serialized custom/dynamic field state.
        :return: Reconstructed field metadata container.
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
