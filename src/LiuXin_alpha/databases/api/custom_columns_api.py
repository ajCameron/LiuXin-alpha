"""
Describe the shared host required by Calibre-style custom-column mixins.

CustomColumnMetadata aliases a mutable string-keyed metadata mapping; CustomColumnDataAdapter accepts a value and metadata mapping. CustomColumnsAPI specifies shared database/cache state and callable contracts. These aliases and protocol declarations do not validate payloads or provide persistence implementations.
"""

from __future__ import annotations

from typing import Any, Callable, Iterable, Mapping, MutableMapping, Optional, Protocol, TYPE_CHECKING, TypeAlias

if TYPE_CHECKING:
    from LiuXin_alpha.catalog.api.field_metadata_api import FieldMetadataAPI
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI
    from LiuXin_alpha.databases.db_types import MainTableName


CustomColumnMetadata: TypeAlias = MutableMapping[str, Any]
CustomColumnDataAdapter: TypeAlias = Callable[[Any, Mapping[str, Any]], Any]


class CustomColumnsAPI(Protocol):
    """
    Specify the shared state and operations consumed by custom-column mixins.

    Hosts supply db, attachment table, row data, preferences, field metadata/index maps, label/number metadata maps and datatype adapters. This structural Protocol is not runtime_checkable; its method bodies are declarations without operational defaults. Existing concrete mixins use some different positional parameter names, notably cc_row_id(s) for public writes and book_ids for cached rename updates. Positional calls avoid those keyword mismatches; this protocol does not repair them.

    Example:
        A mixin can annotate ``self: CustomColumnsAPI`` and read ``self.custom_column_num_map[num]`` before calling ``self.get_custom(owner_id, num=num, index_is_id=True)``.
    """

    db: "DatabaseAPI"
    table: "MainTableName"
    data: Any
    prefs: Any
    FIELD_MAP: MutableMapping[Any, int]
    field_metadata: "FieldMetadataAPI"
    custom_column_label_map: MutableMapping[str, CustomColumnMetadata]
    custom_column_num_map: MutableMapping[int, CustomColumnMetadata]
    custom_column_num_to_label_map: MutableMapping[int, str]
    custom_data_adapters: Mapping[str, CustomColumnDataAdapter]

    @property
    def conn(self) -> Any:
        """
        Expose the connection supplied or resolved by the implementing host.

        The current driver-wrapper host can resolve a fresh driver connection and lazily check a supplied override. Callers should obtain it when needed rather than assume a permanent connection identity.

        Example:
            For a provisioned host, ``connection = host.conn`` retrieves the connection used by its custom-column helpers.


        :return: Host connection object; the protocol does not enforce its concrete type or liveness.
        """

    @conn.setter
    def conn(self, value: Any) -> None:
        """
        Accept an explicit connection override for the host.

        The protocol does not validate the value. Current implementations retain the override and check usability when it is later read.

        Example:
            ``host.conn = None`` lets a driver-wrapper host resolve its owning driver connection again.


        :param value: Connection override, or None to clear it in the current driver-wrapper host.
        :return: None; the implementing host performs the described operation.
        """

    @property
    def custom_tables(self) -> Iterable[str]:
        """
        Expose the custom-column storage and link-table names available through the host.

        Example:
            ``set(host.custom_tables)`` collects the backing tables discovered by the host.


        :return: Iterable of custom-table names; callers should not assume an ordering.
        """

    def get_custom_tables(self) -> set[str]:
        """
        Discover custom-column storage and link tables from the owning database.

        Example:
            After creating a custom column, ``host.get_custom_tables()`` can be used to inspect its backing table names.


        :return: Set of discovered table names.
        """

    def all_custom(self, label: Optional[str] = None, num: Optional[int] = None) -> set[Any]:
        """
        Return the distinct stored values for a selected custom column.

        The legacy getter flattens database value rows into a set; normalized and unnormalized storage use different distinct-query hints but both expose set results.

        Example:
            With a configured "state" column, ``host.all_custom(label="state")`` returns its stored choices.


        :param label: Custom-column label; when supplied, legacy mixins prefer it over num.
        :param num: Custom-column definition ID, used when label is absent.
        :return: Set of values read from the selected storage table.
        :raises NotImplementedError: The legacy getter receives neither label nor num.
        :raises KeyError: The selected metadata entry is unknown.
        """

    @staticmethod
    def cleanup_tags(tags_list: list[str]) -> list[str]:
        """
        Normalize tag strings using the host custom-column cleanup policy.

        The current host delegates to the shared database utility; the protocol itself does not perform normalization.

        Example:
            Pass proposed values through ``host.cleanup_tags([" first ", "second"])`` before using them in a bulk tag update.


        :param tags_list: Tag strings to clean before a multiple-text write.
        :return: Cleaned list of tag strings.
        """

    def create_custom_column(
        self,
        name: str,
        datatype: str = "text",
        is_multiple: bool = False,
        label: Optional[str] = None,
        editable: bool = True,
        display: Optional[Mapping[str, Any]] = None,
        in_table: str = "books",
        table: Optional[str] = None,
        make_category: Optional[bool] = None,
    ) -> int:
        """
        Create a custom-column definition and the backing storage required by its datatype.

        Creation and validation belong to the concrete host/driver. Choose an insertable attachment table for the active schema; a historical books name may be a compatibility view. Normalized datatypes have value/link tables, while unnormalized datatypes can store values directly in one backing table.

        Example:
            With a supported attachment table, ``host.create_custom_column("Reading state", datatype="text", in_table="manifestations")`` returns a definition ID.


        :param name: Display name for the new column.
        :param datatype: Legacy custom datatype, defaulting to text.
        :param is_multiple: Whether the selected datatype supports multiple values.
        :param label: Optional explicit custom-column label.
        :param editable: Whether user edits are allowed.
        :param display: Optional formatting/display metadata passed to the storage owner.
        :param in_table: Attachment table; books is the compatibility default.
        :param table: Optional alias for in_table, subject to host conflict checking.
        :param make_category: Optional override for category creation.
        :return: Numeric ID of the new custom-column definition.
        """

    def custom_dirty_books_referencing(self, field: str, book_id: Any, commit: bool = True) -> Iterable[Any]:
        """
        Find and mark owners that reference one custom-column value.

        The concrete CustomColumns helper names the second parameter id. Its lookup rows are returned after a flattened owner-ID list is passed to dirtied.

        Example:
            ``references = host.custom_dirty_books_referencing("#state", value_id, commit=False)`` marks owners of that value and retains their lookup rows.


        :param field: Field-metadata key, commonly a prefixed custom label.
        :param book_id: ID of the referenced custom value, despite the historical book_id parameter name.
        :param commit: Whether the host dirtying operation should commit.
        :return: Iterable of owner references; the current helper returns raw one-column database rows.
        """

    def custom_field_metadata(
        self,
        label: Optional[str] = None,
        num: Optional[int] = None,
    ) -> "CustomColumnMetadata":
        """
        Return the shared metadata mapping for a label or definition ID.

        Example:
            ``host.custom_field_metadata(num=column_id)["datatype"]`` reads the selected column datatype.


        :param label: Custom-column label; when supplied, legacy mixins prefer it over num.
        :param num: Custom-column definition ID, used when label is absent.
        :return: Mutable metadata record owned by the host, not a detached copy.
        :raises KeyError: The requested label/number has no metadata entry.
        """

    @staticmethod
    def custom_table_names(num: int, in_table: str = "books") -> tuple[str, str]:
        """
        Derive legacy value-table and link-table names for one custom-column ID.

        This naming operation does not prove that either table exists; unnormalized custom columns may have no link table.

        Example:
            For the current naming host, ``host.custom_table_names(4, in_table="manifestations")`` gives ("custom_column_4", "manifestations_custom_column_4_link").


        :param num: Custom-column definition ID.
        :param in_table: Attachment table name included in the link-table name.
        :return: Pair (custom_column_N, attachment_custom_column_N_link).
        """

    def delete_custom_column(self, label: Optional[str] = None, num: Optional[int] = None) -> None:
        """
        Mark a selected definition for removal during a later custom-column load.

        This contract separates marking from the loader operation that drops backing tables. Existing storage/metadata can remain until that later cleanup runs.

        Example:
            ``host.delete_custom_column(num=column_id)`` marks the definition; a subsequent loader pass performs its backing-table removal.


        :param label: Custom-column label; when supplied, legacy mixins prefer it over num.
        :param num: Custom-column definition ID, used when label is absent.
        :return: None; the implementing host performs the described operation.
        """

    def delete_custom_item_using_id(
        self,
        idx: int,
        label: Optional[str] = None,
        num: Optional[int] = None,
    ) -> None:
        """
        Delete one normalized value and update references through the host helpers.

        The legacy implementation ignores false IDs, dirties referencing owners before deletion, then requests a cache update. Its cache-update call uses target_ids, but the current concrete rename helper names that parameter book_ids; that keyword mismatch is not resolved by this protocol.

        Example:
            For a compatible host, ``host.delete_custom_item_using_id(value_id, num=column_id)`` removes that normalized choice and updates its owners.


        :param idx: Stored custom-value ID, rather than an owning book ID.
        :param label: Custom-column label; when supplied, legacy mixins prefer it over num.
        :param num: Custom-column definition ID, used when label is absent.
        :return: None; the implementing host performs the described operation.
        """

    def delete_item_from_multiple(
        self,
        item: str,
        label: Optional[str] = None,
        num: Optional[int] = None,
    ) -> list[int]:
        """
        Remove one case-insensitively matched choice from a multiple-text column.

        The current implementation rejects other datatypes, deletes both links and the matched value row, and commits when a matching value ID exists.

        Example:
            ``host.delete_item_from_multiple("finished", label="states")`` returns owners that referenced the removed multiple-text choice.


        :param item: Text choice to locate and delete.
        :param label: Custom-column label; when supplied, legacy mixins prefer it over num.
        :param num: Custom-column definition ID, used when label is absent.
        :return: List of owner IDs affected by a matching value; empty when none is found.
        :raises ValueError: The selected legacy column is not text with is_multiple enabled.
        """

    def direct_get_custom_extra(self, link_table: str, index: int) -> Any:
        """
        Retrieve the extra link value for one persistent owner ID.

        Example:
            ``host.direct_get_custom_extra(link_table, owner_id)`` retrieves the stored position for a custom-series link.


        :param link_table: Custom-column link-table name.
        :param index: Persistent owner ID used by the underlying extra-value query.
        :return: Backend extra value, or its missing-value result.
        """

    def direct_get_custom_id_val_pairs(self, table: str) -> tuple[int, Any]:
        """
        Retrieve stored value IDs and values from a custom-column table.

        The driver-wrapper host forwards get_all_cc_id_val_pairs without reshaping its result. Consumers such as get_custom_items_with_ids use the result as a collection of pairs.

        Example:
            ``pairs = host.direct_get_custom_id_val_pairs("custom_column_4")`` retrieves choices and their stored IDs.


        :param table: Physical custom value-table name.
        :return: Backend ID/value row collection; the retained legacy annotation does not express the outer collection accurately.
        """

    def dirtied(self, ids: Iterable[int], commit: bool = True) -> None:
        """
        Mark persistent owner records as needing metadata refresh through the host.

        The concrete host can supply a real dirty-record callback or a compatibility fallback; this protocol provides no callback implementation.

        Example:
            ``host.dirtied({7, 8}, commit=False)`` records dirty owners within an enclosing write operation.


        :param ids: Persistent owner IDs to dirty.
        :param commit: Whether the host dirtying operation should commit.
        :return: None; the implementing host performs the described operation.
        """

    def get_custom(
        self,
        idx: int,
        label: Optional[str] = None,
        num: Optional[int] = None,
        index_is_id: bool = False,
    ) -> Any:
        """
        Read a selected custom value from the host row-data cache.

        The current getter resolves the column field index from metadata. Multiple text is split using cache_to_list. Its optional sort_alpha branch still uses the obsolete list.sort(cmp=...) form and can fail on current Python; the protocol does not supply a replacement implementation.

        Example:
            ``host.get_custom(7, num=column_id, index_is_id=True)`` reads owner 7 regardless of the current row ordering.


        :param idx: Current data-row position, or persistent owner ID when index_is_id is True.
        :param label: Custom-column label; when supplied, legacy mixins prefer it over num.
        :param num: Custom-column definition ID, used when label is absent.
        :param index_is_id: Whether idx already identifies a persistent owner rather than a current row position.
        :return: Cached scalar value or a list for a multiple-text column.
        """

    def get_custom_extra(
        self,
        idx: int,
        label: Optional[str] = None,
        num: Optional[int] = None,
        index_is_id: bool = False,
    ) -> Any:
        """
        Read the custom-series extra value for a row position or owner ID.

        Example:
            ``host.get_custom_extra(7, num=series_column_id, index_is_id=True)`` retrieves the stored series position.


        :param idx: Current data-row position, or persistent owner ID when index_is_id is True.
        :param label: Custom-column label; when supplied, legacy mixins prefer it over num.
        :param num: Custom-column definition ID, used when label is absent.
        :param index_is_id: Whether idx already identifies a persistent owner rather than a current row position.
        :return: Series extra value, or None when the selected datatype has no series extra.
        """

    def get_custom_and_extra(
        self,
        idx: int,
        label: Optional[str] = None,
        num: Optional[int] = None,
        index_is_id: bool = False,
    ) -> tuple[Any, Any]:
        """
        Read a cached custom value together with its optional series extra.

        The value uses the host cache while a series extra comes from the link query. The legacy multiple-text sort_alpha branch has the same cmp-keyword limitation as get_custom.

        Example:
            ``value, position = host.get_custom_and_extra(7, num=column_id, index_is_id=True)`` reads both projections for owner 7.


        :param idx: Current data-row position, or persistent owner ID when index_is_id is True.
        :param label: Custom-column label; when supplied, legacy mixins prefer it over num.
        :param num: Custom-column definition ID, used when label is absent.
        :param index_is_id: Whether idx already identifies a persistent owner rather than a current row position.
        :return: Pair (value, extra); non-series columns return None for extra.
        """

    def get_custom_items_with_ids(
        self,
        label: Optional[str] = None,
        num: Optional[int] = None,
    ) -> Any:
        """
        Expose ID/value choices for a normalized custom column.

        Example:
            ``host.get_custom_items_with_ids(label="state")`` supplies normalized choices for an editor that needs stable value IDs.


        :param label: Custom-column label; when supplied, legacy mixins prefer it over num.
        :param num: Custom-column definition ID, used when label is absent.
        :return: Backend ID/value pair collection, or an empty list for unnormalized storage.
        """

    def get_next_cc_series_num_for(
        self,
        series: str,
        label: Optional[str] = None,
        num: Optional[int] = None,
    ) -> Optional[float]:
        """
        Choose the next index for a named series in a selected custom-series column.

        Existing series indexes are delegated to _get_next_series_num_for_list. The current getter uses a numeric configured increment or 1.0 for a series without a stored value row.

        Example:
            ``host.get_next_cc_series_num_for("Cycle", num=series_column_id)`` asks the configured policy for the next position.


        :param series: Series value to look up.
        :param label: Custom-column label; when supplied, legacy mixins prefer it over num.
        :param num: Custom-column definition ID, used when label is absent.
        :return: Next index according to host preference policy, or None for a non-series column.
        """

    def id(self, idx: int) -> int:
        """
        Translate a current row-data position into its persistent owner ID.

        Example:
            ``owner_id = host.id(0)`` identifies the first currently displayed row without assuming its database ID is zero.


        :param idx: Position in the host current row ordering.
        :return: Persistent owner ID supplied by the host row-data interface.
        """

    def notify(self, event: str, ids: Iterable[int]) -> None:
        """
        Deliver a metadata-change event through the host notification hook.

        The host supplies notification behavior; standalone compatibility hosts may install a fallback callback.

        Example:
            ``host.notify("metadata", [7])`` signals the configured hook after owner 7 metadata changes.


        :param event: Event name, commonly metadata.
        :param ids: Persistent owner IDs associated with the event.
        :return: None; the implementing host performs the described operation.
        """

    def rename_custom_item_in_data(
        self,
        target_ids: Iterable[Any],
        column_num: Any,
        new_value: Any,
    ) -> None:
        """
        Replace cached custom values for the supplied owner-reference rows.

        The concrete helper parameter is named book_ids, so positional calls are compatible with both vocabularies. This changes the row-data cache only and does not persist a database rename.

        Example:
            ``host.rename_custom_item_in_data([(7,), (8,)], column_id, None)`` clears the cached projection for two owners.


        :param target_ids: Owner references; the current helper expects indexable rows with the ID at position zero.
        :param column_num: Custom-column key used in FIELD_MAP.
        :param new_value: Value assigned in the row-data cache, including None for clearing.
        :return: None; the implementing host performs the described operation.
        """

    def set_custom_bulk_multiple(
        self,
        ids: Iterable[int],
        add: Optional[Iterable[str]] = None,
        remove: Optional[Iterable[str]] = None,
        label: Optional[str] = None,
        num: Optional[int] = None,
        notify: bool = False,
    ) -> None:
        """
        Add and remove choices across owners in an editable multiple-text column.

        The current mixin names the first parameter cc_row_ids. It validates editable multiple text, performs a bulk database operation, commits and then refreshes row-data values. This protocol does not guarantee transaction rollback or compensate for later cache/notification failures.

        Example:
            ``host.set_custom_bulk_multiple([7, 8], add=["finished"], remove=["reading"], label="states")`` changes the selected choices for both owners.


        :param ids: Reusable collection of persistent owner IDs.
        :param add: Choices to add; None means none.
        :param remove: Choices to remove; additions take precedence for overlapping cleaned values.
        :param label: Custom-column label; when supplied, legacy mixins prefer it over num.
        :param num: Custom-column definition ID, used when label is absent.
        :param notify: Whether to call the host metadata notification hook.
        :return: None; the implementing host performs the described operation.
        """

    def set_custom_bulk(
        self,
        ids: Iterable[int],
        val: Any,
        label: Optional[str] = None,
        num: Optional[int] = None,
        append: bool = False,
        notify: bool = True,
        extras: Optional[Mapping[int, Any]] = None,
    ) -> None:
        """
        Apply one custom value to multiple owners, with optional per-position extras.

        The concrete mixin names its first parameter cc_row_ids, checks equal lengths for extras and owners, calls _set_custom per owner, then dirties the owners and commits. Its implementation accepts positional-indexed extras even though its own annotation is a list and this protocol retains Mapping.

        Example:
            ``host.set_custom_bulk([7, 8], "Cycle", num=series_column_id, extras={0: 1.0, 1: 2.0})`` assigns per-position series indexes.


        :param ids: Sized, reusable owner-ID collection for the current mixin implementation.
        :param val: Value applied to each owner.
        :param label: Custom-column label; when supplied, legacy mixins prefer it over num.
        :param num: Custom-column definition ID, used when label is absent.
        :param append: Whether supported multiple values should be appended.
        :param notify: Whether individual delegated writes request notification.
        :param extras: Optional extra values indexed by zero-based input position; integer mapping keys are positions, not owner IDs.
        :return: None; the implementing host performs the described operation.
        :raises ValueError: The current mixin receives extras and owner collections with different lengths.
        """

    def set_custom(
        self,
        id: int,
        val: Any,
        label: Optional[str] = None,
        num: Optional[int] = None,
        append: bool = False,
        notify: bool = True,
        extra: Any = None,
        commit: bool = True,
        allow_case_change: bool = False,
    ) -> set[int]:
        """
        Write one owner custom value, dirty affected records and optionally commit.

        The current outer method calls _set_custom, dirties the requested ID together with the returned refresh IDs, and commits when requested. The returned set need not include the owner being written.

        Example:
            ``additional = host.set_custom(7, "finished", label="state", commit=False)`` updates owner 7 while leaving the caller to commit.


        :param id: Persistent owner ID; the concrete mixin names this parameter cc_row_id.
        :param val: Value to adapt and write.
        :param label: Custom-column label; when supplied, legacy mixins prefer it over num.
        :param num: Custom-column definition ID, used when label is absent.
        :param append: Whether to append for supported multiple columns.
        :param notify: Whether the inner write should request a metadata notification.
        :param extra: Optional series extra value.
        :param commit: Whether to commit after dirtying.
        :param allow_case_change: Whether matching normalized values may change display case.
        :return: Set of additional owners needing refresh from the inner write; the requested owner is dirtied separately.
        """

    def set_custom_column_metadata(
        self,
        num: int,
        name: Optional[str] = None,
        label: Optional[str] = None,
        is_editable: Optional[bool] = None,
        display: Optional[str] = None,
        in_table: Optional[str] = None,
        notify: bool = True,
        update_last_modified: bool = False,
    ) -> Any:
        """
        Update selected attributes of a custom-column definition.

        The current CRUD mixin updates its cached editable flag when supplied and can notify with an empty owner list. Other metadata cache synchronization is host-specific; this declaration does not enforce it.

        Example:
            ``host.set_custom_column_metadata(column_id, name="Reading status", is_editable=True)`` requests those definition changes.


        :param num: Custom-column definition ID to change.
        :param name: Replacement display name, or None to retain it.
        :param label: Replacement label, or None to retain it.
        :param is_editable: Replacement editable flag, or None to retain it.
        :param display: Replacement serialized/display metadata accepted by the host.
        :param in_table: Replacement attachment-table metadata, or None to retain it.
        :param notify: Whether to request a metadata notification.
        :param update_last_modified: Compatibility flag; the current CRUD mixin does not use it.
        :return: Host-specific change result, forwarded from the storage metadata update.
        """

    def _get_next_series_num_for_list(self, series_indices: Iterable[int | float]) -> Optional[float]:
        """
        Apply the host next-series-index policy to existing positions.

        The concrete helper uses unwrap=True and indexes each supplied item at zero. Flat numeric elements therefore fail for nonempty input. Empty input chooses a numeric configured default or 1.0; the current helper does not return None.

        Example:
            ``host._get_next_series_num_for_list([])`` chooses the configured default position for a new series.


        :param series_indices: Existing positions; the current host forwards nonempty input to a helper expecting one-column rows, despite the numeric-iterable annotation.
        :return: Next numeric position under the current helper; the protocol leaves None available to other hosts.
        """

    def _get_series_values(self, val: Any) -> tuple[str, Optional[float]]:
        """
        Parse a legacy series value into a name and optional position.

        The concrete host sends the last element of a nonempty list to its shared parser and handles false inputs as empty text. Other unsupported shapes raise NotImplementedError.

        Example:
            ``host._get_series_values("Cycle [2.0]")`` extracts the series name and position for a custom-series write.


        :param val: Series text, a supported legacy list form, or a false value.
        :return: Pair of series name and parsed optional float index.
        """

    def _set_custom(
        self,
        id_: int,
        val: Any,
        label: Optional[str] = None,
        num: Optional[int] = None,
        append: bool = False,
        notify: bool = True,
        extra: Any = None,
        allow_case_change: bool = False,
    ) -> set[int]:
        """
        Adapt and persist one custom value and return additional owners requiring refresh.

        The current helper handles normalized, series and direct-value storage, updates row-data projections and may notify. It does not perform the outer set_custom dirtying/commit steps. Unsupported adapters, invalid enum values and persistence/cache errors can propagate after partial work.

        Example:
            Within a host write implementation, ``host._set_custom(7, "finished", label="state", notify=False)`` performs the inner update before the caller dirties and commits.


        :param id_: Persistent owner ID to update.
        :param val: Value passed to the selected custom datatype adapter.
        :param label: Custom-column label; when supplied, legacy mixins prefer it over num.
        :param num: Custom-column definition ID, used when label is absent.
        :param append: Whether supported multiple values retain existing choices.
        :param notify: Whether to request notification after the write.
        :param extra: Optional series index; a parsed or default index can be used when absent.
        :param allow_case_change: Whether case-equivalent normalized choices may have their display value changed.
        :return: Set of additional owner IDs affected by shared value case changes; composite writes return an empty set.
        """
