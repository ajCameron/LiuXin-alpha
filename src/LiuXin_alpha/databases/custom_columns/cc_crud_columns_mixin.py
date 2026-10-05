
"""
Provide cooperative custom-column schema wrappers and a deletion marker helper.

The CRUD wrappers add preference/notification updates around super calls. In the current CustomColumns inheritance order, CustomColumnsDriverWrapperMixin precedes this mixin and shadows all three public names. Directly invoking the cooperative wrappers on that concrete class does not find a backend after this mixin.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from LiuXin_alpha.databases.api.custom_columns_api import CustomColumnsAPI

if TYPE_CHECKING:
    from LiuXin_alpha.databases.db_types import MainTableName



class CCCRUDColumnsMixin:
    """
    Wrap backend column creation and metadata edits with facade bookkeeping.

    For cooperative methods, place this mixin before a backend implementing the same names. Current CustomColumns places it last, so the driver-wrapper methods resolve first. delete_custom_column is independent of super but is also shadowed there.

    Example:
        A host declared as class Enhanced(CCCRUDColumnsMixin, Backend): can run these wrappers when Backend provides compatible creation/update methods and the host supplies preferences, metadata maps and notify.
    """
    def create_custom_column(
        self: "CustomColumnsAPI",
        name: str,
        # Todo: We can tightly type this?
        datatype: str = "text",
        is_multiple: bool = False,
        label: Optional[str] = None,
        editable: bool = True,
        display: Optional[str] = None,
        # Todo: Typeable?
        in_table: str = "books",
        table: Optional[str] = None,
        make_category: Optional[bool] = None,
    ) -> int:
        """
        Resolve the table alias, delegate column creation and request date refresh.

        After success, call prefs.set("update_all_last_mod_dates_on_start", True). Any AttributeError during that preference access/call is ignored; other errors can propagate after creation. This wrapper does not itself validate datatype or create SQL tables. In current CustomColumns, the earlier driver-wrapper method shadows it.

        Example:
            On a cooperatively ordered host, create_custom_column("Shelf", label="shelf", table="works") delegates creation and then attempts to mark dates for refresh.


        :param name: Display name forwarded to the backend.
        :param datatype: Custom-column datatype forwarded unchanged.
        :param is_multiple: Multiple-value flag forwarded unchanged.
        :param label: Optional backend label; default behavior belongs to the backend.
        :param editable: Backend editability flag.
        :param display: Display configuration passed through despite the optional-string annotation.
        :param in_table: Attachment table, default books.
        :param table: Alias overriding the default in_table; conflicting explicit choices raise.
        :param make_category: Optional category flag forwarded to the backend.
        :return: The delegated creation result, normally the new custom-column ID.
        :raises TypeError: table conflicts with an explicitly different non-default in_table.
        :raises AttributeError: No compatible create_custom_column exists after this mixin in the host MRO.
        """
        # Support newer/clearer keyword alias: `table=` (same as `in_table=`)
        if table is not None:
            if in_table != "books" and in_table != table:
                raise TypeError("Pass only one of table= or in_table= (or keep them identical).")
            in_table = table

        num = super().create_custom_column(
            label=label,
            name=name,
            datatype=datatype,
            is_multiple=is_multiple,
            editable=editable,
            display=display,
            in_table=in_table,
            make_category=make_category,
        )

        try:
            self.prefs.set("update_all_last_mod_dates_on_start", True)
        except AttributeError:
            pass

        return num


    def set_custom_column_metadata(
        self: "CustomColumnsAPI",
        num: int,
        name: Optional[str] = None,
        label: Optional[str] = None,
        is_editable: Optional[bool] = None,
        display: Optional[str] = None,
        in_table: "MainTableName" = None,
        notify: bool = True,
        update_last_modified: bool = False,
    ) -> set[int]:
        """
        Delegate column metadata changes, then update editability and optionally notify.

        After the backend returns, update custom_column_num_map[num]["is_editable"] only when a value was supplied. No other local metadata map is refreshed here. Notification or map errors can occur after a successful backend write. Current CustomColumns resolves the earlier driver-wrapper implementation instead.

        Example:
            On a cooperatively ordered host, set_custom_column_metadata(num, is_editable=False, notify=False) updates the backend and the local is_editable flag.


        :param num: Custom-column ID passed to the backend.
        :param name: Optional new name.
        :param label: Optional new label.
        :param is_editable: Optional editability value; also bool-converted into the local metadata map.
        :param display: Optional display configuration forwarded unchanged.
        :param in_table: Attachment table passed through, including the default None.
        :param notify: Whether to call notify("metadata", []) after delegation, default True.
        :param update_last_modified: Compatibility flag currently ignored.
        :return: Delegated backend result, without coercion to the annotated set type.
        :raises AttributeError: The host has no compatible super implementation or notification hook.
        :raises KeyError: The local numeric metadata map lacks num during editability refresh.
        """
        # Actually update the database with the changes made
        changed = super().set_custom_column_metadata(
            num=num,
            name=name,
            label=label,
            is_editable=is_editable,
            display=display,
            in_table=in_table,
        )

        if is_editable is not None:
            self.custom_column_num_map[num]["is_editable"] = bool(is_editable)

        if notify:
            self.notify("metadata", [])

        return changed

    def delete_custom_column(
            self: "CustomColumnsAPI",
            label: Optional[str] = None,
            num: Optional[int] = None) -> None:
        """
        Resolve facade metadata and mark the selected column for later deletion.

        Do not drop tables, refresh maps or notify here. Current CustomColumns shadows this label-aware helper with the driver-wrapper method of the same name.

        Example:
            On a host using this helper, delete_custom_column(label="shelf") marks the resolved column; a later cleanup/load performs deletion.


        :param label: Optional label selecting custom_column_label_map; takes precedence over num.
        :param num: Numeric custom-column ID used when label is None.
        :return: None; forward the resolved metadata num to db.macros.mark_custom_column_for_delete.
        :raises KeyError: custom_field_metadata cannot resolve the chosen label/number.
        """
        data = self.custom_field_metadata(label, num)

        self.db.macros.mark_custom_column_for_delete(num=data["num"])
