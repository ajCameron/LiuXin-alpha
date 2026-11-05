"""Wrap custom-column storage mutations with facade bookkeeping."""

from __future__ import annotations

from LiuXin_alpha.databases.database_driver_plugins._macro_state import require_legacy_macros

from collections.abc import Mapping
from typing import Any, TYPE_CHECKING, cast

from LiuXin_alpha.databases.driver_wrapper.driver_wrapper_custom_columns_mixin import (
    CustomColumnStorageMixin,
)
from LiuXin_alpha.databases.db_types.driver_types import SQLConnection

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.custom_columns_api import CustomColumnsAPI


def _facade(host: object) -> CustomColumnsAPI:
    """Resolve the shared service contract when composed into CustomColumns."""
    return cast("CustomColumnsAPI", host)


class CCCRUDColumnsMixin(CustomColumnStorageMixin[SQLConnection]):
    """Run before the driver adapter in the CustomColumns method resolution order."""

    def create_custom_column(
        self,
        name: str,
        datatype: str = "text",
        is_multiple: bool = False,
        label: str | None = None,
        editable: bool = True,
        display: Mapping[str, Any] | None = None,
        in_table: str | None = None,
        table: str | None = None,
        make_category: bool | None = None,
    ) -> int:
        """Create storage, refresh definition maps, and request a date refresh.

        The facade's attachment table is the default. An explicit table alias
        must agree with an explicitly supplied in_table. Host preferences are
        optional; when present their failures propagate rather than being hidden.
        """
        if table is not None:
            if in_table not in (None, "books", table):
                raise TypeError(
                    "Pass only one of table= or in_table= (or keep them identical)."
                )
            in_table = table
        num = super().create_custom_column(
            name=name,
            datatype=datatype,
            is_multiple=is_multiple,
            label=label,
            editable=editable,
            display=display,
            in_table=_facade(self).table if in_table is None else in_table,
            make_category=make_category,
        )
        _facade(self).refresh_db_custom_columns_metadata()
        prefs = _facade(self).prefs
        if prefs is not None:
            prefs.set("update_all_last_mod_dates_on_start", True)
        return num

    def set_custom_column_metadata(
        self,
        num: int,
        name: str | None = None,
        label: str | None = None,
        is_editable: bool | None = None,
        display: Mapping[str, Any] | None = None,
        in_table: str | None = None,
        notify: bool = True,
        update_last_modified: bool = False,
    ) -> bool:
        """Update storage, refresh both definition maps, then notify if requested.

        Resolve the current definition before mutating. Reloading uses the stored
        editable field and also keeps renamed labels and display values current.
        The optional date-refresh request uses the host's preference store.
        """
        _facade(self).custom_field_metadata(num=num)
        changed = super().set_custom_column_metadata(
            num=num,
            name=name,
            label=label,
            is_editable=is_editable,
            display=display,
            in_table=in_table,
        )
        _facade(self).refresh_db_custom_columns_metadata()
        prefs = _facade(self).prefs
        if update_last_modified and prefs is not None:
            prefs.set("update_all_last_mod_dates_on_start", True)
        if notify:
            _facade(self).notify("metadata", [])
        return changed

    def delete_custom_column(
        self, num: int | None = None, *, label: str | None = None
    ) -> None:
        """Mark a resolved definition for deferred deletion by ID or label.

        Keep positional numeric IDs accepted by the driver-facing facade. Actual
        table deletion remains a loader operation, so current maps stay available
        until that reload.
        """
        data = _facade(self).custom_field_metadata(label=label, num=num)
        require_legacy_macros(_facade(self).db.macros).mark_custom_column_for_delete(num=data["num"])
