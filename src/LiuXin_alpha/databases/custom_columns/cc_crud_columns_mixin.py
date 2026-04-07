
"""
CRUD custom columns themselves.
"""

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from LiuXin_alpha.databases.db_types import MainTableName



class CCCRUDColumnsMixin:
    """
    Custom columns themselves CRUD operations.
    """
    def create_custom_column(
        self,
        name,
        datatype="text",
        is_multiple=False,
        label=None,
        editable=True,
        display=None,
        in_table="books",
        table=None,
        make_category=None,
    ):
        """
        Add a custom column to the books table.
        :param label:
        :param name:
        :param datatype: Must be one of the following - rating, int, text, comments, series, composite, enumeration,
                         float, datetime, bool
        :param is_multiple:
        :param editable: Is the column editable?
        :param display:
        :param in_table: Which table should the custom column be created in? (Defaults to books for historical reasons)
        :return:
        """
        # Support newer/clearer keyword alias: `table=` (same as `in_table=`)
        if table is not None:
            if in_table != "books" and in_table != table:
                raise TypeError("Pass only one of table= or in_table= (or keep them identical).")
            in_table = table

        num = super(self).create_custom_column(
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
        self,
        num: int,
        name: Optional[str] = None,
        label: Optional[str] = None,
        is_editable: Optional[bool] = None,
        display: Optional[str] = None,
        in_table: MainTableName=None,
        notify=True,
        update_last_modified=False,
    ):
        """
        Change the metadata for a custom column - identified with the num

        Update the metadata for a custom column - changes the entry in the custom_columns table.
        For all parameters (apart from num) if None, no change will be made.
        :param num: The number of the custom column (the custom column can usually be identified from the num or the
                    name - but you might want change the column's name
        :param name: The name of the custom column
        :param label: The label - which will be displayed when the custom_columns are presented in the viewer
        :param is_editable:
        :param display:
        :param in_table:
        :param notify:
        :param update_last_modified:
        :return:
        """
        # Actually update the database with the changes made
        changed = super(self).set_custom_column_metadata(
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

    def delete_custom_column(self, label=None, num=None):
        """
        Mark a custom column for later deletion.
        :param label:
        :param num:
        :return:
        """
        data = self.custom_field_metadata(label, num)

        self.db.macros.mark_custom_column_for_delete(num=data["num"])


