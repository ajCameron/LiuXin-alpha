
"""
Method for handling the names, and name generation, for custom columns.
"""


from __future__ import annotations

from typing import Any, TYPE_CHECKING, Optional

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.custom_columns_api import CustomColumnsAPI


class CCNamesMixin:
    """
    Names methods for custom columns.
    """

    def custom_field_name(
            self: "CustomColumnsAPI",
            label: Optional[str] = None,
            num: Optional[int] = None) -> str:
        """
        Gets the name for a custom field.

        :param label:
        :param num:
        :return:
        """
        if label is not None:
            return self.field_metadata.custom_field_prefix + label
        return self.field_metadata.custom_field_prefix + self.custom_column_num_to_label_map[num]
