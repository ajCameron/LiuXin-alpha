
"""
Build prefixed custom-field names from labels or a host-provided number map.
"""


from __future__ import annotations

from typing import Any, TYPE_CHECKING, Optional

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.custom_columns_api import CustomColumnsAPI


class CCNamesMixin:
    """
    Expose custom-field naming through the host’s FieldMetadata prefix.

    The label path needs only field_metadata. Numeric lookup additionally requires custom_column_num_to_label_map, which standalone CustomColumns does not initialize.

    Example:
        A host with custom_field_prefix="#" can call custom_field_name(label="shelf") to obtain "#shelf".
    """

    def custom_field_name(
            self: "CustomColumnsAPI",
            label: Optional[str] = None,
            num: Optional[int] = None) -> str:
        """
        Prefix a supplied label or a label obtained from the numeric lookup map.

        An explicitly empty label is accepted and yields the prefix alone. When label is None, even a None num is looked up; standalone CustomColumns lacks the required number-to-label map.

        Example:
            >>> from types import SimpleNamespace
            >>> host = SimpleNamespace(field_metadata=SimpleNamespace(custom_field_prefix="#"))
            >>> CCNamesMixin.custom_field_name(host, label="shelf")
            '#shelf'


        :param label: Optional custom-column label; takes precedence over num.
        :param num: Numeric metadata key used when label is None.
        :return: Prefix concatenated with the resolved label, without validation or coercion.
        :raises AttributeError: The host lacks field metadata or the numeric label map.
        :raises KeyError: The numeric label map has no selected entry.
        """
        if label is not None:
            return self.field_metadata.custom_field_prefix + label
        return self.field_metadata.custom_field_prefix + self.custom_column_num_to_label_map[num]
