"""Apply normalized label metadata to catalogue entities."""


from LiuXin_alpha.databases.api import RowAPI

from LiuXin_alpha.databases.api import DatabaseAPI

from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError

from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.libraries.liuxin_six import six_string_types as string_types

from typing import Union, Iterable


class LabelApplyMixin:
    """
    Apply a label to a resource.
    """

    db: DatabaseAPI

    def tag(self, tag: Union[RowAPI, Iterable[str]], resource: RowAPI) -> None:
        """
        Apply a tag to the given resource.

        If the tag is a row, apply it directly. If the tag is text, then ensure the tag, and then use that row.
        :param tag: A row, string or iterable.
        :param resource: Something which can have a tag applied to it.
        :return:
        """
        if isinstance(tag, RowAPI):
            tag_row = tag

        elif hasattr(tag, "__iter__"):
            for tag_str in tag:
                self.tag(tag=tag_str, resource=resource)
            return

        elif isinstance(tag, (list, set)):
            for tag_str in tag:
                self.tag(tag=tag_str, resource=resource)
            return

        elif isinstance(tag, string_types):
            tag_row = self.ensure.tag(tag_text=tag)

        else:
            err_str = "Tag must be a string or row"
            err_str = default_log.log_variables(err_str, "ERROR", ("tag", tag), ("tag_type", type(tag)))
            raise InputIntegrityError(err_str)

        if not isinstance(resource, RowAPI):
            err_str = "Resource must be a row"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("resource", resource),
                ("resource_type", type(resource)),
            )
            raise InputIntegrityError(err_str)

        interlink_table = self.db.driver_wrapper.get_link_table_name("tags", resource.table)
        if not interlink_table:
            err_str = "Resource cannot be tagged - no link table exists between them"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("resource", resource),
                ("tag_row", tag_row),
                ("tag", tag),
            )
            raise InputIntegrityError(err_str)

        try:
            self.db.interlink_rows(primary_row=tag_row, secondary_row=resource)
        # Thrown if the tag is already applied to this row
        # Todo: Need to broaden the exception types
        except DatabaseIntegrityError:
            pass
