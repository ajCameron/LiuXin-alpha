"""
Retain the legacy Tag-link mixin and its iterable dispatch behavior.
"""


from LiuXin_alpha.databases.api import RowAPI

from LiuXin_alpha.databases.api import DatabaseAPI

from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError

from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.libraries.liuxin_six import six_string_types as string_types

from typing import Union, Iterable


class LabelApplyMixin:
    """
    Provide Tag linking with runtime RowAPI checks to the Apply host.

    Despite the class name, the implementation targets tags. Its current
    iterable-before-string ordering is unsafe for nonempty text inputs.

    Example:
        Use a resolved Tag Row to take the direct link path.
    """

    db: DatabaseAPI

    def tag(self, tag: Union[RowAPI, Iterable[str]], resource: RowAPI) -> None:
        """
        Link Tag Rows or recursively process iterables using the legacy branch order.

        An empty iterable, including empty text, returns without validating the
        resource. The iterable branch makes later list/set and string resolution
        branches unreachable for normal values of those types. Row inputs check
        a tags-to-resource link table, then link Tag as primary. No transaction
        wraps processing of several elements.

        Example:
            Pass an existing Tag Row for the direct linking path. A nonempty string
            recurses into its own characters and can raise RecursionError.


        :param tag: RowAPI object or iterable; string iteration currently precedes the text branch.
        :param resource: RowAPI resource, validated only once a Tag Row has been resolved.
        :return: None; duplicate/integrity errors during linking are suppressed.
        :raises InputIntegrityError: A noniterable value, resource or link route is unsupported.
        :raises RecursionError: A nonempty string is recursively iterated instead of resolved.
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
