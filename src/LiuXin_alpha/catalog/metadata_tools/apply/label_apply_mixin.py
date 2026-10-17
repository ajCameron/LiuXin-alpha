"""Resolve and link Tag values through the Catalog relationship owner."""

from collections.abc import Iterable, Mapping

from LiuXin_alpha.catalog.row_mutations import atomic_write, link_rows
from LiuXin_alpha.databases.api import DatabaseAPI, RowAPI
from LiuXin_alpha.errors import InputIntegrityError


class LabelApplyMixin:
    """Provide atomic Tag linking to the Apply host."""

    db: DatabaseAPI

    @atomic_write
    def tag(self, tag: RowAPI | str | Iterable[RowAPI | str], resource: RowAPI) -> None:
        """Resolve text before iterables, then link each Tag in one transaction.

        Existing Tag Rows are accepted directly. Lists, sets and generators are
        processed together; a later invalid value or persistence failure rolls
        back earlier creations and links. The relationship owner handles reuse
        of an existing link. Empty iterables are a no-op for a valid resource.

        Example:
            catalog.apply.tag(["Science fiction", "Adventure"], work_row)

        :param tag: Tag Row, complete text value, or iterable of either.
        :param resource: Row to which all supplied Tags will be linked.
        :return: None after all values have been linked.
        :raises InputIntegrityError: An input or relationship is unsupported.
        """
        if not isinstance(resource, RowAPI):
            raise InputIntegrityError("Resource must be a row")
        if isinstance(tag, RowAPI):
            if tag.table != "tags":
                raise InputIntegrityError("Tag row must belong to the tags table")
            tag_row = tag
        elif isinstance(tag, str):
            tag_row = self.ensure.tag(tag_text=tag)
        elif isinstance(tag, Iterable) and not isinstance(
            tag, (bytes, bytearray, Mapping)
        ):
            for value in tag:
                self.tag(tag=value, resource=resource)
            return
        else:
            raise InputIntegrityError(
                "Tag must be text, a Tag row, or an iterable of Tags"
            )

        if not self.db.driver_wrapper.get_link_table_name("tags", resource.table):
            raise InputIntegrityError("Resource cannot be tagged: no link table exists")
        link_rows(self, primary_row=tag_row, secondary_row=resource)
