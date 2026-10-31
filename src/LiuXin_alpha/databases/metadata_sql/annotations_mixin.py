"""Read physical annotations rows through shared portable SQL machinery."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class AnnotationsSQLMixin:
    """Own table-specific reads while callers retain interpretation and validation.

    Example:
        Use these methods through ``db.metadata_sql`` on an open database.
    """

    db: DatabaseAPI

    def get_annotations_for_items(
        self,
        item_ids: Iterable[int],
        *,
        user_id: int | None = None,
        kind: str | None = None,
    ) -> dict[int, tuple[Mapping[str, Any], ...]]:
        """Batch annotations rows by their stored annotation_item_id references.

        Keep an ID-ordered tuple for every requested owner, including empty
        groups. No owner selection, validation or value normalization is applied.
        Optional filters use exact stored values; None leaves them unfiltered.
        Read the current connection without committing or creating a snapshot.
        """
        where = {}
        if user_id is not None:
            where["annotation_user_id"] = user_id
        if kind is not None:
            where["annotation_kind"] = kind
        return self.db.macros.get_rows_for_ids(
            "annotations",
            "annotation_item_id",
            item_ids,
            where=where,
        )
