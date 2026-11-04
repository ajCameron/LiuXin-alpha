"""Reject legacy cover flags until cover selection has an explicit asset owner.

Cover presence is derived from WEMI attachments and stored assets. A boolean
write cannot create or remove those resources safely.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from LiuXin_alpha.caches.write.base_writer import BaseWriter

if TYPE_CHECKING:
    from LiuXin_alpha.caches.api.storage_cache_api import FieldBasicInterfaceAPI
    from LiuXin_alpha.catalog.api import CatalogAPI


class CoversWrite(BaseWriter):
    """Keep the cache entry point as an explicit unsupported-operation boundary."""

    def __init__(self, field: FieldBasicInterfaceAPI) -> None:
        """Bind the refusal hook after initializing the shared field adapter."""
        super().__init__(field=field)
        self.set_books_func = self.set_cover_exists

    @staticmethod
    def set_cover_exists(
        book_id_val_map: dict[int, bool],
        db: CatalogAPI,
        field: FieldBasicInterfaceAPI,
        *args,
    ):
        """Refuse nonempty cover writes without touching the database or cache.

        Empty mappings remain no-ops. Actual cover updates need explicit WEMI
        attachments, asset selection and replica retention policy.
        """
        if book_id_val_map:
            raise NotImplementedError(
                "Legacy cover flags are retired. Resolve WEMI cover attachments "
                "and asset/replica selection before updating cover presence."
            )
        return set()
