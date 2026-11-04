"""Batch reads of physical asset_replicas rows; policy stays with callers."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class AssetReplicasSQLMixin:
    """Own queries returning asset_replicas rows on the current connection.

    Example:
        Access these operations through ``db.metadata_sql``.
    """

    db: DatabaseAPI

    def count_asset_replicas_for_assets(
        self, asset_ids: Iterable[int]
    ) -> dict[int, tuple[Mapping[str, Any], ...]]:
        """Count replicas by raw presence, integrity and mode for each asset.

        Return stored labels with row_count, leaving health/default rules to
        the caller. This does not interpret scratch records or touch bytes.
        """
        return self.db.macros.get_grouped_counts_for_ids(
            "asset_replicas",
            "asset_replica_digital_asset_id",
            asset_ids,
            group_by=(
                "asset_replica_presence_status",
                "asset_replica_integrity_status",
                "asset_replica_mode",
            ),
        )

    def get_asset_replicas_for_assets(
        self,
        asset_ids: Iterable[int],
    ) -> dict[int, tuple[Mapping[str, Any], ...]]:
        """Batch rows by asset_replica_digital_asset_id, retaining empty owner groups.

        Order each group by physical row ID. Do not require owners to exist or
        interpret stored metadata. Reads share the caller's current transaction.
        """
        return self.db.macros.get_rows_for_ids(
            "asset_replicas",
            "asset_replica_digital_asset_id",
            asset_ids,
        )

    def get_asset_replicas_for_stores(
        self,
        store_ids: Iterable[int],
    ) -> dict[int, tuple[Mapping[str, Any], ...]]:
        """Batch rows by asset_replica_store_id, retaining empty owner groups.

        Order each group by physical row ID. Do not require owners to exist or
        interpret stored metadata. Reads share the caller's current transaction.
        """
        return self.db.macros.get_rows_for_ids(
            "asset_replicas",
            "asset_replica_store_id",
            store_ids,
        )
