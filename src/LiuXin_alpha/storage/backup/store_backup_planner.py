"""Helpers for turning indexed store contents into pack-shaped backup workflow specs."""

from __future__ import annotations

import dataclasses
import pathlib
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from LiuXin_alpha.storage.api.backup_api import BackupSourceKind, BackupSourceSpec, BackupWorkflowKind, BackupWorkflowSpec

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI


@dataclasses.dataclass(slots=True, frozen=True)
class PlannedBackupPack:
    pack_index: int
    workflow_spec: BackupWorkflowSpec
    source_count: int
    estimated_size_bytes: int


class StoreBackupPlanner:
    """Plan squashfs backup packs from an indexed local-ish store."""

    def __init__(self, db: "DatabaseAPI") -> None:
        self.db = db

    def plan_squashfs_packs_for_store(
        self,
        *,
        source_store_id: int,
        output_dir: str,
        target_pack_size_bytes: int,
        workflow_name_prefix: str | None = None,
        max_files_per_pack: int | None = None,
        allowed_extensions: Iterable[str] | None = None,
    ) -> tuple[PlannedBackupPack, ...]:
        if target_pack_size_bytes <= 0:
            raise ValueError("target_pack_size_bytes must be > 0")
        store_row = self.db.get_row_from_id("stores", int(source_store_id))
        if store_row is None:
            raise KeyError(f"Unknown source store id: {source_store_id!r}")
        root_uri = str(store_row["store_root_uri"] or "")
        if root_uri == "":
            raise ValueError("Source store has no store_root_uri; cannot plan local-path backups.")
        root_path = pathlib.Path(root_uri).expanduser()
        rows = self._inventory_rows_for_store(int(source_store_id))
        ext_filter = None
        if allowed_extensions is not None:
            ext_filter = {self._normalize_ext(item) for item in allowed_extensions if str(item).strip() != ""}
        normalized: list[dict[str, Any]] = []
        for row in rows:
            entry = self._normalize_inventory_row(row)
            if entry is None:
                continue
            if ext_filter is not None and self._normalize_ext(entry.get("file_extension")) not in ext_filter:
                continue
            full_path = root_path / str(entry["file_storage_key"])
            source_kind = BackupSourceKind.LOCAL_PATH
            normalized.append({
                **entry,
                "source_kind": source_kind,
                "source_identifier": str(full_path),
            })
        if not normalized:
            return ()
        prefix = workflow_name_prefix or (str(store_row["store_name"] or f"store_{source_store_id}").strip() or f"store_{source_store_id}")
        output_root = pathlib.Path(output_dir).expanduser()
        packs: list[PlannedBackupPack] = []
        current: list[dict[str, Any]] = []
        current_size = 0
        pack_index = 1

        def flush() -> None:
            nonlocal current, current_size, pack_index
            if not current:
                return
            spec = BackupWorkflowSpec(
                workflow_name=f"{prefix}-pack-{pack_index:04d}",
                workflow_kind=BackupWorkflowKind.SQUASHFS_PACK,
                output_url=str(output_root / f"{prefix}-pack-{pack_index:04d}.sqsh"),
                sources=tuple(
                    BackupSourceSpec(
                        source_kind=item["source_kind"],
                        source_identifier=item["source_identifier"],
                        archive_path=str(item["file_storage_key"]),
                        expected_size=item.get("file_size_bytes"),
                        expected_hash=item.get("file_hash_sha256"),
                        source_file_id=item.get("source_file_id"),
                        source_asset_replica_id=item.get("source_asset_replica_id"),
                        source_store_id=int(source_store_id),
                    )
                    for item in current
                ),
            )
            packs.append(PlannedBackupPack(
                pack_index=pack_index,
                workflow_spec=spec,
                source_count=len(current),
                estimated_size_bytes=current_size,
            ))
            current = []
            current_size = 0
            pack_index += 1

        for item in normalized:
            item_size = int(item.get("file_size_bytes") or 0)
            would_overflow = bool(current and current_size + item_size > target_pack_size_bytes)
            would_hit_count = bool(max_files_per_pack is not None and current and len(current) >= int(max_files_per_pack))
            if would_overflow or would_hit_count:
                flush()
            current.append(item)
            current_size += item_size
        flush()
        return tuple(packs)

    def _inventory_rows_for_store(self, source_store_id: int):
        try:
            rows = self.db.search("file_inventory_v", "file_store_id", int(source_store_id))
        except Exception:
            rows = []
        if rows:
            return rows
        tables = set(self.db.get_tables())
        if "files" in tables:
            try:
                return self.db.search("files", "file_store_id", int(source_store_id))
            except Exception:
                pass
        if "asset_replicas" in tables:
            rows = []
            for replica in self.db.search("asset_replicas", "asset_replica_store_id", int(source_store_id)):
                digital_asset = None
                da_id = replica["asset_replica_digital_asset_id"] if "asset_replica_digital_asset_id" in replica.allowed_columns else None
                if da_id not in (None, "") and "digital_assets" in tables:
                    try:
                        digital_asset = self.db.get_row_from_id("digital_assets", int(da_id))
                    except Exception:
                        digital_asset = None
                rows.append({
                    "file_storage_key": replica["asset_replica_storage_key"],
                    "file_size_bytes": replica["asset_replica_observed_size_bytes"] if "asset_replica_observed_size_bytes" in replica.allowed_columns else (digital_asset["digital_asset_size_bytes"] if digital_asset is not None and "digital_asset_size_bytes" in digital_asset.allowed_columns else 0),
                    "file_hash_sha256": replica["asset_replica_observed_hash_sha256"] if "asset_replica_observed_hash_sha256" in replica.allowed_columns else (digital_asset["digital_asset_hash_sha256"] if digital_asset is not None and "digital_asset_hash_sha256" in digital_asset.allowed_columns else None),
                    "file_extension": replica["asset_replica_extension"] if "asset_replica_extension" in replica.allowed_columns else (digital_asset["digital_asset_extension"] if digital_asset is not None and "digital_asset_extension" in digital_asset.allowed_columns else None),
                    "file_id": replica["asset_replica_id"] if "asset_replica_id" in replica.allowed_columns else None,
                    "asset_replica_id": replica["asset_replica_id"] if "asset_replica_id" in replica.allowed_columns else None,
                })
            return rows
        return []

    @staticmethod
    def _normalize_inventory_row(row) -> dict[str, Any] | None:
        if isinstance(row, dict):
            storage_key = row.get("file_storage_key")
            if storage_key in (None, ""):
                return None
            return {
                "file_storage_key": str(storage_key),
                "file_size_bytes": int(row.get("file_size_bytes") or 0),
                "file_hash_sha256": (str(row.get("file_hash_sha256")).lower() if row.get("file_hash_sha256") not in (None, "") else None),
                "file_extension": row.get("file_extension"),
                "source_file_id": int(row.get("file_id")) if row.get("file_id") not in (None, "") else None,
                "source_asset_replica_id": int(row.get("asset_replica_id")) if row.get("asset_replica_id") not in (None, "") else None,
            }
        storage_key = row["file_storage_key"]
        if storage_key in (None, ""):
            return None
        ext = row["file_extension"] if "file_extension" in row.allowed_columns else None
        return {
            "file_storage_key": str(storage_key),
            "file_size_bytes": int(row["file_size_bytes"] or 0),
            "file_hash_sha256": (str(row["file_hash_sha256"]).lower() if "file_hash_sha256" in row.allowed_columns and row["file_hash_sha256"] not in (None, "") else None),
            "file_extension": ext,
            "source_file_id": int(row["file_id"]) if "file_id" in row.allowed_columns and row["file_id"] not in (None, "") else None,
            "source_asset_replica_id": int(row["asset_replica_id"]) if "asset_replica_id" in row.allowed_columns and row["asset_replica_id"] not in (None, "") else None,
        }

    @staticmethod
    def _normalize_ext(value: Any) -> str | None:
        if value in (None, ""):
            return None
        return str(value).lower().lstrip(".")


__all__ = ["PlannedBackupPack", "StoreBackupPlanner"]
