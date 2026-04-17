from __future__ import annotations

import sqlite3

from LiuXin_alpha.databases.database import Database


_FILES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `files` (
    `file_id` INTEGER PRIMARY KEY,
    `file_item_id` INTEGER NULL,
    `file_store_id` INTEGER NULL,
    `file_folder_id` INTEGER NULL,
    `file_storage_key` TEXT NULL,
    `file_name` TEXT NULL,
    `file_base_name` TEXT NULL,
    `file_extension` TEXT NULL,
    `file_tag` TEXT NULL,
    `file_auto_name` TEXT NULL,
    `file_use_auto_name` INTEGER NULL,
    `file_mime_type` TEXT NULL,
    `file_role` TEXT NULL,
    `file_media_category` TEXT NULL,
    `file_class_mask` TEXT NULL,
    `file_visibility_mask` TEXT NULL,
    `file_critical` INTEGER NULL,
    `file_size_bytes` INTEGER NULL,
    `file_hash_sha256` TEXT NULL,
    `file_hash_blake3` TEXT NULL,
    `file_phash` TEXT NULL,
    `file_corrupt` INTEGER NULL,
    `file_integrity_status` TEXT NULL,
    `file_last_seen_timestamp_ep_k` INTEGER NULL,
    `file_last_integrity_check_timestamp_ep_k` INTEGER NULL,
    `file_acquired_timestamp_ep_k` INTEGER NULL,
    `file_source` TEXT NULL,
    `file_original_name` TEXT NULL,
    `file_original_path` TEXT NULL,
    `file_anthology` TEXT NULL,
    `file_parent` TEXT NULL,
    `file_conversion_settings` TEXT NULL,
    `file_processed` INTEGER NULL,
    `file_created_timestamp_ep_k` INTEGER NULL,
    `file_modified_timestamp_ep_k` INTEGER NULL,
    `file_source_created_datestamp_ep_k` INTEGER NULL,
    `file_source_modified_datestamp_ep_k` INTEGER NULL,
    `file_scratch` TEXT NULL
);
"""


_IMAGES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `images` (
    `image_id` INTEGER PRIMARY KEY,
    `image_item_id` INTEGER NULL,
    `image_store_id` INTEGER NULL,
    `image_folder_id` INTEGER NULL,
    `image_storage_key` TEXT NULL,
    `image_name` TEXT NULL,
    `image_base_name` TEXT NULL,
    `image_extension` TEXT NULL,
    `image_tag` TEXT NULL,
    `image_auto_name` TEXT NULL,
    `image_use_auto_name` INTEGER NULL,
    `image_mime_type` TEXT NULL,
    `image_role` TEXT NULL,
    `image_media_category` TEXT NULL,
    `image_class_mask` TEXT NULL,
    `image_visibility_mask` TEXT NULL,
    `image_critical` INTEGER NULL,
    `image_size_bytes` INTEGER NULL,
    `image_hash_sha256` TEXT NULL,
    `image_hash_blake3` TEXT NULL,
    `image_phash` TEXT NULL,
    `image_corrupt` INTEGER NULL,
    `image_integrity_status` TEXT NULL,
    `image_last_seen_timestamp_ep_k` INTEGER NULL,
    `image_last_integrity_check_timestamp_ep_k` INTEGER NULL,
    `image_acquired_timestamp_ep_k` INTEGER NULL,
    `image_source` TEXT NULL,
    `image_original_name` TEXT NULL,
    `image_original_path` TEXT NULL,
    `image_anthology` TEXT NULL,
    `image_parent` TEXT NULL,
    `image_conversion_settings` TEXT NULL,
    `image_processed` INTEGER NULL,
    `image_created_timestamp_ep_k` INTEGER NULL,
    `image_modified_timestamp_ep_k` INTEGER NULL,
    `image_source_created_datestamp_ep_k` INTEGER NULL,
    `image_source_modified_datestamp_ep_k` INTEGER NULL,
    `image_scratch` TEXT NULL
);
"""


_FILE_STORE_LINKS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `file_store_links` (
    `file_store_link_id` INTEGER PRIMARY KEY,
    `file_store_link_file_id` INTEGER NOT NULL,
    `file_store_link_store_id` INTEGER NOT NULL,
    `file_store_link_priority` INTEGER NULL,
    `file_store_link_type` TEXT NOT NULL,
    `file_store_link_policy` TEXT NULL
);
"""


_FILE_FOLDER_LINKS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `file_folder_links` (
    `file_folder_link_id` INTEGER PRIMARY KEY,
    `file_folder_link_file_id` INTEGER NOT NULL,
    `file_folder_link_folder_id` INTEGER NOT NULL,
    `file_folder_link_datestamp` TEXT NULL,
    `file_folder_link_scratch` TEXT NULL
);
"""


def _refresh_surface_db_metadata(db: Database) -> None:
    db.refresh_db_metadata()
    db.driver_wrapper.all_tables = db.all_tables
    db.driver_wrapper.main_tables = db.main_tables
    db.driver_wrapper.interlink_tables = db.interlink_tables
    db.driver_wrapper.intralink_tables = db.intralink_tables
    db.driver_wrapper.helper_tables = db.helper_tables
    db.driver_wrapper.dirtiable_tables = db.dirtiable_tables


def ensure_surface_asset_tables_sqlite(
    conn: sqlite3.Connection,
    *,
    include_images: bool = False,
    include_file_store_links: bool = False,
    include_file_folder_links: bool = False,
) -> bool:
    existing = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
    }
    created = False

    if "files" not in existing:
        conn.execute(_FILES_TABLE_SQL)
        created = True

    if include_images and "images" not in existing:
        conn.execute(_IMAGES_TABLE_SQL)
        created = True

    if include_file_store_links and "file_store_links" not in existing:
        conn.execute(_FILE_STORE_LINKS_TABLE_SQL)
        created = True

    if include_file_folder_links and "file_folder_links" not in existing:
        conn.execute(_FILE_FOLDER_LINKS_TABLE_SQL)
        created = True

    return created


def ensure_surface_asset_tables(
    db: Database,
    *,
    include_images: bool = False,
    include_file_store_links: bool = False,
    include_file_folder_links: bool = False,
) -> None:
    created = ensure_surface_asset_tables_sqlite(
        db.conn,
        include_images=include_images,
        include_file_store_links=include_file_store_links,
        include_file_folder_links=include_file_folder_links,
    )

    if not created:
        return

    db.conn.commit()
    _refresh_surface_db_metadata(db)
