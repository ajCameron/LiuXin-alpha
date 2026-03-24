from __future__ import annotations

from pathlib import Path

from tests.support.test_databases._semantic_fixture_builders import (
    JPEG_FAKE_BYTES,
    PNG_1X1_BYTES,
    build_base_profiled_db,
    bundle_token_path,
    finalize_fixture,
    open_fixture_db,
    ordered_ids,
)


DB_NAME = "images_covers_db_1"


def populate_bundle(bundle_dir: Path) -> None:
    bundle_dir = Path(bundle_dir)
    bundle_dir.mkdir(parents=True, exist_ok=True)
    db_path = build_base_profiled_db(bundle_dir=bundle_dir, db_name=DB_NAME, books=4)

    store_root = bundle_dir / "cover_variants_store"
    fronts_root = store_root / "fronts"
    extras_root = store_root / "extras"
    fronts_root.mkdir(parents=True, exist_ok=True)
    extras_root.mkdir(parents=True, exist_ok=True)

    files = {
        "w1-cover": fronts_root / "w1-cover.png",
        "w1-ill": extras_root / "w1-illustration.jpg",
        "w1-map": extras_root / "w1-map.png",
        "w2-cover": fronts_root / "w2-cover.jpg",
        "w2-diagram": extras_root / "w2-diagram.png",
        "w3-cover": fronts_root / "w3-cover.png",
    }
    files["w1-cover"].write_bytes(PNG_1X1_BYTES)
    files["w1-ill"].write_bytes(JPEG_FAKE_BYTES)
    files["w1-map"].write_bytes(PNG_1X1_BYTES)
    files["w2-cover"].write_bytes(JPEG_FAKE_BYTES)
    files["w2-diagram"].write_bytes(PNG_1X1_BYTES)
    files["w3-cover"].write_bytes(PNG_1X1_BYTES)

    conn = open_fixture_db(db_path)
    try:
        work_ids = ordered_ids(conn, "works", "work_id")
        item_ids = ordered_ids(conn, "items", "item_id")
        if len(work_ids) != 4 or len(item_ids) != 4:
            raise AssertionError(f"Unexpected seeded WEMI counts for {DB_NAME}")

        for work_id, title in zip(
            work_ids,
            ("Cover Variant Book One", "Cover Variant Book Two", "Cover Variant Book Three", "Cover Variant Book Four"),
            strict=True,
        ):
            conn.execute(
                "UPDATE works SET work_title = ?, work_canonical_title = ?, work_sort_title = ?, work_creator_sort = ? WHERE work_id = ?;",
                (title, title, title, title, work_id),
            )

        store_id = int(
            conn.execute(
                "INSERT INTO stores (store_name, store_kind, store_access_protocol, store_root_uri, store_online_status, store_supports_folders, store_supports_random_read, store_is_read_only, store_location_note) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
                (
                    "cover_variants_store",
                    "on_disk_existing_unmanaged_drive",
                    "file",
                    bundle_token_path("cover_variants_store"),
                    "online",
                    1,
                    1,
                    1,
                    f"fixture:{DB_NAME}",
                ),
            ).lastrowid
        )
        fronts_folder_id = int(conn.execute("INSERT INTO folders (folder_store_id, folder_name, folder_relpath, folder_policy_json) VALUES (?, ?, ?, ?);", (store_id, "fronts", "fronts", "{}")).lastrowid)
        extras_folder_id = int(conn.execute("INSERT INTO folders (folder_store_id, folder_name, folder_relpath, folder_policy_json) VALUES (?, ?, ?, ?);", (store_id, "extras", "extras", "{}")).lastrowid)

        conn.executemany(
            "INSERT INTO folder_work_links (folder_work_link_folder_id, folder_work_link_work_id, folder_work_link_priority) VALUES (?, ?, ?);",
            (
                (fronts_folder_id, work_ids[0], 1),
                (extras_folder_id, work_ids[0], 1),
                (fronts_folder_id, work_ids[1], 2),
                (extras_folder_id, work_ids[1], 2),
                (fronts_folder_id, work_ids[2], 3),
            ),
        )

        image_rows = (
            (item_ids[0], work_ids[0], fronts_folder_id, files["w1-cover"], "image/png", "cover", "cover", 1),
            (item_ids[0], work_ids[0], extras_folder_id, files["w1-ill"], "image/jpeg", "illustration", "illustration", 2),
            (item_ids[0], work_ids[0], extras_folder_id, files["w1-map"], "image/png", "map", "map", 3),
            (item_ids[1], work_ids[1], fronts_folder_id, files["w2-cover"], "image/jpeg", "cover", "cover", 1),
            (item_ids[1], work_ids[1], extras_folder_id, files["w2-diagram"], "image/png", "diagram", "diagram", 2),
            (item_ids[2], work_ids[2], fronts_folder_id, files["w3-cover"], "image/png", "cover", "cover", 1),
        )
        for item_id, work_id, folder_id, path, mime, role, link_type, priority in image_rows:
            image_id = int(
                conn.execute(
                    "INSERT INTO images (image_item_id, image_store_id, image_folder_id, image_storage_key, image_name, image_base_name, image_extension, image_mime_type, image_role, image_media_category, image_size_bytes, image_source, image_original_name, image_original_path) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
                    (
                        item_id,
                        store_id,
                        folder_id,
                        f"{Path(path).parent.name}/{path.name}",
                        path.name,
                        path.stem,
                        path.suffix.lstrip("."),
                        mime,
                        role,
                        "cover",
                        path.stat().st_size,
                        "fixture-cover-store",
                        path.name,
                        bundle_token_path("cover_variants_store", path.parent.name, path.name),
                    ),
                ).lastrowid
            )
            conn.execute(
                "INSERT INTO image_work_links (image_work_link_image_id, image_work_link_work_id, image_work_link_priority, image_work_link_type) VALUES (?, ?, ?, ?);",
                (image_id, work_id, priority, link_type),
            )

        finalize_fixture(conn, db_name=DB_NAME)
    finally:
        conn.close()

