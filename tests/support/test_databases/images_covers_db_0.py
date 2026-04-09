from __future__ import annotations

from pathlib import Path

from tests.support.test_databases._semantic_fixture_builders import (
    build_base_profiled_db,
    bundle_token_path,
    finalize_fixture,
    open_fixture_db,
    ordered_ids,
)


DB_NAME = "images_covers_db_0"


_PNG_BYTES = (
    b"\x89PNG\r\n\x1a\n"
    b"\x00\x00\x00\rIHDR"
    b"\x00\x00\x00\x01\x00\x00\x00\x01\x08\x02\x00\x00\x00"
    b"\x90wS\xde"
    b"\x00\x00\x00\x0cIDATx\x9cc``\xf8\xcf\xc0\x00\x00\x03\x01\x01\x00"
    b"\xc9\xfe\x92\xef"
    b"\x00\x00\x00\x00IEND\xaeB`\x82"
)
_JPEG_BYTES = b"\xff\xd8\xff\xe0FAKEJPEG\xff\xd9"


def populate_bundle(bundle_dir: Path) -> None:
    bundle_dir = Path(bundle_dir)
    bundle_dir.mkdir(parents=True, exist_ok=True)
    db_path = build_base_profiled_db(bundle_dir=bundle_dir, db_name=DB_NAME, books=3)

    store_root = bundle_dir / "covers_store"
    covers_root = store_root / "covers"
    covers_root.mkdir(parents=True, exist_ok=True)

    cover_paths = {
        "book-one-cover.png": covers_root / "book-one-cover.png",
        "book-one-alt.jpg": covers_root / "book-one-alt.jpg",
        "book-two-cover.jpg": covers_root / "book-two-cover.jpg",
        "book-three-cover.png": covers_root / "book-three-cover.png",
        "book-three-thumb.png": covers_root / "book-three-thumb.png",
    }
    cover_paths["book-one-cover.png"].write_bytes(_PNG_BYTES)
    cover_paths["book-one-alt.jpg"].write_bytes(_JPEG_BYTES)
    cover_paths["book-two-cover.jpg"].write_bytes(_JPEG_BYTES)
    cover_paths["book-three-cover.png"].write_bytes(_PNG_BYTES)
    cover_paths["book-three-thumb.png"].write_bytes(_PNG_BYTES)

    conn = open_fixture_db(db_path)
    try:
        work_ids = ordered_ids(conn, "works", "work_id")
        item_ids = ordered_ids(conn, "items", "item_id")
        if len(work_ids) != 3 or len(item_ids) != 3:
            raise AssertionError(f"Unexpected seeded WEMI counts for {DB_NAME}")

        titles = (
            (work_ids[0], "Cover Rich Book One"),
            (work_ids[1], "Cover Rich Book Two"),
            (work_ids[2], "Cover Rich Book Three"),
        )
        for work_id, title in titles:
            conn.execute(
                "UPDATE works SET work_title = ?, work_canonical_title = ?, work_sort_title = ?, work_creator_sort = ? WHERE work_id = ?;",
                (title, title, title, title, work_id),
            )

        store_id = int(
            conn.execute(
                "INSERT INTO stores (store_name, store_kind, store_access_protocol, store_root_uri, store_online_status, store_supports_folders, store_supports_random_read, store_is_read_only, store_location_note) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
                (
                    "fixture_cover_store",
                    "on_disk_existing_unmanaged_drive",
                    "file",
                    bundle_token_path("covers_store"),
                    "online",
                    1,
                    1,
                    1,
                    f"fixture:{DB_NAME}",
                ),
            ).lastrowid
        )
        folder_id = int(
            conn.execute(
                "INSERT INTO folders (folder_store_id, folder_name, folder_relpath, folder_policy_json) VALUES (?, ?, ?, ?);",
                (store_id, "covers", "covers", "{}"),
            ).lastrowid
        )

        conn.executemany(
            "INSERT INTO folder_work_links (folder_work_link_folder_id, folder_work_link_work_id, folder_work_link_priority) VALUES (?, ?, ?);",
            (
                (folder_id, work_ids[0], 1),
                (folder_id, work_ids[1], 2),
                (folder_id, work_ids[2], 3),
            ),
        )

        image_rows = (
            {
                "path": cover_paths["book-one-cover.png"],
                "item_id": item_ids[0],
                "work_id": work_ids[0],
                "priority": 1,
                "link_type": "cover",
                "role": "cover",
                "mime": "image/png",
                "bytes": _PNG_BYTES,
            },
            {
                "path": cover_paths["book-one-alt.jpg"],
                "item_id": item_ids[0],
                "work_id": work_ids[0],
                "priority": 2,
                "link_type": "illustration",
                "role": "alternate",
                "mime": "image/jpeg",
                "bytes": _JPEG_BYTES,
            },
            {
                "path": cover_paths["book-two-cover.jpg"],
                "item_id": item_ids[1],
                "work_id": work_ids[1],
                "priority": 1,
                "link_type": "cover",
                "role": "cover",
                "mime": "image/jpeg",
                "bytes": _JPEG_BYTES,
            },
            {
                "path": cover_paths["book-three-cover.png"],
                "item_id": item_ids[2],
                "work_id": work_ids[2],
                "priority": 1,
                "link_type": "cover",
                "role": "cover",
                "mime": "image/png",
                "bytes": _PNG_BYTES,
            },
            {
                "path": cover_paths["book-three-thumb.png"],
                "item_id": item_ids[2],
                "work_id": work_ids[2],
                "priority": 2,
                "link_type": "diagram",
                "role": "thumbnail",
                "mime": "image/png",
                "bytes": _PNG_BYTES,
            },
        )
        for payload in image_rows:
            image_path = payload["path"]
            image_id = int(
                conn.execute(
                    "INSERT INTO images (image_item_id, image_store_id, image_folder_id, image_storage_key, image_name, image_base_name, image_extension, image_mime_type, image_role, image_media_category, image_size_bytes, image_source, image_original_name, image_original_path) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
                    (
                        payload["item_id"],
                        store_id,
                        folder_id,
                        f"covers/{image_path.name}",
                        image_path.name,
                        image_path.stem,
                        image_path.suffix.lstrip("."),
                        payload["mime"],
                        payload["role"],
                        "cover",
                        len(payload["bytes"]),
                        "fixture-cover-store",
                        image_path.name,
                        bundle_token_path("covers_store", "covers", image_path.name),
                    ),
                ).lastrowid
            )
            conn.execute(
                "INSERT INTO image_work_links (image_work_link_image_id, image_work_link_work_id, image_work_link_priority, image_work_link_type) VALUES (?, ?, ?, ?);",
                (image_id, payload["work_id"], payload["priority"], payload["link_type"]),
            )

        finalize_fixture(conn, db_name=DB_NAME)
    finally:
        conn.close()
