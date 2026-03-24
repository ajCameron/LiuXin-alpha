from __future__ import annotations

from pathlib import Path

from tests.support.test_databases._semantic_fixture_builders import (
    build_base_profiled_db,
    bundle_token_path,
    finalize_fixture,
    open_fixture_db,
    ordered_ids,
)


DB_NAME = "stores_assets_db_0"


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
    db_path = build_base_profiled_db(bundle_dir=bundle_dir, db_name=DB_NAME, books=2)

    store_root = bundle_dir / "store_root"
    books_root = store_root / "books"
    images_root = store_root / "images"
    books_root.mkdir(parents=True, exist_ok=True)
    images_root.mkdir(parents=True, exist_ok=True)

    book_one_path = books_root / "store-asset-one.epub"
    book_two_path = books_root / "store-asset-two.pdf"
    cover_one_path = images_root / "store-asset-one-cover.png"
    cover_two_path = images_root / "store-asset-two-cover.jpg"

    book_one_bytes = b"EPUB-ASSET-ONE\n"
    book_two_bytes = b"%PDF-1.4\nSTORE-ASSET-TWO\n"

    book_one_path.write_bytes(book_one_bytes)
    book_two_path.write_bytes(book_two_bytes)
    cover_one_path.write_bytes(_PNG_BYTES)
    cover_two_path.write_bytes(_JPEG_BYTES)

    conn = open_fixture_db(db_path)
    try:
        work_ids = ordered_ids(conn, "works", "work_id")
        manifestation_ids = ordered_ids(conn, "manifestations", "manifestation_id")
        item_ids = ordered_ids(conn, "items", "item_id")
        if len(work_ids) != 2 or len(manifestation_ids) != 2 or len(item_ids) != 2:
            raise AssertionError(f"Unexpected seeded WEMI counts for {DB_NAME}")

        conn.execute(
            "UPDATE works SET work_title = ?, work_canonical_title = ?, work_sort_title = ?, work_creator_sort = ? WHERE work_id = ?;",
            ("Store Asset Book One", "Store Asset Book One", "Store Asset Book One", "Asset, One", work_ids[0]),
        )
        conn.execute(
            "UPDATE works SET work_title = ?, work_canonical_title = ?, work_sort_title = ?, work_creator_sort = ? WHERE work_id = ?;",
            ("Store Asset Book Two", "Store Asset Book Two", "Store Asset Book Two", "Asset, Two", work_ids[1]),
        )
        conn.execute(
            "UPDATE manifestations SET manifestation_format_detail = ?, manifestation_carrier_type = ? WHERE manifestation_id = ?;",
            ("epub", "digital", manifestation_ids[0]),
        )
        conn.execute(
            "UPDATE manifestations SET manifestation_format_detail = ?, manifestation_carrier_type = ? WHERE manifestation_id = ?;",
            ("pdf", "digital", manifestation_ids[1]),
        )

        store_id = int(
            conn.execute(
                "INSERT INTO stores (store_name, store_kind, store_access_protocol, store_root_uri, store_online_status, store_supports_folders, store_supports_random_read, store_is_read_only, store_location_note) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
                (
                    "fixture_assets_store",
                    "on_disk_existing_unmanaged_drive",
                    "file",
                    bundle_token_path("store_root"),
                    "online",
                    1,
                    1,
                    1,
                    f"fixture:{DB_NAME}",
                ),
            ).lastrowid
        )

        books_folder_id = int(
            conn.execute(
                "INSERT INTO folders (folder_store_id, folder_name, folder_relpath, folder_policy_json) VALUES (?, ?, ?, ?);",
                (store_id, "books", "books", "{}"),
            ).lastrowid
        )
        images_folder_id = int(
            conn.execute(
                "INSERT INTO folders (folder_store_id, folder_name, folder_relpath, folder_policy_json) VALUES (?, ?, ?, ?);",
                (store_id, "images", "images", "{}"),
            ).lastrowid
        )

        conn.executemany(
            "INSERT INTO folder_work_links (folder_work_link_folder_id, folder_work_link_work_id, folder_work_link_priority) VALUES (?, ?, ?);",
            (
                (books_folder_id, work_ids[0], 1),
                (books_folder_id, work_ids[1], 2),
                (images_folder_id, work_ids[0], 1),
                (images_folder_id, work_ids[1], 2),
            ),
        )

        conn.execute(
            "UPDATE items SET item_type = ?, item_source = ?, item_source_detail = ?, item_source_path = ?, item_source_name = ? WHERE item_id = ?;",
            ("digital", "fixture-store", "primary-file", bundle_token_path("store_root", "books", book_one_path.name), book_one_path.name, item_ids[0]),
        )
        conn.execute(
            "UPDATE items SET item_type = ?, item_source = ?, item_source_detail = ?, item_source_path = ?, item_source_name = ? WHERE item_id = ?;",
            ("digital", "fixture-store", "primary-file", bundle_token_path("store_root", "books", book_two_path.name), book_two_path.name, item_ids[1]),
        )

        file_rows = (
            {
                "item_id": item_ids[0],
                "folder_id": books_folder_id,
                "storage_key": f"books/{book_one_path.name}",
                "name": book_one_path.name,
                "base_name": book_one_path.stem,
                "extension": book_one_path.suffix.lstrip("."),
                "mime_type": "application/epub+zip",
                "original_path": bundle_token_path("store_root", "books", book_one_path.name),
                "size_bytes": len(book_one_bytes),
            },
            {
                "item_id": item_ids[1],
                "folder_id": books_folder_id,
                "storage_key": f"books/{book_two_path.name}",
                "name": book_two_path.name,
                "base_name": book_two_path.stem,
                "extension": book_two_path.suffix.lstrip("."),
                "mime_type": "application/pdf",
                "original_path": bundle_token_path("store_root", "books", book_two_path.name),
                "size_bytes": len(book_two_bytes),
            },
        )
        file_ids: list[int] = []
        for payload in file_rows:
            file_id = int(
                conn.execute(
                    "INSERT INTO files (file_item_id, file_store_id, file_folder_id, file_storage_key, file_name, file_base_name, file_extension, file_mime_type, file_role, file_media_category, file_size_bytes, file_source, file_original_name, file_original_path) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
                    (
                        payload["item_id"],
                        store_id,
                        payload["folder_id"],
                        payload["storage_key"],
                        payload["name"],
                        payload["base_name"],
                        payload["extension"],
                        payload["mime_type"],
                        "primary",
                        "ebook",
                        payload["size_bytes"],
                        "fixture-store",
                        payload["name"],
                        payload["original_path"],
                    ),
                ).lastrowid
            )
            file_ids.append(file_id)
            conn.execute(
                "INSERT INTO file_folder_links (file_folder_link_file_id, file_folder_link_folder_id) VALUES (?, ?);",
                (file_id, payload["folder_id"]),
            )

        image_rows = (
            {
                "item_id": item_ids[0],
                "work_id": work_ids[0],
                "storage_key": f"images/{cover_one_path.name}",
                "name": cover_one_path.name,
                "base_name": cover_one_path.stem,
                "extension": cover_one_path.suffix.lstrip("."),
                "mime_type": "image/png",
                "original_path": bundle_token_path("store_root", "images", cover_one_path.name),
                "size_bytes": len(_PNG_BYTES),
            },
            {
                "item_id": item_ids[1],
                "work_id": work_ids[1],
                "storage_key": f"images/{cover_two_path.name}",
                "name": cover_two_path.name,
                "base_name": cover_two_path.stem,
                "extension": cover_two_path.suffix.lstrip("."),
                "mime_type": "image/jpeg",
                "original_path": bundle_token_path("store_root", "images", cover_two_path.name),
                "size_bytes": len(_JPEG_BYTES),
            },
        )
        for payload in image_rows:
            image_id = int(
                conn.execute(
                    "INSERT INTO images (image_item_id, image_store_id, image_folder_id, image_storage_key, image_name, image_base_name, image_extension, image_mime_type, image_role, image_media_category, image_size_bytes, image_source, image_original_name, image_original_path) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
                    (
                        payload["item_id"],
                        store_id,
                        images_folder_id,
                        payload["storage_key"],
                        payload["name"],
                        payload["base_name"],
                        payload["extension"],
                        payload["mime_type"],
                        "cover",
                        "cover",
                        payload["size_bytes"],
                        "fixture-store",
                        payload["name"],
                        payload["original_path"],
                    ),
                ).lastrowid
            )
            conn.execute(
                "INSERT INTO image_work_links (image_work_link_image_id, image_work_link_work_id, image_work_link_priority, image_work_link_type) VALUES (?, ?, ?, ?);",
                (image_id, payload["work_id"], 1, "cover"),
            )

        finalize_fixture(conn, db_name=DB_NAME)
    finally:
        conn.close()
