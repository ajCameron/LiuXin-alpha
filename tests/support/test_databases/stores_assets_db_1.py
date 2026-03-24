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


DB_NAME = "stores_assets_db_1"


def populate_bundle(bundle_dir: Path) -> None:
    bundle_dir = Path(bundle_dir)
    bundle_dir.mkdir(parents=True, exist_ok=True)
    db_path = build_base_profiled_db(bundle_dir=bundle_dir, db_name=DB_NAME, books=3)

    primary_root = bundle_dir / "primary_store_root"
    secondary_root = bundle_dir / "secondary_store_root"
    (primary_root / "books").mkdir(parents=True, exist_ok=True)
    (primary_root / "images").mkdir(parents=True, exist_ok=True)
    (secondary_root / "books").mkdir(parents=True, exist_ok=True)
    (secondary_root / "images").mkdir(parents=True, exist_ok=True)

    book_one_epub = primary_root / "books" / "multi-store-one.epub"
    book_one_pdf = primary_root / "books" / "multi-store-one.pdf"
    book_two_mobi = secondary_root / "books" / "multi-store-two.mobi"
    book_three_txt = secondary_root / "books" / "multi-store-three.txt"
    cover_one = primary_root / "images" / "multi-store-one-cover.png"
    cover_two = secondary_root / "images" / "multi-store-two-cover.jpg"
    cover_three = secondary_root / "images" / "multi-store-three-cover.png"

    book_one_epub.write_bytes(b"PRIMARY-EPUB-ONE\n")
    book_one_pdf.write_bytes(b"%PDF-1.4\nPRIMARY-PDF-ONE\n")
    book_two_mobi.write_bytes(b"MOBI-TWO\n")
    book_three_txt.write_bytes(b"Plain text three\n")
    cover_one.write_bytes(PNG_1X1_BYTES)
    cover_two.write_bytes(JPEG_FAKE_BYTES)
    cover_three.write_bytes(PNG_1X1_BYTES)

    conn = open_fixture_db(db_path)
    try:
        work_ids = ordered_ids(conn, "works", "work_id")
        manifestation_ids = ordered_ids(conn, "manifestations", "manifestation_id")
        item_ids = ordered_ids(conn, "items", "item_id")
        if len(work_ids) != 3 or len(manifestation_ids) != 3 or len(item_ids) != 3:
            raise AssertionError(f"Unexpected seeded WEMI counts for {DB_NAME}")

        for work_id, title in zip(
            work_ids,
            ("Store Variant Book One", "Store Variant Book Two", "Store Variant Book Three"),
            strict=True,
        ):
            conn.execute(
                "UPDATE works SET work_title = ?, work_canonical_title = ?, work_sort_title = ?, work_creator_sort = ? WHERE work_id = ?;",
                (title, title, title, title, work_id),
            )
        for manifestation_id, detail in zip(manifestation_ids, ("epub", "mobi", "txt"), strict=True):
            conn.execute(
                "UPDATE manifestations SET manifestation_format_detail = ?, manifestation_carrier_type = ? WHERE manifestation_id = ?;",
                (detail, "digital", manifestation_id),
            )

        primary_store_id = int(
            conn.execute(
                "INSERT INTO stores (store_name, store_kind, store_access_protocol, store_root_uri, store_online_status, store_supports_folders, store_supports_random_read, store_is_read_only, store_location_note) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
                (
                    "primary_fixture_store",
                    "on_disk_existing_unmanaged_drive",
                    "file",
                    bundle_token_path("primary_store_root"),
                    "online",
                    1,
                    1,
                    1,
                    f"fixture:{DB_NAME}:primary",
                ),
            ).lastrowid
        )
        secondary_store_id = int(
            conn.execute(
                "INSERT INTO stores (store_name, store_kind, store_access_protocol, store_root_uri, store_online_status, store_supports_folders, store_supports_random_read, store_is_read_only, store_location_note) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
                (
                    "secondary_fixture_store",
                    "on_disk_existing_unmanaged_drive",
                    "file",
                    bundle_token_path("secondary_store_root"),
                    "online",
                    1,
                    1,
                    1,
                    f"fixture:{DB_NAME}:secondary",
                ),
            ).lastrowid
        )

        folders = {
            "primary_books": int(conn.execute("INSERT INTO folders (folder_store_id, folder_name, folder_relpath, folder_policy_json) VALUES (?, ?, ?, ?);", (primary_store_id, "books", "books", "{}")).lastrowid),
            "primary_images": int(conn.execute("INSERT INTO folders (folder_store_id, folder_name, folder_relpath, folder_policy_json) VALUES (?, ?, ?, ?);", (primary_store_id, "images", "images", "{}")).lastrowid),
            "secondary_books": int(conn.execute("INSERT INTO folders (folder_store_id, folder_name, folder_relpath, folder_policy_json) VALUES (?, ?, ?, ?);", (secondary_store_id, "books", "books", "{}")).lastrowid),
            "secondary_images": int(conn.execute("INSERT INTO folders (folder_store_id, folder_name, folder_relpath, folder_policy_json) VALUES (?, ?, ?, ?);", (secondary_store_id, "images", "images", "{}")).lastrowid),
        }

        conn.executemany(
            "INSERT INTO folder_work_links (folder_work_link_folder_id, folder_work_link_work_id, folder_work_link_priority) VALUES (?, ?, ?);",
            (
                (folders["primary_books"], work_ids[0], 1),
                (folders["primary_images"], work_ids[0], 1),
                (folders["secondary_books"], work_ids[1], 1),
                (folders["secondary_books"], work_ids[2], 2),
                (folders["secondary_images"], work_ids[1], 1),
                (folders["secondary_images"], work_ids[2], 2),
            ),
        )

        conn.execute(
            "UPDATE items SET item_type = ?, item_source = ?, item_source_detail = ?, item_source_path = ?, item_source_name = ? WHERE item_id = ?;",
            ("digital", "fixture-store", "primary-asset", bundle_token_path("primary_store_root", "books", book_one_epub.name), book_one_epub.name, item_ids[0]),
        )
        conn.execute(
            "UPDATE items SET item_type = ?, item_source = ?, item_source_detail = ?, item_source_path = ?, item_source_name = ? WHERE item_id = ?;",
            ("digital", "fixture-store", "secondary-asset", bundle_token_path("secondary_store_root", "books", book_two_mobi.name), book_two_mobi.name, item_ids[1]),
        )
        conn.execute(
            "UPDATE items SET item_type = ?, item_source = ?, item_source_detail = ?, item_source_path = ?, item_source_name = ? WHERE item_id = ?;",
            ("digital", "fixture-store", "secondary-asset", bundle_token_path("secondary_store_root", "books", book_three_txt.name), book_three_txt.name, item_ids[2]),
        )

        file_rows = (
            (item_ids[0], primary_store_id, folders["primary_books"], book_one_epub, "application/epub+zip", "primary"),
            (item_ids[0], primary_store_id, folders["primary_books"], book_one_pdf, "application/pdf", "alternate"),
            (item_ids[1], secondary_store_id, folders["secondary_books"], book_two_mobi, "application/x-mobipocket-ebook", "primary"),
            (item_ids[2], secondary_store_id, folders["secondary_books"], book_three_txt, "text/plain", "primary"),
        )
        for item_id, store_id, folder_id, path, mime, role in file_rows:
            file_id = int(
                conn.execute(
                    "INSERT INTO files (file_item_id, file_store_id, file_folder_id, file_storage_key, file_name, file_base_name, file_extension, file_mime_type, file_role, file_media_category, file_size_bytes, file_source, file_original_name, file_original_path) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
                    (
                        item_id,
                        store_id,
                        folder_id,
                        f"books/{path.name}",
                        path.name,
                        path.stem,
                        path.suffix.lstrip("."),
                        mime,
                        role,
                        "ebook",
                        path.stat().st_size,
                        "fixture-store",
                        path.name,
                        bundle_token_path(path.parents[1].name, "books", path.name),
                    ),
                ).lastrowid
            )
            conn.execute("INSERT INTO file_folder_links (file_folder_link_file_id, file_folder_link_folder_id) VALUES (?, ?);", (file_id, folder_id))

        image_rows = (
            (item_ids[0], work_ids[0], primary_store_id, folders["primary_images"], cover_one, "image/png"),
            (item_ids[1], work_ids[1], secondary_store_id, folders["secondary_images"], cover_two, "image/jpeg"),
            (item_ids[2], work_ids[2], secondary_store_id, folders["secondary_images"], cover_three, "image/png"),
        )
        for item_id, work_id, store_id, folder_id, path, mime in image_rows:
            image_id = int(
                conn.execute(
                    "INSERT INTO images (image_item_id, image_store_id, image_folder_id, image_storage_key, image_name, image_base_name, image_extension, image_mime_type, image_role, image_media_category, image_size_bytes, image_source, image_original_name, image_original_path) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
                    (
                        item_id,
                        store_id,
                        folder_id,
                        f"images/{path.name}",
                        path.name,
                        path.stem,
                        path.suffix.lstrip("."),
                        mime,
                        "cover",
                        "cover",
                        path.stat().st_size,
                        "fixture-store",
                        path.name,
                        bundle_token_path(path.parents[1].name, "images", path.name),
                    ),
                ).lastrowid
            )
            conn.execute(
                "INSERT INTO image_work_links (image_work_link_image_id, image_work_link_work_id, image_work_link_priority, image_work_link_type) VALUES (?, ?, ?, ?);",
                (image_id, work_id, 1, "cover"),
            )

        finalize_fixture(conn, db_name=DB_NAME)
    finally:
        conn.close()

