from __future__ import annotations

from pathlib import Path

from tests.support.test_databases._semantic_fixture_builders import (
    PNG_1X1_BYTES,
    build_base_profiled_db,
    bundle_token_path,
    finalize_fixture,
    norm_text,
    open_fixture_db,
    ordered_ids,
)


DB_NAME = "weird_data_db_0"


def populate_bundle(bundle_dir: Path) -> None:
    bundle_dir = Path(bundle_dir)
    bundle_dir.mkdir(parents=True, exist_ok=True)
    db_path = build_base_profiled_db(bundle_dir=bundle_dir, db_name=DB_NAME, books=3)

    store_root = bundle_dir / "weird_store"
    books_root = store_root / "books"
    images_root = store_root / "images"
    books_root.mkdir(parents=True, exist_ok=True)
    images_root.mkdir(parents=True, exist_ok=True)

    file_names = (
        "El Ni\u00f1o \u2014 \u00e9dition finale.EPUB",
        "\u6f22\u5b57\u3068\u304b\u306a\u306e\u672c.PDF",
        "emoji-field-notes-\u2615.txt",
    )
    cover_name = "caf\u00e9-cover.png"
    book_paths = [books_root / name for name in file_names]
    cover_path = images_root / cover_name

    book_paths[0].write_bytes("Texto con acentos y saltos.\n".encode("utf-8"))
    book_paths[1].write_bytes("日本語の本文です。\n".encode("utf-8"))
    book_paths[2].write_bytes("emoji field notes\n".encode("utf-8"))
    cover_path.write_bytes(PNG_1X1_BYTES)

    conn = open_fixture_db(db_path)
    try:
        work_ids = ordered_ids(conn, "works", "work_id")
        manifestation_ids = ordered_ids(conn, "manifestations", "manifestation_id")
        item_ids = ordered_ids(conn, "items", "item_id")
        if len(work_ids) != 3 or len(manifestation_ids) != 3 or len(item_ids) != 3:
            raise AssertionError(f"Unexpected seeded WEMI counts for {DB_NAME}")

        titles = (
            "El Ni\u00f1o \u2014 \u00e9dition finale",
            "\u6f22\u5b57\u3068\u304b\u306a\u306e\u672c",
            "Emoji Field Notes \u2615",
        )
        sorts = (
            "Arc, Jos\u00e9",
            "\u6771\u4eac, \u4f5c\u8005",
            "Curator, Emoji",
        )
        for work_id, title, creator_sort in zip(work_ids, titles, sorts, strict=True):
            conn.execute(
                "UPDATE works SET work_title = ?, work_canonical_title = ?, work_sort_title = ?, work_creator_sort = ?, work_discovery_note = ? WHERE work_id = ?;",
                (title, title, title, creator_sort, f"fixture:{DB_NAME}", work_id),
            )

        for manifestation_id, detail in zip(manifestation_ids, ("epub", "pdf", "txt"), strict=True):
            conn.execute(
                "UPDATE manifestations SET manifestation_format_detail = ?, manifestation_carrier_type = ? WHERE manifestation_id = ?;",
                (detail, "digital", manifestation_id),
            )

        def _insert_simple_row(table: str, column: str, value: str) -> int:
            return int(conn.execute(f"INSERT INTO {table} ({column}) VALUES (?);", (value,)).lastrowid)

        note_id = _insert_simple_row(
            "notes",
            "note",
            "Long weird note with newline\\nsecond line\\nand punctuation []{}<> -- plus caf\u00e9 and kana \u304b\u306a.",
        )
        comment_id = _insert_simple_row(
            "comments",
            "comment",
            "<p>Messy-but-valid HTML-ish comment with caf\u00e9, em dash \u2014, and math 1 < 2.</p>",
        )
        synopsis_id = _insert_simple_row(
            "synopses",
            "synopsis",
            "Synopsis with mixed scripts: Espa\u00f1ol, \u65e5\u672c\u8a9e, and symbols \u2615\u2605.",
        )
        conn.execute("INSERT INTO note_work_links (note_work_link_note_id, note_work_link_work_id, note_work_link_priority) VALUES (?, ?, ?);", (note_id, work_ids[0], 1))
        conn.execute("INSERT INTO comment_work_links (comment_work_link_comment_id, comment_work_link_work_id, comment_work_link_priority) VALUES (?, ?, ?);", (comment_id, work_ids[1], 1))
        conn.execute(
            "INSERT INTO synopsis_work_links (synopsis_work_link_synopsis_id, synopsis_work_link_work_id, synopsis_work_link_priority, synopsis_work_link_type) VALUES (?, ?, ?, ?);",
            (synopsis_id, work_ids[2], 1, "short"),
        )

        label_ids = {
            "caf\u00e9": _insert_simple_row("labels", "label_text", "caf\u00e9"),
            "\u6771\u4eac": _insert_simple_row("labels", "label_text", "\u6771\u4eac"),
            "emoji-\u2615": _insert_simple_row("labels", "label_text", "emoji-\u2615"),
        }
        for text, label_id in label_ids.items():
            conn.execute("UPDATE labels SET label_text_norm = ?, label_description = ? WHERE label_id = ?;", (norm_text(text), f"fixture:{DB_NAME}:{text}", label_id))
        conn.executemany(
            "INSERT INTO label_work_links (label_work_link_label_id, label_work_link_work_id, label_work_link_priority) VALUES (?, ?, ?);",
            (
                (label_ids["caf\u00e9"], work_ids[0], 1),
                (label_ids["\u6771\u4eac"], work_ids[1], 1),
                (label_ids["emoji-\u2615"], work_ids[2], 1),
            ),
        )

        store_id = int(
            conn.execute(
                "INSERT INTO stores (store_name, store_kind, store_access_protocol, store_root_uri, store_online_status, store_supports_folders, store_supports_random_read, store_is_read_only, store_location_note) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
                (
                    "weird_fixture_store",
                    "on_disk_existing_unmanaged_drive",
                    "file",
                    bundle_token_path("weird_store"),
                    "online",
                    1,
                    1,
                    1,
                    f"fixture:{DB_NAME}",
                ),
            ).lastrowid
        )
        books_folder_id = int(conn.execute("INSERT INTO folders (folder_store_id, folder_name, folder_relpath, folder_policy_json) VALUES (?, ?, ?, ?);", (store_id, "books", "books", "{}")).lastrowid)
        images_folder_id = int(conn.execute("INSERT INTO folders (folder_store_id, folder_name, folder_relpath, folder_policy_json) VALUES (?, ?, ?, ?);", (store_id, "images", "images", "{}")).lastrowid)

        conn.executemany(
            "INSERT INTO folder_work_links (folder_work_link_folder_id, folder_work_link_work_id, folder_work_link_priority) VALUES (?, ?, ?);",
            (
                (books_folder_id, work_ids[0], 1),
                (books_folder_id, work_ids[1], 2),
                (books_folder_id, work_ids[2], 3),
                (images_folder_id, work_ids[0], 1),
            ),
        )

        mime_types = ("application/epub+zip", "application/pdf", "text/plain")
        for item_id, path, mime in zip(item_ids, book_paths, mime_types, strict=True):
            conn.execute(
                "UPDATE items SET item_type = ?, item_source = ?, item_source_detail = ?, item_source_path = ?, item_source_name = ? WHERE item_id = ?;",
                ("digital", "fixture-weird", path.name, bundle_token_path("weird_store", "books", path.name), path.name, item_id),
            )
            file_id = int(
                conn.execute(
                    "INSERT INTO files (file_item_id, file_store_id, file_folder_id, file_storage_key, file_name, file_base_name, file_extension, file_mime_type, file_role, file_media_category, file_size_bytes, file_source, file_original_name, file_original_path) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
                    (
                        item_id,
                        store_id,
                        books_folder_id,
                        f"books/{path.name}",
                        path.name,
                        path.stem,
                        path.suffix.lstrip("."),
                        mime,
                        "primary",
                        "ebook",
                        path.stat().st_size,
                        "fixture-weird",
                        path.name,
                        bundle_token_path("weird_store", "books", path.name),
                    ),
                ).lastrowid
            )
            conn.execute("INSERT INTO file_folder_links (file_folder_link_file_id, file_folder_link_folder_id) VALUES (?, ?);", (file_id, books_folder_id))

        image_id = int(
            conn.execute(
                "INSERT INTO images (image_item_id, image_store_id, image_folder_id, image_storage_key, image_name, image_base_name, image_extension, image_mime_type, image_role, image_media_category, image_size_bytes, image_source, image_original_name, image_original_path) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
                (
                    item_ids[0],
                    store_id,
                    images_folder_id,
                    f"images/{cover_path.name}",
                    cover_path.name,
                    cover_path.stem,
                    cover_path.suffix.lstrip("."),
                    "image/png",
                    "cover",
                    "cover",
                    cover_path.stat().st_size,
                    "fixture-weird",
                    cover_path.name,
                    bundle_token_path("weird_store", "images", cover_path.name),
                ),
            ).lastrowid
        )
        conn.execute(
            "INSERT INTO image_work_links (image_work_link_image_id, image_work_link_work_id, image_work_link_priority, image_work_link_type) VALUES (?, ?, ?, ?);",
            (image_id, work_ids[0], 1, "cover"),
        )

        finalize_fixture(conn, db_name=DB_NAME)
    finally:
        conn.close()
