"""FRBR generator: WEMI aggregate views.

These tests ensure the generator ships a small set of "read-model" views that
project book-ish surfaces out of the WEMI graph.

The views are intended as a UI/compatibility layer and are deliberately lossy.
"""

from __future__ import annotations

import pathlib
import sqlite3

from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import database_generator as frbr_gen
from LiuXin_alpha.databases.database_driver_plugins.SQL.utility_mixins import ColumnNameMixin


def _views(conn: sqlite3.Connection) -> set[str]:
    return {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='view';")}


def _insert_atomic_asset_bundle(
    cur: sqlite3.Cursor,
    *,
    item_id: int,
    store_id: int,
    storage_key: str,
    link_type: str = "primary_payload",
    size_bytes: int | None = None,
    media_category: str | None = None,
    folder_id: int | None = None,
    name: str | None = None,
) -> tuple[int, int]:
    cur.execute(
        "INSERT INTO digital_assets (digital_asset_name, digital_asset_size_bytes, digital_asset_media_category) VALUES (?, ?, ?);",
        (name or storage_key.rsplit('/', 1)[-1], size_bytes, media_category),
    )
    digital_asset_id = int(cur.lastrowid)

    cur.execute(
        "INSERT INTO digital_asset_item_links (digital_asset_item_link_digital_asset_id, digital_asset_item_link_item_id, digital_asset_item_link_type, digital_asset_item_link_priority, digital_asset_item_link_primary, digital_asset_item_link_origin) VALUES (?, ?, ?, ?, ?, ?);",
        (digital_asset_id, item_id, link_type, 0, 1 if link_type == 'primary_payload' else 0, 'test'),
    )

    cur.execute(
        "INSERT INTO asset_replicas (asset_replica_digital_asset_id, asset_replica_store_id, asset_replica_folder_id, asset_replica_storage_key, asset_replica_mode) VALUES (?, ?, ?, ?, ?);",
        (digital_asset_id, store_id, folder_id, storage_key, 'active'),
    )
    asset_replica_id = int(cur.lastrowid)
    return digital_asset_id, asset_replica_id


def _insert_composite_asset_bundle(
    cur: sqlite3.Cursor,
    *,
    item_id: int,
    composite_name: str,
    member_asset_ids: list[int],
) -> int:
    cur.execute(
        "INSERT INTO composite_digital_assets (composite_digital_asset_name, composite_digital_asset_media_category) VALUES (?, ?);",
        (composite_name, 'audiobook'),
    )
    composite_id = int(cur.lastrowid)

    cur.execute(
        "INSERT INTO composite_digital_asset_item_links (composite_digital_asset_item_link_composite_digital_asset_id, composite_digital_asset_item_link_item_id, composite_digital_asset_item_link_type, composite_digital_asset_item_link_priority, composite_digital_asset_item_link_primary, composite_digital_asset_item_link_origin) VALUES (?, ?, ?, ?, ?, ?);",
        (composite_id, item_id, 'primary_payload', 0, 1, 'test'),
    )

    for seq, digital_asset_id in enumerate(member_asset_ids, start=1):
        cur.execute(
            "INSERT INTO composite_digital_asset_digital_asset_links (composite_digital_asset_digital_asset_link_composite_digital_asset_id, composite_digital_asset_digital_asset_link_digital_asset_id, composite_digital_asset_digital_asset_link_type, composite_digital_asset_digital_asset_link_origin, composite_digital_asset_digital_asset_link_sequence_number, composite_digital_asset_digital_asset_link_is_required) VALUES (?, ?, ?, ?, ?, ?);",
            (composite_id, digital_asset_id, 'chapter', 'test', seq, 1),
        )

    return composite_id


def test_frbr_generator_creates_wemi_views(tmp_path: pathlib.Path) -> None:
    db_path = tmp_path / "frbr_views_smoke.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        frbr_gen.create_new_database(conn)

        views = _views(conn)

        expected = {
            "wemi_rays_v",
            "wemi_primary_rays_v",
            "wemi_ray_items_v",
            "wemi_work_stats_v",
            "titles_v",
            "titles",

            "books_v",
            "books",
            "agent_credits_v",
            "digital_asset_inventory_v",
            "file_inventory_v",
            "book_publishers_v",
            "publishers_v",
            "subjects_tags_v",

            "ingest_audit_v",
            "ingest_audit",

            "identifiers_v",
            "identifiers",            "duplicate_candidates_v",
            "duplicate_candidates",
            "search_seed_v",
            "search_seed",


        }
        missing = sorted(expected - views)
        assert not missing, f"Missing expected WEMI views: {missing}. Present: {sorted(views)[:50]}"

        # Views should be views, not tables.
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table';")}
        assert not (expected & tables)

        # Empty database should yield empty view results.
        assert conn.execute("SELECT COUNT(*) FROM wemi_rays_v;").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM wemi_primary_rays_v;").fetchone()[0] == 0

    finally:
        conn.close()


def test_identifiers_v_unifies_entity_and_item_identifiers(tmp_path: pathlib.Path) -> None:
    """Identifiers: a single view should expose curated (entity) + raw (item) identifiers."""

    db_path = tmp_path / "frbr_identifiers_view.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        frbr_gen.create_new_database(conn)

        cur = conn.cursor()

        # Minimal ray.
        cur.execute("INSERT INTO works (work_title, work_canonical_title) VALUES (?, ?);", ("Dune", "Dune"))
        work_id = cur.lastrowid

        cur.execute("INSERT INTO expressions (expression_label, expression_year) VALUES (?, ?);", ("rev", 1987))
        expression_id = cur.lastrowid

        cur.execute(
            "INSERT INTO manifestations (manifestation_edition_statement, manifestation_pub_year) VALUES (?, ?);",
            ("First print", 1988),
        )
        manifestation_id = cur.lastrowid

        we_table, we_base = ColumnNameMixin.get_interlink_table_name("works", "expressions")
        em_table, em_base = ColumnNameMixin.get_interlink_table_name("expressions", "manifestations")

        cur.execute(
            f"INSERT INTO `{we_table}` (`{we_base}_work_id`, `{we_base}_expression_id`, `{we_base}_priority`, `{we_base}_primary`, `{we_base}_origin`) VALUES (?, ?, ?, ?, ?);",
            (work_id, expression_id, 0, 1, "test"),
        )
        cur.execute(
            f"INSERT INTO `{em_table}` (`{em_base}_expression_id`, `{em_base}_manifestation_id`, `{em_base}_priority`, `{em_base}_primary`, `{em_base}_origin`) VALUES (?, ?, ?, ?, ?);",
            (expression_id, manifestation_id, 0, 1, "test"),
        )

        cur.execute("INSERT INTO items (item_manifestation_id, item_type) VALUES (?, ?);", (manifestation_id, "digital"))
        item_id = cur.lastrowid

        # Curated entity identifier (work)
        cur.execute(
            "INSERT INTO entity_identifiers (entity_identifier_entity_type, entity_identifier_entity_id, entity_identifier_scheme, entity_identifier_value, entity_identifier_is_primary, entity_identifier_provenance) "
            "VALUES (?, ?, ?, ?, ?, ?);",
            ("work", work_id, "uuid", "test-work-dune", 1, "import"),
        )

        # Raw observed identifier (item)
        cur.execute(
            "INSERT INTO item_identifiers (item_identifier_item_id, item_identifier_scheme, item_identifier_value, item_identifier_source) "
            "VALUES (?, ?, ?, ?);",
            (item_id, "asin", "B000123456", "calibre"),
        )

        conn.commit()

        rows = conn.execute(
            "SELECT identifier_origin, entity_type, entity_id, identifier_scheme, identifier_value, identifier_is_primary, identifier_provenance, identifier_source, entity_display_text "
            "FROM identifiers_v ORDER BY identifier_origin, identifier_scheme;"
        ).fetchall()

        assert rows == [
            ("entity", "work", work_id, "uuid", "test-work-dune", 1, "import", None, "Dune"),
            ("item", "item", item_id, "asin", "B000123456", None, None, "calibre", "digital"),
        ]

        # Alias view should mirror identifiers_v.
        rows2 = conn.execute(
            "SELECT identifier_origin, entity_type, entity_id, identifier_scheme, identifier_value FROM identifiers ORDER BY identifier_origin, identifier_scheme;"
        ).fetchall()
        assert [(r[0], r[1], r[2], r[3], r[4]) for r in rows2] == [(r[0], r[1], r[2], r[3], r[4]) for r in rows]

    finally:
        conn.close()


def test_file_inventory_v_projects_assets_with_ray_context(tmp_path: pathlib.Path) -> None:
    """Inventory: files should be projected with (ray/book) context."""

    db_path = tmp_path / "frbr_file_inventory_view.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        frbr_gen.create_new_database(conn)

        cur = conn.cursor()

        # Work / Expression / Manifestation
        cur.execute(
            "INSERT INTO works (work_title, work_canonical_title) VALUES (?, ?);",
            ("Dune", "Dune"),
        )
        work_id = cur.lastrowid

        cur.execute("INSERT INTO expressions (expression_label, expression_year) VALUES (?, ?);", ("rev", 1987))
        expression_id = cur.lastrowid

        cur.execute(
            "INSERT INTO manifestations (manifestation_edition_statement, manifestation_pub_year) VALUES (?, ?);",
            ("First print", 1988),
        )
        manifestation_id = cur.lastrowid

        we_table, we_base = ColumnNameMixin.get_interlink_table_name("works", "expressions")
        em_table, em_base = ColumnNameMixin.get_interlink_table_name("expressions", "manifestations")

        cur.execute(
            f"INSERT INTO `{we_table}` (`{we_base}_work_id`, `{we_base}_expression_id`, `{we_base}_priority`, `{we_base}_primary`, `{we_base}_origin`) VALUES (?, ?, ?, ?, ?);",
            (work_id, expression_id, 0, 1, "test"),
        )
        cur.execute(
            f"INSERT INTO `{em_table}` (`{em_base}_expression_id`, `{em_base}_manifestation_id`, `{em_base}_priority`, `{em_base}_primary`, `{em_base}_origin`) VALUES (?, ?, ?, ?, ?);",
            (expression_id, manifestation_id, 0, 1, "test"),
        )

        # Item + store + folder + files
        cur.execute("INSERT INTO items (item_manifestation_id, item_type) VALUES (?, ?);", (manifestation_id, "digital"))
        item_id = cur.lastrowid

        cur.execute(
            "INSERT INTO stores (store_name, store_kind, store_root_uri) VALUES (?, ?, ?);",
            ("test-store", "filesystem", "file:///vault"),
        )
        store_id = cur.lastrowid

        cur.execute(
            "INSERT INTO folders (folder_store_id, folder_name, folder_relpath) VALUES (?, ?, ?);",
            (store_id, "books", "books"),
        )
        folder_id = cur.lastrowid

        digital_asset_content_id, file_content_id = _insert_atomic_asset_bundle(
            cur,
            item_id=item_id,
            store_id=store_id,
            folder_id=folder_id,
            storage_key="books/dune.epub",
            link_type="primary_payload",
            size_bytes=100,
            name="dune.epub",
        )

        digital_asset_cover_id, file_cover_id = _insert_atomic_asset_bundle(
            cur,
            item_id=item_id,
            store_id=store_id,
            folder_id=folder_id,
            storage_key="books/cover.jpg",
            link_type="cover",
            size_bytes=10,
            media_category="cover",
            name="cover.jpg",
        )

        conn.commit()

        ray_id = f"{work_id}:{expression_id}:{manifestation_id}"

        rows = conn.execute(
            "SELECT file_id, book_id, ray_id, item_id, file_storage_key, file_uri, file_role, file_is_cover, file_size_bytes "
            "FROM file_inventory_v WHERE ray_id = ? ORDER BY file_id;",
            (ray_id,),
        ).fetchall()

        assert len(rows) == 2
        assert rows[0][0] == file_content_id
        assert rows[1][0] == file_cover_id

        # Both rows should carry the ray context.
        assert rows[0][1] == ray_id
        assert rows[0][2] == ray_id
        assert rows[0][3] == item_id
        assert rows[0][4] == "books/dune.epub"
        assert rows[0][5] == "file:///vault/books/dune.epub"
        assert rows[0][6] == "content"
        assert rows[0][7] == 0
        assert rows[0][8] == 100

        assert rows[1][4] == "books/cover.jpg"
        assert rows[1][5] == "file:///vault/books/cover.jpg"
        assert rows[1][6] == "cover"
        assert rows[1][7] == 1
        assert rows[1][8] == 10

    finally:
        conn.close()



def test_ingest_audit_v_unifies_item_and_file_events(tmp_path: pathlib.Path) -> None:
    """Audit: unified workflow events should project with ray/book context."""

    db_path = tmp_path / "frbr_ingest_audit_view.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        frbr_gen.create_new_database(conn)

        cur = conn.cursor()

        # Minimal ray.
        cur.execute("INSERT INTO works (work_title, work_canonical_title) VALUES (?, ?);", ("Dune", "Dune"))
        work_id = cur.lastrowid

        cur.execute("INSERT INTO expressions (expression_label, expression_year) VALUES (?, ?);", ("rev", 1987))
        expression_id = cur.lastrowid

        cur.execute(
            "INSERT INTO manifestations (manifestation_edition_statement, manifestation_pub_year) VALUES (?, ?);",
            ("First print", 1988),
        )
        manifestation_id = cur.lastrowid

        we_table, we_base = ColumnNameMixin.get_interlink_table_name("works", "expressions")
        em_table, em_base = ColumnNameMixin.get_interlink_table_name("expressions", "manifestations")

        cur.execute(
            f"INSERT INTO `{we_table}` (`{we_base}_work_id`, `{we_base}_expression_id`, `{we_base}_priority`, `{we_base}_primary`, `{we_base}_origin`) VALUES (?, ?, ?, ?, ?);",
            (work_id, expression_id, 0, 1, "test"),
        )
        cur.execute(
            f"INSERT INTO `{em_table}` (`{em_base}_expression_id`, `{em_base}_manifestation_id`, `{em_base}_priority`, `{em_base}_primary`, `{em_base}_origin`) VALUES (?, ?, ?, ?, ?);",
            (expression_id, manifestation_id, 0, 1, "test"),
        )

        # Item + store + file
        cur.execute("INSERT INTO items (item_manifestation_id, item_type) VALUES (?, ?);", (manifestation_id, "digital"))
        item_id = cur.lastrowid

        cur.execute(
            "INSERT INTO stores (store_name, store_kind, store_root_uri) VALUES (?, ?, ?);",
            ("test-store", "filesystem", "file:///vault"),
        )
        store_id = cur.lastrowid

        digital_asset_id, file_id = _insert_atomic_asset_bundle(
            cur,
            item_id=item_id,
            store_id=store_id,
            storage_key="books/dune.epub",
            link_type="primary_payload",
            size_bytes=100,
            name="dune.epub",
        )

        # Workflow step
        cur.execute(
            "INSERT INTO workflow_steps (workflow_step_code, workflow_step_label, workflow_step_group, workflow_step_scope) VALUES (?, ?, ?, ?);",
            ("ingest", "Ingest", "ingest", "both"),
        )
        step_id = cur.lastrowid

        # Events
        cur.execute(
            "INSERT INTO digital_asset_workflow_events (digital_asset_workflow_event_digital_asset_id, digital_asset_workflow_event_step_id, digital_asset_workflow_event_from_status, digital_asset_workflow_event_to_status, digital_asset_workflow_event_actor, digital_asset_workflow_event_tool, digital_asset_workflow_event_run_id, digital_asset_workflow_event_note) VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
            (digital_asset_id, step_id, "todo", "done", "tester", "unit", "run-1", "file ok"),
        )

        cur.execute(
            "INSERT INTO item_workflow_events (item_workflow_event_item_id, item_workflow_event_step_id, item_workflow_event_from_status, item_workflow_event_to_status, item_workflow_event_actor, item_workflow_event_tool, item_workflow_event_run_id, item_workflow_event_note) VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
            (item_id, step_id, "todo", "done", "tester", "unit", "run-1", "item ok"),
        )

        conn.commit()

        ray_id = f"{work_id}:{expression_id}:{manifestation_id}"

        rows = conn.execute(
            "SELECT audit_scope, book_id, ray_id, item_id, file_id, step_code, from_status, to_status, actor, tool, run_id, note, file_uri "
            "FROM ingest_audit_v WHERE ray_id = ? ORDER BY audit_scope;",
            (ray_id,),
        ).fetchall()

        assert rows == [
            ("digital_asset", ray_id, ray_id, item_id, digital_asset_id, "ingest", "todo", "done", "tester", "unit", "run-1", "file ok", "file:///vault/books/dune.epub"),
            ("item", ray_id, ray_id, item_id, None, "ingest", "todo", "done", "tester", "unit", "run-1", "item ok", None),
        ]

        # Alias should match.
        rows2 = conn.execute(
            "SELECT audit_scope, step_code FROM ingest_audit ORDER BY audit_scope;"
        ).fetchall()
        assert rows2 == [("digital_asset", "ingest"), ("item", "ingest")]

    finally:
        conn.close()


def test_books_v_compatibility_projection(tmp_path: pathlib.Path) -> None:
    """Book-ish: the FRBR DB should expose a `books`-shaped view (per ray)."""

    db_path = tmp_path / "frbr_books_view.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        frbr_gen.create_new_database(conn)

        cur = conn.cursor()

        # Work / Expression / Manifestation
        cur.execute(
            "INSERT INTO works (work_title, work_canonical_title, work_sort_title, work_original_year) VALUES (?, ?, ?, ?);",
            ("Dune", "Dune", "Dune", 1965),
        )
        work_id = cur.lastrowid

        cur.execute("INSERT INTO expressions (expression_label, expression_year) VALUES (?, ?);", ("1987 revised text", 1987))
        expression_id = cur.lastrowid

        cur.execute(
            "INSERT INTO manifestations (manifestation_edition_statement, manifestation_format_detail, manifestation_pub_year) VALUES (?, ?, ?);",
            ("Printed by Holder", "EPUB", 1988),
        )
        manifestation_id = cur.lastrowid

        we_table, we_base = ColumnNameMixin.get_interlink_table_name("works", "expressions")
        em_table, em_base = ColumnNameMixin.get_interlink_table_name("expressions", "manifestations")

        cur.execute(
            f"INSERT INTO `{we_table}` (`{we_base}_work_id`, `{we_base}_expression_id`, `{we_base}_priority`, `{we_base}_primary`, `{we_base}_origin`) VALUES (?, ?, ?, ?, ?);",
            (work_id, expression_id, 0, 1, "test"),
        )
        cur.execute(
            f"INSERT INTO `{em_table}` (`{em_base}_expression_id`, `{em_base}_manifestation_id`, `{em_base}_priority`, `{em_base}_primary`, `{em_base}_origin`) VALUES (?, ?, ?, ?, ?);",
            (expression_id, manifestation_id, 0, 1, "test"),
        )

        # Add an item + a couple of files so book_size/cover can be derived.
        cur.execute("INSERT INTO items (item_manifestation_id, item_type) VALUES (?, ?);", (manifestation_id, "digital"))
        item_id = cur.lastrowid

        cur.execute("INSERT INTO stores (store_name, store_kind) VALUES (?, ?);", ("test-store", "filesystem"))
        store_id = cur.lastrowid

        _insert_atomic_asset_bundle(
            cur,
            item_id=item_id,
            store_id=store_id,
            storage_key="dune.epub",
            link_type="primary_payload",
            size_bytes=100,
            name="dune.epub",
        )
        _insert_atomic_asset_bundle(
            cur,
            item_id=item_id,
            store_id=store_id,
            storage_key="cover.jpg",
            link_type="cover",
            media_category="cover",
            size_bytes=10,
            name="cover.jpg",
        )

        conn.commit()

        ray_id = f"{work_id}:{expression_id}:{manifestation_id}"

        row = conn.execute(
            "SELECT book_id, book_pubdate, book_copyright_date, book_has_cover, book_size, book_work_id, book_expression_id, book_manifestation_id "
            "FROM books_v WHERE book_id = ?;",
            (ray_id,),
        ).fetchone()

        assert row is not None
        (
            book_id,
            pub_date,
            cr_date,
            has_cover,
            size_bytes,
            work_id_2,
            expression_id_2,
            manifestation_id_2,
        ) = row

        assert book_id == ray_id
        assert pub_date == "1988-01-01"
        assert cr_date == "1965-01-01"
        assert has_cover == 1
        assert size_bytes == 110
        assert work_id_2 == work_id
        assert expression_id_2 == expression_id
        assert manifestation_id_2 == manifestation_id

        # Alias view should mirror books_v.
        row2 = conn.execute("SELECT book_id, book_size FROM books WHERE book_id = ?;", (ray_id,)).fetchone()
        assert row2 is not None
        assert row2[0] == book_id
        assert row2[1] == size_bytes

    finally:
        conn.close()



def test_books_v_and_inventory_expand_composite_item_assets(tmp_path: pathlib.Path) -> None:
    """Composite item links should expand through to atomic asset inventory and book size."""

    db_path = tmp_path / "frbr_composite_inventory_view.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        frbr_gen.create_new_database(conn)

        cur = conn.cursor()
        cur.execute("INSERT INTO works (work_title, work_canonical_title) VALUES (?, ?);", ("Dune Audio", "Dune Audio"))
        work_id = cur.lastrowid
        cur.execute("INSERT INTO expressions (expression_label, expression_year) VALUES (?, ?);", ("audio", 1987))
        expression_id = cur.lastrowid
        cur.execute("INSERT INTO manifestations (manifestation_edition_statement, manifestation_pub_year) VALUES (?, ?);", ("Audio release", 1988))
        manifestation_id = cur.lastrowid

        we_table, we_base = ColumnNameMixin.get_interlink_table_name("works", "expressions")
        em_table, em_base = ColumnNameMixin.get_interlink_table_name("expressions", "manifestations")
        cur.execute(
            f"INSERT INTO `{we_table}` (`{we_base}_work_id`, `{we_base}_expression_id`, `{we_base}_priority`, `{we_base}_primary`, `{we_base}_origin`) VALUES (?, ?, ?, ?, ?);",
            (work_id, expression_id, 0, 1, "test"),
        )
        cur.execute(
            f"INSERT INTO `{em_table}` (`{em_base}_expression_id`, `{em_base}_manifestation_id`, `{em_base}_priority`, `{em_base}_primary`, `{em_base}_origin`) VALUES (?, ?, ?, ?, ?);",
            (expression_id, manifestation_id, 0, 1, "test"),
        )

        cur.execute("INSERT INTO items (item_manifestation_id, item_type) VALUES (?, ?);", (manifestation_id, "digital"))
        item_id = int(cur.lastrowid)
        cur.execute("INSERT INTO stores (store_name, store_kind, store_root_uri) VALUES (?, ?, ?);", ("archive", "filesystem", "file:///vault"))
        store_id = int(cur.lastrowid)

        chapter1_id, replica1_id = _insert_atomic_asset_bundle(
            cur, item_id=item_id, store_id=store_id, storage_key="audio/ch1.mp3", link_type="supplement", size_bytes=50, name="ch1.mp3"
        )
        chapter2_id, replica2_id = _insert_atomic_asset_bundle(
            cur, item_id=item_id, store_id=store_id, storage_key="audio/ch2.mp3", link_type="supplement", size_bytes=70, name="ch2.mp3"
        )
        # These are members of the composite; remove direct item links so we exercise the composite path only.
        cur.execute("DELETE FROM digital_asset_item_links WHERE digital_asset_item_link_digital_asset_id IN (?, ?);", (chapter1_id, chapter2_id))

        composite_id = _insert_composite_asset_bundle(
            cur,
            item_id=item_id,
            composite_name="Dune audiobook",
            member_asset_ids=[chapter1_id, chapter2_id],
        )

        conn.commit()

        ray_id = f"{work_id}:{expression_id}:{manifestation_id}"
        row = conn.execute("SELECT book_size FROM books_v WHERE book_id = ?;", (ray_id,)).fetchone()
        assert row is not None
        assert row[0] == 120

        rows = conn.execute(
            "SELECT digital_asset_id, asset_replica_id, composite_digital_asset_id, digital_asset_attachment_scope, composite_member_sequence_number FROM digital_asset_inventory_v WHERE ray_id = ? ORDER BY composite_member_sequence_number;",
            (ray_id,),
        ).fetchall()
        assert rows == [
            (chapter1_id, replica1_id, composite_id, "composite_member", 1),
            (chapter2_id, replica2_id, composite_id, "composite_member", 2),
        ]
    finally:
        conn.close()

def test_wemi_rays_v_projects_expected_fields(tmp_path: pathlib.Path) -> None:
    """Insert a minimal WEMI chain and confirm the ray view returns one row."""

    db_path = tmp_path / "frbr_views_roundtrip.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        frbr_gen.create_new_database(conn)

        # Create one Work / Expression / Manifestation.
        cur = conn.cursor()
        cur.execute("INSERT INTO works (work_title, work_canonical_title) VALUES (?, ?);", ("Dune", "Dune"))
        work_id = cur.lastrowid

        cur.execute(
            "INSERT INTO expressions (expression_label, expression_year) VALUES (?, ?);",
            ("1987 revised text", 1987),
        )
        expression_id = cur.lastrowid

        cur.execute(
            "INSERT INTO manifestations (manifestation_edition_statement, manifestation_format_detail, manifestation_pub_year) VALUES (?, ?, ?);",
            ("Printed by Holder", "EPUB", 1988),
        )
        manifestation_id = cur.lastrowid

        # Link them.
        we_table, we_base = ColumnNameMixin.get_interlink_table_name("works", "expressions")
        em_table, em_base = ColumnNameMixin.get_interlink_table_name("expressions", "manifestations")

        cur.execute(
            f"INSERT INTO `{we_table}` (`{we_base}_work_id`, `{we_base}_expression_id`, `{we_base}_priority`, `{we_base}_primary`, `{we_base}_origin`) VALUES (?, ?, ?, ?, ?);",
            (work_id, expression_id, 0, 1, "test"),
        )
        cur.execute(
            f"INSERT INTO `{em_table}` (`{em_base}_expression_id`, `{em_base}_manifestation_id`, `{em_base}_priority`, `{em_base}_primary`, `{em_base}_origin`) VALUES (?, ?, ?, ?, ?);",
            (expression_id, manifestation_id, 0, 1, "test"),
        )

        conn.commit()

        row = conn.execute(
            "SELECT ray_id, display_work_title, display_expression_bit, display_manifestation_bit, calibre_like_title "
            "FROM wemi_rays_v;"
        ).fetchone()

        assert row is not None
        ray_id, work_title, expr_bit, man_bit, calibre_like = row

        assert ray_id == f"{work_id}:{expression_id}:{manifestation_id}"
        assert work_title == "Dune"
        assert expr_bit in {"1987 revised text", "1987"}
        assert "Printed by Holder" in (man_bit or "")

        # Combined title should be human-friendly and include the key components.
        assert "Dune" in calibre_like
        assert "1987" in calibre_like
        assert "Printed by Holder" in calibre_like

        # Primary rays view should select this row.
        primary = conn.execute("SELECT ray_id FROM wemi_primary_rays_v WHERE work_id = ?;", (work_id,)).fetchone()
        assert primary is not None
        assert primary[0] == ray_id

    finally:
        conn.close()


def test_titles_v_compatibility_projection(tmp_path: pathlib.Path) -> None:
    """Legacy-compat: the FRBR DB should expose a `titles`-shaped view."""

    db_path = tmp_path / "frbr_titles_view.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        frbr_gen.create_new_database(conn)

        cur = conn.cursor()

        # Work (legacy title row)
        cur.execute(
            "INSERT INTO works (work_title, work_canonical_title, work_sort_title, work_original_year, work_discovery_note) "
            "VALUES (?, ?, ?, ?, ?);",
            ("Dune", "Dune", "Dune", 1965, "test_import"),
        )
        work_id = cur.lastrowid

        # Minimal ray, to populate pub year from primary manifestation.
        cur.execute("INSERT INTO expressions (expression_label, expression_year) VALUES (?, ?);", ("1987 revised text", 1987))
        expression_id = cur.lastrowid
        cur.execute(
            "INSERT INTO manifestations (manifestation_edition_statement, manifestation_format_detail, manifestation_pub_year) "
            "VALUES (?, ?, ?);",
            ("Printed by Holder", "EPUB", 1988),
        )
        manifestation_id = cur.lastrowid

        we_table, we_base = ColumnNameMixin.get_interlink_table_name("works", "expressions")
        em_table, em_base = ColumnNameMixin.get_interlink_table_name("expressions", "manifestations")

        cur.execute(
            f"INSERT INTO `{we_table}` (`{we_base}_work_id`, `{we_base}_expression_id`, `{we_base}_priority`, `{we_base}_primary`, `{we_base}_origin`) "
            "VALUES (?, ?, ?, ?, ?);",
            (work_id, expression_id, 0, 1, "test"),
        )
        cur.execute(
            f"INSERT INTO `{em_table}` (`{em_base}_expression_id`, `{em_base}_manifestation_id`, `{em_base}_priority`, `{em_base}_primary`, `{em_base}_origin`) "
            "VALUES (?, ?, ?, ?, ?);",
            (expression_id, manifestation_id, 0, 1, "test"),
        )

        conn.commit()

        row = conn.execute(
            "SELECT title_id, title, title_sort, title_pub_date, title_copyright_date, title_source "
            "FROM titles_v WHERE title_id = ?;",
            (work_id,),
        ).fetchone()

        assert row is not None
        title_id, title, title_sort, pub_date, cr_date, source = row

        assert title_id == work_id
        assert title == "Dune"
        assert title_sort == "Dune"

        # Prefer manifestation pub year when a primary ray exists.
        assert pub_date == "1988-01-01"
        assert cr_date == "1965-01-01"

        # Prefer link-origin, else discovery note.
        assert source in {"test", "test_import"}

        # Alias view should mirror titles_v.
        row2 = conn.execute(
            "SELECT title_id, title, title_sort, title_pub_date FROM titles WHERE title_id = ?;",
            (work_id,),
        ).fetchone()
        assert row2 is not None
        assert row2[0] == title_id
        assert row2[1] == title
        assert row2[2] == title_sort
        assert row2[3] == pub_date

    finally:
        conn.close()


def test_agent_credits_v_flattens_credits_per_ray(tmp_path: pathlib.Path) -> None:
    """UI helper: credits flattened onto rays, so book-ish screens can show authors/roles easily."""

    db_path = tmp_path / "frbr_agent_credits_view.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        frbr_gen.create_new_database(conn)

        cur = conn.cursor()

        # Minimal ray.
        cur.execute("INSERT INTO works (work_title, work_canonical_title) VALUES (?, ?);", ("Dune", "Dune"))
        work_id = cur.lastrowid

        cur.execute("INSERT INTO expressions (expression_label, expression_year) VALUES (?, ?);", ("1987 revised text", 1987))
        expression_id = cur.lastrowid

        cur.execute(
            "INSERT INTO manifestations (manifestation_edition_statement, manifestation_format_detail, manifestation_pub_year) VALUES (?, ?, ?);",
            ("Printed by Holder", "EPUB", 1988),
        )
        manifestation_id = cur.lastrowid

        we_table, we_base = ColumnNameMixin.get_interlink_table_name("works", "expressions")
        em_table, em_base = ColumnNameMixin.get_interlink_table_name("expressions", "manifestations")

        cur.execute(
            f"INSERT INTO `{we_table}` (`{we_base}_work_id`, `{we_base}_expression_id`, `{we_base}_priority`, `{we_base}_primary`, `{we_base}_origin`) VALUES (?, ?, ?, ?, ?);",
            (work_id, expression_id, 0, 1, "test"),
        )
        cur.execute(
            f"INSERT INTO `{em_table}` (`{em_base}_expression_id`, `{em_base}_manifestation_id`, `{em_base}_priority`, `{em_base}_primary`, `{em_base}_origin`) VALUES (?, ?, ?, ?, ?);",
            (expression_id, manifestation_id, 0, 1, "test"),
        )

        # Agents
        cur.execute(
            "INSERT INTO agents (agent_type, agent_canonical_name, agent_sort_name) VALUES (?, ?, ?);",
            ("person", "Frank Herbert", "Herbert, Frank"),
        )
        author_agent_id = cur.lastrowid

        cur.execute(
            "INSERT INTO agents (agent_type, agent_canonical_name, agent_sort_name) VALUES (?, ?, ?);",
            ("person", "John Translator", "Translator, John"),
        )
        translator_agent_id = cur.lastrowid

        # Credits. In this schema the credit kind is the link `*_type` (MARC relator code).
        cur.execute(
            "INSERT INTO agent_work_links (agent_work_link_agent_id, agent_work_link_work_id, agent_work_link_priority, agent_work_link_type) VALUES (?, ?, ?, ?);",
            (author_agent_id, work_id, 0, "aut"),
        )
        cur.execute(
            "INSERT INTO agent_expression_links (agent_expression_link_agent_id, agent_expression_link_expression_id, agent_expression_link_priority, agent_expression_link_type, agent_expression_link_origin) VALUES (?, ?, ?, ?, ?);",
            (translator_agent_id, expression_id, 0, "trl", "test"),
        )

        conn.commit()

        ray_id = f"{work_id}:{expression_id}:{manifestation_id}"

        rows = conn.execute(
            "SELECT book_id, credit_entity_type, credit_type, agent_canonical_name, credit_scope_rank "
            "FROM agent_credits_v WHERE book_id = ? ORDER BY credit_scope_rank, agent_canonical_name;",
            (ray_id,),
        ).fetchall()

        assert rows == [
            (ray_id, "work", "aut", "Frank Herbert", 1),
            (ray_id, "expression", "trl", "John Translator", 2),
        ]

    finally:
        conn.close()


def test_publishers_v_selects_best_publisher_per_book(tmp_path: pathlib.Path) -> None:
    """Publisher projection: select a deterministic publisher (MARC relator 'pbl') per ray."""

    db_path = tmp_path / "frbr_publishers_view.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        frbr_gen.create_new_database(conn)

        cur = conn.cursor()

        # Minimal ray.
        cur.execute("INSERT INTO works (work_title, work_canonical_title) VALUES (?, ?);", ("Dune", "Dune"))
        work_id = cur.lastrowid

        cur.execute("INSERT INTO expressions (expression_label, expression_year) VALUES (?, ?);", ("rev", 1987))
        expression_id = cur.lastrowid

        cur.execute(
            "INSERT INTO manifestations (manifestation_edition_statement, manifestation_pub_year) VALUES (?, ?);",
            ("First print", 1988),
        )
        manifestation_id = cur.lastrowid

        we_table, we_base = ColumnNameMixin.get_interlink_table_name("works", "expressions")
        em_table, em_base = ColumnNameMixin.get_interlink_table_name("expressions", "manifestations")

        cur.execute(
            f"INSERT INTO `{we_table}` (`{we_base}_work_id`, `{we_base}_expression_id`, `{we_base}_priority`, `{we_base}_primary`, `{we_base}_origin`) VALUES (?, ?, ?, ?, ?);",
            (work_id, expression_id, 0, 1, "test"),
        )
        cur.execute(
            f"INSERT INTO `{em_table}` (`{em_base}_expression_id`, `{em_base}_manifestation_id`, `{em_base}_priority`, `{em_base}_primary`, `{em_base}_origin`) VALUES (?, ?, ?, ?, ?);",
            (expression_id, manifestation_id, 0, 1, "test"),
        )

        # Publisher agent.
        cur.execute(
            "INSERT INTO agents (agent_type, agent_canonical_name, agent_sort_name) VALUES (?, ?, ?);",
            ("organisation", "Ace Books", "Ace Books"),
        )
        pub_agent_id = cur.lastrowid

        # Manifestation-scoped publisher credit ('pbl').
        cur.execute(
            "INSERT INTO agent_manifestation_links (agent_manifestation_link_agent_id, agent_manifestation_link_manifestation_id, agent_manifestation_link_priority, agent_manifestation_link_type) VALUES (?, ?, ?, ?);",
            (pub_agent_id, manifestation_id, 0, "pbl"),
        )

        conn.commit()

        ray_id = f"{work_id}:{expression_id}:{manifestation_id}"

        row = conn.execute(
            "SELECT book_id, publisher_name, publisher_agent_type, publisher_scope FROM publishers_v WHERE book_id = ?;",
            (ray_id,),
        ).fetchone()

        assert row == (ray_id, "Ace Books", "organisation", "manifestation")

        # The non-deduped view should include the same publisher credit.
        rows = conn.execute(
            "SELECT publisher_name, publisher_credit_type FROM book_publishers_v WHERE book_id = ? ORDER BY publisher_name;",
            (ray_id,),
        ).fetchall()
        assert rows == [("Ace Books", "pbl")]

    finally:
        conn.close()

def test_subjects_tags_v_unifies_subjects_genres_and_tags(tmp_path: pathlib.Path) -> None:
    """Facets: subject/genre/tag projections should appear per book(ray)."""

    db_path = tmp_path / "frbr_subjects_tags_view.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        frbr_gen.create_new_database(conn)

        cur = conn.cursor()

        # Work / Expression / Manifestation
        cur.execute(
            "INSERT INTO works (work_title, work_canonical_title) VALUES (?, ?);",
            ("Dune", "Dune"),
        )
        work_id = cur.lastrowid

        cur.execute(
            "INSERT INTO expressions (expression_label, expression_year) VALUES (?, ?);",
            ("rev", 1987),
        )
        expression_id = cur.lastrowid

        cur.execute(
            "INSERT INTO manifestations (manifestation_edition_statement, manifestation_pub_year) VALUES (?, ?);",
            ("First print", 1988),
        )
        manifestation_id = cur.lastrowid

        we_table, we_base = ColumnNameMixin.get_interlink_table_name("works", "expressions")
        em_table, em_base = ColumnNameMixin.get_interlink_table_name("expressions", "manifestations")
        cur.execute(
            f"INSERT INTO `{we_table}` (`{we_base}_work_id`, `{we_base}_expression_id`, `{we_base}_priority`, `{we_base}_primary`, `{we_base}_origin`) "
            f"VALUES (?, ?, ?, ?, ?);",
            (work_id, expression_id, 0, 1, "test"),
        )
        cur.execute(
            f"INSERT INTO `{em_table}` (`{em_base}_expression_id`, `{em_base}_manifestation_id`, `{em_base}_priority`, `{em_base}_primary`, `{em_base}_origin`) "
            f"VALUES (?, ?, ?, ?, ?);",
            (expression_id, manifestation_id, 0, 1, "test"),
        )

        # Item
        cur.execute(
            "INSERT INTO items (item_manifestation_id, item_type) VALUES (?, ?);",
            (manifestation_id, "digital"),
        )
        item_id = cur.lastrowid

        # Subject
        cur.execute(
            "INSERT INTO subjects (subject, subject_sort, subject_full) VALUES (?, ?, ?);",
            ("Science Fiction", "science fiction", "Fiction > Science Fiction"),
        )
        subject_id = cur.lastrowid
        sw_table, sw_base = ColumnNameMixin.get_interlink_table_name("subjects", "works")
        cur.execute(
            f"INSERT INTO `{sw_table}` (`{sw_base}_subject_id`, `{sw_base}_work_id`, `{sw_base}_priority`) VALUES (?, ?, ?);",
            (subject_id, work_id, 0),
        )

        # Genre
        cur.execute(
            "INSERT INTO genres (genre, genre_sort, genre_full) VALUES (?, ?, ?);",
            ("SF", "sf", "Science Fiction"),
        )
        genre_id = cur.lastrowid
        gw_table, gw_base = ColumnNameMixin.get_interlink_table_name("genres", "works")
        cur.execute(
            f"INSERT INTO `{gw_table}` (`{gw_base}_genre_id`, `{gw_base}_work_id`, `{gw_base}_priority`, `{gw_base}_type`) VALUES (?, ?, ?, ?);",
            (genre_id, work_id, 0, "primary"),
        )

        # Tags (work/expression/item scopes)
        cur.execute(
            "INSERT INTO tags (tag, tag_phash, tag_description) VALUES (?, ?, ?);",
            ("classic", "classic", None),
        )
        tag_work_id = cur.lastrowid

        cur.execute(
            "INSERT INTO tags (tag, tag_phash, tag_description) VALUES (?, ?, ?);",
            ("translated", "translated", None),
        )
        tag_expr_id = cur.lastrowid

        cur.execute(
            "INSERT INTO tags (tag, tag_phash, tag_description) VALUES (?, ?, ?);",
            ("ocr", "ocr", None),
        )
        tag_item_id = cur.lastrowid

        lw_table, lw_base = ColumnNameMixin.get_interlink_table_name("tags", "works")
        cur.execute(
            f"INSERT INTO `{lw_table}` (`{lw_base}_tag_id`, `{lw_base}_work_id`, `{lw_base}_priority`) VALUES (?, ?, ?);",
            (tag_work_id, work_id, 0),
        )

        el_table, el_base = ColumnNameMixin.get_interlink_table_name("expressions", "tags")
        cur.execute(
            f"INSERT INTO `{el_table}` (`{el_base}_expression_id`, `{el_base}_tag_id`, `{el_base}_priority`) VALUES (?, ?, ?);",
            (expression_id, tag_expr_id, 0),
        )

        il_table, il_base = ColumnNameMixin.get_interlink_table_name("items", "tags")
        cur.execute(
            f"INSERT INTO `{il_table}` (`{il_base}_item_id`, `{il_base}_tag_id`, `{il_base}_priority`) VALUES (?, ?, ?);",
            (item_id, tag_item_id, 0),
        )

        conn.commit()

        ray_id = f"{work_id}:{expression_id}:{manifestation_id}"

        rows = conn.execute(
            "SELECT facet_kind, facet_scope, facet_text "
            "FROM subjects_tags_v "
            "WHERE book_id = ? "
            "ORDER BY facet_kind, facet_scope, facet_text;",
            (ray_id,),
        ).fetchall()

        assert rows == [
            ("genre", "work", "Science Fiction"),
            ("subject", "work", "Fiction > Science Fiction"),
            ("tag", "expression", "translated"),
            ("tag", "item", "ocr"),
            ("tag", "work", "classic"),
        ]

    finally:
        conn.close()
def test_duplicate_candidates_v_groups_isbn_and_title_author_year(tmp_path: pathlib.Path) -> None:
    """Dedup helper: should emit candidate groups for obvious ISBN matches and TYA matches."""

    db_path = tmp_path / "frbr_duplicate_candidates_view.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        frbr_gen.create_new_database(conn)
        cur = conn.cursor()

        # Two distinct rays with the same ISBN on the manifestation.
        def make_ray(work_title: str, expr_label: str, pub_year: int, isbn: str) -> tuple[int, int, int, str]:
            cur.execute("INSERT INTO works (work_title, work_canonical_title) VALUES (?, ?);", (work_title, work_title))
            work_id = cur.lastrowid
            cur.execute("INSERT INTO expressions (expression_label, expression_year) VALUES (?, ?);", (expr_label, pub_year))
            expression_id = cur.lastrowid
            cur.execute(
                "INSERT INTO manifestations (manifestation_edition_statement, manifestation_pub_year) VALUES (?, ?);",
                ("First print", pub_year),
            )
            manifestation_id = cur.lastrowid

            we_table, we_base = ColumnNameMixin.get_interlink_table_name("works", "expressions")
            em_table, em_base = ColumnNameMixin.get_interlink_table_name("expressions", "manifestations")
            cur.execute(
                f"INSERT INTO `{we_table}` (`{we_base}_work_id`, `{we_base}_expression_id`, `{we_base}_priority`, `{we_base}_primary`, `{we_base}_origin`) VALUES (?, ?, ?, ?, ?);",
                (work_id, expression_id, 0, 1, "test"),
            )
            cur.execute(
                f"INSERT INTO `{em_table}` (`{em_base}_expression_id`, `{em_base}_manifestation_id`, `{em_base}_priority`, `{em_base}_primary`, `{em_base}_origin`) VALUES (?, ?, ?, ?, ?);",
                (expression_id, manifestation_id, 0, 1, "test"),
            )

            # Curated identifier attached to manifestation (picked up by duplicate_candidates_v).
            cur.execute(
                "INSERT INTO entity_identifiers (entity_identifier_entity_type, entity_identifier_entity_id, entity_identifier_scheme, entity_identifier_value, entity_identifier_is_primary, entity_identifier_provenance) "
                "VALUES (?, ?, ?, ?, ?, ?);",
                ("manifestation", manifestation_id, "isbn_13", isbn, 1, "import"),
            )

            ray_id = f"{work_id}:{expression_id}:{manifestation_id}"
            return work_id, expression_id, manifestation_id, ray_id

        _, _, _, ray_a = make_ray("Dune", "rev", 1988, "978-0441172719")
        _, _, _, ray_b = make_ray("Dune", "rev2", 1988, "9780441172719")

        # Two more rays with same title+author+year but no ISBN.

        def make_tya_ray(work_title: str, year: int) -> str:
            cur.execute("INSERT INTO works (work_title, work_canonical_title, work_original_year) VALUES (?, ?, ?);", (work_title, work_title, year))
            work_id = cur.lastrowid
            cur.execute("INSERT INTO expressions (expression_label, expression_year, expression_title_override) VALUES (?, ?, ?);", ("orig", year, "Foundation"))
            expression_id = cur.lastrowid
            cur.execute("INSERT INTO manifestations (manifestation_edition_statement, manifestation_pub_year) VALUES (?, ?);", ("First print", year))
            manifestation_id = cur.lastrowid

            we_table, we_base = ColumnNameMixin.get_interlink_table_name("works", "expressions")
            em_table, em_base = ColumnNameMixin.get_interlink_table_name("expressions", "manifestations")
            cur.execute(
                f"INSERT INTO `{we_table}` (`{we_base}_work_id`, `{we_base}_expression_id`, `{we_base}_priority`, `{we_base}_primary`, `{we_base}_origin`) VALUES (?, ?, ?, ?, ?);",
                (work_id, expression_id, 0, 1, "test"),
            )
            cur.execute(
                f"INSERT INTO `{em_table}` (`{em_base}_expression_id`, `{em_base}_manifestation_id`, `{em_base}_priority`, `{em_base}_primary`, `{em_base}_origin`) VALUES (?, ?, ?, ?, ?);",
                (expression_id, manifestation_id, 0, 1, "test"),
            )

            # Author at work scope.
            cur.execute(
                "INSERT INTO agents (agent_type, agent_canonical_name, agent_sort_name) VALUES (?, ?, ?);",
                ("person", "Isaac Asimov", "Asimov, Isaac"),
            )
            author_agent_id = cur.lastrowid

            cur.execute(
                "INSERT INTO agent_work_links (agent_work_link_agent_id, agent_work_link_work_id, agent_work_link_priority, agent_work_link_type) VALUES (?, ?, ?, ?);",
                (author_agent_id, work_id, work_id, "aut"),
            )

            return f"{work_id}:{expression_id}:{manifestation_id}"

        ray_c = make_tya_ray("Foundation A", 1951)
        ray_d = make_tya_ray("Foundation B", 1951)

        conn.commit()

        # ISBN candidate should exist and include both rays.
        row = conn.execute(
            "SELECT candidate_key, member_count, book_ids_csv "
            "FROM duplicate_candidates_v "
            "WHERE candidate_kind = 'isbn' AND candidate_key = 'ISBN:9780441172719';"
        ).fetchone()
        assert row is not None
        assert row[1] == 2
        assert ray_a in row[2]
        assert ray_b in row[2]

        # TYA candidate should exist for the two Foundation rays.
        rows = conn.execute(
            "SELECT candidate_key, member_count, book_ids_csv "
            "FROM duplicate_candidates_v "
            "WHERE candidate_kind = 'title_author_year';"
        ).fetchall()
        assert any((r[1] == 2 and ray_c in r[2] and ray_d in r[2]) for r in rows)

    finally:
        conn.close()


def test_search_seed_v_produces_seed_text(tmp_path: pathlib.Path) -> None:
    """Search helper: seed row should contain title/authors/publisher/identifiers in a single text field."""

    db_path = tmp_path / "frbr_search_seed_view.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        frbr_gen.create_new_database(conn)
        cur = conn.cursor()

        # Minimal ray
        cur.execute("INSERT INTO works (work_title, work_canonical_title) VALUES (?, ?);", ("Dune", "Dune"))
        work_id = cur.lastrowid
        cur.execute("INSERT INTO expressions (expression_label, expression_year) VALUES (?, ?);", ("rev", 1987))
        expression_id = cur.lastrowid
        cur.execute(
            "INSERT INTO manifestations (manifestation_edition_statement, manifestation_pub_year) VALUES (?, ?);",
            ("First print", 1988),
        )
        manifestation_id = cur.lastrowid

        we_table, we_base = ColumnNameMixin.get_interlink_table_name("works", "expressions")
        em_table, em_base = ColumnNameMixin.get_interlink_table_name("expressions", "manifestations")
        cur.execute(
            f"INSERT INTO `{we_table}` (`{we_base}_work_id`, `{we_base}_expression_id`, `{we_base}_priority`, `{we_base}_primary`, `{we_base}_origin`) VALUES (?, ?, ?, ?, ?);",
            (work_id, expression_id, 0, 1, "test"),
        )
        cur.execute(
            f"INSERT INTO `{em_table}` (`{em_base}_expression_id`, `{em_base}_manifestation_id`, `{em_base}_priority`, `{em_base}_primary`, `{em_base}_origin`) VALUES (?, ?, ?, ?, ?);",
            (expression_id, manifestation_id, 0, 1, "test"),
        )

        # Author + publisher agents
        cur.execute("INSERT INTO agents (agent_type, agent_canonical_name, agent_sort_name) VALUES (?, ?, ?);", ("person", "Frank Herbert", "Herbert, Frank"))
        author_id = cur.lastrowid
        cur.execute("INSERT INTO agents (agent_type, agent_canonical_name, agent_sort_name) VALUES (?, ?, ?);", ("organisation", "Ace Books", "Ace Books"))
        publisher_id = cur.lastrowid

        cur.execute(
            "INSERT INTO agent_work_links (agent_work_link_agent_id, agent_work_link_work_id, agent_work_link_priority, agent_work_link_type) VALUES (?, ?, ?, ?);",
            (author_id, work_id, 0, "aut"),
        )
        cur.execute(
            "INSERT INTO agent_manifestation_links (agent_manifestation_link_agent_id, agent_manifestation_link_manifestation_id, agent_manifestation_link_priority, agent_manifestation_link_type) VALUES (?, ?, ?, ?);",
            (publisher_id, manifestation_id, 0, "pbl"),
        )

        # Identifier
        cur.execute(
            "INSERT INTO entity_identifiers (entity_identifier_entity_type, entity_identifier_entity_id, entity_identifier_scheme, entity_identifier_value, entity_identifier_is_primary, entity_identifier_provenance) "
            "VALUES (?, ?, ?, ?, ?, ?);",
            ("manifestation", manifestation_id, "isbn_13", "978-0441172719", 1, "import"),
        )

        conn.commit()

        ray_id = f"{work_id}:{expression_id}:{manifestation_id}"

        seed = conn.execute(
            "SELECT seed_title, seed_authors, seed_publisher, seed_identifiers, seed_text "
            "FROM search_seed_v WHERE book_id = ?;",
            (ray_id,),
        ).fetchone()

        assert seed is not None
        seed_title, seed_authors, seed_publisher, seed_identifiers, seed_text = seed

        assert seed_title is not None and len(seed_title) > 0
        assert seed_authors == "Frank Herbert"
        assert seed_publisher == "Ace Books"
        assert "isbn_13:978-0441172719" in seed_identifiers

        # Seed text should contain the core pieces.
        assert "Frank Herbert" in seed_text
        assert "Ace Books" in seed_text
        assert "isbn_13:978-0441172719" in seed_text

    finally:
        conn.close()
