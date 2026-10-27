"""Check generated identifier discovery against a real disposable PostgreSQL schema."""

from __future__ import annotations

import pytest

from LiuXin_alpha.caches.read_cache import ReadCache
from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.errors import DatabaseIntegrityError
from tests.databases.test_database_creation_postgres_live import target as target


def test_generated_link_columns_support_full_schema_and_cache_discovery(target):
    """An existing truncated column resolves in both directions without schema repair."""
    metadata, _raw = target
    with Database(
        metadata=metadata,
        db_type="PostgreSQL",
        create=True,
        backup=False,
        repair_bootstrap_rows=False,
    ) as db:
        wrapper = db.driver_wrapper
        table = "composite_digital_asset_digital_asset_links"
        before = tuple(wrapper.get_column_headings(table))
        forward = wrapper.get_link_spec("composite_digital_assets", "digital_assets")
        reverse = wrapper.get_link_spec("digital_assets", "composite_digital_assets")
        assert forward.primary_link_col in before
        assert forward.primary_link_col == reverse.secondary_link_col
        assert forward.primary_link_col.endswith("composite_digital_as")
        assert table in wrapper.get_schema_spec().tables
        for storage_type in ("schema_backed", "numpy_vectorized"):
            cache = ReadCache(db, storage_type=storage_type)
            try:
                cache.load()
                # Loading must not leave a swallowed backend error on the shared handle.
                assert db.macros.get_rows("works") == ()
            finally:
                cache.close()
        catalog = Catalog(db)
        work = catalog.works.create({"title": "Discoverable"})
        catalog.write_one("works", "work_title", work, "Updated")
        assert catalog.works.require(work)["work_title"] == "Updated"
        assert tuple(wrapper.get_column_headings(table)) == before
        with pytest.raises(DatabaseIntegrityError, match="column_type not recognized"):
            wrapper.get_link_column(
                "composite_digital_assets", "digital_assets", "absent"
            )


def test_identifier_resolution_matches_server_bytes_and_rejects_ddl_collisions(target):
    """Byte clipping preserves Unicode; colliding generated names cannot create a table."""
    metadata, raw = target
    with Database(
        metadata=metadata,
        db_type="PostgreSQL",
        create=True,
        backup=False,
        repair_bootstrap_rows=False,
    ) as db:
        with raw.cursor() as cursor:
            cursor.execute("SHOW max_identifier_length")
            limit = int(cursor.fetchone()[0])
        prefix = "a" * (limit - 1)
        name = prefix + "é_more"
        assert db.driver._physical_identifier(name) == prefix
        assert db.driver._physical_identifier("Case_preserved") == "Case_preserved"
        with db.macros.transaction() as connection:
            connection.execute(f'CREATE TABLE "identifier_bytes" ("{name}" text)')
        assert db.driver.direct_get_column_headings("identifier_bytes") == [prefix]
        # PostgreSQL, not an application prefix guess, rejects the ambiguity.
        with pytest.raises(Exception) as error:
            with raw:
                with raw.cursor() as cursor:
                    cursor.execute(
                        f'CREATE TABLE "{metadata["schema"]}"."identifier_collision" '
                        f'("{prefix}é_first" text, "{prefix}é_second" text)'
                    )
        assert (
            getattr(error.value, "sqlstate", getattr(error.value, "pgcode", None))
            == "42701"
        )
        with raw.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
                (metadata["schema"], "identifier_collision"),
            )
            assert cursor.fetchone() is None
