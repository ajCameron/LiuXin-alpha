from __future__ import annotations

import sqlite3

import pytest

from LiuXin_alpha.caches import create_storage_cache
from LiuXin_alpha.databases.database import Database


def test_storage_cache_schema_spec_tolerates_scratchless_helper_tables(
    provision_named_test_database,
    tmp_path,
) -> None:
    provisioned = provision_named_test_database(name="test_db_13", dst_dir=tmp_path)

    with Database(metadata={"database_path": str(provisioned.db_path)}) as db:
        conversion_options = db.driver_wrapper.get_table_spec("conversion_options")
        database_version = db.driver_wrapper.get_table_spec("database_version")

        assert conversion_options.scratch_column is None
        assert database_version.scratch_column is None

        cache = create_storage_cache(db, "numpy_vectorized", require_numpy=False)
        cache.read()

        assert cache.get_main_table("works") is not None


def test_storage_cache_catalog_writer_round_trips_through_real_database(
    provision_named_test_database,
    tmp_path,
) -> None:
    provisioned = provision_named_test_database(name="test_db_13", dst_dir=tmp_path)
    with sqlite3.connect(provisioned.db_path) as connection:
        connection.executescript(
            """
            CREATE TABLE cache_write_sources (
                cache_write_source_id INTEGER PRIMARY KEY,
                cache_write_source_title TEXT NOT NULL
            );
            CREATE TABLE cache_write_values (
                cache_write_value_id INTEGER PRIMARY KEY,
                cache_write_value_name TEXT NOT NULL UNIQUE
            );
            CREATE TABLE cache_write_source_cache_write_value_links (
                cache_write_source_cache_write_value_link_cache_write_source_id
                    INTEGER NOT NULL,
                cache_write_source_cache_write_value_link_cache_write_value_id
                    INTEGER NOT NULL,
                cache_write_source_cache_write_value_link_type TEXT,
                UNIQUE(
                    cache_write_source_cache_write_value_link_cache_write_source_id,
                    cache_write_source_cache_write_value_link_cache_write_value_id,
                    cache_write_source_cache_write_value_link_type
                ),
                FOREIGN KEY(
                    cache_write_source_cache_write_value_link_cache_write_source_id
                ) REFERENCES cache_write_sources(cache_write_source_id),
                FOREIGN KEY(
                    cache_write_source_cache_write_value_link_cache_write_value_id
                ) REFERENCES cache_write_values(cache_write_value_id)
            );
            CREATE TABLE cache_write_source_cache_write_value_links__types (
                type TEXT PRIMARY KEY
            );
            INSERT INTO cache_write_sources VALUES (1, 'before');
            INSERT INTO cache_write_source_cache_write_value_links__types
                VALUES ('author');
            """
        )

    with Database(metadata={"database_path": str(provisioned.db_path)}) as db:
        cache = create_storage_cache(db, "schema_backed")
        cache.read()

        assert cache.write_one(
            "cache_write_sources",
            "cache_write_source_title",
            1,
            "after",
        ) == {1: "after"}
        assert cache.get_cached_value(
            1,
            "cache_write_sources.cache_write_source_title",
        ) == "after"

        writer = cache.create_writer(
            "cache_write_sources",
            "cache_write_value_name",
        )
        with pytest.raises(ValueError, match="does not exist in allowed-types"):
            writer.write_one(1, "Ada", link_type="reviewer")
        assert next(
            row[0]
            for row in db.driver_wrapper.execute(
                "SELECT COUNT(*) FROM cache_write_values"
            )
        ) == 0

        rows = cache.write_one(
            "cache_write_sources",
            "cache_write_value_name",
            1,
            "Ada",
            link_type="author",
        )

        assert rows[1][0].link_type == "author"
        assert tuple(
            cache.get_field(
                "cache_write_sources.cache_write_values.cache_write_value_name"
            ).get_values_from_src_id(1)
        ) == ("Ada",)
