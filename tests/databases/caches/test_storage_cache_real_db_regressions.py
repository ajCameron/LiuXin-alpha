from __future__ import annotations

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
