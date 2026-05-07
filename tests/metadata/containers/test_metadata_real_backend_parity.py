from __future__ import annotations

import importlib.util
import sqlite3
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from LiuXin_alpha.caches import create_storage_cache
from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.metadata.containers import (
    LazyLiuXinWEMIMetadataHydrator,
    LiuXinWEMIMetadataHydrator,
)
from LiuXin_alpha.metadata.read_sources import CacheMetadataReadSource


def _load_isfdb_builder_script() -> Any:
    script_path = Path(__file__).resolve().parents[3] / "scripts" / "build_isfdb_test_db.py"
    spec = importlib.util.spec_from_file_location("build_isfdb_test_db", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _new_stage_conn(module: Any) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    for spec in module.STAGE_SPECS.values():
        conn.execute(spec.create_sql)
    return conn


def _build_small_isfdb_backed_test_db(output_db: Path) -> None:
    module = _load_isfdb_builder_script()
    stage_conn = _new_stage_conn(module)
    try:
        stage_conn.executemany(
            module.STAGE_SPECS["authors"].insert_sql,
            [(1, "Jane Author", "Jane Author", 504, "Author", 1, "Seeded author row note")],
        )
        stage_conn.executemany(
            module.STAGE_SPECS["titles"].insert_sql,
            [
                (
                    10,
                    "Arabian Frights",
                    None,
                    None,
                    None,
                    7,
                    1,
                    "2001-01-01",
                    "NOVEL",
                    None,
                    None,
                    1,
                    None,
                    None,
                    None,
                ),
            ],
        )
        stage_conn.executemany(
            module.STAGE_SPECS["pubs"].insert_sql,
            [
                (
                    100,
                    "Arabian Frights First",
                    "First",
                    "2002-01-01",
                    5,
                    "320",
                    "NOVEL",
                    "hc",
                    "978-0-306-40615-7",
                    None,
                    None,
                    None,
                    None,
                ),
            ],
        )
        stage_conn.executemany(
            module.STAGE_SPECS["pub_content"].insert_sql,
            [(1000, 10, 100, None)],
        )
        stage_conn.executemany(
            module.STAGE_SPECS["publishers"].insert_sql,
            [(5, "Acme Press", 505)],
        )
        stage_conn.executemany(
            module.STAGE_SPECS["series"].insert_sql,
            [(7, "Shared Saga", None, None, None)],
        )
        stage_conn.executemany(
            module.STAGE_SPECS["canonical_author"].insert_sql,
            [(1, 10, 1, 1)],
        )
        stage_conn.executemany(
            module.STAGE_SPECS["languages"].insert_sql,
            [(1, "English", "en")],
        )
        stage_conn.executemany(
            module.STAGE_SPECS["identifier_types"].insert_sql,
            [
                (1, "ASIN", "Amazon Standard Identification Number"),
            ],
        )
        stage_conn.executemany(
            module.STAGE_SPECS["identifiers"].insert_sql,
            [(1, 1, "B000123456", 100)],
        )
        stage_conn.executemany(
            module.STAGE_SPECS["tags"].insert_sql,
            [
                (1, "Space Opera", 0),
                (4, "sci fi", 0),
            ],
        )
        stage_conn.executemany(
            module.STAGE_SPECS["tag_mapping"].insert_sql,
            [
                (1, 1, 10, 2),
                (2, 4, 10, 2),
            ],
        )
        stage_conn.commit()

        module._create_stage_indexes(stage_conn)
        module._materialize_selected_subset(stage_conn, max_pubs=None)
        module._build_frbr_target(stage_conn=stage_conn, output_db=output_db)
    finally:
        stage_conn.close()


def _item_id_for_pub_scratch(db_path: Path, scratch: str) -> int:
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(
            "SELECT item_id FROM items WHERE item_scratch = ?;",
            (scratch,),
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    return int(row[0])


def _metadata_values(raw: Any) -> tuple[str, ...]:
    if raw is None:
        return ()
    if isinstance(raw, Mapping):
        values = raw.keys()
    elif isinstance(raw, str):
        values = (raw,)
    else:
        try:
            values = tuple(raw)
        except TypeError:
            values = (raw,)
    return tuple(sorted(str(value) for value in values))


def _identifier_snapshot(metadata: Any) -> dict[str, tuple[str, ...]]:
    return {
        str(scheme): _metadata_values(values)
        for scheme, values in sorted(metadata.get_identifiers().items())
    }


def _metadata_snapshot(metadata: Any) -> dict[str, Any]:
    return {
        "title": metadata.display_title,
        "tags": _metadata_values(metadata.tags),
        "labels": _metadata_values(metadata.labels),
        "genres": _metadata_values(metadata.genre),
        "series": _metadata_values(metadata.series),
        "identifiers": _identifier_snapshot(metadata),
    }


def _cache_metadata(db: Any, item_id: int) -> Any:
    cache = create_storage_cache(db, "schema_backed")
    cache.read()
    cache_source = CacheMetadataReadSource(
        cache,
        database=db,
        allow_database_fallback=False,
    )
    return LiuXinWEMIMetadataHydrator(cache_source).get_liuxin_wemi_metadata(item_id=item_id)


def _lazy_metadata(db: Any, item_id: int) -> Any:
    metadata = LazyLiuXinWEMIMetadataHydrator(db).get_lazy_liuxin_wemi_metadata(
        item_id=item_id,
    )
    metadata.force_hydrate(fields=("tags", "labels", "genre", "series", "identifiers"))
    return metadata


def test_isfdb_backed_metadata_database_cache_lazy_parity_and_write_back(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "isfdb_metadata_parity.test_db"
    _build_small_isfdb_backed_test_db(db_path)
    item_id = _item_id_for_pub_scratch(db_path, "isfdb:pub:100")

    with Database(metadata={"database_path": str(db_path)}, create=False, backup=False) as db:
        database_metadata = LiuXinWEMIMetadataHydrator(db).get_liuxin_wemi_metadata(
            item_id=item_id,
        )
        cache_metadata = _cache_metadata(db, item_id)
        lazy_metadata = _lazy_metadata(db, item_id)

        database_snapshot = _metadata_snapshot(database_metadata)
        assert database_snapshot == _metadata_snapshot(cache_metadata)
        assert database_snapshot == _metadata_snapshot(lazy_metadata)
        assert set(database_snapshot["tags"]) >= {"Arabian", "Frights", "Space Opera", "sci fi"}
        assert "new_entry" in database_snapshot["labels"]
        assert set(database_snapshot["genres"]) >= {"Science Fiction", "Space Opera"}
        assert "Shared Saga" in database_snapshot["series"]
        assert database_snapshot["identifiers"]["amazon"] == ("B000123456",)
        assert database_snapshot["identifiers"]["isbn"] == ("9780306406157",)
        assert str(database_metadata) == str(cache_metadata)
        assert "Arabian Frights" in str(lazy_metadata)

        database_metadata.tags = "real-backend-contract-tag"
        database_metadata.labels = "real-backend-contract-label"
        database_metadata.genre = "Real Backend Contract Genre"
        database_metadata.series = "Real Backend Contract Series"
        database_metadata.set_identifier("doi", "10.5555/real-backend-contract")

        report = database_metadata.write_to_database(
            db,
            fields=("tags", "labels", "genre", "series", "identifiers"),
        )
        assert report.changed is True

        rehydrated = LiuXinWEMIMetadataHydrator(db).get_liuxin_wemi_metadata(
            item_id=item_id,
        )
        rehydrated_snapshot = _metadata_snapshot(rehydrated)
        assert "real-backend-contract-tag" in rehydrated_snapshot["tags"]
        assert "real-backend-contract-label" in rehydrated_snapshot["labels"]
        assert "Real Backend Contract Genre" in rehydrated_snapshot["genres"]
        assert "Real Backend Contract Series" in rehydrated_snapshot["series"]
        assert rehydrated_snapshot["identifiers"]["doi"] == (
            "10.5555/real-backend-contract",
        )
        assert rehydrated_snapshot == _metadata_snapshot(_cache_metadata(db, item_id))
        assert rehydrated_snapshot == _metadata_snapshot(_lazy_metadata(db, item_id))
