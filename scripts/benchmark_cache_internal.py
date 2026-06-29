#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys

from pathlib import Path
from typing import Any, Callable, Optional

from _benchmark_common import (
    DEFAULT_RESULTS_DIR,
    environment_payload,
    print_report_summary,
    run_benchmark,
    stderr_progress,
    utc_now_iso,
    write_json_report,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"

for candidate in (str(REPO_ROOT), str(SRC_ROOT)):
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from LiuXin_alpha.caches import create_storage_cache  # noqa: E402
from LiuXin_alpha.databases.schema_specs import LinkCardinality, StorageLinkSpec, StorageSchemaSpec  # noqa: E402
from tests.support.storage_cache_test_harness import CACHE_PLUGIN_KWARGS, make_fake_db, make_table  # noqa: E402


DEFAULT_SCENARIOS = (
    "load_cache",
    "read_tables_only",
    "initialize_tables_only",
    "read_fields_only",
    "initialize_fields_only",
    "scalar_get_cached_value_loop",
    "scalar_get_cached_row_values_loop",
    "relation_single_get_value_loop",
    "relation_multi_get_values_loop",
    "numpy_scalar_arrays",
    "numpy_relation_arrays",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark storage-cache internals against an in-memory synthetic fake DB."
    )
    parser.add_argument("--books", type=int, default=5000, help="Number of synthetic books.")
    parser.add_argument("--tag-pool", type=int, default=1000, help="Size of the reusable tag pool.")
    parser.add_argument("--tags-per-book", type=int, default=4, help="Synthetic many-to-many fanout per book.")
    parser.add_argument("--sample-size", type=int, default=2048, help="Number of owner ids to touch in loop scenarios.")
    parser.add_argument(
        "--cache-types",
        default="schema_backed,database_backed,numpy_vectorized",
        help="Comma-separated cache backend list.",
    )
    parser.add_argument(
        "--scenarios",
        default=",".join(DEFAULT_SCENARIOS),
        help="Comma-separated scenario list.",
    )
    parser.add_argument("--iterations", type=int, default=5, help="Measured iterations per scenario.")
    parser.add_argument("--warmups", type=int, default=1, help="Warmup iterations per scenario.")
    parser.add_argument("--quiet", action="store_true", help="Suppress progress logging and only emit the final summary line.")
    parser.add_argument("--output", default="", help="Write JSON report to this path instead of stdout.")
    return parser.parse_args()


def _parse_csv(raw: str) -> list[str]:
    return [one.strip() for one in str(raw or "").split(",") if one.strip()]


def _expanded_ids(max_id: int, sample_size: int) -> tuple[int, ...]:
    if max_id <= 0:
        return ()
    target = max(1, int(sample_size))
    return tuple((index % max_id) + 1 for index in range(target))


def _build_synthetic_cache_db(*, books: int, tag_pool: int, tags_per_book: int):
    books_table = make_table(
        "books",
        ("id", "title", "shared_code", "slug"),
        is_main_table=True,
        linked_tables=("covers", "tags"),
    )
    covers_table = make_table(
        "covers",
        ("id", "path", "shared_code"),
        is_main_table=True,
        linked_tables=("books",),
    )
    tags_table = make_table(
        "tags",
        ("id", "tag_name", "tag_group"),
        is_main_table=True,
        linked_tables=("books",),
    )
    book_covers_table = make_table(
        "book_covers",
        ("id", "book_id", "cover_id"),
        is_link_table=True,
        linked_tables=("books", "covers"),
    )
    book_tags_table = make_table(
        "book_tags",
        ("id", "book_id", "tag_id"),
        is_link_table=True,
        linked_tables=("books", "tags"),
    )

    schema = StorageSchemaSpec(
        tables={
            "books": books_table,
            "covers": covers_table,
            "tags": tags_table,
            "book_covers": book_covers_table,
            "book_tags": book_tags_table,
        },
        interlinks=(
            StorageLinkSpec(
                primary_table="books",
                secondary_table="covers",
                link_table="book_covers",
                cardinality=LinkCardinality.ONE_TO_ONE,
                primary_link_col="book_id",
                secondary_link_col="cover_id",
            ),
            StorageLinkSpec(
                primary_table="books",
                secondary_table="tags",
                link_table="book_tags",
                cardinality=LinkCardinality.MANY_TO_MANY,
                primary_link_col="book_id",
                secondary_link_col="tag_id",
            ),
        ),
        intralinks=(),
    )

    total_books = max(1, int(books))
    total_tag_pool = max(1, int(tag_pool))
    tag_fanout = max(1, int(tags_per_book))

    rows_by_table = {
        "books": [
            {
                "id": book_id,
                "title": f"Book {book_id:06d}",
                "shared_code": f"B-{book_id % 97:02d}",
                "slug": f"book-{book_id:06d}",
            }
            for book_id in range(1, total_books + 1)
        ],
        "covers": [
            {
                "id": cover_id,
                "path": f"/covers/{cover_id:06d}.jpg",
                "shared_code": f"C-{cover_id % 89:02d}",
            }
            for cover_id in range(1, total_books + 1)
        ],
        "tags": [
            {
                "id": tag_id,
                "tag_name": f"Tag {tag_id:05d}",
                "tag_group": f"group-{tag_id % 13:02d}",
            }
            for tag_id in range(1, total_tag_pool + 1)
        ],
        "book_covers": [
            {
                "id": link_id,
                "book_id": link_id,
                "cover_id": link_id,
            }
            for link_id in range(1, total_books + 1)
        ],
        "book_tags": [],
    }

    link_id = 1
    for book_id in range(1, total_books + 1):
        base = (book_id - 1) % total_tag_pool
        for offset in range(tag_fanout):
            tag_id = ((base + offset) % total_tag_pool) + 1
            rows_by_table["book_tags"].append(
                {
                    "id": link_id,
                    "book_id": book_id,
                    "tag_id": tag_id,
                }
            )
            link_id += 1

    return make_fake_db(schema=schema, rows_by_table=rows_by_table)


def _create_cache(db: Any, cache_type: str) -> Any:
    kwargs = dict(CACHE_PLUGIN_KWARGS.get(str(cache_type), {}))
    return create_storage_cache(db, cache_type, **kwargs)


def _scenario_load_cache(db: Any, cache_type: str) -> dict[str, object]:
    cache = _create_cache(db, cache_type)
    cache.read()
    return {
        "cache_type": cache_type,
        "main_tables": len(cache.main_tables),
        "link_tables": len(cache.link_tables),
        "fields": len(tuple(cache.iter_fields())),
        "vectorized_helpers": bool(cache.capabilities.vectorized_helpers),
    }


def _scenario_read_tables_only(db: Any, cache_type: str) -> dict[str, object]:
    cache = _create_cache(db, cache_type)
    cache.clear()
    cache.read_tables(db)
    return {
        "cache_type": cache_type,
        "main_tables": len(cache.main_tables),
        "link_tables": len(cache.link_tables),
    }


def _scenario_initialize_tables_only(db: Any, cache_type: str) -> dict[str, object]:
    cache = _create_cache(db, cache_type)
    cache.clear()
    cache.read_tables(db)
    cache.initialize_tables(db)
    total_rows = sum(len(tuple(table.row_ids)) for table in cache.main_tables.values())
    return {
        "cache_type": cache_type,
        "main_tables": len(cache.main_tables),
        "link_tables": len(cache.link_tables),
        "total_rows": total_rows,
    }


def _scenario_read_fields_only(db: Any, cache_type: str) -> dict[str, object]:
    cache = _create_cache(db, cache_type)
    cache.clear()
    cache.read_tables(db)
    cache.initialize_tables(db)
    cache.read_fields(db)
    return {
        "cache_type": cache_type,
        "fields": len(tuple(cache.iter_fields())),
    }


def _scenario_initialize_fields_only(db: Any, cache_type: str) -> dict[str, object]:
    cache = _create_cache(db, cache_type)
    cache.clear()
    cache.read_tables(db)
    cache.initialize_tables(db)
    cache.read_fields(db)
    cache.initialize_fields(db)
    return {
        "cache_type": cache_type,
        "fields": len(tuple(cache.iter_fields())),
    }


def _scenario_scalar_get_cached_value_loop(cache: Any, owner_ids: tuple[int, ...], cache_type: str) -> dict[str, object]:
    total_length = 0
    for owner_id in owner_ids:
        total_length += len(str(cache.get_cached_value(owner_id, "books.title", default_value="") or ""))
    return {
        "cache_type": cache_type,
        "count": len(owner_ids),
        "total_length": total_length,
    }


def _scenario_scalar_get_cached_row_values_loop(cache: Any, owner_ids: tuple[int, ...], cache_type: str) -> dict[str, object]:
    total_values = 0
    total_length = 0
    for owner_id in owner_ids:
        values = cache.get_cached_row_values(owner_id, ("books.title", "books.shared_code"), default_value=None)
        total_values += len(values)
        total_length += sum(len(str(value)) for value in values if value is not None)
    return {
        "cache_type": cache_type,
        "count": len(owner_ids),
        "total_values": total_values,
        "total_length": total_length,
    }


def _scenario_relation_single_get_value_loop(cache: Any, owner_ids: tuple[int, ...], cache_type: str) -> dict[str, object]:
    field = cache.get_field("books.covers.path")
    total_length = 0
    for owner_id in owner_ids:
        total_length += len(str(field.get_value_from_src_id(owner_id) or ""))
    return {
        "cache_type": cache_type,
        "count": len(owner_ids),
        "total_length": total_length,
    }


def _scenario_relation_multi_get_values_loop(cache: Any, owner_ids: tuple[int, ...], cache_type: str) -> dict[str, object]:
    field = cache.get_field("books.tags.tag_name")
    total_values = 0
    total_length = 0
    for owner_id in owner_ids:
        values = tuple(field.get_values_from_src_id(owner_id, require_ordering=True))
        total_values += len(values)
        total_length += sum(len(str(value)) for value in values if value is not None)
    return {
        "cache_type": cache_type,
        "count": len(owner_ids),
        "total_values": total_values,
        "total_length": total_length,
    }


def _scenario_numpy_scalar_arrays(cache: Any, cache_type: str) -> dict[str, object]:
    row_ids = cache.get_numpy_row_id_array("books")
    owner_ids = cache.get_numpy_field_owner_ids("books.title")
    values = cache.get_numpy_field_array("books.title")
    return {
        "cache_type": cache_type,
        "row_ids_len": len(row_ids),
        "owner_ids_len": len(owner_ids),
        "values_len": len(values),
    }


def _scenario_numpy_relation_arrays(cache: Any, cache_type: str) -> dict[str, object]:
    owner_ids = cache.get_numpy_field_owner_ids("books.tags.tag_name")
    values = cache.get_numpy_field_array("books.tags.tag_name")
    return {
        "cache_type": cache_type,
        "owner_ids_len": len(owner_ids),
        "values_len": len(values),
    }


def _skip_result(cache_type: str, scenario_name: str, reason: str) -> dict[str, object]:
    return {
        "name": "{}.{}".format(cache_type, scenario_name),
        "cache_type": cache_type,
        "scenario": scenario_name,
        "skipped": True,
        "reason": reason,
    }


def run_internal_cache_benchmarks(
    *,
    books: int,
    tag_pool: int,
    tags_per_book: int,
    sample_size: int,
    cache_types: list[str],
    scenario_names: list[str],
    iterations: int,
    warmups: int,
    progress: Optional[Callable[[str], None]] = None,
) -> dict[str, object]:
    db = _build_synthetic_cache_db(books=books, tag_pool=tag_pool, tags_per_book=tags_per_book)
    owner_ids = _expanded_ids(max(1, int(books)), sample_size)
    results: list[dict[str, object]] = []

    for cache_type in cache_types:
        if progress is not None:
            progress("preparing cache_type={}".format(cache_type))

        loaded_cache = _create_cache(db, cache_type)
        loaded_cache.read()

        scenario_map: dict[str, Callable[[], dict[str, object]]] = {
            "load_cache": lambda cache_type=cache_type: _scenario_load_cache(db, cache_type),
            "read_tables_only": lambda cache_type=cache_type: _scenario_read_tables_only(db, cache_type),
            "initialize_tables_only": lambda cache_type=cache_type: _scenario_initialize_tables_only(db, cache_type),
            "read_fields_only": lambda cache_type=cache_type: _scenario_read_fields_only(db, cache_type),
            "initialize_fields_only": lambda cache_type=cache_type: _scenario_initialize_fields_only(db, cache_type),
            "scalar_get_cached_value_loop": lambda cache_type=cache_type: _scenario_scalar_get_cached_value_loop(loaded_cache, owner_ids, cache_type),
            "scalar_get_cached_row_values_loop": lambda cache_type=cache_type: _scenario_scalar_get_cached_row_values_loop(loaded_cache, owner_ids, cache_type),
            "relation_single_get_value_loop": lambda cache_type=cache_type: _scenario_relation_single_get_value_loop(loaded_cache, owner_ids, cache_type),
            "relation_multi_get_values_loop": lambda cache_type=cache_type: _scenario_relation_multi_get_values_loop(loaded_cache, owner_ids, cache_type),
        }
        if bool(loaded_cache.capabilities.vectorized_helpers):
            scenario_map["numpy_scalar_arrays"] = lambda cache_type=cache_type: _scenario_numpy_scalar_arrays(loaded_cache, cache_type)
            scenario_map["numpy_relation_arrays"] = lambda cache_type=cache_type: _scenario_numpy_relation_arrays(loaded_cache, cache_type)

        for scenario_name in scenario_names:
            scenario = scenario_map.get(scenario_name)
            if scenario is None:
                results.append(_skip_result(cache_type, scenario_name, "unsupported_or_missing_helper"))
                continue
            report = run_benchmark(
                name="{}.{}".format(cache_type, scenario_name),
                func=scenario,
                iterations=iterations,
                warmups=warmups,
                progress=(
                    (lambda message, cache_type=cache_type, scenario_name=scenario_name: progress("{} {} {}".format(cache_type, scenario_name, message)))
                    if progress is not None
                    else None
                ),
            )
            report["cache_type"] = cache_type
            report["scenario"] = scenario_name
            results.append(report)

    return {
        "script": "benchmark_cache_internal",
        "created_utc": utc_now_iso(),
        "environment": environment_payload(),
        "inputs": {
            "books": max(1, int(books)),
            "tag_pool": max(1, int(tag_pool)),
            "tags_per_book": max(1, int(tags_per_book)),
            "sample_size": max(1, int(sample_size)),
            "cache_types": cache_types,
            "scenario_names": scenario_names,
            "iterations": max(1, int(iterations)),
            "warmups": max(0, int(warmups)),
        },
        "results": results,
    }


def main(argv: Optional[list[str]] = None) -> int:
    del argv
    args = parse_args()
    cache_types = _parse_csv(str(args.cache_types))
    scenario_names = _parse_csv(str(args.scenarios))
    progress = None if bool(args.quiet) else stderr_progress
    payload = run_internal_cache_benchmarks(
        books=max(1, int(args.books)),
        tag_pool=max(1, int(args.tag_pool)),
        tags_per_book=max(1, int(args.tags_per_book)),
        sample_size=max(1, int(args.sample_size)),
        cache_types=cache_types,
        scenario_names=scenario_names,
        iterations=max(1, int(args.iterations)),
        warmups=max(0, int(args.warmups)),
        progress=progress,
    )
    print_report_summary(payload)
    write_json_report(payload, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
