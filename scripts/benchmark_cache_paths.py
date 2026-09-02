#!/usr/bin/env python3
"""Benchmark scalar and relationship reads through the cache facade."""

from __future__ import annotations

import argparse
import sys
import tracemalloc

from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Any, Callable, Iterable, Optional, Sequence

from _benchmark_common import (
    DEFAULT_CACHE_DIR,
    environment_payload,
    print_report_summary,
    resolved_benchmark_database,
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

from LiuXin_alpha.caches import Cache, CacheQuery, CacheSort, create_storage_cache  # noqa: E402
from LiuXin_alpha.surfaces.web_readonly.app import _open_database  # noqa: E402


DEFAULT_SCENARIOS = (
    "load_cache",
    "reload_cache",
    "scalar_get_cached_value_loop",
    "scalar_get_cached_row_values_loop",
    "scalar_field_get_value_loop",
    "main_table_row_snapshot_loop",
    "relation_single_get_value_loop",
    "relation_multi_get_values_loop",
    "reload_main_table",
    "reload_scalar_field",
    "reload_relation_field",
    "numpy_scalar_arrays",
    "numpy_relation_arrays",
    "facade_exact_lookup_loop",
    "facade_sorted_page",
    "facade_text_search",
)


@dataclass(frozen=True)
class ScalarProbe:
    """One scalar field and representative owner IDs selected for timing."""

    field_key: str
    table_name: str
    column_name: str
    owner_ids: tuple[int, ...]
    row_field_keys: tuple[str, ...]


@dataclass(frozen=True)
class RelationProbe:
    """One relationship field and representative owners selected for timing."""

    field_key: str
    owner_ids: tuple[int, ...]
    relation_kind: str


@dataclass(frozen=True)
class CacheProbeSet:
    """Available scalar and relationship probes for one cache backend."""

    scalar: Optional[ScalarProbe]
    relation_single: Optional[RelationProbe]
    relation_multi: Optional[RelationProbe]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark storage-cache backends on a LiuXin database.")
    parser.add_argument("--db-name", default="benchmark_db_medium", help="Named test DB to provision.")
    parser.add_argument("--database", default="", help="Existing database path to benchmark instead of provisioning.")
    parser.add_argument("--cache-dir", default=str(DEFAULT_CACHE_DIR), help="Cache directory for named DB provisioning.")
    parser.add_argument("--regenerate", action="store_true", help="Force regeneration of named DB templates.")
    parser.add_argument("--keep-provisioned", action="store_true", help="Do not delete the temporary provisioned DB copy.")
    parser.add_argument("--iterations", type=int, default=5, help="Measured iterations per scenario.")
    parser.add_argument("--warmups", type=int, default=1, help="Warmup iterations per scenario.")
    parser.add_argument(
        "--sample-size",
        type=int,
        default=512,
        help="Number of owner ids to touch in looped read scenarios.",
    )
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
    parser.add_argument("--quiet", action="store_true", help="Suppress progress logging and only emit the final summary line.")
    parser.add_argument("--output", default="", help="Write JSON report to this path instead of stdout.")
    return parser.parse_args()


def _open_benchmark_database(database_path: Path):
    return _open_database(database_path=str(database_path), db_type="sqlite")


def _parse_csv(raw: str) -> list[str]:
    return [one.strip() for one in str(raw or "").split(",") if one.strip()]


def _expanded_ids(ids: Sequence[int], sample_size: int) -> tuple[int, ...]:
    ordered = tuple(int(value) for value in ids)
    if not ordered:
        return ()
    target = max(1, int(sample_size))
    return tuple(ordered[index % len(ordered)] for index in range(target))


def _field_score(field_key: str, column_name: str) -> tuple[int, int, str]:
    full = str(field_key).lower()
    column = str(column_name).lower()
    preferred = (
        "works.title",
        "books.title",
        "titles.title",
        "title",
        "name",
        "path",
        "label",
        "tag_name",
        "series_name",
    )
    for index, needle in enumerate(preferred):
        if full == needle or column == needle or full.endswith("." + needle):
            return (0, index, full)
    if column == "id" or column.endswith("_id"):
        return (2, len(preferred), full)
    return (1, len(preferred), full)


def _src_table_score(field: Any) -> tuple[int, str]:
    src_table = str(
        getattr(
            field,
            "src_table_name",
            getattr(field, "table_name", ""),
        )
    ).lower()
    preferred = {
        "works": 0,
        "books": 1,
        "titles": 2,
        "expressions": 3,
        "manifestations": 4,
        "items": 5,
    }
    return (preferred.get(src_table, 10), src_table)


def _collect_ids(field: Any) -> tuple[int, ...]:
    raw_ids = getattr(field, "ids", set()) or ()
    return tuple(sorted(int(value) for value in raw_ids))


def _sample_scalar_value(field: Any, owner_ids: Sequence[int]) -> Any:
    getter = getattr(field, "get_value_from_id", None)
    if not callable(getter):
        return None
    for owner_id in owner_ids[:32]:
        value = getter(int(owner_id))
        if value is not None:
            return value
    return None


def _sample_relation_single_value(field: Any, owner_ids: Sequence[int]) -> Any:
    getter = getattr(field, "get_value_from_src_id", None)
    if not callable(getter):
        return None
    for owner_id in owner_ids[:32]:
        value = getter(int(owner_id))
        if value is not None:
            return value
    return None


def _sample_relation_multi_values(field: Any, owner_ids: Sequence[int]) -> tuple[Any, ...]:
    getter = getattr(field, "get_values_from_src_id", None)
    if not callable(getter):
        return ()
    for owner_id in owner_ids[:32]:
        values = tuple(getter(int(owner_id), require_ordering=True))
        if values:
            return values
    return ()


def _prepare_scalar_probe(cache: Any, *, sample_size: int) -> Optional[ScalarProbe]:
    candidates: list[tuple[tuple[int, int, str], str, str, tuple[int, ...]]] = []
    for field in cache.iter_fields():
        if getattr(field, "dst_table_name", None) is not None:
            continue
        getter = getattr(field, "get_value_from_id", None)
        if not callable(getter):
            continue
        field_key = str(getattr(field, "field_key", ""))
        table_name = str(getattr(field, "table_name", ""))
        column_name = str(getattr(field, "column_name", field_key.rsplit(".", 1)[-1]))
        owner_ids = _collect_ids(field)
        if not owner_ids:
            continue
        sample_value = _sample_scalar_value(field, owner_ids)
        if sample_value is None and (column_name == "id" or column_name.endswith("_id")):
            continue
        candidates.append((_field_score(field_key, column_name), field_key, table_name, owner_ids))

    if not candidates:
        return None

    _score, field_key, table_name, owner_ids = min(candidates, key=lambda item: item[0])
    table_fields = []
    for field in cache.get_fields_for_table(table_name):
        if getattr(field, "dst_table_name", None) is not None:
            continue
        getter = getattr(field, "get_value_from_id", None)
        if not callable(getter):
            continue
        candidate_key = str(getattr(field, "field_key", ""))
        table_fields.append((str(getattr(field, "column_name", candidate_key.rsplit(".", 1)[-1])), candidate_key))
    table_fields.sort(key=lambda item: _field_score(item[1], item[0]))

    row_field_keys = tuple(field_key for _column_name, field_key in table_fields[:3])
    if not row_field_keys:
        row_field_keys = (field_key,)

    return ScalarProbe(
        field_key=field_key,
        table_name=table_name,
        column_name=field_key.rsplit(".", 1)[-1],
        owner_ids=_expanded_ids(owner_ids, sample_size),
        row_field_keys=row_field_keys,
    )


def _prepare_relation_probe(
    cache: Any,
    *,
    sample_size: int,
    multi: bool,
) -> Optional[RelationProbe]:
    candidates: list[tuple[tuple[int, int, int, str], str, tuple[int, ...]]] = []
    for field in cache.iter_fields():
        if getattr(field, "dst_table_name", None) is None:
            continue
        if multi:
            getter = getattr(field, "get_values_from_src_id", None)
            if not callable(getter):
                continue
            owner_ids = _collect_ids(field)
            if not owner_ids:
                continue
            sample_values = _sample_relation_multi_values(field, owner_ids)
            if not sample_values:
                continue
        else:
            getter = getattr(field, "get_value_from_src_id", None)
            if not callable(getter):
                continue
            if callable(getattr(field, "get_values_from_src_id", None)):
                continue
            owner_ids = _collect_ids(field)
            if not owner_ids:
                continue
            sample_value = _sample_relation_single_value(field, owner_ids)
            if sample_value is None:
                continue

        field_key = str(getattr(field, "field_key", ""))
        column_name = str(getattr(field, "column_name", field_key.rsplit(".", 1)[-1]))
        src_table_rank, src_table_name = _src_table_score(field)
        field_rank, column_rank, full_key = _field_score(field_key, column_name)
        candidates.append(((src_table_rank, field_rank, column_rank, full_key), field_key, owner_ids))

    if not candidates:
        return None

    _score, field_key, owner_ids = min(candidates, key=lambda item: item[0])
    return RelationProbe(
        field_key=field_key,
        owner_ids=_expanded_ids(owner_ids, sample_size),
        relation_kind="multi" if multi else "single",
    )


def _prepare_probes(cache: Any, *, sample_size: int) -> CacheProbeSet:
    return CacheProbeSet(
        scalar=_prepare_scalar_probe(cache, sample_size=sample_size),
        relation_single=_prepare_relation_probe(cache, sample_size=sample_size, multi=False),
        relation_multi=_prepare_relation_probe(cache, sample_size=sample_size, multi=True),
    )


def _create_cache(db: Any, cache_type: str) -> Any:
    return create_storage_cache(db, cache_type)


def _scenario_load_cache(database_path: Path, cache_type: str) -> dict[str, object]:
    with _open_benchmark_database(database_path) as db:
        tracemalloc.start()
        try:
            cache = _create_cache(db, cache_type)
            cache.read()
            current_bytes, peak_bytes = tracemalloc.get_traced_memory()
            return {
                "cache_type": cache_type,
                "main_tables": len(tuple(cache.iter_main_tables())),
                "fields": len(tuple(cache.iter_fields())),
                "vectorized_helpers": bool(cache.capabilities.vectorized_helpers),
                "traced_current_bytes": current_bytes,
                "traced_peak_bytes": peak_bytes,
            }
        finally:
            tracemalloc.stop()


def _scenario_reload_cache(cache: Any, cache_type: str) -> dict[str, object]:
    cache.reload()
    return {
        "cache_type": cache_type,
        "main_tables": len(tuple(cache.iter_main_tables())),
        "fields": len(tuple(cache.iter_fields())),
    }


def _scenario_scalar_get_cached_value_loop(cache: Any, probe: ScalarProbe, cache_type: str) -> dict[str, object]:
    total_length = 0
    non_null = 0
    for owner_id in probe.owner_ids:
        value = cache.get_cached_value(owner_id, probe.field_key, default_value=None)
        if value is not None:
            non_null += 1
            total_length += len(str(value))
    return {
        "cache_type": cache_type,
        "field_key": probe.field_key,
        "count": len(probe.owner_ids),
        "non_null": non_null,
        "total_length": total_length,
    }


def _scenario_scalar_get_cached_row_values_loop(cache: Any, probe: ScalarProbe, cache_type: str) -> dict[str, object]:
    total_values = 0
    total_length = 0
    for owner_id in probe.owner_ids:
        values = cache.get_cached_row_values(owner_id, probe.row_field_keys, default_value=None)
        total_values += len(values)
        total_length += sum(len(str(value)) for value in values if value is not None)
    return {
        "cache_type": cache_type,
        "field_keys": list(probe.row_field_keys),
        "count": len(probe.owner_ids),
        "total_values": total_values,
        "total_length": total_length,
    }


def _scenario_scalar_field_get_value_loop(cache: Any, probe: ScalarProbe, cache_type: str) -> dict[str, object]:
    field = cache.get_field(probe.field_key)
    total_length = 0
    non_null = 0
    for owner_id in probe.owner_ids:
        value = field.get_value_from_id(owner_id)
        if value is not None:
            non_null += 1
            total_length += len(str(value))
    return {
        "cache_type": cache_type,
        "field_key": probe.field_key,
        "count": len(probe.owner_ids),
        "non_null": non_null,
        "total_length": total_length,
    }


def _scenario_main_table_row_snapshot_loop(cache: Any, probe: ScalarProbe, cache_type: str) -> dict[str, object]:
    table = cache.get_main_table(probe.table_name)
    total_columns = 0
    total_length = 0
    for owner_id in probe.owner_ids:
        snapshot = table.get_row_snapshot(owner_id)
        total_columns += len(snapshot)
        total_length += sum(len(str(value)) for value in snapshot.values() if value is not None)
    return {
        "cache_type": cache_type,
        "table_name": probe.table_name,
        "count": len(probe.owner_ids),
        "total_columns": total_columns,
        "total_length": total_length,
    }


def _scenario_relation_single_get_value_loop(cache: Any, probe: RelationProbe, cache_type: str) -> dict[str, object]:
    field = cache.get_field(probe.field_key)
    total_length = 0
    non_null = 0
    for owner_id in probe.owner_ids:
        value = field.get_value_from_src_id(owner_id)
        if value is not None:
            non_null += 1
            total_length += len(str(value))
    return {
        "cache_type": cache_type,
        "field_key": probe.field_key,
        "count": len(probe.owner_ids),
        "non_null": non_null,
        "total_length": total_length,
    }


def _scenario_relation_multi_get_values_loop(cache: Any, probe: RelationProbe, cache_type: str) -> dict[str, object]:
    field = cache.get_field(probe.field_key)
    total_values = 0
    total_length = 0
    for owner_id in probe.owner_ids:
        values = tuple(field.get_values_from_src_id(owner_id, require_ordering=True))
        total_values += len(values)
        total_length += sum(len(str(value)) for value in values if value is not None)
    return {
        "cache_type": cache_type,
        "field_key": probe.field_key,
        "count": len(probe.owner_ids),
        "total_values": total_values,
        "total_length": total_length,
    }


def _scenario_reload_main_table(cache: Any, probe: ScalarProbe, cache_type: str) -> dict[str, object]:
    cache.reload_main_table(probe.table_name)
    table = cache.get_main_table(probe.table_name)
    return {
        "cache_type": cache_type,
        "table_name": probe.table_name,
        "rows": len(tuple(table.row_ids)),
    }


def _scenario_reload_scalar_field(cache: Any, probe: ScalarProbe, cache_type: str) -> dict[str, object]:
    cache.reload_field(probe.field_key)
    field = cache.get_field(probe.field_key)
    return {
        "cache_type": cache_type,
        "field_key": probe.field_key,
        "ids": len(tuple(sorted(int(value) for value in field.ids))),
    }


def _scenario_reload_relation_field(cache: Any, probe: RelationProbe, cache_type: str) -> dict[str, object]:
    cache.reload_field(probe.field_key)
    field = cache.get_field(probe.field_key)
    return {
        "cache_type": cache_type,
        "field_key": probe.field_key,
        "ids": len(tuple(sorted(int(value) for value in field.ids))),
    }


def _scenario_numpy_scalar_arrays(cache: Any, probe: ScalarProbe, cache_type: str) -> dict[str, object]:
    row_ids = cache.get_numpy_row_id_array(probe.table_name)
    owner_ids = cache.get_numpy_field_owner_ids(probe.field_key)
    values = cache.get_numpy_field_array(probe.field_key)
    return {
        "cache_type": cache_type,
        "field_key": probe.field_key,
        "row_ids_len": len(row_ids),
        "owner_ids_len": len(owner_ids),
        "values_len": len(values),
    }


def _scenario_numpy_relation_arrays(cache: Any, probe: RelationProbe, cache_type: str) -> dict[str, object]:
    owner_ids = cache.get_numpy_field_owner_ids(probe.field_key)
    values = cache.get_numpy_field_array(probe.field_key)
    return {
        "cache_type": cache_type,
        "field_key": probe.field_key,
        "owner_ids_len": len(owner_ids),
        "values_len": len(values),
    }


def _scenario_facade_exact_lookup_loop(
    facade: Cache,
    probe: ScalarProbe,
    cache_type: str,
) -> dict[str, object]:
    hits = 0
    for owner_id in probe.owner_ids:
        if facade.get(probe.table_name, owner_id).is_hit:
            hits += 1
    return {
        "cache_type": cache_type,
        "table_name": probe.table_name,
        "count": len(probe.owner_ids),
        "hits": hits,
    }


def _scenario_facade_sorted_page(
    facade: Cache,
    probe: ScalarProbe,
    cache_type: str,
) -> dict[str, object]:
    result = facade.query(
        CacheQuery(
            table=probe.table_name,
            sort=(CacheSort(probe.column_name),),
            limit=50,
        )
    )
    return {
        "cache_type": cache_type,
        "table_name": probe.table_name,
        "returned": len(result.records),
        "total_count": result.total_count,
    }


def _scenario_facade_text_search(
    facade: Cache,
    cache: Any,
    probe: ScalarProbe,
    cache_type: str,
) -> dict[str, object]:
    sample_value = cache.get_cached_value(
        probe.owner_ids[0],
        probe.field_key,
        default_value="",
    )
    term = str(sample_value).strip()[:8] or "a"
    result = facade.query(
        CacheQuery(
            table=probe.table_name,
            text=term,
            text_fields=(probe.column_name,),
            limit=50,
        )
    )
    return {
        "cache_type": cache_type,
        "table_name": probe.table_name,
        "term_length": len(term),
        "returned": len(result.records),
        "total_count": result.total_count,
    }


def _skip_result(cache_type: str, scenario_name: str, reason: str) -> dict[str, object]:
    return {
        "name": "{}.{}".format(cache_type, scenario_name),
        "cache_type": cache_type,
        "scenario": scenario_name,
        "skipped": True,
        "reason": reason,
    }


def _result_name(cache_type: str, scenario_name: str) -> str:
    return "{}.{}".format(cache_type, scenario_name)


def _build_scenarios(
    *,
    database_path: Path,
    cache_type: str,
    cache: Any,
    probes: CacheProbeSet,
) -> dict[str, Callable[[], dict[str, object]]]:
    facade = Cache.from_storage(cache)
    scenarios: dict[str, Callable[[], dict[str, object]]] = {
        "load_cache": lambda: _scenario_load_cache(database_path, cache_type),
        "reload_cache": lambda: _scenario_reload_cache(cache, cache_type),
    }
    if probes.scalar is not None:
        scenarios["scalar_get_cached_value_loop"] = lambda: _scenario_scalar_get_cached_value_loop(cache, probes.scalar, cache_type)
        scenarios["scalar_get_cached_row_values_loop"] = lambda: _scenario_scalar_get_cached_row_values_loop(cache, probes.scalar, cache_type)
        scenarios["scalar_field_get_value_loop"] = lambda: _scenario_scalar_field_get_value_loop(cache, probes.scalar, cache_type)
        scenarios["main_table_row_snapshot_loop"] = lambda: _scenario_main_table_row_snapshot_loop(cache, probes.scalar, cache_type)
        scenarios["reload_main_table"] = lambda: _scenario_reload_main_table(cache, probes.scalar, cache_type)
        scenarios["reload_scalar_field"] = lambda: _scenario_reload_scalar_field(cache, probes.scalar, cache_type)
        scenarios["facade_exact_lookup_loop"] = lambda: _scenario_facade_exact_lookup_loop(facade, probes.scalar, cache_type)
        scenarios["facade_sorted_page"] = lambda: _scenario_facade_sorted_page(facade, probes.scalar, cache_type)
        scenarios["facade_text_search"] = lambda: _scenario_facade_text_search(facade, cache, probes.scalar, cache_type)
    if probes.relation_single is not None:
        scenarios["relation_single_get_value_loop"] = lambda: _scenario_relation_single_get_value_loop(cache, probes.relation_single, cache_type)
    if probes.relation_multi is not None:
        scenarios["relation_multi_get_values_loop"] = lambda: _scenario_relation_multi_get_values_loop(cache, probes.relation_multi, cache_type)
        scenarios["reload_relation_field"] = lambda: _scenario_reload_relation_field(cache, probes.relation_multi, cache_type)
    elif probes.relation_single is not None:
        scenarios["reload_relation_field"] = lambda: _scenario_reload_relation_field(cache, probes.relation_single, cache_type)
    if bool(cache.capabilities.vectorized_helpers) and probes.scalar is not None:
        scenarios["numpy_scalar_arrays"] = lambda: _scenario_numpy_scalar_arrays(cache, probes.scalar, cache_type)
    if bool(cache.capabilities.vectorized_helpers):
        relation_probe = probes.relation_multi or probes.relation_single
        if relation_probe is not None:
            scenarios["numpy_relation_arrays"] = lambda: _scenario_numpy_relation_arrays(cache, relation_probe, cache_type)
    return scenarios


def _cache_creation_error(exc: Exception) -> str:
    text = str(exc).strip()
    return text or exc.__class__.__name__


def run_cache_path_benchmarks(
    *,
    db_name: str,
    database: str,
    cache_dir: str,
    regenerate: bool,
    keep_provisioned: bool,
    iterations: int,
    warmups: int,
    sample_size: int,
    cache_types: list[str],
    scenario_names: list[str],
    progress: Optional[Callable[[str], None]] = None,
) -> dict[str, object]:
    if progress is not None:
        progress("preparing cache-path benchmark target={}".format(db_name or database))

    with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
        with resolved_benchmark_database(
            db_name=db_name,
            db_path=database,
            cache_dir=Path(cache_dir),
            regenerate=regenerate,
            keep_provisioned=keep_provisioned,
        ) as handle:
            if progress is not None:
                progress("resolved database source={} path={}".format(handle.source, handle.db_path))

            results: list[dict[str, object]] = []
            for cache_type in cache_types:
                if progress is not None:
                    progress("preparing cache_type={}".format(cache_type))

                with _open_benchmark_database(handle.db_path) as db:
                    try:
                        cache = _create_cache(db, cache_type)
                        cache.read()
                    except Exception as exc:
                        reason = _cache_creation_error(exc)
                        if progress is not None:
                            progress("skipping cache_type={} reason={}".format(cache_type, reason))
                        results.extend(
                            _skip_result(cache_type, scenario_name, reason)
                            for scenario_name in scenario_names
                        )
                        continue

                    probes = _prepare_probes(cache, sample_size=sample_size)
                    scenarios = _build_scenarios(
                        database_path=handle.db_path,
                        cache_type=cache_type,
                        cache=cache,
                        probes=probes,
                    )

                    for scenario_name in scenario_names:
                        scenario = scenarios.get(scenario_name)
                        if scenario is None:
                            reason = "unsupported_or_missing_probe"
                            if progress is not None:
                                progress(
                                    "skipping cache_type={} scenario={} reason={}".format(
                                        cache_type,
                                        scenario_name,
                                        reason,
                                    )
                                )
                            results.append(_skip_result(cache_type, scenario_name, reason))
                            continue

                        report = run_benchmark(
                            name=_result_name(cache_type, scenario_name),
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
        "script": "benchmark_cache_paths",
        "created_utc": utc_now_iso(),
        "environment": environment_payload(),
        "database": {
            "source": handle.source,
            "db_path": str(handle.db_path),
            "provision_root": str(handle.provision_root) if handle.provision_root is not None else "",
        },
        "inputs": {
            "db_name": db_name,
            "database": database,
            "iterations": iterations,
            "warmups": warmups,
            "sample_size": sample_size,
            "cache_types": cache_types,
            "scenario_names": scenario_names,
        },
        "results": results,
    }


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args()
    cache_types = _parse_csv(str(args.cache_types))
    scenario_names = _parse_csv(str(args.scenarios))
    progress = None if bool(args.quiet) else stderr_progress
    payload = run_cache_path_benchmarks(
        db_name=str(args.db_name),
        database=str(args.database),
        cache_dir=str(args.cache_dir),
        regenerate=bool(args.regenerate),
        keep_provisioned=bool(args.keep_provisioned),
        iterations=max(1, int(args.iterations)),
        warmups=max(0, int(args.warmups)),
        sample_size=max(1, int(args.sample_size)),
        cache_types=cache_types,
        scenario_names=scenario_names,
        progress=progress,
    )
    print_report_summary(payload)
    write_json_report(payload, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
