# Cache Performance Exploration

Date: 2026-04-21

## Summary

The current `numpy_vectorized` backend is not a materially different cache
backend. It subclasses `schema_backed`, keeps the same Python object graph,
then adds NumPy arrays on top for a narrow helper surface.

If we want a genuinely higher-performance cache backend, the best fit for the
current API is an optional immutable columnar backend built on `pyarrow`.

## What I Looked At

- Cache API and registry:
  - `src/LiuXin_alpha/caches/api/storage_cache_api/storage_cache_api.py`
  - `src/LiuXin_alpha/caches/cache_plugins/registry.py`
- Existing backends:
  - `src/LiuXin_alpha/caches/cache_plugins/schema_backed/storage_cache.py`
  - `src/LiuXin_alpha/caches/cache_plugins/database_backed/storage_cache.py`
  - `src/LiuXin_alpha/caches/cache_plugins/numpy_vectorized/storage_cache.py`
- Hot storage objects:
  - `src/LiuXin_alpha/caches/cache_plugins/schema_backed/storage_tables/single_table.py`
  - `src/LiuXin_alpha/caches/cache_plugins/schema_backed/storage_fields/one_one_field.py`
  - `src/LiuXin_alpha/caches/cache_plugins/schema_backed/storage_fields/relation_base.py`
- Cache contract tests:
  - `tests/databases/caches/test_cache_plugin_contract.py`
  - `tests/support/storage_cache_test_harness.py`
- Dependency constraints:
  - `pyproject.toml`

## Current State

- Base project dependencies are intentionally small:
  - `chardet`
  - `cssselect`
  - `lxml`
- There is no existing heavy dataframe/columnar/query dependency in the default
  install.
- In the current environment, `numpy`, `pyarrow`, `duckdb`, and `polars` are
  not installed.
- The cache tests are already written around backend capabilities, which is
  good: a new backend can plug into the existing contract instead of inventing
  a separate semantics layer.

## What Is Actually Expensive Today

The main allocation-heavy parts are the `schema_backed` structures themselves:

- per-row Python `dict` snapshots in `SchemaBackedMainTableCache`
- per-column reverse indexes as nested Python `dict[Any, set[int]]`
- relation field caches as multiple Python maps:
  - `src -> dst ids`
  - `dst -> src ids`
  - `src -> values`
  - `dst -> value`

That means a faster backend needs to replace the underlying storage model, not
just add vector helpers on top.

## Option Comparison

## NumPy

Good:

- fast fixed-width numeric arrays
- mature and well understood

Bad fit here:

- this cache is heavily string- and relation-shaped, not numeric-matrix-shaped
- heterogenous rows push NumPy toward structured arrays or `dtype=object`
- `dtype=object` loses much of the memory/performance win
- relation traversal still needs separate adjacency/index structures

Conclusion:

- good helper layer
- weak choice for the primary storage model of this cache API

## DuckDB

Good:

- excellent query engine
- strong joins/filtering/aggregation
- can query Arrow directly

Bad fit here:

- current cache API is object/field/table oriented, not query oriented
- many hot paths are small point lookups and relation-object traversals
- making field objects and table snapshots sit on top of SQL would add a lot of
  impedance mismatch

Conclusion:

- strong future read/query engine
- not the best first replacement for the storage cache backend itself

## Polars

Good:

- fast columnar dataframe engine
- pleasant expression API

Bad fit here:

- similar mismatch to DuckDB for this object-heavy cache contract
- fewer obvious advantages than Arrow as a base storage representation

Conclusion:

- interesting, but not the most direct fit for this layer

## PyArrow

Good:

- columnar storage model fits immutable snapshot cache semantics
- native UTF-8/string columns are a much better fit than NumPy object arrays
- efficient integer columns suit ids and link-table edges
- can materialize Python objects only at the edges
- provides filter/join/grouping primitives if needed later
- gives a clearer path to measuring true buffer size and memory profile

Tradeoffs:

- new optional dependency
- immutable model means mutation paths will usually rebuild affected arrays or
  whole table snapshots
- row/field facade objects still need to be implemented over the columnar core

Conclusion:

- best first candidate for a genuinely faster snapshot cache backend

## Recommendation

Implement an optional `pyarrow_columnar` cache plugin.

That backend should be treated as:

- snapshot-based
- non-live
- reload-required for external changes
- optimized for read-heavy scalar access and relation traversal

Suggested capabilities:

- `live_reads=False`
- `live_child_objects=False`
- `vectorized_helpers=True`
- `requires_reload_for_external_changes=True`

## Likely Shape

Use Arrow tables as the authoritative in-memory representation:

- one Arrow table per main table
- one Arrow table per link table
- cached scalar-column accessors for same-table fields
- integer adjacency/index arrays for relation fields

Then build thin API facades on top:

- `ArrowBackedMainTableCache`
  - `get_row_snapshot()` materializes a Python `dict` on demand
  - `get_ids_for_value()` uses a backend index or filtered scan
- `ArrowBackedSameTableField`
  - direct column lookup by row-id position
- relation fields
  - owner/dst id arrays plus optional per-src offsets
  - values resolved from destination tables lazily or through prepared arrays

## Scope I Would Start With

1. Add optional dependency group, not a required base dependency.
2. Add a new backend plugin entry:
   - `pyarrow_columnar`
3. Implement same-table scalar fields first.
4. Implement one-to-one and many-to-many relation reads.
5. Keep mutation support conservative:
   - support current API
   - accept that updates rebuild affected in-memory structures
6. Add benchmarks before trying to outsmart the design further.

## Why Not Start With A "Better NumPy" Backend

If the real goal is lower memory and faster large-string / mixed-type access,
Arrow gets there more directly. A richer NumPy backend would still have to
recreate a lot of the missing string, nullability, and heterogenous-table
machinery that Arrow already has.

## Useful External References

- Arrow Python `Table` API:
  - https://arrow.apache.org/docs/python/generated/pyarrow.Table.html
- Arrow Python compute/filter API:
  - https://arrow.apache.org/docs/python/generated/pyarrow.compute.filter.html
- DuckDB Python client overview:
  - https://duckdb.org/docs/current/clients/python/overview.html
- DuckDB SQL on Arrow:
  - https://duckdb.org/docs/current/guides/python/sql_on_arrow.html
- NumPy structured arrays:
  - https://numpy.org/doc/stable/user/basics.rec.html

## Next Step

If we want to pursue this branch, the sensible next move is not full
implementation yet. It is:

1. add a small benchmark fixture for cache read hot paths
2. add optional `pyarrow` dependency plumbing
3. scaffold `pyarrow_columnar` with same-table scalar reads only
4. compare it against `schema_backed` and the current `numpy_vectorized`
