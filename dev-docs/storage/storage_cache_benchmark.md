# Storage catalogue cache benchmark

Updated: 2026-08-21

## Purpose

Storage uses LiuXin's shared cache when Core has configured one, but the
database remains authoritative and the manager also works without a cache.
The production-hardening question was whether a single Asset or Replica write
forced an increasingly expensive whole-catalogue refresh.

The answer for the default schema-backed cache is now no. `Cache.invalidate()`
accepts an `ids={table: ids}` dependency, and the schema-backed storage cache
reloads only those rows. It updates row/value indexes and scalar and relation
field projections from the refreshed table state. Whole-table invalidation
remains available for bulk writes and migrations, and is the conservative
fallback for plugins without efficient row replacement.

## Reproduction

The synthetic benchmark is part of `scripts/benchmark_cache_internal.py`:

```bash
python3 scripts/benchmark_cache_internal.py \
  --books 50000 \
  --tag-pool 10000 \
  --tags-per-book 4 \
  --cache-types schema_backed \
  --scenarios targeted_id_refresh \
  --iterations 10 \
  --warmups 2
```

This creates 50,000 books, 50,000 covers, 10,000 reusable tags, 50,000 cover
links, and 200,000 tag links. Each measured iteration applies one external row
update, invalidates that ID, reads it through the cache, checks the new value,
and reports database row/table reads.

## 2026-08-21 checkpoint

Environment: CPython 3.12.3 on the development Linux/WSL host.

| Measure | Result |
| --- | ---: |
| Median targeted refresh | 1.164 ms |
| Mean targeted refresh | 1.256 ms |
| Minimum / maximum | 1.029 / 1.789 ms |
| Database row reads per refresh | 1 |
| Whole-table reads per refresh | 0 |
| Peak process RSS, including synthetic graph construction | 762,964 KiB (~745 MiB) |

The timing is a development checkpoint, not a universal latency promise. The
read counts are the architectural invariant. The peak footprint also matters:
large relationship-rich snapshots are expensive. Storage therefore must not
create or own a second cache. Deployments can use the direct database-backed
repository path when memory matters more than repeated-read latency, while
Core deployments that already pay for the shared schema cache get bounded
storage metadata reconciliation.

The NumPy cache currently uses the documented correct whole-table fallback for
ID refresh; optimize that representation only when a measured workload needs
it. The live database-backed plugin intentionally re-reads current database
state and is the correctness/low-retained-state option, not the snapshot-speed
option.
