# Metadata search correctness and optimization baseline

Date: **2026-11-17**. The user requested aggressive testing before optimization.
This extends the [metadata search milestone](metadata-search.md); it does not
sign off content FTS, notes, annotations, every Calibre application integration,
or the entire cache subsystem.

## Defects exposed and repaired

- A backend exception after `clear()` or `close()` could leave the facade READY
  with retained query membership. All full lifecycle operations now evict derived
  state before backend mutation and leave DIRTY state on failure. Recovery also
  works when a partially failed operation has made storage uninitialized. Failed
  close remains retryable; successful close remains terminal.
- Native `ComparisonMatch(..., "in", (..., None))` excluded null values. Membership
  now agrees with the SQL compiler and independent reference scans, including
  under Boolean negation.
- Provider values were borrowed into the Calibre adapter. Mutable supplied values
  are now copied when the snapshot is constructed.
- Public field values exposed mutable snapshot containers. Value reads and value
  maps now return copies, so mutation cannot disagree with retained search results.
- Case-only search aliases could collide silently, and inferred mixed-case field
  names were not consistently searchable. Alias resolution is case-insensitive
  and rejects ambiguous fields. Definition revisions are published only after
  successful reconstruction.
- Series sorting treated index `0` as missing. Both native projected series and
  custom series retain zero; only `None` receives the default index.

These changes are in LiuXin-owned lifecycle, predicate and adapter code. The
pinned Calibre files and their existing GPL notices/provenance are unchanged.

## Repeatable correctness checks

- [Native stress tests](../../tests/databases/caches/test_search_stress.py) generate
  **960 expression trees over 96 rows**, using eight fixed seeds and independent
  Python oracles. Trees include empty Boolean groups, negation, nulls, collection
  counts, numeric comparisons and literal string matching. Every result is checked
  again through a different sort/page/projection request using cached membership.
- **405 mutation steps** across three seeds and all three native backends compare
  cached answers with fresh source scans. Updates include insertion, replacement,
  deletion, null transitions and both bounded and whole-table invalidation.
- Fault injection covers invalidation and refresh of tables, row IDs, fields and
  links, plus initial load, clear, close and failed membership repair. Concurrent
  readers must wait for a complete refresh before seeing a result.
- [Calibre stress tests](../../tests/databases/caches/test_calibre_search_stress.py)
  check **800 generated Boolean/restriction cases**, each twice, over 64 explicit
  identities, with caching enabled and disabled. Independent predicates cover
  stars, counts, custom numbers, tristate booleans and a simple regex language.
  Additional tests exercise aliases, copied values, duplicate Expression contexts,
  malformed queries, recursive definitions, stable sorts, concurrent clocks and
  competing attempts to consume one template capability.
- [Database differential tests](../../tests/catalog/test_cache_search_differential.py)
  compare 12 graph expressions at every WEMI level, including pagination, across
  three cache backends and both SQLite drivers. Nullable physical predicates are
  checked against an independent scan and SQLite SQL. The
  [PostgreSQL lane](../../tests/databases/caches/test_search_differential_postgres_live.py)
  runs the same checks on disposable schemas. PostgreSQL currently uses Catalog's
  semantic fallback rather than the SQLite normalized-search compiler; the tests
  preserve that distinction. The immutable corpus reuses one real captured live
  view per case. The original capture method is then restored for three requests
  around an explicitly committed Catalog write, checking freshness without manual
  invalidation. This avoids repeating an unchanged whole-catalog capture for each
  Boolean combination while retaining the full predicate matrix.
- Structural performance assertions protect bounded LRUs, no row evaluation for
  warm membership, operand-copy isolation, and the exact 50 changed-ID/query-pair
  repair boundary. These have no wall-clock threshold.

The seeds are fixed so failures reproduce in CI. This is broad deterministic
sampling, not an exhaustive proof over all expressions or all Unicode locales.

```sh
.venv/bin/python -m pytest -q \
  tests/databases/caches/test_search_stress.py \
  tests/databases/caches/test_calibre_search_stress.py

.venv/bin/python -m pytest -q tests/catalog/test_cache_search_differential.py

# Requires a disposable local test database; each fixture owns a unique schema.
LIUXIN_TEST_POSTGRES_URL=postgresql://user@localhost/test_database \
  .venv/bin/python -m pytest -q \
  tests/databases/caches/test_search_differential_postgres_live.py

# Whole database suite, including all named mandatory regressions.
LIUXIN_TEST_POSTGRES_URL=postgresql://user@localhost/test_database \
  .venv/bin/python scripts/run_database_regression_ratchet.py \
  --artifacts /tmp/metadata-search-regression
```

The database ratchet now requires **89 new cases** under
`metadata-search-optimization-correctness-baseline`, bringing its total to
**2,623**. Missing, skipped or expected-failure mandatory cases fail that gate.
The two Catalog differential tests add 12 SQLite parameter cases outside the
database-only manifest. PostgreSQL differential cases are expensive at this
baseline; keep their actual capture and committed-write checks when optimizing.

## Performance baseline

[benchmark_metadata_search_matrix.py](../../scripts/benchmark_metadata_search_matrix.py)
runs separate processes for 1k, 10k and 100k copied records. It checks independently
calculated answers for absent, broad, selective-ID, numeric, count, regex and
Boolean searches; reports cold/warm samples, median and p95; and measures native
sorting/paging, cache churn and bounded repair. Construction allocation is measured
separately with `tracemalloc`; query timings run with tracing disabled.

```sh
.venv/bin/python scripts/benchmark_metadata_search_matrix.py \
  --scales 1000 10000 100000 --repeats 7 > /tmp/metadata-search-baseline.json
```

The benchmark excludes input/import allocations, database capture, disk I/O, GUI
work and whole-process RSS. It must not become a universal CI latency threshold.
Run on the same machine with the same load when comparing optimization changes.

The [recorded baseline](metadata-search-baseline-2026-11-17.json) contains all
samples, machine details and hashes of the measured implementation. Seven samples
per workload were recorded on Linux/WSL with Python 3.12.3 while other regression
suites were active, so these are directional measurements:

| Records | Calibre Boolean cold median | Warm median | Adapter construction peak traced allocation |
| --- | --- | --- | --- |
| 1,000 | 36.9 ms | 0.45 ms | 1.01 MiB |
| 10,000 | 319.8 ms | 6.63 ms | 9.20 MiB |
| 100,000 | 1,430.5 ms | 46.23 ms | 104.46 MiB |

At 100k records, warm adapter searches returning one ID and no IDs had medians
of 22.1 ms and 49.9 ms respectively. Native warm counting took 18.9 ms; returning
a sorted 20-ID page took 470.2 ms. These samples share the load caveat above and
help separate candidate-assembly and sorting costs from predicate evaluation.

Useful optimization targets already visible in the implementation are repeated
projection copying, assembling candidate identities on warm adapter searches,
sorting all matching native IDs before paging, and constructing whole-table ID
sets during otherwise bounded repair. The repair budget bounds predicate visits,
not all set copying or backend refresh work. Preserve the correctness and work
count assertions when changing these paths.

Real-database testing also exposed expensive schema discovery. Timed stack dumps
from PostgreSQL cache loading and Core field-metadata refresh repeatedly reached
`DriverWrapper.get_link_table_name()` → `direct_get_schema_version()` → a short
PostgreSQL connection and schema fingerprint query. This is a priority for the
next optimization pass: preserve independent-connection schema-change detection
while avoiding repeated discovery within one complete schema read. The synthetic
benchmark above does not include this cost. A focused profile also confirmed
that a one-ID repair calls `_all_ids()` for the complete table.

## Run evidence

The initial stress runs exposed the defects above; corrected targeted runs passed.
The complete database gate passed **5,121 tests**, with **94 skips** and 26
warnings. Its original **2,534/2,534 mandatory cases** passed every phase, with
no violations. This long run began before the stress fixes and new files; the
subsequent cache and differential runs below exercised the corrected code.

The 94 skips cover disabled legacy modules, missing optional Hypothesis and
external-fixture inputs, native-Windows-only checks, and backend/schema-specific
cases. None was mandatory. The warning summary is retained with the run log.

The final cache/Catalog run passed **459 tests**, with **16 skips** for disabled
legacy Calibre-cache modules and backend-specific contracts. PostgreSQL was
enabled. All **83 new portable mandatory cases** passed setup, call and teardown
through `RegressionGate`, with no skips or expected failures. Six further new
PostgreSQL differential cases passed across the three backends, completing all
**89 new mandatory cases**. The SQLite differential matrix passed **12 cases**;
the final committed-write branch also passed again on both SQLite drivers.
Together these are **101 new regression cases**, validated across focused runs.
Combined with the original gate, all **2,623 currently required cases** have
passing evidence. This is evidence across runs, not a claim that the updated
manifest was executed in one complete-suite invocation.

Modern quality gates passed: format/lint, annotations, import cycles/layers,
import budgets, basedpyright/mypy and their negative contracts. The extra explicit
ReadCache/predicate type check passed. Tooling tests passed **64**, documentation
tests **16**. Wheel and source distribution checks confirmed exact corrected
sources, GPL notices/provenance and an isolated installed-wheel query.

Run logs, the mandatory-case audit and interrupted diagnostic runs are recorded
in the [working handoff](../../working-memory/cache-search-stress-2026-11-17.md).
The private PostgreSQL cluster was stopped after validation.

The long-running integration session crossed a host wall-clock jump. Its overall
session duration is unsuitable for performance comparison; use the separately
recorded benchmark samples and their stated conditions instead.

Remaining platform limits: no native Windows execution, full PyICU locale matrix,
whole-Calibre-application differential run, remote regex time budget, or content
search parity claim. Existing backend-specific and legacy skips must be reported
separately from passing mandatory cases.

Return to [cache documentation](README.md).
