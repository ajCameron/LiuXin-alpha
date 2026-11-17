# PostgreSQL schema discovery optimization — 2026-11-17

The first optimization from the
[cache architecture review](../architectural_reviews/caches-review-2026-11-17.md)
removes repeated schema fingerprint queries during a complete schema rebuild.
The change belongs in databases: the expensive work originates in physical schema
discovery and is shared by cache loading and field-metadata refresh.

## Implementation and contract

`DriverWrapper.get_schema_spec()` now assembles an uncached result inside the
driver's `schema_read_scope()`. The shared SQL implementation leaves existing
lookup behavior unchanged. PostgreSQL records one fingerprint in a context local
to that driver and execution context; nested scopes reuse it. Internal table,
column and link-name lookups reuse the fingerprint instead of opening another
connection and querying `information_schema.columns` each time.

On successful exit, PostgreSQL reads a fresh fingerprint through a separate short
connection. A mismatch raises `DatabaseIntegrityError` and asks the caller to
retry the complete read. The wrapper publishes its `StorageSchemaSpec` only after
that check succeeds. Entry, body and exit failures, including cancellation, clear
partial backend/wrapper schema caches. Scoped state is always released.

Each outer scope checks for a changed version before reusing backend metadata.
Independent calls outside the scope still query normally, including calls from
another thread. No persistent freshness timeout or process-wide suppression is
introduced. Owned probe connections close normally; the primary connection and
its pending transaction remain caller-owned.

The existing complete-spec cache contract is preserved: callers request
`get_schema_spec(force_refresh=True)` after schema changes. Returning an already
cached complete spec does not run another discovery scope. The PostgreSQL
fingerprint still covers schema/table/column/type facts; it does not encode every
index, constraint, trigger or default. This scope is not a repeatable-read SQL
transaction or a new guarantee of arbitrary concurrent DDL snapshot isolation.

## Measured work and timing

The [benchmark script](../../scripts/benchmark_schema_discovery.py) creates and
cleans up a uniquely named test schema. Setup/teardown are outside its measurements.
It forces wrapper reconstruction against the generated native catalogue, with
driver caches already warmed by opening the database. Counts instrument actual
fingerprint SQL, all adapter statements, and calls to the driver's connection
factory, including schema-admission overhead.

| Measurement | Before | After |
| --- | --- | --- |
| Discovered schema | 125 tables/views, 44 interlinks, 4 intralinks | Same |
| Fingerprint SQL per rebuild | 8,492 | 2 |
| New connections per rebuild | 8,667 | 176–177 |
| Adapter statements per rebuild | 35,168 | 1,204–1,208 |
| Elapsed time | 201.30 s, one sample | 4.84–5.41 s, three samples; median 4.90 s |

The measured rebuild was about 41 times faster. This is a Linux/WSL development
measurement, with other checks active during parts of the session, not a
production latency guarantee or a controlled statistical performance claim.
The work-count reduction is the stable regression target. Physical relation
inspection and remaining short connections still have measurable cost.
Individual introspection calls outside a complete schema read retain their normal
freshness checks; this change does not batch every introspection entry point.
A final-source confirmation measured **4.21 s**, with the same two fingerprints,
177 connections and 1,208 statements. It is retained separately in the artifact
from the initial three optimized samples.

Raw measurements and source hashes are retained in the
[baseline artifact](schema-discovery-baseline-2026-11-17.json). To reproduce,
configure `LIUXIN_TEST_POSTGRES_URL` for a disposable PostgreSQL server, then run:

```bash
.venv/bin/python scripts/benchmark_schema_discovery.py --repetitions 3
```

The script only drops its own generated schema. It never prints the connection
URL. Runtime results belong outside the checkout unless deliberately retained as
a dated baseline artifact.

## Regression protection and validation

[PostgreSQL regressions](../../tests/databases/test_schema_discovery_postgres_live.py)
compare complete scoped/unscoped schema specifications, including physical facts,
typed/ordered links, intralinks and views. They bound fingerprint queries while
increasing the number of main-table pairs. Further cases exercise nested and
independent scopes, external DDL between/during reads, another thread's fresh
probe, entry/body/exit failure, cancellation, recovery and pending writes.
Both SQLite drivers also exercise full-schema inspection with pending writes in
the [portable wrapper tests](../../tests/databases/test_driver_wrapper_review.py).
With the same test PostgreSQL environment configured, run the new cases with:

```bash
.venv/bin/python -m pytest -q tests/databases/test_schema_discovery_postgres_live.py
.venv/bin/python -m pytest -q tests/databases/test_driver_wrapper_review.py -k 'schema_inspection and schema_spec'
```

These **14 new mandatory cases** are registered under
`bounded-schema-discovery-preserves-freshness-and-transactions`, bringing the
database regression manifest to **2,637 cases**. Wider integration and quality
validation is complete at the scope below.

The final focused gate passed **14/14 cases**, including setup, call and teardown,
with no skips or expected failures. Full modern quality passed: typing, negative
type contracts, lint, annotations, import layers/cycles and cold-import budgets.
The expanded formatting scope passed **404 files**; tooling checks passed **57**
and documentation checks **16**. Documentation validation used an isolated index
containing the pending changes; the real Git index remained unchanged.

The broader selected wrapper/schema/cache integration run passed **312 tests**,
with **four backend-specific skips**, in 2,611.70 s. It includes both SQLite
drivers, live PostgreSQL wrapper and generated-link checks, native field metadata,
predicate execution, Catalog cache integration and all six PostgreSQL search
differential cases across the three cache backends. The skips concern retained
membership on live backends and execution-view capture on snapshot backends.
No mandatory case was skipped.

That broader run collected the first 13 new mandatory cases; all passed every
phase. The subsequent standalone-link regression and final connection-cleanup
assertions were verified by the separate **14-case final gate**. Counts overlap;
this was not a rerun of the full 2,637-case database manifest or the full project
suite. The modern quality run was supplemented by lint/format checks for the two
new files and **35 passing workflow checks** after adding them to the lint runner.

The private PostgreSQL cluster was stopped after validation. Source hashes and a
final-source benchmark repeat match the recorded implementation. The
[working handoff](../../working-memory/schema-discovery-optimization-2026-11-17.md)
retains the run commands, audit locations and earlier corrected test attempts.

Return to the [database guide](README.md) or [cache guide](../caches/README.md).
