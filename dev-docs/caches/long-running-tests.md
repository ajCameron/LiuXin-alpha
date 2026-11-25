# Longer cache tests and profiles

The dedicated [runner](../../scripts/run_cache_long_tests.py) extends the
[correctness baseline](metadata-search-testing-2026-11-17.md) with repeated
mutations, recovery, query churn and real database capture. Ordinary pytest runs
skip these tests before fixtures run. The `slow` marker alone does not enable
them; the runner explicitly sets `LIUXIN_RUN_CACHE_LONG_TESTS=1`.

## Workloads

| Lane | Required cases | Coverage |
| --- | --- | --- |
| `synthetic` | 4 | Three native backends plus the explicit Calibre adapter. |
| `sqlite` | 6 | Three cache backends on both SQLite and SQLite_apsw. |
| `postgres` | 3 | Three cache backends on disposable PostgreSQL schemas. |
| `all` | 13 | Every case above; all must pass. |

| Size | Synthetic records | Mutation/query steps per case | Database work rows | Load/write/rollback cycles per database case |
| --- | --- | --- | --- | --- |
| `smoke` | 128 | 12 | 12 | 2 |
| `full` (default) | 10,000 | 1,000 | 1,000 | 12 |

The reduced size validates the harness; it is not evidence that the full soak
passed. Both sizes use seed `20261117`, recorded in their artifacts.

The [synthetic cases](../../tests/databases/caches/test_search_long.py) compare
each changed native collection with independent Python predicates, including
deletion, replacement, null transitions, sorted pages and a repeated hot query.
They cycle through targeted and full invalidation, partial destructive failure,
recovery and clear/load. Bounded plan/result caches are checked throughout.
The Calibre case varies candidate restrictions and Boolean expressions, repeats
warm queries, changes/deletes saved definitions and periodically submits four
concurrent requests with different candidate sets. It uses independent expected
sets rather than comparing two production evaluators.

The [database cases](../../tests/databases/caches/test_search_long_database.py)
create complete native catalogues, seed work rows, then repeatedly construct,
load, query and close each cache. Every cycle checks committed Catalog changes
and rollback against a Python model, including counts, sorted IDs and projected
titles. Snapshot caches receive explicit invalidation; live caches must capture
again on all four requests in a cycle. No captured view is reused or substituted.
PostgreSQL checks that owned connections return to their original set after each
close. The existing differential suite remains responsible for populated WEMI
graphs and relationship semantics; these new database workloads populate works.

## Running locally or in CI

The additional [vocabulary tree stress cases](../../tests/databases/caches/test_metadata_trees_long.py)
use the same opt-in and size settings, and run separately from the fixed search
runner matrix. They cover uncached reads and all three cache backends. Full size
constructs a chain of 1,001 nodes and a root with 10,000 children, checks iterative
traversal and limits, then reverses the complete sibling order. Use smoke size to
validate this harness before requesting the longer run:

```sh
LIUXIN_RUN_CACHE_LONG_TESTS=1 LIUXIN_CACHE_LONG_SIZE=smoke \
  .venv/bin/python -m pytest -q tests/databases/caches/test_metadata_trees_long.py
```

Set `LIUXIN_CACHE_LONG_ARTIFACTS` to retain the per-operation timing JSON outside
pytest's temporary directory. Full-size execution is opt-in evidence, not implied
by a passing smoke run.

Use a **new artifact directory** for each invocation:

```sh
.venv/bin/python scripts/run_cache_long_tests.py \
  --lane synthetic --artifacts /tmp/cache-synthetic-full

.venv/bin/python scripts/run_cache_long_tests.py \
  --lane sqlite --size smoke --artifacts /tmp/cache-sqlite-smoke

LIUXIN_TEST_POSTGRES_URL=postgresql://user@localhost/test_database \
  .venv/bin/python scripts/run_cache_long_tests.py \
  --lane all --artifacts /tmp/cache-all-full
```

Install the project test and PostgreSQL extras plus APSW. A selected PostgreSQL
lane requires an explicit test URL. Existing fixtures create and drop only their
uniquely named schemas. The runner does not start or stop a database server.
It runs serially and requires every named case to pass setup, call and teardown.
Missing cases, skips, xfails and test/teardown failures fail the lane. The existing
mandatory database manifest remains unchanged; expensive opt-in cases do not
weaken or replace its ordinary regressions.

The manual [Cache Long Tests workflow](../../.github/workflows/cache-long-tests.yml)
offers the same lane/size choices and optional profiling, provides PostgreSQL 16,
has a 180-minute job limit and uploads evidence even after failure. It adds no
automatic soak to pushes or PRs. Small runner/workflow contract tests remain in
the ordinary quality checks. The GitHub workflow must be published before it can
be dispatched; adding it locally does not execute it remotely.

## Evidence and interpretation

Each invocation retains:

- `run.json`: exact workload, selected node IDs, environment and working-source
  hashes, written before pytest starts. It deliberately excludes the database URL.
- `gate.json`: required count, cases passing every phase, failures and pytest exit
  code. A missing gate file means the run did not finish; timing files are not
  passing evidence on their own.
- `junit.xml` and durable test telemetry, including Linux resource samples when
  available. These retain setup/teardown and whole-process cost separately.
- Per-case measurement JSON: raw operation durations, workload and whether
  profiling was enabled. Partial measurements are retained on test failure.

Database `load` measures a fresh cache owner over an already opened and populated
database; it includes the cache's actual load path. It excludes database creation,
population and imports. Later cycles may benefit from database/driver caches.
`first_query`, `repeated_query`, `after_commit` and `after_rollback` distinguish
query phases. A live backend's repeated request still captures the database; it
does not promise retained membership. `live_capture` is nested within its query
sample, so those durations must not be added together.

Synthetic timings distinguish construction/load, changed queries, repeated sorted
pages and warm adapter searches. Oracle scans and assertions are outside the
samples. Whole-suite wall time includes them. Memory telemetry observes process
usage; it is not a proof of bounded retained object graphs or a leak threshold.

For call attribution, repeat a lane with `--cprofile` and another fresh directory:

```sh
.venv/bin/python scripts/run_cache_long_tests.py \
  --lane sqlite --size smoke --cprofile --artifacts /tmp/cache-sqlite-profile

# Replace the path with one of the emitted per-case profiles.
.venv/bin/python -m pstats /tmp/cache-sqlite-profile/measurements/example.prof
```

Profiles cover measured operations, not fixture setup or oracle assertions.
Their timings include profiler overhead and must be compared separately from
unprofiled samples. No portable wall-clock performance threshold is asserted.
Compare matching workloads and source hashes on the same machine under similar
load before drawing optimization conclusions.

This lane broadens coverage; it does not close the write-route/registry audit,
physical replica lease work, native Windows validation, full locale matrix or
remaining Calibre application/search parity milestones.

## Validation — 2026-11-17

All **13 full-size cases passed** across the three lane invocations. Every
required setup/call/teardown phase passed, with no skips, xfails or gate violations:

| Lane | Required cases passed | Pytest elapsed time |
| --- | --- | --- |
| Synthetic | 4/4 | 1,937.53 s |
| SQLite and APSW | 6/6 | 787.76 s |
| PostgreSQL | 3/3 | 983.07 s |

This covers 3,000 native mutation steps, 1,000 Calibre query/definition steps,
108 fresh database-backed load/close cycles and 144 actual live captures. Each
database cycle also checks committed changes and rollback. All three lanes ran
on Linux/WSL with Python 3.12.3, with overlapping execution; their timings are
diagnostic evidence, not isolated performance comparisons or CI thresholds.

Reduced-size runs also passed all 13 cases, and four synthetic cProfile artifacts
were readable. The initial reduced SQLite run exposed an error in the new fixture:
it held an outer writer while calling a bulk insert that owns a fresh SQLite
connection. The fixture was corrected before the full runs; no production change
was necessary. The final Calibre full run includes the improved definition-churn
schedule added after its reduced run.

The existing stress tests and supporting tooling passed **188 tests**. Final
runner checks passed **eight**, including two additional nested-profile cases;
these counts overlap. Documentation passed **22**, formatting covered **409**
files, and new-file lint passed. Ordinary selection skipped all 13 gated cases
before their fixtures ran. The private PostgreSQL server was stopped after testing.
The complete database regression suite was not rerun for this test-only change.

The [working handoff](../../working-memory/cache-long-tests-2026-11-17.md) records
local artifact paths and the final audit. The manual GitHub workflow was added
and checked locally; it has not been dispatched remotely.

Return to the [cache guide](README.md).
