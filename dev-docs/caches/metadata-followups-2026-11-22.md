# Metadata cache follow-up review — 2026-11-22

The native metadata interface now has a usable application boundary: detached
reads, literal pickers, hydrated search, explicit relationships and transactional
edit receipts. The follow-up work strengthens transaction lifecycle and gives
existing PostgreSQL catalogues a deliberate upgrade path. The optimized full-size
soak passes in all four modes. The measurements below identify remaining costs;
this is correctness acceptance, not a production latency guarantee.

## Completed implementation

Schema-directed writers enter the cache lifetime guard before resolving values
or acquiring a SQL unit. They use the same outer-transaction completion mechanism
as metadata writes, including live storage. Every thread observes pending
publication; only the owning transaction can keep writing. Commit, rollback and
savepoint rollback preserve the completion boundary, and clear/close cannot be
undone by a late callback. Borrowed raw transactions reject before persistence.

Operations that may touch SQL acquire the database mutex before retaining the
cache mutex. This includes reads and reloads: a reader waiting behind an outer
transaction must not hold the cache mutex its owner needs for the first write.
The boundary is deliberately coarse, including snapshot reads; narrower locking
needs workload evidence and the same ordering guarantees.

The new reload concurrency case also exposed repeated whole-catalogue rebuilding
inside the live backend's inherited `reload_data()`: the routine walked live
proxies, and their attribute accesses each triggered a rebuild. Live data reloads
now perform one guarded read. A real native multi-table regression checks fresh
values, retained proxy identity and exactly one rebuild through both entry points.

At the user's request, the full soak was stopped after its uncached baseline to
address live capture cost first. Native SQL query views now construct schema
descriptors without eagerly copying every table and field. Point lookups capture
only requested identities, including misses. Population queries load the needed
tables, and relationship reads capture their physical route once, sharing it
between directions. The existing query evaluator retains filtering, ordering,
projection and completeness semantics. Lightweight adapters without the portable
row-reading capabilities retain their eager capture path.

Each public operation creates a new view. Metadata reads retain their surrounding
SQL snapshot; standalone live queries now own a read transaction and close the
temporary view on success or failure. A caller's existing managed transaction is
borrowed without being completed. Portable raw values pass through the existing
driver conversion policy before copying, retaining SQLite declared-value and
PostgreSQL binary-buffer behavior. No cross-call live row cache or second write
authority is introduced. A relationship lookup still materializes its whole link
table, so its cost can grow with that route's population.

Core field-write dispatch is delivered separately in
[PR #215](https://github.com/ajCameron/LiuXin-alpha/pull/215). Native cached writes
use the shared metadata coordinator; explicit external writer injection retains
its own capabilities. The neighboring Genre/Subject/Series write audit also
repaired native name selection and serialized hierarchy transactions.

The [PostgreSQL upgrade](../databases/postgres-catalog-upgrade.md) repairs Series
sibling position and generated typed-link identity/priority constraints. It
previews a fingerprinted plan, rejects customized or stale shapes, and rechecks
under exclusive locks before transactional DDL. Existing records are preserved.
Opening an existing catalogue never invokes it automatically.

## Measurements and interpretation

The [workflow benchmark](../../scripts/benchmark_metadata_workflows.py) measures
picker, selection, hydrated search, edit snapshots, conditional saves and changed
relationship saves against real SQLite catalogues. It records nested live captures
and full refreshes separately; these samples must not be added to parent latency.
Each mode rotates membership so later backends exercise changes rather than no-ops.

Use an unprofiled run for timings and `--profile` for attribution. The initial
100-Tag/20-Work profile shows picker overhead in Row construction and repeated
schema/interlink discovery. Snapshot saves spend heavily in full table rebuilds;
live captures repeatedly perform that rebuild too, including opening many SQL
connections. The independent-handle contention experiment confirms that even
disjoint metadata edits wait behind the same writer reservation.

These are specific optimization targets, not evidence that the API needs another
catalogue authority. Preserve complete composed-read visibility, explicit fallback,
edit preconditions and receipt accuracy when reducing row construction, capture
scope or refresh dependencies. Measure a representative picker/select/edit/save
loop after each change; raw query-engine speed alone does not establish UI latency.

The first larger benchmark mistakenly reused unchanged relationship memberships
in later backends. Its relationship timings are excluded. Corrected unprofiled
1,000-Tag/100-Work/seven-repeat runs completed before and after capture optimization:

| Live operation | Before median | After median |
| --- | ---: | ---: |
| Tag picker, 17 records with total count | 9.472 s | 3.802 s |
| Selected Tag | 4.375 s | 0.023 s |
| Hydrated Work search, seven records | 4.734 s | 0.510 s |
| Read for edit | 0.130 s | 0.093 s |
| Conditional scalar save | 0.204 s | 0.174 s |
| Changed Tag membership save | 0.069 s | 0.037 s |

These are complete parent-operation samples. View creation fell from a 4.384 s
median to about 1 ms, but the new view defers reads into the parent operation;
comparing that nested timer alone would overstate the improvement. Runs overlap
other regression work, and unchanged backends also vary with system load. Treat
these measurements as diagnostic evidence, not a controlled speedup ratio or a
production latency guarantee. Point-lookup and route-scope regressions independently
check that unrelated table populations are no longer read.

The optimized run still shows picker medians around 3 s across modes and snapshot
save/refresh medians of several seconds. Row construction and conservative full
reconciliation remain follow-up targets. The independent writer experiment passed,
with a 0.177 s competing save including the deliberate 0.100 s reservation hold.

## Validation status

Core has 214 distinct targeted passing cases, including the neighboring hierarchy
callers uncovered by its first full CI run. At `c641c488`, its full CI suite passes
10,784 cases, with 2,306 skips and 11 expected failures; its database gate passes
5,611 cases with 125 permitted skips. The metadata PR is stacked above the
separate Core PR for combined validation. At `7b2f5a4c`, its full CI suite passes
10,861 cases, with 2,348 skips and 11 expected failures. Its push-event database
gate passes 5,721 cases with 133 permitted skips; the duplicate PR-event database
job is still running at this documentation update. PostgreSQL has nine passing live
upgrade cases, with a further three-case rerun after extending the locked table
set. Documentation/tooling checks pass after tracking new destinations. All 45
cache transaction/reload cases, nine PostgreSQL cases and sixteen live-capture
cases are retained in the mandatory manifest, increasing it from 3,069 to 3,139
without removing prior requirements. An acceptance audit maps every one of the
70 newly mandatory cases to a passing JUnit record; none relies on an expected
skip or an interrupted run. The expanded lifecycle run passed 138 cases with
six expected skips. All sixteen focused capture cases pass: fourteen SQLite/APSW and two live
PostgreSQL. The broader capture run passed 150 cases with four expected skips;
a further eighteen affected live scenarios pass after the value-conversion audit.
The complete formatting, architecture, lint and both typing gates pass.
The broader live PostgreSQL metadata run also passed all 12 cases in 2,286.59
seconds, including independent/raw writer reservations and custom-field/revision
workflows. It began before the declared-value refinement; the final two PostgreSQL
capture cases cover that adjustment separately. Its disposable cluster is stopped.

The isolated Core/metadata integration has 73 distinct passing cases across
field routing, Catalog mutations and live captures, plus 22 documentation checks
and the complete quality gates. Five initial teardown errors came from concurrent
quality/pytest tools creating cache directories under the repository artifact
guard; all affected cases pass in clean reruns. Keep those tools sequential within
one checkout. The integration also corrects a stale query-isolation docstring;
the full soak used the frozen cache implementation rather than this combined
branch. Its 481 recorded source hashes still match at completion. The integrated
read-cache executable AST matches; the two imported Catalog container writers
are covered by the combined targeted cases, not the soak workload.

The integrated full suite passed 10,858 cases and reported that same Core mock
failure plus two differential-search failures. The database gate passed 5,723
cases and failed the two PostgreSQL versions of those differential tests. Their
old helper returned one shared execution view repeatedly, but live query cleanup
now correctly closes each view after one call. The helper is removed: every
Boolean, nullable-comparison and paging query now uses a fresh real capture.
All 12 SQLite/APSW differential cases pass with unchanged SQL-oracle assertions;
both PostgreSQL live cases also pass in 1,337.47 seconds, and their disposable
cluster stopped cleanly. No production runtime change was needed for these CI
corrections. The successful full-suite counts above are from the corrected heads;
the earlier failures are retained here to explain the changed test scaffolding.

The metadata long lane uses 10,000 Tags, 1,000 Works and 12 relationship cycles
for uncached operation and each of three backends. The original uncached case
passed its full call in 3,524.95 seconds before the user requested interruption
and optimization. That interrupted lane is not acceptance evidence. The unchanged
full workload now passes all four cases on cache commit `44dd5fe9`, after the
four-case smoke pass in 117.36 seconds. The final gate records four required and
four fully passing cases, no failures and exit code zero. All setup, call and
teardown phases passed, including 24,000 individual relationship reads per mode.

| Mode | Full test call | Picker median | Hydration median |
| --- | ---: | ---: | ---: |
| Uncached | 1,855.54 s | 23.687 s | 0.342 s |
| Database-backed | 5,333.19 s | 21.635 s | 0.501 s |
| NumPy | 1,706.21 s | 18.250 s | 0.274 s |
| Schema-backed | 1,385.06 s | 18.303 s | 0.266 s |

Total pytest duration was 10,288.13 seconds (2 h 51 m 28 s). These timings include
setup and verification work beyond the sampled operations. The separate
`relationship_cycle` samples time batch writes; the 24,000 verification reads
are included in the full calls, not in that sample. Sampled process-tree RSS
peaked at 267.90 MiB, with zero process swap and no increase in host OOM or swap
counters. The live case remains slowest; demand-driven capture avoids unrelated
tables but still reads a complete relationship route for each public operation.

Artifacts are retained under `/tmp/liuxin-metadata-capture-final-full-20261122`:
`gate.json`, `junit.xml`, `run.json`, per-mode measurements and telemetry. The final
phase/hash/resource audit is `/tmp/liuxin-full-soak-final-audit.json`.
See [long-running tests](long-running-tests.md) for reproduction. Source fingerprints
identify the tested implementation; historical interrupted runs are not substitutes.

During the live case, the host wall clock jumped forward by 35,729 seconds while
the monotonic sampler advanced only 1.68 seconds. The process remained alive.
Operation timings use the monotonic clock; they do not represent the full civil
wall-clock interval across that interruption. Retain this caveat with the final
soak evidence.
