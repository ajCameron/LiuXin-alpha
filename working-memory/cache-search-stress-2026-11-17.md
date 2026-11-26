# Cache metadata search stress testing — 2026-11-17

User requested aggressive testing before heavy optimization. This pass extends
[metadata implementation](cache-metadata-search-2026-11-17.md), preserving all
existing dirty work, submodule changes, review drafts and stash. No commit or PR
requested. Branch remains `caches-calibre-compat-boundary`.

Changes and repeatable workloads are described in
[the stress-test report](../dev-docs/caches/metadata-search-testing-2026-11-17.md).
The local PostgreSQL 16 cluster at `/tmp/liuxin-cc-followup-pg/data` was started
on loopback port 55439, database `liuxin_followup_utf8`, and stopped after all tests
finished. `pg_ctl status` confirmed no server running.

## Final validation status — complete

The full database gate (session 74956) completed **5,121 passed, 94 skipped,
26 warnings in 11033.64s**. `/tmp/liuxin-search-stress-ratchet/ratchet.json` confirms
**2,534/2,534 mandatory cases passed**, no failures and pytest exit 0. Its manifest
and imported runtime predate the stress fixes/new files. Corrected affected code
was validated in the subsequent runs below. The current manifest's 2,623 cases
all have passing evidence across the original gate and the new focused cases;
do not describe this as one full invocation of the updated manifest.

The full-run skips are nonmandatory: disabled legacy modules, missing optional
Hypothesis/external fixtures, native Windows, and backend/schema-specific cases.
The complete warning/skip summary is in `/tmp/liuxin-search-stress-ratchet.log`.

Final corrected cache/Catalog validation passed 459 tests with 16 documented
skips; all 83 new portable mandatory cases passed every phase. The final SQLite
differential matrix passed 12 cases, and its exact committed live-write branch
passed again for both drivers (session 86516, 2 passed in 93.97s).

Both PostgreSQL schema-backed differential cases passed. Session 40768
(`/tmp/liuxin-search-stress-pg-completed.log`) completed **4 passed, 2 deselected
in 3797.50s**: nullable and graph cases for both live and NumPy backends, including
the live committed-write freshness check. Thus all six PG cases, all 89 new
mandatory cases and all 101 new regression cases passed across focused runs.
The historical timeout stack dumps are diagnostics, not failures. The 100k-record benchmark,
quality, native type checks, tooling64/docs16 and final package checks passed.

The host wall clock jumped from about 03:55 to 14:16 while monotonic process
elapsed time continued normally. Whole-session durations are not performance
baselines. Both test processes continued; read-only checks found no PostgreSQL
blocking locks. A refreshed isolated-index documentation run passed all 16 checks
again (`/tmp/liuxin-search-stress-doc-current.log`, 137.22s). Later report edits
only add existing command paths, measured values and the clock-change caveat.

No tests remain running and the private PostgreSQL cluster is stopped. No runtime
edits followed the verified type-only cast; final harness edits are described
below. Final report and index record completion. No commit, staging or push.

## Run history — earlier active statuses are superseded

- Complete database ratchet: `/tmp/liuxin-search-stress-ratchet.log`, artifacts
  `/tmp/liuxin-search-stress-ratchet/`. Started against the existing 2,534-case
  manifest before stress fixes. Existing tests were collected before new files.
- Real database differential run: `/tmp/liuxin-search-stress-sql.log`. Started
  before null-IN correction and PostgreSQL reference-scan clarification, so its
  initial failures must be followed by a final corrected run.
- Full cache/Catalog run on corrected code: `/tmp/liuxin-search-stress-broad.log`.
- Modern quality: `/tmp/liuxin-search-stress-quality.log`.
- Current benchmark: `/tmp/liuxin-search-stress-benchmark.json` (1k/10k/100k, seven
  repetitions). This preliminary timing predates copying public field values;
  rerun after final changes and preserve the final data in dev-docs.

Focused checks already passed: 49 native stress/failure cases; 51 adapter cases;
104 combined native/adapter/existing adapter cases after public-field isolation.
Ten malformed-query/sort cases were then added. Capture final totals after all
files stop changing. Temporary run logs remain under `/tmp/liuxin-search-stress-*`.

New mandatory IDs collected to `/tmp/liuxin-search-stress-nodes.json`: **89** cases
(52 native, 31 adapter, six PostgreSQL differential). Added to the manifest under
`metadata-search-optimization-correctness-baseline`, bringing the total to
**2,623**. The 83 non-PostgreSQL cases passed `RegressionGate` with every setup,
call and teardown passing: `/tmp/liuxin-search-stress-mandatory.json`. The final
six PostgreSQL cases run with the database differential suite. New Catalog paths
remain outside that database-only manifest. Keep the real Git index untouched.

## Later validation and active sessions

- The superseded initial differential process was interrupted deliberately; its
  completion is not claimed. Final run is `/tmp/liuxin-search-stress-sql-final.log`,
  session 70321, on finalized runtime behavior. Expect 18 cases (12 SQLite, six PG).
- Full database gate session 74956, full cache/Catalog session 77164 remain active.
  They run serially within each suite to match repository gate policy. Do not
  restart these lengthy runs or change production runtime behavior unnecessarily.
- Quality finished successfully: modern format 402, annotation 463, cycle 256,
  layer 1933, import 25, basedpyright zero, 95 negative contracts, mypy 476 clean,
  93 negative contracts. Final explicit predicates/ReadCache typing zero after a
  type-only cast on dynamic `query_view()` capture. That cast does not change runtime.
- Final wheel/sdist verification passed after refreshing the cast: exact source,
  GPL notices/provenance, metadata, isolated installed-wheel smoke.
  `/tmp/liuxin-search-stress-package-verified.log`.
- Tooling checks: **64 passed**, `/tmp/liuxin-search-stress-tooling.log`.
- Ten malformed query/multisort cases passed separately, then all 83 new portable
  cases passed as above. The full cache run includes them all.
- Final benchmark is complete and copied with samples, source hashes and load
  caveat to `dev-docs/caches/metadata-search-baseline-2026-11-17.json`. `/tmp` final
  files: `liuxin-search-stress-benchmark-final.json` and `.log`. This final run
  includes copied public field values. Measurements ran alongside regressions.
- Doc check session 79469 uses `/tmp/liuxin-search-doc-check.py` with isolated Git
  index/objects. Wrapper `/tmp/liuxin-search-stress-doc-wrapper.log`; result log
  `/tmp/liuxin-search-doc-check.txt`. New baseline artifact was added afterward;
  refresh isolated index if documentation membership validation needs it.

Remaining: await full suites, fix any failures, verify six new mandatory PG cases,
record final counts/skips/limits in report and index, stop our private PG cluster,
check diff cleanliness. No source staging/commit/push. No subagents.

### 02:34 UTC checkpoint

- Full cache/Catalog session 77164 completed **459 passed, 16 skipped**, 830.37s.
  Log `/tmp/liuxin-search-stress-broad.log`. PostgreSQL enabled; skips are disabled
  legacy cache modules and backend-specific contracts, not unavailable PG.
- Documentation final session 89529 completed **16 passed**, 93.41s.
  `/tmp/liuxin-search-stress-doc-final.log`. Original doc run was 79 passed/one
  failure because the isolated candidate index predated the newly written
  benchmark JSON. Added that artifact to the isolated index only and reran all
  documentation checks successfully. Combined with separate tooling64, all80 pass.
- Main database gate remains active at about 28%, no failures reported. Final
  differential session 70321 remains active; slow graph traversal is expected.
- Final report updated with completed evidence. Still no source-control writes.

### 02:49 UTC checkpoint — integration is slow, do not discard evidence

- Main gate session74956/PID1097436 has reached ~30%, still no test failures.
  Apparent stalls in the Core PostgreSQL route were diagnosed with read-only
  `pg_stat_activity`/process checks and a separate reproduction. No blocking
  PostgreSQL locks were found. The expensive path is repeated schema fingerprint
  lookup during `get_schema_spec()` (many short connections from link-name discovery).
- Reproduction session25986, log `/tmp/liuxin-search-stress-stall.log`, executes
  `test_native_metadata_routes_preserve_enum_values_and_revisions[core]` with
  `faulthandler_timeout=60`. Still running; stack points to CoreServices.field_metadata
  → get_schema_spec → get_link_table_name → direct_get_schema_version. It is a
  performance target, not a proven correctness failure. Do not optimize it as part
  of this test-only follow-up unless necessary to fix a demonstrated defect.
- Final differential session70321 was interrupted after **six SQLite graph tests
  passed** and seven nullable tests failed due solely to a harness typo:
  `macros.get_rows("works", limit=None)` does not accept `limit`. Fixed to `get_rows("works")`.
  Log has full failures and durations, **7 failed/6 passed in1439.79s**. The seventh
  failure was PostgreSQL *nullable*, not graph (import collection order differs).
- Corrected nullable run session94352, log `/tmp/liuxin-search-stress-nullable-final.log`:
  nine cases (six SQLite + three PG), currently passing as it progresses. No
  production source change was needed for this harness correction.
- PostgreSQL graph diagnostic session2471, log
  `/tmp/liuxin-search-stress-pg-graph-diagnosis.log`, runs only schema_backed with
  `faulthandler_timeout=60`; it has not failed. Stack during initial load shows the
  same expensive schema discovery. Once it passes, run the other two PG graph
  parameters (database_backed, numpy_vectorized); do not repeat already proven
  SQLite graph cases or schema_backed PG needlessly.
- No test-only snapshot substitution or mocking was added to speed integration.
  Live capture still runs freshly for every query. This deliberately exposes its
  current cost. SQLite live graph cases took 464s/315s for96 requests each.
- Need final Ruff/format/doc check for the harness/report update, then complete
  runtime evidence, clean up our PG cluster, update final report and index.

The diagnostic Core PostgreSQL reproduction subsequently **passed in461.06s**
(7m41s). It was slow, not deadlocked. Main gate advanced to31% afterward. The
attempt to interrupt the redundant diagnostic found it had already exited and
did not signal any process. Final harness Ruff check passes.

### 03:04 UTC checkpoint

- Final nullable session94352 has passed seven cases: all six SQLite parameters
  and PostgreSQL schema_backed. PostgreSQL live/NumPy remain in that same run.
- PostgreSQL schema-backed graph session2471 remains active; its timeout stack
  was diagnostic only. The other two graph parameters now run in session39842,
  `/tmp/liuxin-search-stress-pg-graph-final.log`, with120s diagnostic stack dumps.
- Main gate74956 remains active at36%, no failures. All these real PostgreSQL
  tests are slow because repeated schema discovery opens many short connections.
  Preserve their actual live capture semantics; no fixture substitution was used.
- Baseline benchmark, final package verification, native types, 459-test broad
  cache/Catalog run, 83-case mandatory portable group, tooling64 and docs16 all
  remain successful. The only production edit after those runtime checks was the
  explicitly verified type-only `cast(StorageCacheAPI, capture())`.

### 03:15 UTC checkpoint

- PostgreSQL schema_backed graph diagnostic2471 **passed in1640.28s (27m20s)**.
  This covers all12 graph expressions at all4 levels plus cache pagination. The
  faulthandler timeout text in its log is a stack dump, not a test failure.
- Main gate74956 is at41%, still no failures. Nullable94352 remains7/9 passed;
  remaining PostgreSQL live/NumPy and graph39842 continue.
- Work-budget assertion now accepts *at most* the changed-row snapshot count,
  allowing future vectorization to read fewer rows. Both threshold cases passed
  again: `/tmp/liuxin-search-stress-work-budget.log`, 2 passed/50 deselected.
  No runtime change, no change to mandatory node IDs.

### 03:36 UTC checkpoint — finalized integration harness

Historical checkpoint: the November 22 demand-driven capture follow-up removes
the shared-view shortcut below. Every differential query now exercises real
per-call capture and cleanup, while retaining all predicate and freshness checks.
See [the current follow-up](cache-followups-2026-11-22.md).

The previous decision to capture the unchanged fixture separately for every
expression was superseded: it was repeating an expensive whole-catalog operation
without testing freshness under any mutation. `_unchanged_query_view` now captures
one **real** view for the immutable corpus. The graph test restores the original
method, checks the original title, commits an external Catalog title update, then
checks both old-title exclusion and new-title inclusion with real fresh captures.
All12 graph expressions ×4 levels and all70 nullable predicate trees remain.
Finally blocks also close a cache whose load failed. Runtime source is unchanged.

- Old nullable94352 interrupted after seven passing cases (six SQLite + PG schema).
  Old graph39842 interrupted while pending, no claimed result. A brief first launch
  of the revised PG run was restarted to make the fixture-owned commit explicit.
- **Current PG session40768**, `/tmp/liuxin-search-stress-pg-completed.log`, runs
  the four remaining live/NumPy cases (`-k 'not schema_backed'`) on the final harness.
- Revised SQLite matrix98466 **12 passed in248.48s**. This started before adding
  the explicit commit (normal SQLite writes already committed); session for exact
  final commit branch reruns only its two live graph cases, log
  `/tmp/liuxin-search-stress-live-commit.log` (session returned in tool output).
- The two already-passed PG schema-backed cases are unchanged in their execution
  logic (helper is a no-op); graph2471 passed, nullable94352's first PG case passed.
- Main full gate74956 is still active at~43%, no failures. Its required manifest
  remains the2,534-case snapshot taken at launch. New89 are validated separately.
- Update report with final counts once PG40768 and main74956 finish; stop private
  PG afterward. Keep all initial failure/interrupt logs as diagnostic evidence.
