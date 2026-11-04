# Database review and validation — 2026-10-31

The requested delivery is a commit/PR checkpoint, live PostgreSQL regression
coverage, a full test run with telemetry, and representative catalogue benchmarks.
Keep all four parts open until their results are recorded.

## Commit boundaries

The existing staged changes form the Catalog/database ownership checkpoint:
WEMI search and repositories, Expression-based Calibre conversion, retired
compatibility SQL, physical-table SQL mixins, adapter/locking moves and caller/docs
updates. The next commit contains SQL pagination, graph batches, aggregate reads,
staged writes, normalized search and explicit query-index migrations.

The previously recorded 397 passing selected cases and one expected skip apply
to the combined worktree, not the intermediate ownership commit in isolation.
The dirty `LiuXin_alpha_data` submodule is outside this delivery.

## Validation checkpoints

- No PR existed for `database-review` when this work began.
- Ownership checkpoint committed as `50782bc6`. The following commit packages
  the SQL efficiency increment; full-suite and benchmark evidence remain pending.
- The first live PostgreSQL selection passed 18 tests. New efficiency tests
  exposed reversed Catalog priority ownership and omitted backup-workflow tables
  in PostgreSQL schema creation. Both are corrected; all three new live efficiency
  regressions and all 32 backend unit tests now pass.
- The local quality runner found two leading blank lines in its format targets;
  those formatting-only corrections are included with validation.
- Full-suite CI already captures resource/event telemetry and removes per-test
  scratch. Use that runner for the full suite rather than the space-limited host.
- Main's last inspected push passed confidence, packaging and the fast format
  lane; its architecture/typing lane failed. Inspect that baseline separately.
- An isolated PostgreSQL 16 cluster is available for disposable-schema tests;
  no configured application database or system cluster is used.
- Benchmark queries must cover pagination, shared WEMI graphs, exact lookups and
  bulk writes, reporting time, memory and query counts. Existing benchmark tools
  supply conventions, but their older read scenarios do not cover every new path.

PR: https://github.com/ajCameron/LiuXin-alpha/pull/152 (`17b3b293` initially).
The first CI run stopped during collection because a moved type module imported
undeclared `typing_extensions`. Python 3.12 supplies the needed `TypedDict`.
The import-boundary check also caught annotation-only imports loading metadata
implementations; deferring those imports restores all 25 cold-import budgets.
Full-suite results remain pending after these corrections.

The corrected CI run is `41161229466` at `17198623`; the full suite is running.
Its remaining quality failure was an import ordering issue in a migrated test,
now corrected locally. Callback annotations fix the subsequent basedpyright
complaint. Basedpyright, fresh-cache mypy (227 files), and both 38-case negative
type-contract checks pass. The default local mypy cache crashed; the isolated
cache succeeded without a tool upgrade.

All 12 benchmark scenarios completed on 50,000 Works. Raw reports and analysis
are in `dev-docs/databases/sql-efficiency-2026-10-31.md`. An ANALYZE-dependent
exact-title scan was reproduced on both SQLite drivers and fixed using independent
indexed ID searches; 43 search/WEMI tests pass and a targeted benchmark rerun
reduces the lookup from 298 ms to 2 ms. The write benchmark reduces SQL events
but not latency; operation-scoped normalization-policy preparation is documented
as the next task. The benchmark script cleans its temporary catalogues. The
disposable PostgreSQL server is stopped and its data removed; logs remain.

Benchmark and query-plan checkpoint: `5f05efd7`, PR run `41162165268`.
The documentation gate exposed missing module/value-type docstrings. Those are
now present, the legacy module description is back at the top of database_types,
and the redundant typing_extensions fallback is removed. All 13 documentation
boundary/type-shape regressions pass. Consult the PR's latest checks and final
description for the full-suite outcome and telemetry; earlier run states above
describe their respective checkpoints.

CI's broader quality selection passed 697 cases and identified two failures:
the missing type docstrings above and a terminal import of database internals.
Core LibraryServices now owns the complete SQLite recovery connection/checkpoint/
integrity operation; the terminal delegates to it. All 11 recovery/unlock/boundary
regressions pass. The push confidence, wheel and format lanes already pass.

Implementation contracts and earlier focused evidence live in
`dev-docs/databases/sql-efficiency-2026-10-31.md`.

The first complete CI run (`41161229466`, head `17198623`) finished with
8,020 passed, 44 failed, 362 skipped and 17 xfailed. Its telemetry sampled a
1.06 GiB peak test-process-tree RSS, at least 84.7 GiB runner disk headroom,
and no OOM kills or swap activity. These are CI runner measurements, not WSL
headroom guarantees. The run at `887cf6ff` passed all quality lanes and finished
the full suite with 8,023 passed and 42 failed; failures repeat the remaining
test-setup groups rather than a resource crash.

The remaining corrections register production index functions on raw fixture
writer/integrity connections, including the shared semantic-builder helper;
fault injection follows batched `get_rows_by_ids` reads; and the optional APSW
function test skips independently without skipping the stdlib check. A first
targeted run exposed the second fixture connection and a subsequent backend
variant observing its partially populated template. A new scratch root is used
for the corrected rerun. Ruff passes the four changed test modules; the support
helper retains exactly its 21 pre-existing lint findings.

The corrected selection passes **68 tests**, with two expected Windows-only
skips, in 243 seconds using fresh fixtures and telemetry. It covers every
remaining full-suite failure group, including both locally installed SQLite
backends. The final pushed head still requires its own complete CI result;
consult PR #152 for that result and its retained telemetry artifact.
