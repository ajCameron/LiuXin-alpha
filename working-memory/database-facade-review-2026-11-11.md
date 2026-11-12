# Database facade review handoff

The user confirmed that the conflict repair was merged and requested review of
`databases.database`. The original checkout is now on `main` at
`47156490aa4c71fecc414f9bb237d977d5d6febc`. This review used that checkout rather
than the older persistent review clone. The dirty `LiuXin_alpha_data` submodule
was preserved.

Read the [full review](../dev-docs/databases/database-facade-review-2026-11-11.md).
The original review identified seven P2 findings: DBF01 priority swap corruption; DBF02 committed
placeholders after failed intralink creation; DBF03 incomplete driver
replacement; DBF04 unfiltered unlink of typed multi-links; DBF05 intralink
queries returning relationship Rows; DBF06 series subtree deletion; DBF07
one-shot iterables in tree search. These include acknowledged legacy defects,
not seven claims of newly introduced regressions.

Existing tests: **776 passed, 40 skipped, 8 xfailed**, 386.65 seconds. Real
generated-schema probes reproduce every finding on `SQLite` and `SQLite_apsw`.
Independent readers confirm the persistent write outcomes. No live PostgreSQL
server was started. The current series cycle trigger rejected the attempted
cycle, so that suspected path was excluded from the findings.

Evidence: `/mnt/c/dev/LiuXin-alpha/.tmp/database-facade-review-2026-11-11/`.
`initial-probes.py`, `probes.log` and `probe-results.json` contain the first
pass, including a rejected cycle and unsuccessful initial type choices for
the typed-unlink setup. `probes.py unlink_typed intralink_valid`, `followup.log`
and `followup-results.json` use supported `aut`/`edt` and `inspired_by` values.
`durability-controls.json` records independent-reader assertions. The initial
invalid intralink type itself reproduced DBF02; the valid duplicate provides
an additional independent reproduction. `baseline-tests.log` and `telemetry/`
contain the standard suite result. All processes finished.

Disposable databases and temporary preferences/configuration directories were
removed after validation. To reproduce again, run `discover.py` first to create
the generated seed, then `probes.py`; use `PYTHONPATH=src:.` and isolated
`LIUXIN_PREFS_DIR`/`LIUXIN_CONFIG_DIR` values as in the recorded commands.

The user subsequently asked to fix all seven findings. Production fixes and
regression tests are implemented and all seven findings are verified as repaired.
No commit, push or PR has been requested for this repair. Keep the original data
submodule unchanged.

Repair details are in the follow-up section of the full review. Six behavioral
fixes use portable macro transactions for writes, return intralink endpoints,
delete subtrees explicitly, and materialize tree-search ID iterables. The
`set_driver` repair prepares schema state before publication, binds the complete
owner graph, preserves same-catalogue services, and retires old handles afterward.
Storage repositories resolve live macros to preserve nested transaction ownership.
Unsafe cross-catalogue switches with services or queued work are rejected; use a
separate facade. Direct SQL users must be quiescent during driver replacement.

New tests: `test_database_facade_repairs.py`,
`test_database_driver_rebinding.py`, and
`test_database_facade_repairs_postgres_live.py`. The 85 new cases are registered
in the required regression manifest (1,529 total; all 1,444 previous cases
retained). The removed endpoint xfail must stay removed. New helpers/tests are
included in formatting and lint gates.

Current repair evidence: `.tmp/database-facade-repairs-2026-11-11/`.
Focused repairs plus lifecycle: **110 passed**; final priority recheck: **10 passed**.
Live PostgreSQL: **7 passed** across the completed run/corrected setup rerun;
final priority recheck: **3 passed**. The first PostgreSQL test assumed the
SQLite multi-role pair constraint; the corrected test uses two Works because
PostgreSQL's generated schema allows one relationship per pair. SQLite still
covers multi-role pairs explicitly.

Broader facade/transaction/storage contracts: **832 passed, 40 skipped,
6 xfailed**, 1,309.99 seconds. Both endpoint-query xfails are removed and now
pass. Remaining legacy xfails cover NULL search/grouping and scalar-get behavior.
All 85 added mandatory cases passed setup, call and teardown; see
`new-case-proof.json`. Manifest: 1,529 required, all prior 1,444 retained.
The complete ratchet was not rerun, only affected suites and all new cases.

Formatting/lint, annotations, architecture/import/complexity, both source type
checkers and internal static contracts passed. The shared-cache contract check reported unexpected Any types after a concurrent
narrow mypy run; the complete configured source-plus-contract command passed
in an isolated cache. The helper's 180-second cold-run timeout was too
short here, so the final identical diagnostic check used 900 seconds. See
`mypy-contract-proof.json` (93 expected rejection locations, no failures).
Documentation: **16 passed** with a temporary alternate index/object directory
including all proposed new files. The original Git index remains untouched.

The PostgreSQL server and disposable databases were removed. Logs, telemetry,
case proof and file fingerprints remain under the evidence directory. No
commit/push/PR has been requested or performed. Keep the dirty data submodule.


## Consistency follow-up requested after sign-off discussion

The user asked to implement the remaining compound-write, NULL-query and tree-cycle
items. Implemented in the same uncommitted checkout. `unlinked_intralink`,
`dupe_interlinks` and `nest_rows` now use macro write units. Duplication testing
exposed an independent interlink allocation connection; `interlink_rows` now
uses macro allocation/population/readback, transaction-aware extrema, and rolls
back every failure. Active search and wrapper ID reads see pending macro writes.
Nesting validates persisted rows and cycles, writes only parent fields, and
updates input Rows after successful writes. Detached Rows need reload after a
later caller rollback. Tree collection uses the safe wrapper walk.

All SQL backends support None equality as IS NULL. NULL grouping and the
reconciled complete-row get(all=False) contracts are no longer xfails. New tests
are `test_database_facade_followup.py` and its `_postgres_live.py` counterpart;
they share behavioral tests. Added 93 manifest requirements (87 new backend
cases and six promoted contracts), total 1,622 with no prior cases removed.

Validation completed under `.tmp/database-facade-followup-2026-11-11/`:
all 93 new required cases passed setup/call/teardown. The 87 new backend cases
are 58 SQLite/APSW and 29 PostgreSQL cases; six promoted SQLite contracts are
also required. Broad suite: 1,013 passed, 40 existing fixture skips, zero xfails,
and two obsolete None-rejection contracts. Those now check omission of the
required argument; their full 72-case file passed. Six additional interlink
creation/readback cases passed separately. Live PostgreSQL: 36 passed, including
the seven earlier repair cases. Adapter/API: 46 passed. Distinct database checks
passing across these runs: 1,103. The complete 1,622-case ratchet was not rerun.

Docs/navigation plus regression-gate checks: 37 passed, using a disposable Git
index/object store. Formatting, lint, annotations, architecture/import/complexity
and both source type checkers passed. Basedpyright contracts passed (95 negative
examples). Default shared-cache mypy contracts reproduced unexpected Any return
types; the isolated complete check passed with a 900-second allowance, followed
by a passing canonical helper run with that cache (93 expected rejections).
See `mypy-contract-proof.json`, `mypy-contracts-confirmed.log`,
`new-case-proof.json` and `followup-result.json` for evidence and fingerprints.

Early logs retain the independent-connection duplication failure, a PostgreSQL
test-reader API mistake, and the nullable-reference typing correction. All were
resolved and verified. The disposable PostgreSQL server and test data were
removed; logs remain. The actual Git index and dirty data submodule were left
untouched during validation. On 2026-11-12 the user requested a local commit,
followed by review of `databases.custom_columns`. No push or PR was requested.
