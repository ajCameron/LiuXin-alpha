# Ingest application and operator-test docstrings — 2026-09-10

Continuation of the [whole-project descriptive reST goal](project-docstrings-2026-09-08.md),
after [CLI storage source completion](project-docstrings-cli-storage-completion-2026-09-10.md).
The intervening PR status turn made no goal progress; GitHub unavailability is not
a blocker for local documentation. Rechecked the clean target paths, read both
files fully and the current style guide, then documented all declarations.

Branch remains `codex/project-docstrings`, based on `edf6bf05`. No runtime code,
test assertion, import, signature, gate policy, commit, push, or PR was changed.

## Source-reviewed contracts

- [ingest/mixed_application.py](../src/LiuXin_alpha/ingest/mixed_application.py):
  TextOutput is structural, not runtime-checkable, and requires only write/flush.
  Request construction stores fields without path/cross-field validation; frozen
  fields do not freeze referenced callbacks/sinks. All request/result attributes
  now have descriptive ivar fields.
- Discovery ignores database_path and database_stdout, opens a transient manager,
  and emits no application setup events. Real runs resolve a local SQLite path
  and choose creation from an unreserved existence observation. Database creation
  disables backup and automatic StorageManager creation.
- Redirection covers Database construction only and is process-global. Explicit
  flush, open-complete callback, and manager construction occur before database
  context entry. Errors there are not covered by the later with-block cleanup.
  Once contexts are entered, manager exits before database. This wrapper adds
  neither all-run rollback nor cleanup of created paths.
- Existing-database Store bootstrap issues/false ok emit warnings but do not stop
  ingestion or independently affect result.ok. The result retains the report and
  original budget, and reads metadata_is_durable from the manager before exit;
  that flag is a metadata binding observation, not independent fsync verification.
- Event delivery is synchronous, passes a shallow details dictionary, and neither
  adds run identity nor catches consumer errors. Coordinator construction forwards
  one verify flag into both source/member verification, retains default handlers,
  and can reject settings after services were already constructed. Setup/run/exit
  exceptions propagate rather than returning an application failure receipt.
- [test_cli_operator_hardening.py](../tests/surfaces/test_cli_operator_hardening.py):
  fully reviewed all 26 functions, the recording Core class, nested resume capture,
  and module introduction. The fake records shallow payload copies and returns
  canned, sometimes mutually inconsistent projections. It does not perform real
  S3, integrity, migration, repair, or recovery work.
- Fixture documentation distinguishes minimal SQLite schema creation from full
  LiuXin initialization and describes manifest publication's partial effects.
  Profile tests inspect private pointer contents, selector precedence, redaction,
  and removal while keeping the real catalogue. PostgreSQL selection does not
  connect to a server. Diagnostic scrubbing is tested against a known password,
  not every possible credential form.
- Wizard tests patch input/interactivity and assert declaration/policy fields and
  command order; local initialization/status tests use real temporary SQLite and
  folders. Completion tests inspect generated text, not a live shell. Discovery
  history is real, while resume captures a reconstructed namespace after a first
  operational attempt that may return zero or one; it does not prove recovery.
- Restore tests verify aliases, digest parity, safety-copy existence, and replaced
  schema contents. No crash/concurrent reader is injected, despite the test name's
  atomicity wording. No assertion was changed to broaden the evidence claim.

Batch: **2 modules, 4 classes, 32 functions = 38 AST declarations**. Cumulative:
**273 modules, 280 classes, 3,010 functions = 3,563 reviewed AST declarations**,
plus previously documented native C and three narrowly verified runtime-doc values.
The full CLI-source milestone remains **47 modules, 10 classes, 394 functions**.

## Verification

- Strict batch audit and normalizer passed. Both also pass for all **273 reviewed
  Python files**, including the data-submodule generator. All 47 CLI source files
  were independently re-audited with no findings.
- Both executable ASTs match HEAD after stripping only genuine leading docstrings.
  No new runtime-doc exception was required. Source Ruff remains clean; the test
  retains its existing I001, F401, three UP032, and E731 findings without additions.
- Full `bash scripts/run_type_checks.sh`: **passed**. Same 159 formatted files,
  456 annotated modules, 221 dependency-protected modules, 188 strict-mypy files,
  and 37 rejected invalid examples per checker. The application file also passed
  its explicit formatter check.
- Application/operator doctests plus storage/operational/dependency/initialization
  and mixed-format-ingest regressions: **127 passed, 63 explicitly skipped**,
  141.54 seconds. Skips are marked integration examples, not suppressed failures.
- Adjacent migration/public-doc/link/ownership selection: **62 passed, 6 failed**,
  54.45 seconds. All six remain the known physical-line/statement guard conflicts
  in Core services/facade and CLI/terminal owners; these two new files add none.
  Do not report that selection as green or relax the guards without direction.
- Mocked application checks passed discovery without database/events, option and
  callback forwarding, both verify flags, SQLite constructor policy, constructor-only
  stdout capture, manager-before-database exit, continued ingest after failed
  bootstrap, report health independent of durability, and pre-context failures
  from flush/open-complete callback/manager construction. No real resource leaked:
  the failure-path Database/manager objects were mocks.
- Recovered previous-batch lifecycle checks passed signal override to 143,
  failure after report publication, early usage refusal before logging/stdout,
  and classification of late session cleanup as logging initialization failure.
  No OS signal was sent and no external Store/database was modified by these checks.
- All **285 local link destinations across twenty-five working-memory files**
  resolve. Root and data-submodule diffs are whitespace-clean; staging remains
  empty. All test, audit, quality, lifecycle, and hygiene processes are terminal.

No whole-project pytest, installed-wheel, remote CI, live PostgreSQL, live S3,
interactive curses, or shell-completion execution is claimed.

## Inventory and next work

Whole inventory: **2,730 modules, 4,400 classes, 35,119 functions**, no parse
failures. Missing/blank docs: **1,100 modules, 2,014 classes, 22,167 functions =
25,281 declarations**. Overlapping findings: 4,133 delimiter-layout,
10,403 missing-example, 3,015 parameter-field, 3,953 return-field, 4,073 empty-param,
5,505 empty-return, and 28 missing-summary. Reports:
`/tmp/liuxin-docstring-ingest-application-operator-2026-09-10.json`,
`/tmp/liuxin-docstring-reviewed-2026-09-10.json`, and
`/tmp/liuxin-docstring-current-2026-09-10.json`.

Next: the remaining ingest package, starting with __init__.py, models.py, stores.py,
adding.py, and remote_html.py, then its sources/pipelines and corresponding tests.
The mixed-format coordinator excerpt was dependency-read, not documented as a
complete module. Continue through all remaining source, scripts, tests, examples,
and tracked data-submodule Python afterward. Preserve the configuration-exec and
runtime-docstring-consumer cautions in the main ledger. The full goal stays active.
