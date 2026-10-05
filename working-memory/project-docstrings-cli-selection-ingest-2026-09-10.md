# CLI selection, initialization, and ingest-history docstrings — 2026-09-10

Continuation of the [whole-project descriptive reST goal](project-docstrings-2026-09-08.md),
following [CLI operations](project-docstrings-cli-operations-2026-09-10.md).
The previous goal turn made verified progress. This turn checked clean target
paths, reread the style guide, and fully reviewed four source owners and two
regression modules, including nested helpers and a runtime-created class.

Branch remains `codex/project-docstrings`, based on `edf6bf05`. Work is local,
uncommitted, and unpushed. No PR change, submodule publication, behavior fix,
test assertion change, or ownership-guard relaxation accompanies this batch.

## Reviewed contracts

- [config_cli.py](../src/LiuXin_alpha/surfaces/cli/config_cli.py): selection delegates
  explicit/environment/persisted precedence to system_profile. Config path selects
  without loading/following a named pointer to its final target; its two existence
  observations can differ under concurrent change. Show uses limited manifest
  redaction, not a comprehensive diagnostic sanitizer.
- Validation catches selection/loading Exceptions with raw text; later unexpected
  validation failures can propagate. Permission issues are warnings. Endpoint
  syntax is not connectivity, SQLite file/parent checks are not schema/integrity,
  and other databases need only a truthy selector. Configured directories require
  read/write/traverse access; absent optional paths are informational. Overall ok
  depends on failed error-severity checks, not complete readiness.
- Named-profile list continues after per-profile load failures and returns zero;
  valid means loaded, not healthy. Add writes a mode-0600 pointer and requests 0700
  for new profile directories without tightening existing parents. Remove requires
  yes and returns one for absence. Neither changes the referenced deployment;
  output failures do not undo completed pointer writes/removal.
- Connect status reports effective and persisted selection but exits according to
  persisted-manifest file existence, even if environment selection works. New
  choices load without fallback, optionally query health without interpreting its
  ok flag, exit Core, then persist. A truthy environment selector reports override
  even for the same target. Health/error text is not comprehensively redacted.
  Disconnect clears only the pointer and succeeds for already-absent state.
- [initialize.py](../src/LiuXin_alpha/surfaces/cli/initialize.py): visible input versus
  redacted display defaults, cancellation, numeric/alias menu behavior, and lack of
  default-range validation are explicit. Plan printing does not sanitize values.
  Local wizard mutation follows confirmation; PostgreSQL uses a separate namespace
  with full checks and writes optional environment output only after zero status.
  The PostgreSQL database/login role must already exist.
- Layout resolves paths and rejects driver/selector conflicts, protected output/
  manifest collisions, and a database inside its Store. It does not protect every
  hard-link alias or concurrent change. Root manifest paths are computed/checked
  even when publication is disabled. Explicit database mode lacks automatic root,
  materialization, and log paths. Directory receipts list requested paths, not
  every recursively created ancestor.
- Init creates directories, rewrites args transport selectors, and uses create=False
  for an observed existing file. Health and optional Store save/refresh/default/list
  precede session exit; manifest replacement and JSON output follow. No transaction
  spans these operations: prior effects survive later failure. Manifests are replaced,
  not merged. Result ok is unconditional at publication; health tests shutdown,
  store.saved tests a non-None receipt, and database_created uses the prior file check.
- [ingest_runs.py](../src/LiuXin_alpha/surfaces/cli/ingest_runs.py): explicit log paths
  bypass profile selection. Reports have a 128-MiB individual limit, but all are
  loaded before paging without an aggregate cap. Invalid reports are skipped;
  truthy invalid run IDs do not fall back to filename IDs. Post-load stat errors
  propagate. Unclaimed JSONL files become incomplete attempts without event reads.
  Claimed-log suppression crosses run groups; references can leave the discovery
  directory, and relative paths use process cwd. Ordering uses filesystem mtime_ns.
- List clamps paging; show can expose report=None for a newer incomplete attempt
  even when an older report exists. Inspection is not redacted. Issues preserve
  report-before-event order and duplicates, collect one extra for truncation, and
  return one for any issue. Event reads cap each physical line at 4 MiB, skip malformed
  data, and emit a synthetic oversized-line issue before stopping. Ordinary scan
  line/byte totals are not capped; overflow and other uncaught value errors propagate.
- Start lookup examines at most 100 physical lines. Resume uses only the newest
  attempt, rejects non-ingest modes, and requires yes for exact complete plus truthy
  ok. It parses defaults, assigns known recorded arguments without conversion,
  resets discovery/preflight/report/profile fields, forces top-level source/database/
  materialization and run UUID, then delegates ingest. It is not an authenticated
  log replay, path confinement mechanism, execution snapshot, or unchanged-source guarantee.
- [storage_audit.py](../src/LiuXin_alpha/surfaces/cli/storage_audit.py): standalone
  SQLite-only audit can write catalogue/registration state. Path checks precede lazy
  Library import. Stdout redirection spans construction, scan, and context exit;
  caught operational Exceptions return two. Path resolution and final projection/
  JSON/output/errors access are outside that catch. Error reports print before
  status two. Creation defaults and refresh-only manager composition are documented.
- [test_storage_audit_cli.py](../tests/surfaces/test_storage_audit_cli.py): all fake
  methods and cases now distinguish shared call observations, chatter, exact routing
  defaults, and early path refusal from actual database creation/scanner proof.
- [test_cli_init_ingest.py](../tests/surfaces/test_cli_init_ingest.py): complete Core
  fake, context/scripted-input helpers, nested callbacks, and ten cases are documented.
  The named idempotent-layout fake case calls init once; repeat-open preservation
  belongs to the real final round trip. APSW/PostgreSQL wizard cases are mocked,
  scripted input does not test terminal password echo, and the .epub fixture is
  arbitrary adoption bytes rather than a validated ebook. A runtime type('Args', ...)
  now receives explicit descriptive reST __doc__, including field descriptions.

Batch: **6 modules, 3 named classes, 71 functions = 80 AST declarations**, plus
the runtime Args fixture. Cumulative: **242 modules, 269 classes, 2,704 functions
= 3,215 reviewed AST declarations**, alongside native C and separately documented
runtime-created vacuum and Args adapters.

## Verification

- Strict audit and normalizer --check pass for all six files and all **242 reviewed
  Python files**, including the data-submodule generator.
- Five batch executable ASTs match HEAD after stripping leading docstrings. The init
  test has one deliberate exception: exactly one literal __doc__ entry inside the
  type('Args', ...) mapping. Removing only that inspected entry makes its remaining
  AST match HEAD. The normalizer guard is unchanged. Isolated execution observed
  the actual class docstring through the real default-application fixture and passed
  its original assertions.
- No new Ruff findings. Each file retains one I001; config has six existing UP032,
  initialize eleven, and ingest_runs three. Tests/storage_audit have no others.
- Final full `bash scripts/run_type_checks.sh`: **passed**, retaining 159 formatted
  files, 456 annotation-covered modules, 221 dependency-protected modules, 188 strict-
  mypy files, and 37 rejected invalid examples per checker. A rerun after adding init
  test docs caught one missing class-docstring separator blank line; that was fixed
  before the final successful gate. No formatter/type/lint guard was weakened.
- Initial five-file doctest plus init/operator/dependency/operational selection:
  **104 passed, 45 explicitly skipped examples**, 162.03 seconds. Complete six-file
  rerun after documenting init tests: **109 passed, 56 explicitly skipped integration
  examples**, 263.58 seconds. Overlapping selections are not additive test totals.
- Adjacent migration/public-doc/link/ownership selection: **63 passed, 5 known
  ownership failures**, 145.97 seconds. These remain the unchanged Core service/
  facade and terminal component/root physical-line conflicts, not new owner failures.
- All **298 parser paths** and root-help/bash/zsh/fish fingerprints match the
  CLI-foundation baseline. Calibre used its existing temporary-config fallback.
- Temporary run-history checks passed grouping/mtime, invalid IDs, claimed-log
  suppression, paging, latest-incomplete projection, issue duplication/truncation,
  malformed/oversized lines, overflow propagation, the 100-line start boundary,
  resume argument preservation/reset, and clean-run confirmation. An initial scratch
  assertion wrongly expected whitespace-prefixed valid JSON to fail report loading;
  the fixture was corrected to assert mapping acceptance and array rejection. The
  complete check set then passed without implementation/doc changes.
- Temporary selector checks passed warning/error distinctions, limited/raw redaction,
  malformed URL boundaries, persisted/effective status, unchecked health flags,
  private pointers, and retained writes/removal after output failure. Initializer
  checks passed hidden-default display, cancellation, protected paths, directory
  accounting, open-versus-create, retained layout/manifest effects, and narrow status.
  Storage-audit checks passed catch/serialization boundaries, SQLite/refresh flags,
  and report-before-error-status ordering. Fixtures were automatically cleaned;
  no real user selection configuration or external server was modified.
- Final post-format audit/normalizer and all six executable-AST comparisons pass.
  All **243 local link destinations across twenty-one working-memory files** resolve.
  Root/data-submodule diffs are whitespace-clean, staging is empty, and every
  verification process started in this turn has completed.

No whole-project pytest, installed-wheel, live PostgreSQL, remote CI, or live HTTP
listener result is claimed. The real local init/ingest regression is included above;
mocked PostgreSQL wizard checks are not server readiness evidence.

## Inventory and next work

Whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**, no parse
failures. Missing/blank docs: **1,101 modules, 2,019 classes, 22,444 functions =
25,564 declarations**. Additional overlapping findings: 4,180 delimiter-layout,
10,438 missing-example, 3,025 parameter-field, 3,964 return-field, 4,091 empty-param,
5,523 empty-return, and 28 missing-summary. Reports:
`/tmp/liuxin-docstring-current-2026-09-10.json`,
`/tmp/liuxin-docstring-reviewed-2026-09-10.json`, and
`/tmp/liuxin-docstring-cli-selection-ingest-2026-09-10.json`.

Next: CLI metadata.py (1,248 pre-review lines), postgres.py (479), storage owners,
and their whole regression modules. test_cli_metadata.py has 871 current lines and
test_cli_postgres.py 632; neither has been fully reviewed here. The 1,205-line operator-hardening suite remains
only partly source-read despite repeated execution; do not count it as documented.
Do not repeat the completed selection/init/history/audit owners or these two tests.
Keep the entire remaining source/tests/scripts/examples/submodule/native scope and
the configuration-exec caution in the main ledger. The goal remains active.
