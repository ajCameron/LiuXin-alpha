# Application storage manager and database reload tests — 2026-09-11

## Scope and checkpoint

Continues the unfinished whole-project documentation goal after the
[backend registry checkpoint](project-docstrings-backend-registry-2026-09-11.md).
Branch remains `codex/project-docstrings` at `edf6bf05`, without commit or push.
Existing worktree and data-submodule changes are retained.

Read and documented the complete application storage/store_manager.py, the
tests/storage/_mini_db.py helper, and the complete database reload regression
module, including all five nested classes and seven nested functions. All three
matched HEAD before editing. Only leading literal docstrings changed; combined
length grew from 3,203 to 4,198 lines (1,762 manager, 435 MiniDB, 2,001 reload tests).
The prepared manager descriptions were reviewed against the current source before
application; preparation alone had not entered the reviewed manifest.

Newly completed: **3 modules, 8 classes, 85 functions = 96 declarations**.
Cumulative: **562 modules, 801 classes, 6,662 functions = 8,025 declarations**.
All **206 storage source modules** now belong to the
[reviewed manifest](project-docstrings-reviewed-files.txt). This source milestone
does not complete the whole-project goal or the storage test tree: **26 of 85
storage test/helper modules** remain outside the reviewed set.

## Contracts clarified

- Construction initializes shared orchestration, optionally migrates/binds durable
  metadata, then attaches explicit Stores and chooses a default. It does not load
  every database Store row. Runtime context objects are borrowed, non-None client
  overrides retain false values, and constructor failures can leave earlier effects.
- Database ownership is fixed by repository binding. load_from_database can change
  self.db for configuration reads without rebinding the metadata repository, cache,
  or unit-of-work factory. Subsequent metadata writes still reach the original owner.
- The durable metadata context requests commit only after normal body exit and
  delegates rollback to its provider. Physical bytes are outside that transaction.
  Binding/cache properties report retained references, not health or commit proof.
- Durable attachment ensures a Store row before base UUID/policy/duplicate checks.
  Update constructs/starts a candidate and persists configuration before swapping
  facades. Some failures close a candidate; final attachment/old-facade close has
  a separate boundary. Explicit forgetting can delete a row before later removal
  fails, and never deletes Store bytes directly.
- Reload preserves valid declared identities through translation/construction
  failures, backfills missing UUIDs before offline exclusion, distinguishes additive
  from authoritative refresh, and retains unloaded identities with Replica claims.
  A late old-facade close failure can leave the replacement installed and then close
  it during error cleanup. Recovery issues are separate from bootstrap row issues;
  the missing-stores-table branch returns without running recovery.
- Recovery enumerates pending journals before UUID filtering. Invalid/started
  requests fail with issues; incomplete Asset/Location metadata is marked failed
  without an issue string. Successful hashing can reuse an existing non-DELETED
  Replica without updating its previous observation/state. The result's verified
  flag describes checked bytes even if that reused record still says CORRUPT.
- Recovery stat/read has no shared version snapshot. Replica, Item-link, and final
  operation writes have separate partial-failure boundaries. Retry returns completed
  results first, forces publication recovery when indicated, and does not fall
  through to source replay after unsuccessful recovery. Lost streams remain explicit.
- Object add_store uses startup_on_add when startup is omitted; the name/configuration
  form retains the inherited start=True default. Backing resolution may retry without
  a preferred Replica and decodes local URI bytes with os.fsdecode. Dependency
  ordering discovers declared references only and uses deterministic cycle fallback.
- Row helpers distinguish broad subscription fallback, int coercions, and UUID
  parsing. UUID backfill leaves nonblank malformed values unchanged and does not
  directly mutate the supplied row. Dependency URI parsing is less restrictive than
  actual encrypted backend construction; it does not require the encrypted scheme.
- MiniDB is a borrowed SQLite/Row test adapter with heuristic table selection,
  stubbed relation discovery, and connection-wide mutation commits. Removing an ID
  before reinference can select a different table. Supplied IDs skip existence
  checks, NULL equality does not mean IS NULL, and composite keys use only their
  first component. No production database/cache/lifecycle protocol is implied.
- Regression descriptions distinguish real local bytes/catalogues from reporting
  doubles, synthetic envelope metadata from hashing, object reopen from process
  crash recovery, and sequential interleaving from simultaneous manager writes.

## Verification

- Strict structural audit and normalizer passed for all three new files and the
  complete **562-file reviewed set**, with no findings or parse errors.
- All three executable ASTs match HEAD after removing only leading literal
  docstrings. Signatures, annotations, assertions, aliases, runtime strings, and
  guard limits are unchanged. No runtime-doc exception was added. Ruff remains
  **5 to 5** (2 manager, 1 MiniDB, 2 reload tests), with no introduced finding.
- Selected production/test doctests, full database reload regressions, backend
  registry, backed Store, manager API/contracts/composition, configuration/container,
  and backup repository regressions: **227 passed, 187 skipped, 1 failed**, 190.23s.
  Skips are explicit +SKIP integration examples, including adjacent documented
  modules. This is local selected-suite evidence, not live PostgreSQL or remote CI.
- The failure remains
  `test_storage_manager_composition.py::test_manager_module_stays_a_small_composition_root`:
  _policy_support.py 1,213, _support.py 1,139, and _contracts.py 902 exceed the
  unchanged 900-line ceiling. This is one failing pytest test; none of this batch's
  files is an offender. All seven documented Core/CLI/terminal/manager guard failures
  remain unresolved; the other six were not rerun. No guard was weakened/deselected.
- Full quality runner passed: 159 formatted files, 456 annotated modules, 221
  protected dependency modules, lint/complexity, both production type checkers,
  188 strict-mypy files, and 37 invalid calls rejected by each checker.
- Migration/public-documentation/developer-link contracts: **38 passed**, 30.69s.
- **22 targeted observations passed**, using doubles, real filesystem bytes, two
  independent SQLite catalogues, and separate SQLite observer connections. Cover
  context/dispatch validation, UUID backfill, startup defaults, recovery record reuse,
  database ownership, MiniDB commit/inference behavior, attachment/reload failure
  ordering, retry, transaction exit, exact URI bytes, and dependency-cycle fallback.
  The initial observation run incorrectly compared the portable get_rows tuple to
  an empty list; corrected that temporary assertion after reading its implementation.
  Also corrected an unexecuted temporary configuration keyword to read_only. The
  initial log/result is retained. No source/assertion change was needed in the repo.
- All verification runs completed with observed terminal exits. Source remained
  unchanged after validation began. Temporary resources were closed/cleaned up;
  no remote services were called.
- Final independent recount confirms 562 reviewed modules, 801 classes, 6,662
  functions, all 206 storage source modules, and 26 remaining storage test/helper
  modules. Root/data-submodule whitespace checks pass; all 119 local link targets
  across the index, main ledger, and this note resolve. Explicit note whitespace
  checks pass too. The existing data-generator edit and bytecode cache remain.

Durable results:
`working-memory/test-results/docstrings-application-storage-manager-2026-09-11-{regression,quality,contracts,observations}.{log,done}`,
the observations.json report, and observations-initial.{log,done} in that directory.
Static reports: `/tmp/liuxin-docstring-application-storage-manager-batch-2026-09-11.json`,
`/tmp/liuxin-application-storage-manager-ast-lint-2026-09-11.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-11.json`.

Whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**, no parse
failures. Missing/blank docs remain **1,069 modules, 1,900 classes, 20,323 functions
= 23,292 declarations**, down 55. Overlapping findings: 3,605 delimiter layout,
9,734 examples, 2,781 parameter fields, 3,671 return fields, 3,152 empty parameter
descriptions, 4,120 empty return descriptions, and 28 missing summaries.

## Next

Continue the 26 remaining storage test/helper modules: all 22 files in
tests/storage/location, api2/test_workflow_api2.py, test_storage_manager_examples.py,
test_live_backend_contracts.py, and test_storage_manager_postgres_live.py. The application
manager, MiniDB, and reload test module are complete manifest entries; do not repeat
their source review.

Then continue the wider source/test/script/example/inherited/data-submodule backlog.
Preserve runtime-doc consumer checks, the three narrow AST exceptions, and the
legacy FRBR fingerprint follow-up recorded in the
[main ledger](project-docstrings-2026-09-08.md). The whole-project goal remains active.
