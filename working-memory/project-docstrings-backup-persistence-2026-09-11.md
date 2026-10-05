# Backup persistence, registration, and prototype — 2026-09-11

## Scope and checkpoint

Continues the unfinished whole-project documentation goal after the
[backup execution checkpoint](project-docstrings-backup-execution-2026-09-11.md).
Branch remains `codex/project-docstrings` at `edf6bf05`, without a commit or push.
Existing unrelated changes and data-submodule work are retained.

Source-reviewed and documented all three remaining backup implementation modules:
`backup_workflow_repository.py`, `backup_artifact_registry.py`, and
`prototype_pipeline.py`, plus their three complete regression modules under
`tests/storage/backup`. The repository and registry were read in full; the full
prototype read included its reporting values, console methods, and nested progress
callback. All six files matched HEAD before editing. Their combined source grew
from 1,846 to 3,216 lines through docstrings only.

Newly completed: **6 modules, 8 classes, 70 functions = 84 declarations**.
Cumulative: **546 modules, 783 classes, 6,465 functions = 7,794 declarations**.
The [reviewed manifest](project-docstrings-reviewed-files.txt) contains 546 unique
paths. All six backup implementation modules and all five backup regression
modules are complete, alongside the previously completed 65 storage API,
22 storage_manager implementation, and two storage/workflows modules.

## Contracts clarified

- Repository writes borrow the database and do not open an encompassing
  transaction or acquire a revision/concurrency guard. Intent replacement updates
  its workflow row, deletes old sources, and inserts replacements in order; a later
  insertion failure can leave new intent with only some sources. Prior state/output/
  presence rows remain, requiring caller coordination.
- Checkpoint writes replace the first state search result before updating workflow
  status/error. Missing state inherits the workflow row's status, and updated_at is
  not encoded. Result recording prefers supplied final_checkpoint without comparing
  every result field. A result with no output leaves an older output row intact.
- Reference envelopes preserve Location routes, with historical raw-input fallback
  for invalid JSON, non-object JSON, and unknown versions. Recognized malformed
  envelopes raise. Version checks use equality, admitting True as version 1.
  Source envelopes carry identifier/provenance, while kind/path/expectations remain
  scalar fields. Recognized source provenance wins over the legacy Replica column.
- Report reconstruction int-converts counters, applies truthiness to ok, retains
  digest_verified, normalizes empty errors, and requires any staged reference to
  decode to a Location. These are selected conversions, not full type/evidence
  validation. Row updates filter unknown fields only for allowed_columns objects;
  they still sync when every proposed field is skipped. Enumeration uses the first
  present adapter interface rather than probing callability or trying after errors.
- Presence links deduplicate by Store and exact archive path. Existing keys retain
  old source/workflow/protection metadata; physical member bytes are not inspected.
  Metadata constraints and database cascades determine deletion behavior.
- Registry reuse returns a visible existing registration before checking the file,
  changing name/link options, or retrying manager attachment. New registration can
  leave Store/output/link metadata before an attachment failure. A subsequent retry
  does not repair the missing attachment. Existing Store rows retain names/kinds/
  capabilities, apart from filling a missing UUID.
- Unnamed member fallback uses the number of links newly inserted, not the source
  ordinal. If source-000000 already exists for the reused Store, subsequent unnamed
  sources can repeatedly collide with it. A fresh call's inserted count can therefore
  differ from lookup's current Store-wide link total.
- Local artifact URI parsing uses the decoded path and ignores authority/query/
  fragment. Routed registry resolution checks current Store-root containment, without
  locking pathname state. Prototype output-path projection separately ignores a
  Location's Store UUID and has no resolved containment check.
- Prototype construction creates output directories before numeric conversion and
  run validation. It captures extension/flag settings, selects schema-dependent
  indexing, and saves the constructed workflow's effective declaration rather than
  the planner's pre-construction intent. Each returned checkpoint is persisted;
  terminal failure raises after that state is saved. Earlier rows/images/packs can
  remain without a returned partial aggregate, and existing workflow IDs are not resumed.
- The FRBR fallback directly inserts/updates Asset and Replica rows by storage key,
  retaining Asset identity when bytes change and leaving absent-source rows intact.
  It stores the legacy get_file_hash result—SHA-512 hex plus decimal byte length—in
  SHA-256-named fields. This behavior was verified and documented, not corrected in
  this docstring pass. Treat digest labeling and raw identity mutation as follow-up
  behavior issues; they are not evidence of SHA-256 verification or immutable identity.
- FRBR traversal uses sorted current files without a version snapshot or additional
  file-symlink containment check. Row/hash/callback errors propagate after prior
  effects; finish time/done occur only on normal completion. Update counters follow
  Replica mutation, so timestamps can count despite unchanged bytes. Console output
  tracks index/pack widths separately, uses character counts/carriage returns without
  terminal detection, and flushes before remembering progress state.
- Regression docs distinguish synthetic persistence evidence and fake archive
  markers from the actual tool-backed test. The real prototype test reopens Library/
  database and reconstructs Stores within the same process after renaming the source
  directory; it does not claim an operating-system process restart.

## Verification

- Strict structural audit and normalizer passed for all six files and the entire
  **546-file reviewed set**. Independent AST recount confirms 783 classes,
  6,465 functions, manifest uniqueness, and nine remaining storage source modules.
- All six executable ASTs match HEAD after stripping only leading literal
  docstrings. Signatures, annotations, aliases, runtime literals, assertions, and
  guard limits remain unchanged. No runtime-doc exception was added. Ruff remains
  **16 to 16**, with no newly introduced finding. No direct docstring consumer was
  found in the backup implementation/test packages.
- Production/test doctests and adjacent workflow/backup/manager/archive regressions:
  **172 passed, 160 explicitly skipped integration examples, 1 failed**, 103.03s.
  Installed mksquashfs/unsquashfs supported the genuine prototype archive build and
  exact Unicode-path/binary-content readback after catalogue reopen and source rename;
  that test passed. The only reported skips were explicitly skipped doctest examples.
- The sole regression failure remains
  `test_storage_manager_composition.py::test_manager_module_stays_a_small_composition_root`:
  `_policy_support.py` 1,213, `_support.py` 1,139, and `_contracts.py` 902 exceed
  the unchanged 900-line ceiling. No newly edited file is an offender.
- Full quality runner passed: 159 formatted files, annotations across 456 modules,
  221 protected dependency modules, lint/complexity, both production type checkers,
  188 strict-mypy files, and 37 invalid examples rejected per checker.
- Migration/public-documentation/developer-link contracts: **38 passed**, 32.83s.
- **19 isolated observations** passed on their first execution. Pure codec/interface
  and progress checks are combined with real temporary SQLite row writes, local
  paths/symlinks, and explicit manager-failure doubles. They verify partial source
  replacement, retained output metadata, attachment retry behavior, fallback-member
  collisions, metadata preservation, the exact FRBR legacy fingerprint, same-identity
  byte updates, absent-source retention, and callback failure after insertion.
  Temporary files and database connections were cleaned up. The observations did
  not invoke archive tools or live remote services; genuine tool verification is
  the separate regression evidence above.
- All verification processes completed with observed terminal exits. The **seven**
  known docstring-sensitive pytest guard failures remain unresolved: the six earlier
  Core/CLI/terminal failures and manager composition above. The earlier six were
  not rerun; no guard was weakened or deselected.

Durable results:
`working-memory/test-results/docstrings-backup-persistence-2026-09-11-{regression,quality,contracts}.{log,done}`
and `docstrings-backup-persistence-2026-09-11-observations.json` in that directory.
Static reports: `/tmp/liuxin-docstring-backup-persistence-batch-2026-09-11.json`,
`/tmp/liuxin-backup-persistence-ast-lint-2026-09-11.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-11.json`.

Whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**, no parse
failures. Missing/blank documentation remains **1,070 modules, 1,908 classes,
20,452 functions = 23,430 declarations**, down 73 in this batch. Overlapping
findings: 3,636 delimiter layout, 9,802 examples, 2,798 parameter fields,
3,685 return fields, 3,196 empty parameter descriptions, 4,174 empty return
descriptions, and 28 missing summaries.

## Next

Storage has **nine unreviewed source modules**, all directly under storage:
backend_registry (1,232 lines), location (8), migrations (188), single_file (214),
storage_types (19), store_container (132), store_factory (34), store_manager
(1,499), and store_spec_utils (436). Continue with the remaining compatibility/
specification/factory helpers and the large registry/application-manager owners,
plus their complete tests. These line counts are inventory, not complete-source
review claims. All backup/workflow implementation batches are now finished.

The broader metadata, catalogue/cache, formats, source/test/script/example,
inherited code, and tracked data-submodule Python remain in scope. Preserve the
runtime-doc consumer cautions and three narrow exceptions in the
[main ledger](project-docstrings-2026-09-08.md).
