# Storage configuration, compatibility, and migrations — 2026-09-11

## Scope and checkpoint

Continues the unfinished whole-project documentation goal after the
[backup persistence checkpoint](project-docstrings-backup-persistence-2026-09-11.md).
Branch remains `codex/project-docstrings` at `edf6bf05`; no commit or push.
Existing changes and data-submodule work are retained.

Read and documented these seven storage source modules in full: location,
storage_types, store_factory, store_container, migrations, single_file, and
store_spec_utils. Also read and documented all of
`tests/storage/test_backend_registry.py`, including its row double and nested
builder. Private helpers, the nested column selector, and both getter/setter
definitions are included. All eight files matched HEAD before editing; their
combined length grew from 1,813 to 2,727 lines through docstrings only.

Newly completed: **8 modules, 5 classes, 65 functions = 78 declarations**.
Cumulative: **554 modules, 788 classes, 6,530 functions = 7,872 declarations**.
The [reviewed manifest](project-docstrings-reviewed-files.txt) contains 554 unique
paths. Exactly two storage source modules remain outside it: backend_registry.py
and the application-facing store_manager.py. Earlier API, manager composition/
mixins, drivers, plugins, and backup/workflow source milestones remain complete.

## Contracts clarified

- Legacy location.Location remains an empty, instantiable compatibility class,
  distinct from the UUID/key Location in storage.api. storage_types identifiers
  are ordinary int aliases and an int-or-str union without identity validation.
- build_store delegates configuration/context and the result to its registry.
  Registry/builders own validation and resource effects; the wrapper does not
  persist configuration, filter credentials, or add a startup step.
- Row translation supports mapping and legacy row interfaces. Non-mapping
  subscription errors can be masked by attribute fallback; declared unavailable
  columns return the default before attribute lookup. Scalar conversions admit
  bool/truncatable integer values, use selected boolean spellings, and preserve
  tag duplicates. Overflow and other unhandled conversion errors propagate.
- Missing Store UUIDs derive from the row ID before the root URI, without
  database identity in the key or a write-back. Equal row IDs across catalogues
  can therefore derive equal UUIDs. Backend kind aliases remain as supplied.
- Projection treats an empty allowed-column set as unrestricted. It computes
  values before filtering, so even excluded fields can raise serialization errors.
  store_id and legacy store_url are not emitted. Default null omission prevents
  an update from clearing old nullable columns/policy values.
- Backend policy filtering examines option names and supported value shapes,
  without scanning scalar values or URLs for secrets. The row's selected kind
  determines its section; the JSON backend label is ignored during reading.
  Stripped keys are not deduplicated and can fail sorting or final validation.
  Serialization builds a fresh payload rather than preserving unknown sections.
- Malformed outer policy JSON is ignored, while malformed recognized manager
  extensions raise. Version equality admits True/1.0; extended modes replace the
  whole legacy mode set. Backing IDs are integer-converted before positive-ID
  checks, and invalid optional Replica IDs become absent. No reference is resolved.
- StoreContainer borrows its Store/database and caches exact status objects.
  Failed backend calls retain prior cache. Reload replaces only configuration,
  checking UUID after decoding; it does not reconfigure the live Store or clear
  status. Save performs row writes before reload validation, with no encompassing
  transaction. A changed UUID can be persisted before reload raises. Default
  null omission leaves old values intact. Missing-row deletion retains the old
  ID; successful deletion clears only the ID and leaves bytes/Store/cache alone.
- SingleFileStatus requires non-None callbacks even for fully supplied facts,
  using assertions rather than callability validation. Missing facts are queried
  existence/size/hash regardless of an absent result. UUID may be None despite
  its str annotation. Public setters reject assignment; callback replacements
  are unvalidated. Rechecks update facts incrementally, retain partial updates
  on failure, never advance last_checked, and return True on normal completion
  even for no-op or absent-file cases.
- Migration capability/dialect checks inspect attribute presence/callability,
  without validating a connection or every required macro. Missing ledger/journal
  DDL runs within the provider's transaction; refresh and ledger-record insertion
  follow outside that context. Late failure can leave created tables and only
  some identities. Retry adopts existing names, without repairing journal columns
  or missing indexes. Existing migration records are not updated. Envelope
  recording performs independent truthiness/int conversions without upgrading
  actual envelopes; caller details can override applied_during_bootstrap.
- Regression docs distinguish real local encryption/archive readback from inert
  S3 injection and synthetic database rows. ISO/ZIP/TAR reopen uses new backend
  objects within one process. The RAR test verifies staged bytes and never invokes
  rar-custom or seals a RAR image.

## Verification

- Strict structural audit and normalizer pass for the entire **554-file reviewed
  set**, including all eight new files. No reviewed audit findings or parse errors.
- All eight executable ASTs match HEAD after removing only leading literal
  docstrings. Signatures, annotations, decorators, runtime strings, assertions,
  and guard limits are unchanged. No runtime-doc exception was added. Ruff is
  **18 to 18**, with no newly introduced finding.
- Selected production/test doctests and database reload/migration, configuration,
  factory/backend, manager, and backed-Store regressions: **107 passed, 74 skipped,
  1 failed**, 170.08s. All skips are explicit +SKIP integration examples, including
  examples in adjacent previously documented modules. Actual local archive and
  encryption regressions passed; this is not a live PostgreSQL/S3 or process-restart
  verification claim.
- The sole failure remains
  `test_storage_manager_composition.py::test_manager_module_stays_a_small_composition_root`:
  _policy_support.py 1,213, _support.py 1,139, and _contracts.py 902 exceed the
  unchanged 900-line ceiling. These are one failing pytest test, not three.
  No file edited in this batch is an offender. All seven previously recorded
  docstring-sensitive pytest guard failures remain unresolved; the other six
  Core/CLI/terminal failures were not rerun and no guard was weakened or deselected.
- Full quality runner passed: 159 formatted files, annotation coverage across
  456 modules, 221 protected dependency modules, lint/complexity, both production
  type checkers, 188 strict-mypy files, and 37 rejected invalid calls per checker.
- Migration/public-documentation/developer-link contracts: **38 passed**, 30.35s.
- **19 targeted observations** passed on their first execution. Real temporary
  SQLite rows verify configuration null omission, writes before identity failure,
  deletion behavior, migration partial effects/retry, and adoption of an incomplete
  journal. Pure/double-based observations cover codec coercion, name-only filtering,
  UUID derivation, callbacks/cache, capability checks, and SQL dialect rendering.
  The PostgreSQL DDL observation checks generated text only. Temporary database
  connections and files were closed/cleaned up.
- All verification processes completed with observed terminal exits. No source
  changes were made after those checks began.
- Final independent recount confirms 554 unique reviewed modules, 788 classes,
  6,530 functions, and exactly two remaining storage source modules. Root and
  data-submodule whitespace checks pass; all 119 local link targets across the
  index, main ledger, and this note resolve. The existing data-generator edit
  and untracked data-submodule bytecode cache remain untouched.

Durable results:
`working-memory/test-results/docstrings-storage-configuration-2026-09-11-{regression,quality,contracts}.{log,done}`
and `docstrings-storage-configuration-2026-09-11-observations.json` in that directory.
Static reports: `/tmp/liuxin-docstring-storage-configuration-batch-2026-09-11.json`,
`/tmp/liuxin-storage-configuration-ast-lint-2026-09-11.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-11.json`.

Whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**, no parse
failures. Missing/blank docs remain **1,070 modules, 1,907 classes, 20,408 functions
= 23,385 declarations**, down 45 in this batch. Overlapping findings: 3,619 delimiter
layout, 9,777 examples, 2,788 parameter fields, 3,676 return fields, 3,188 empty
parameter descriptions, 4,162 empty return descriptions, and 28 missing summaries.

## Next

Read and document the complete backend_registry.py (1,232 lines) and application
store_manager.py (1,499 lines), plus remaining adjacent helpers/tests. Their
dependency excerpts in this batch are not complete-source review claims. The full
backend-registry test module is now reviewed; database reload tests and the
miniature database adapter remain outside the manifest despite earlier selections
for regression evidence.

Then continue the whole source/test/script/example/inherited/data-submodule
backlog. Preserve the behavioral cautions, runtime-doc consumer checks, and three
narrow exceptions in the [main ledger](project-docstrings-2026-09-08.md). Do not
silently repair execution while documenting compatibility behavior or count this
storage milestone as completion of the whole-project goal.
