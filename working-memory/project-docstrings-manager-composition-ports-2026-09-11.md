# Manager composition, routing, and persistence ports — 2026-09-11

## Scope and checkpoint

Continues the unfinished whole-project documentation goal after the
[private manager-support checkpoint](project-docstrings-manager-support-2026-09-11.md).
Branch remains `codex/project-docstrings` at `edf6bf05`, without a commit or push.
Existing unrelated changes and data-submodule work are retained.

Source-reviewed and documented in full:

- `storage/storage_manager/__init__.py`, `manager.py`, `mixins/__init__.py`, and
  `mixins/router.py`: transient/compatibility exports, both composition classes,
  the complete router, and all eight routing methods.
- `storage/api/storage_manager_api/__init__.py` and `repositories_api.py`:
  public facade/exports, both context methods, and historical persistence imports.
- `storage/api/persistence_api/__init__.py` and `repositories.py`: all six
  structural protocols, four repository families, and transaction/factory ports.

Newly completed: **8 modules, 10 classes, 39 functions = 57 declarations**.
Cumulative: **517 modules, 741 classes, 6,087 functions = 7,345 declarations**.
The [reviewed manifest](project-docstrings-reviewed-files.txt) contains 517 unique
paths. All eight targets matched HEAD before editing. The database unit-of-work
implementation was also read in full to substantiate provider-specific contracts;
it remains unedited and outside the reviewed manifest. Selected database-repository
methods are dependency evidence, not a completed-file claim.

## Contracts clarified

- Router primitives resolve attached facades without updating Asset/Replica
  metadata. A known publication size receives a per-object preflight before a
  separate Store lookup. Manager placement/loss policy and Replica observation
  updates are not added by these direct byte operations.
- get forwards ranges without independent validation and omits a None if_version
  keyword for narrower provider signatures. The caller owns the returned reader.
  delete always forwards both missing_ok and if_version, including defaults.
- Inventory selection and prefix validation are lazy. Prefix chooses its Store;
  an explicit mismatch rejects before lookup. All-Store selection snapshots
  attached facades in UUID order, without filtering offline Stores. Each Store
  controls its inventory order/completeness; earlier results survive a later
  failure, with no aggregate byte snapshot or error suppression.
- Optional characteristics require an attached Store and return a fresh unknown
  profile only when the optional interface is absent. Property/provider failures
  propagate. Capability/status forwarding adds no separate health probe or
  runtime validation of returned values.
- Transient manager metadata and retry state belong to one instance while Store
  bytes can persist. Context entry returns the existing manager without startup;
  exit dynamically calls close, returns None, and lets close errors propagate.
  Historical InMemoryStorageManager and journal request/result names preserve
  object identity. Persistence imports likewise re-export the exact protocols.
- Persistence protocols declare member shapes without implementing behavior or
  validating signatures at runtime. Their docs distinguish manager policy and
  reference checks from lower-level metadata operations and backend constraints.
  Repository removal does not perform physical deletion or common loss-policy
  traversal. Database adapters raise domain not-found errors on unknown removals.
- Asset digest lookup uses stored evidence, with an optional exact size and the
  database adapter's identifier ordering. Replica queries retain tombstones;
  tombstoning retains the claim and checked_at but clears other observation
  evidence. Provenance filters match direct references, not graph expansion;
  exact recreation capability is recorded evidence, not executed replay.
- Database factory begin returns a fresh inactive unit with shared repository
  ports. Enter opens the transaction; commit and rollback only request the exit
  outcome. Uncommitted normal exit rolls back, rollback overrides prior commit,
  and a later body exception prevents a requested commit. Metadata transaction
  scope excludes external Store bytes.

## Verification

- Strict structural audit and normalizer passed for the eight targets and full
  **517-file reviewed set**. Manifest uniqueness and declaration counts were
  independently recounted from source ASTs.
- All eight executable ASTs match HEAD after stripping only leading literal
  docstrings. Signatures, annotations, protocol ellipses, runtime strings,
  compatibility exports, and guard limits are unchanged. No runtime-doc exception
  was added. Ruff findings remain unchanged: four files have one pre-existing
  finding each, and the other four remain clean; no new finding was introduced.
- Selected doctests and Store/API/manager/ingest/filesystem/archive/database
  regressions: **440 passed, 301 explicitly skipped integration examples, 1 failed**,
  138.87s. The sole failure remains
  `test_storage_manager_composition.py::test_manager_module_stays_a_small_composition_root`:
  `_policy_support.py` **1,213**, `_support.py` **1,139**, and `_contracts.py` **902**
  exceed its unchanged **900-line** ceiling. This batch adds no offending file.
- Full quality runner passed: 159 formatted files, annotations across 456 modules,
  221 protected dependency modules, lint/complexity, both production type checkers,
  188 strict-mypy files, and 37 invalid examples rejected per checker.
- Migration/public-documentation/developer-link contracts: **38 passed**, 18.14s.
- **28 isolated observations** passed for identity exports, manager context cleanup,
  reader/version forwarding, publication preflight, lazy inventory and failure
  propagation, optional profiles, direct bytes without catalogue claims, structural
  protocol limitations, deferred transaction outcomes, query snapshots, and
  tombstone/not-found behavior. They use transient memory Stores and explicit
  argument, transaction, and metadata doubles. Actual database commit/rollback
  remains covered by the selected existing database regression suite. All readers
  and managers close in finally; the observation run passed on its first execution.
- All verification processes completed with observed terminal exit codes. The
  **seven** known docstring-sensitive pytest guard failures remain unresolved:
  six earlier Core/CLI/terminal ownership failures plus this composition failure.
  The earlier six were not rerun; no guard was weakened or deselected.
- Final root and data-submodule whitespace checks passed. All 113 index links,
  three main-ledger links, and three links in this note resolve locally. Source
  census and remaining-package counts agree with the manifest. The two database
  adapters and public convenience module remain byte-identical to HEAD and outside
  the manifest; existing data-generator edits and bytecode artifacts are retained.

Durable results:
`working-memory/test-results/docstrings-manager-composition-ports-2026-09-11-{regression,quality,contracts}.{log,done}`
and `docstrings-manager-composition-ports-2026-09-11-observations.json` in that directory.
Static reports: `/tmp/liuxin-docstring-manager-composition-ports-batch-2026-09-11.json`,
`/tmp/liuxin-manager-composition-ports-ast-lint-2026-09-11.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-11.json`.

Whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**, no parse
failures. Missing/blank docs remain **1,076 modules, 1,908 classes, 20,579 functions
= 23,563 declarations**. This batch completed existing incomplete documentation.
Overlapping findings: 3,676 delimiter layout, 9,983 examples, 2,808 parameter fields,
3,700 return fields, 3,370 empty parameter descriptions, 4,410 empty return
descriptions, and 28 missing summaries.

## Next

Storage has **13 unreviewed API modules** and **2 unreviewed storage_manager
implementation modules**. The remaining implementation files are
`database_unit_of_work.py` (858 lines) and `database_repository.py` (2,851 lines).
The former was read in full in this batch and still needs documentation; the
latter has only dependency excerpts reviewed. Public manager conveniences
(`convenience_api.py`, 2,002 lines) and workflow APIs also remain. Continue those
and application-manager/registry/workflow owners outside the package.

The broader metadata, catalogue/cache, formats, source/test/script/example,
inherited code, and tracked data-submodule Python remain in scope. Preserve the
runtime-doc consumer cautions and three narrow exceptions in the
[main ledger](project-docstrings-2026-09-08.md).
