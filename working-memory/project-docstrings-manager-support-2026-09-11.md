# Private manager state, contracts, and support — 2026-09-11

## Scope and checkpoint

Continues the unfinished whole-project documentation goal after the
[manager ingest/reconciliation checkpoint](project-docstrings-manager-ingest-reconciliation-2026-09-11.md).
Branch remains `codex/project-docstrings` at `edf6bf05`, without a commit or push.

Source-reviewed and documented in full:

- `storage/storage_manager/mixins/_types.py`: request/result values, replay branch,
  hash protocol, policy-ID converters, backed-Store UUID helper, and legacy type naming.
- `_state.py`: complete shared-state base and Store attachment initialization.
- `_contracts.py`: all three abstract protocols and all 39 method declarations.
- `_support.py`: all 27 revision, identity, publication/journal, inspection, and
  destination helpers, including the four no-op journal hooks.

Newly completed: **4 modules, 12 classes, 72 functions = 88 declarations**.
Cumulative: **509 modules, 731 classes, 6,048 functions = 7,288 declarations**.
The [reviewed manifest](project-docstrings-reviewed-files.txt) contains 509 unique
paths. All four targets matched HEAD before editing. Earlier partial reads of these
files are now completed source reviews; application-manager and repository excerpts
remain dependency evidence, not completed-file claims.

## Contracts clarified

- Private request/branch dataclasses retain supplied evidence without validation or
  deep copying. Request class, explicit expectations, and every retained option affect
  equality. Five request/result classes retain their historical manager module names
  for journal identity; those assignments and compatibility exports are unchanged.
- Policy-ID helpers return a record's retained ID without checking positivity, while
  other values use int conversion and a positive comparison. Conversion errors remain
  distinct from the explicit TypeError for a nonpositive converted value.
- Backed-Store UUID5 inputs use size, one preferred digest, normalized kind, and repr
  of options sorted by top-level name. Asset ID, metadata, and extra digests are omitted.
  Nested mapping order, duplicate-name order, and unstable object representations can
  change the UUID; stability is conditional on those representations.
- State initialization creates fresh maps, counters, and locks, retains supplied
  factory/resolver/policy values, attaches Stores in order, then validates the default.
  A later failure does not clean up earlier attachment/startup. Internal abstract
  contracts distinguish callers' lock responsibilities from helper-owned mutations.
- Transient metadata contexts provide no commit, rollback, or locking. Journal hooks
  are no-ops and status reporting remains empty even with completed in-memory ingests.
  Revision tokens wrap a numeric counter; lexical token ordering is not revision order.
- Digest identity matching requires a nonempty overlap with no shared disagreement,
  not identical digest sets. Digest maps retain the last repeated algorithm value.
  Hash-name normalization can collapse different raw names; requesting no algorithms
  still opens, consumes, and closes the Location reader.
- Ingest completion locks are cached by the exact size/ordered-digest tuple, not UUID
  or every equivalent content identity. Completed request reuse precedes destination
  lookup. The publication/registration catch covers Exception only; a failure hook
  can replace the original error and prevent cleanup. BaseException, earlier setup,
  later verification, and final linking/completion lie outside that catch.
- Replica inspection ignores recorded lifecycle state, compares size, and can use
  authoritative SHA-256 stat evidence alone even when other digests exist. A supplied
  Asset without SHA-256 can be classified corrupt on that path without hashing its
  other algorithms. Store lookup after stat lies outside stat-error conversion.
- Equal observation replacement still advances revision/generation. Claim registration
  requires references and an unoccupied live Location but not bytes, writable Store
  configuration, or placement compliance. First-claim lookup does not filter health.
- Destination checks compare current mode/status/create capability and per-object
  limits without reserving capacity or checking total free bytes. Unknown size skips
  characteristics lookup. Allocation uses original-name preference, only forwards
  advertised hints, and falls back to an opaque UUID key on StoreUnsupportedOperation;
  it does not independently validate returned Location ownership or uniqueness.

## Verification

- Strict structural audit and normalizer passed for all four targets and the full
  **509-file reviewed set**, with source-AST declaration counts as recorded above.
- Executable ASTs match HEAD after stripping only leading literal docstrings, including
  the no-op hook bodies. Signatures, annotations, assertions, runtime strings, type
  module identities, and guard limits are unchanged. All four Ruff baselines and
  final result sets are clean. No runtime-doc exception was added.
- Selected doctests and Store/API/manager/ingest/filesystem/archive/database regressions:
  **444 passed, 322 explicitly skipped integration examples, 1 failed**, 133.59s.
  The sole failure remains
  `test_storage_manager_composition.py::test_manager_module_stays_a_small_composition_root`.
  It now reports three files over its unchanged **900-line** limit:
  `_policy_support.py` **1,213**, `_support.py` **1,139**, and `_contracts.py` **902**.
  `_types.py` is 440 lines and `_state.py` 139. No guard was weakened or deselected.
- Full quality runner passed: 159 formatted files, annotations across 456 modules,
  221 protected dependency modules, lint/complexity, both production type checkers,
  188 strict-mypy files, and 37 rejected invalid examples per checker.
- Migration/public-documentation/developer-link contracts: **38 passed**, 19.57s.
- **30 isolated observations** passed for legacy module names and pickle round trips,
  request equality/retention, UUID representation sensitivity, constructor partial
  state, no-op transactions, counters, digest matching, reader lifetime, inspection,
  allocation, claim registration, exact lock keys, and failure-hook/interrupt boundaries.
  These use transient memory Stores and explicit isolated doubles, without claiming
  durable journal recovery, actual concurrent scheduling, or live-service coverage.
  All managers close in finally; no harness corrections were needed.
- All verification processes completed with observed terminal exit codes. The seven
  known docstring-sensitive pytest guard failures remain unresolved: six earlier
  Core/CLI/terminal ownership failures plus the composition failure above. Additional
  offending files in the same composition test do not create new distinct pytest
  failures. The earlier six were not rerun.

Durable results:
`working-memory/test-results/docstrings-manager-support-2026-09-11-{regression,quality,contracts}.{log,done}`
and `docstrings-manager-support-2026-09-11-observations.json` in that directory.
Static reports: `/tmp/liuxin-docstring-manager-support-batch-2026-09-11.json`,
`/tmp/liuxin-manager-support-ast-lint-2026-09-11.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-11.json`.

Whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**, no parse
failures. Missing/blank docs remain **1,076 modules, 1,908 classes, 20,579 functions
= 23,563 declarations**. This batch completed existing incomplete documentation.
Overlapping findings: 3,713 delimiter layout, 9,994 examples, 2,827 parameter fields,
3,729 return fields, 3,379 empty parameter descriptions, 4,420 empty return
descriptions, and 28 missing summaries.

## Next

Storage has **17 unreviewed API modules** and **6 unreviewed storage_manager
implementation modules**: remaining routing/composition/export owners and the
database repository/unit-of-work adapters. Continue through those and public manager
conveniences, then the application-manager/registry/workflow owners outside the package.
Private state/contracts/support are now completed reviews; do not count prior excerpts
as additional work. The broader metadata, catalogue/cache, formats, tests/scripts/
examples, inherited code, and tracked data-submodule Python remain in scope. Retain
the runtime-doc cautions and three narrow exceptions in the
[main ledger](project-docstrings-2026-09-08.md).
