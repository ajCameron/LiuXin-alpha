# Manager Store administration and operational status — 2026-09-11

## Scope and checkpoint

Continues the unfinished whole-project documentation goal after the
[manager-routing checkpoint](project-docstrings-manager-routing-2026-09-11.md).
Branch remains `codex/project-docstrings`, based on `edf6bf05`, without a commit
or push. Existing unrelated changes and data-submodule work are retained.

Source-reviewed and documented in full:

- `storage/api/storage_manager_api/models/stores.py` and `models/operational.py`.
- `storage/api/storage_manager_api/stores_api.py` and `operational_api.py`.
- `storage/storage_manager/mixins/stores.py` and `mixins/operational.py`.
- All of `tests/storage/api/test_storage_manager_contracts.py`, including its
  mutable row/database doubles and nested replacement-failure factory.

Newly completed: **7 modules, 20 classes, 94 functions = 121 declarations**.
Cumulative: **473 modules, 657 classes, 5,739 functions = 6,869 declarations**.
The [reviewed manifest](project-docstrings-reviewed-files.txt) contains 473 unique
paths. Partial application-manager and shared-helper reads remain dependency
evidence, not completed-file claims.

## Contracts clarified

- Direct configuration validation retains original values and checks only selected
  fields. Factory normalization, positive int-convertibility without coercion,
  shallow option collection, nonfinite scalar options, path/URI distinctions,
  default policies, and backed-Store identity derivation have explicit limits.
- Bootstrap `ok` checks only failed counts; skipped or unhandled configurations
  need not make it false. Reconciliation `conclusive` requires the COMPLETE enum
  identity and excludes unavailable/error evidence. A report can be clean without
  application, digest verification, or absence of warnings.
- Operational values retain text and aware timestamps. Health depends on issue
  severities, including equal StrEnum strings, rather than Store statuses or
  suggested actions. Exact code filtering preserves duplicate issues.
- Registry changes, startup, and old-facade cleanup have separate failure
  boundaries. Attachment ignores startup's availability result. Replacement or
  removal can take effect before close raises. Reload may therefore report a
  failed attempt after the new facade is installed; failed candidates have no
  universal cleanup guarantee.
- Registry iterators capture UUID-ordered tuples of references. Default selection
  checks facade presence without probing availability. Status iteration is lazy,
  omits the refresh keyword when false, and translates only StoreUnavailable.
  Close attempts every facade, catches BaseException, then rethrows the first;
  registry/default references remain retained.
- Operational inspection combines separate Store, journal, Replica, policy, and
  deferred-recovery reads. Actions are deduplicated in encounter order, issues
  remain, and the timestamp is sampled after collection. Each helper's error
  boundary is explicit. The transient recovery implementation returns empty;
  retry raises because there is no durable journal. Application overrides own
  actual recovery and publication replay.
- Tests identify real temporary filesystem bytes separately from disposable
  metadata and mutable row-list updates. Construction-failure retention does not
  establish rollback after attachment. The explicit-destination routing test
  does not independently prove implicit-default placement.

## Verification

- Strict structural audit and normalizer pass for all seven files and the full
  473-file reviewed set. Counts were recounted from source ASTs.
- All seven executable ASTs match HEAD after removing only leading literal
  docstrings. Signatures, annotations, assertions, runtime strings, and limits
  are unchanged; no runtime-doc exception was added. Ruff result multisets match
  the baseline for every file.
- Selected source/test doctests and Store/API/manager/ingest/filesystem/archive
  regressions, including the database reload/recovery suite: **450 passed,
  296 explicitly skipped integration examples**, 126.45s. Skips are not executed
  integration proof. The database reload suite was verification scope only.
- Full quality runner passed: 159 formatted files, annotation coverage across
  456 modules, 221 protected dependency modules, selected lint/complexity, both
  production type checkers, 188 strict-mypy files, and 37 rejected invalid examples
  per checker.
- Migration/public-documentation/developer-link contracts: **38 passed**, 19.37s.
- **25 isolated observations** passed for model predicates, validation limits,
  lifecycle failures, default selection, lazy status errors, action deduplication,
  and transient recovery. The temporary harness first needed three corrections:
  bool IDs raise ValueError, the field is supported_replica_modes, and removal
  must explicitly forget configuration to isolate a subsequent one-Store reload.
  These were harness mistakes, not runtime changes. Counter-only facade doubles
  and the status generator were closed after inspection.
- The six known docstring-sensitive ownership-test failures remain outstanding;
  those guards were neither rerun nor weakened. Root and data-submodule whitespace
  checks passed. Every verification process completed with an observed exit code.

Durable logs/exit metadata:
`working-memory/test-results/docstrings-manager-stores-2026-09-11-{regression,quality,contracts}.{log,done}`.
Observations:
`working-memory/test-results/docstrings-manager-stores-2026-09-11-observations.json`.
Static reports: `/tmp/liuxin-docstring-manager-stores-batch-2026-09-11.json`,
`/tmp/liuxin-manager-stores-ast-lint-2026-09-11.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-11.json`.

Whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**, with no
parse failures. Missing/blank docs: **1,078 modules, 1,908 classes, 20,590 functions
= 23,576 declarations**. Overlapping findings: 3,783 delimiter layout, 10,190
missing examples, 2,884 parameter fields, 3,793 return fields, 3,542 empty parameter
descriptions, 4,650 empty return descriptions, and 28 missing summaries. Structural
counts alone do not establish descriptive completeness.

## Next

Storage still has **35 unreviewed API modules** and **20 unreviewed
storage_manager implementation modules**, plus the application manager, registries,
repositories, and workflows outside that package. Continue with Asset/Replica
identity and catalogue models/contracts, then policy, retrieval, reconciliation,
ingest, and persistence families with their implementations and full regressions.

`storage/store_manager.py`, shared state/support/type/policy helpers, and the
database reload tests are not yet fully reviewed. Prior excerpts or successful
regression runs must not promote them into the completed manifest. The remaining
metadata, catalogue/cache, formats, source/test/script/example, inherited code,
and tracked data-submodule Python stay in scope. Preserve the runtime-doc consumer
cautions and three narrow exceptions in the [main ledger](project-docstrings-2026-09-08.md).
