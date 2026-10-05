# Manager routing, Location handles, and regressions — 2026-09-11

## Scope and checkpoint

Continues the unfinished whole-project documentation goal after the
[storage-values checkpoint](project-docstrings-storage-values-2026-09-11.md).
Branch remains `codex/project-docstrings`, based on `edf6bf05`, without a commit
or push. Existing unrelated changes and data-submodule work are retained.

Source-reviewed and documented in full:

- `storage/api/storage_manager_api/location_api.py`, including its private router
  protocol and every BoundLocation operation.
- `router_api.py`, `location_factory.py`, and the manager `errors.py` hierarchy.
- The complete previously 2,941-line
  `tests/storage/api2/test_storage_manager_api2.py`: all memory sessions/Stores,
  routing/ingest/retrieval/topology fixtures, nested repositories, failure hooks,
  recipe/factory helpers, and every regression function.

Newly completed: **5 modules, 25 classes, 170 functions = 200 declarations**.
Cumulative: **466 modules, 637 classes, 5,645 functions = 6,748 declarations**.
The [reviewed manifest](project-docstrings-reviewed-files.txt) contains 466 unique
paths. Prior read-ahead was confirmed unchanged from HEAD before editing.

## Contracts clarified

- BoundLocation and LocationFactory retain live manager references without
  constructor validation or resource ownership. Frozen eq=False wrappers retain
  identity equality. Their operational values are not cached and their manager
  is omitted from generated representations.
- Bound reads omit the if_version keyword for None, preserving unconditional
  calls through older/narrower router signatures. Explicit versions are forwarded
  and may expose unsupported signatures. Range checks and errors belong to the
  implementation; the handle adds no suppression or stream cleanup.
- Router try_stat catches only StoreNotFound; exists compares its result with
  None. Default characteristics returns an unknown profile without Store lookup.
  iter_file_infos is lazy and can yield records before later stat failures.
- read_bytes owns its returned reader context and materializes the selected
  stream without an independent byte limit. write_bytes wraps data in BytesIO,
  forwards len(data), and does not explicitly close that temporary source.
- Default copy stats the source, uses its version only with advertised conditional
  reads, then forwards size/digest expectations to destination put. The destination
  owns integrity validation. A source-reader close failure can occur after successful
  destination publication; no rollback is added.
- Default move checks conditional deletion and a source version before copy.
  Default copy stats again, but deletion uses the first version. A changed source
  can therefore leave both new payloads present when deletion rejects. Dynamic
  overrides remain authoritative; this base provides no cross-Store transaction.
- LocationFactory forwards Asset selection preferences/verification and exact
  Replica IDs. Its explicit Asset alias calls from_id dynamically, preserving
  overrides. It performs no physical resolution work itself.
- Manager error categories distinguish unknown configuration, missing catalogue
  identities, selection with no eligible Replica, incomplete required composite
  members, unsatisfied policy, and stale reconciliation plans. They remain within
  the shared StorageError hierarchy, separate from legacy package errors.
- Test descriptions identify real in-memory hashes/bytes and selected temporary
  file/ZIP exports separately from disposable catalogue state, fixture declarations
  of verification, synthetic capacity/version data, or unexecuted recipe plans.
  The memory session's post-publication stat, recorder-only options, ignored
  revisions, nested failures, and constructor/default limitations are explicit.

## Verification

- Strict structural audit and normalizer pass on all five files and the complete
  466-file reviewed set. Final counts were recounted from source ASTs.
- All five executable ASTs match HEAD after removing only leading literal
  docstrings, including the final example refinement. Signatures, annotations,
  assertions, runtime strings, and limits are unchanged. No runtime-doc exception
  was added. Final Ruff result multisets match their baselines.
- Final selected source/test doctests and Store/API/manager/ingest/filesystem/archive
  regressions: **383 passed, 268 explicitly skipped integration examples**, 32.05s.
  Skips are not executed integration proof. The first run also passed (384/267);
  one default-characteristics example was then changed from an artificial unbound
  call to a normal manager-context example, and the full selection rerun. Its
  explicitly skipped context explains the one-count shift; no failure was hidden.
- Full quality runner passed: 159 formatted files, annotation coverage across 456
  modules, 221 protected dependency modules, selected lint/complexity, both production
  type checkers, 188 strict-mypy files, and 37 invalid examples rejected per checker.
- Migration/public-documentation/developer-link contracts: **38 passed**, 18.09s.
- **15 isolated observations** passed for identity/no-validation wrappers, None
  versus explicit version forwarding, unknown default characteristics, temporary
  source ownership, post-publication reader-close errors, changing-version move
  failures, lazy enumeration failures, and factory alias dispatch. Memory Stores,
  retained streams, and the generator were closed after inspection.
- The six known docstring-sensitive ownership-test failures remain outstanding.
  Those unrelated guards were neither rerun nor weakened by this batch.
- Root and data-submodule whitespace checks passed. All verification handles
  completed with observed exit-zero results. Existing data-generator edits and
  bytecode artifacts remain retained.
- Final local-link checks resolve all 113 index links, three main-ledger links,
  and three links in this note. The three read-ahead modules are byte-identical
  to HEAD and absent from the completed manifest; remaining package counts match.

Durable logs/exit metadata:
`working-memory/test-results/docstrings-manager-routing-2026-09-11-{regression,quality,contracts}.{log,done}`.
The earlier passing regression result is retained with the `regression-initial`
suffix. Observations:
`working-memory/test-results/docstrings-manager-routing-2026-09-11-observations.json`.
Static reports: `/tmp/liuxin-docstring-manager-routing-batch-2026-09-11.json`,
`/tmp/liuxin-manager-routing-ast-lint-2026-09-11.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-11.json`.

Whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**, with no
parse failures. Missing/blank docs: **1,078 modules, 1,912 classes, 20,618 functions
= 23,608 declarations**. Overlapping findings: 3,784 delimiter layout, 10,217
missing examples, 2,884 parameter fields, 3,793 return fields, 3,579 empty parameter
descriptions, 4,714 empty return descriptions, and 28 missing summaries. Structural
counts alone do not establish descriptive completeness.

## Next: manager Store and operational contracts

Storage still has **39 unreviewed API modules**, all 22 storage_manager implementation
modules, and additional registries/repositories/workflows. Continue with manager
`models/stores.py` (810 lines), `stores_api.py` (518), `models/operational.py` (196),
and `operational_api.py` (110), then remaining model/API families and persistence.
The complete stores model, operational model, and operational API were read ahead;
they remain unedited and outside the manifest. stores_api.py still needs a full read.
Relevant read-ahead findings:

- StoreBackingReference checks bool/positive int-convertibility but retains supplied
  IDs; conversion is not coercion. Materialization Store references must be UUIDs.
- StoreConfiguration directly validates selected UUIDs/backing requirements,
  nonblank required text, and backend-option names/value types. It retains original
  text and does not generally validate tags, modes, default policy IDs, optional
  strings, or boolean types. Backing requires truthy read_only and rejects direct
  self-materialization. Scalar options include nonfinite floats; do not claim strict
  JSON serialization merely from the accepted type list.
- for_backend normalizes mode enums, tuple-collects tags/options, uses uuid4 only
  for None UUID, and strips string roots. PathLike roots resolve/expand to file URIs.
  for_backed_backend builds an asset URI and read-only backing reference, without
  deriving a stable UUID itself. filesystem forwards through the general factory.
- _filesystem_root_uri rejects non-file parsed schemes, but accepts a file URI
  unchanged after stripping without checking hostname/path. Windows drive strings
  can be interpreted as schemes on this host. _option_pairs only shallowly collects
  mapping items or iterable pairs; it does not freeze nested values.
- Bootstrap issue values add no validation. Bootstrap counts reject negativity
  and handled totals above discovered; ok tests only failed_configurations == 0,
  so skipped/unhandled configurations and issues need not make it false.
- StoreStatusObservation checks only its UUID. Reconciliation plans validate UUIDs
  and count comparisons; conclusive requires enum identity COMPLETE, no unavailable
  Replicas, and no errors, but does not require digest verification or no warnings.
  Report clean additionally excludes missing/unexpected/corrupt evidence and report
  errors; it does not require applied. Preview reports reject nonempty updated IDs.
- Operational issue/action strings are tested with strip but retained unchanged;
  severity and attribution types are not coerced. Status validates timestamp
  awareness and derives healthy solely from warning/error issue severities, not
  Store statuses or suggested actions. issues_for compares codes exactly.
- The operational API mixes health/journal inspection with explicit recovery/retry
  methods. Inspect implementations before describing their mutation, availability,
  retry, and partial-publication guarantees; abstract prose alone is insufficient.

The remaining metadata, catalogue/cache, formats, source/test/script/example,
inherited code, and tracked data-submodule Python stay in the whole-project goal.
Preserve the runtime-doc consumer cautions and three narrow exceptions in the
[main ledger](project-docstrings-2026-09-08.md).
