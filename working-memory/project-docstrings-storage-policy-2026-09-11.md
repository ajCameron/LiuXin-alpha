# Storage policy contracts and shared mechanics — 2026-09-11

## Scope and checkpoint

Continues the unfinished whole-project documentation goal after the
[Composite/retrieval checkpoint](project-docstrings-composite-retrieval-2026-09-11.md).
The interrupted batch's saved verification was inspected before resuming; its
completed regression, quality, and contract jobs were not restarted. Branch
remains `codex/project-docstrings` at `edf6bf05`, without a commit or push.

Source-reviewed and documented in full:

- `storage/api/storage_manager_api/models/policies.py` and `policies_api.py`.
- `storage/storage_manager/mixins/policies.py` and `_policy_support.py`, including
  the nested derivation-cycle traversal.
- Complete `tests/storage/api/test_storage_policy_apis.py`.

Newly completed: **5 modules, 14 classes, 78 functions = 97 declarations**.
Cumulative: **497 modules, 700 classes, 5,915 functions = 7,112 declarations**.
The [reviewed manifest](project-docstrings-reviewed-files.txt) contains 497 unique
paths. Each target matched HEAD before editing; no runtime-doc exception was added.

## Contracts clarified

- Policy constructors check selected numeric relationships without coercing integer
  or enum values, validating every field, or copying nested containers. Assessments
  and plans retain supplied evidence; their predicates do not independently prove
  readability, policy compliance, or recoverability.
- Policy CRUD uses registered IDs and optional revisions. Candidate updates and
  Asset policy assignment restore the old value when global recreation validation
  raises, including BaseException. Later revision-allocation/final-assignment failures
  lie outside that explicit restoration block; a transient manager can retain a
  candidate definition with the old revision. No broader rollback is promised.
- Asset policy assignment replaces both references; an omitted or explicit None
  clears that reference. Declared policy IDs are validated before Asset lookup.
  Effective policy resolution distinguishes Asset references from manager defaults;
  first-placement capture is a separate mutation and live claims prevent recapture.
- Assessments combine current status/size readability with recorded VERIFIED state
  and configured tags. Copy planners instead count recorded VERIFIED eligible claims
  without that current readability probe. Proposed work neither reserves destinations
  nor executes publication, verification, removal, or repair.
- Separation capacity takes the minimum of per-dimension bucket counts. It is not
  a search for a jointly compatible subset. Missing topology values share buckets;
  dimensions are selected by enum identity. Destination ranking uses preferred tags,
  default Store, name, and UUID, then checks occupancy, writable capacity, and buckets.
- Current recoverability and policy-based retention feasibility use different
  predicates and cycle-check ordering. Recreation planning memoizes Asset outcomes,
  chooses viable routes by step count and derivation ID, and orders prerequisites
  before consumers. External artefact checks delegate to the configured resolver;
  ordinary resolver exceptions mean unavailable while BaseException propagates.
- Source expansion includes every Composite member and recipe input, with managed
  executable/dependency Assets included for recovery/cycle checks. This is distinct
  from provenance graph traversal, which can exclude those artefacts.

## Verification

- Strict structural audit and normalizer passed for the five targets and the full
  **497-file** reviewed set. AST counts were independently recounted.
- All five executable ASTs match HEAD after removing only leading literal docstrings.
  Signatures, annotations, assertions, runtime strings, and guard limits are unchanged.
  Per-file Ruff result multisets match their baselines; the two existing findings in
  the model/API files remain unchanged.
- Saved selected doctest/Store/API/manager/ingest/filesystem/archive/database regression
  result: **458 passed, 324 explicitly skipped integration examples, 1 failed**, 149.96s.
  The sole failure is
  `test_storage_manager_composition.py::test_manager_module_stays_a_small_composition_root`:
  `_policy_support.py` is **1,213 lines**, exceeding the unchanged **900-line** ceiling.
  The policy mixin is 783 lines. No guard was weakened or deselected.
- Saved full quality runner passed: 159 formatted files, annotations across 456
  modules, 221 protected dependency modules, lint/complexity, both production type
  checkers, 188 strict-mypy files, and 37 rejected invalid examples per checker.
- Saved migration/public-documentation/developer-link contracts: **38 passed**, 22.06s.
- **28 isolated observations** passed over model values and transient-manager memory
  Stores, including offline assessment/planning differences, capacity overestimation,
  field clearing, first-placement capture, candidate restoration boundaries, recursive
  feasibility, and resolver failures. These do not claim durable or external-service
  coverage; manager cleanup ran in finally.
- All three saved jobs have terminal `.done` records with exit codes 1/0/0 respectively.
  Root and data-submodule whitespace checks passed when resuming the checkpoint.
  Existing data-generator edits and bytecode artifacts remain retained.

There are now **seven known docstring-sensitive pytest guard failures**: the six
earlier Core/CLI/terminal ownership failures plus the manager composition failure
above. The earlier six were neither rerun nor weakened. Their unresolved status is
separate from the passing behavioral, lint, formatting, typing, and dependency work.

Durable evidence:
`working-memory/test-results/docstrings-storage-policy-2026-09-11-{regression,quality,contracts}.{log,done}`
and `docstrings-storage-policy-2026-09-11-observations.json` in that directory.
Static reports: `/tmp/liuxin-docstring-storage-policy-batch-2026-09-11.json`,
`/tmp/liuxin-storage-policy-ast-lint-2026-09-11.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-11.json`.

Whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**, no parse
failures. Missing/blank docs: **1,076 modules, 1,908 classes, 20,579 functions =
23,563 declarations**. Overlapping findings: 3,756 delimiter layout, 10,105 examples,
2,860 parameter fields, 3,767 return fields, 3,452 empty parameter descriptions,
4,511 empty return descriptions, and 28 missing summaries.

## Next

Storage has **22 unreviewed API modules** and **13 unreviewed storage_manager
implementation modules**, plus application-manager, registry, repository, and workflow
owners outside those packages. Continue with derivation models/API/implementation,
then reconciliation, ingest, persistence, and remaining composition/support owners.
The rest of the project, including metadata, catalogue/cache, file formats, tests,
scripts, examples, inherited code, and tracked data-submodule Python remains in scope.
Retain the runtime-doc consumer cautions and three narrow exceptions in the
[main ledger](project-docstrings-2026-09-08.md).
