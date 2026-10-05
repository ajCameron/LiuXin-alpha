# Derivation provenance, recipe values, and replay plans — 2026-09-11

## Scope and checkpoint

Continues the unfinished whole-project documentation goal after the
[storage-policy checkpoint](project-docstrings-storage-policy-2026-09-11.md).
That interrupted batch was first reconciled into the manifest and handoff from
its completed saved jobs, without restarting those runs. Branch remains
`codex/project-docstrings`, based on `edf6bf05`, without a commit or push.

Source-reviewed and documented in full:

- `storage/api/storage_manager_api/models/derivations.py`, including all six
  private text, digest, position, path, and JSON validators.
- `storage/api/storage_manager_api/derivations_api.py`, including the external
  artefact resolver protocol and both concrete traversal conveniences.
- `storage/storage_manager/mixins/derivations.py`, including the private mutable
  breadth-first traversal class and all registration/query/planning methods.

Newly completed: **3 modules, 15 classes, 39 functions = 57 declarations**.
Cumulative: **500 modules, 715 classes, 5,954 functions = 7,169 declarations**.
The [reviewed manifest](project-docstrings-reviewed-files.txt) has 500 unique paths.
Relevant existing manager regressions were read for dependency/behavior evidence;
the already reviewed API2 test module is not counted again. Shared `_support.py`
excerpts remain dependency evidence, not completed-file claims.

## Contracts clarified

- Provenance sources and recipe inputs retain caller ordering and nested values.
  Contiguous-position checks accept unsorted sequences; repeated Asset IDs and
  logical paths can remain. Numeric comparisons do not enforce integer types.
  Declaration and record constructors check selected fields without resolving
  references or revalidating every retained nested value.
- Recipe completeness requires pinned inputs, retrieval hints, commands, and output
  paths. Exactness uses enum identity, with no plain-string coercion. The claim is
  separate from current tool/input availability, deterministic execution, and byte
  verification. Artefact references can retain both managed and external routes.
- Workspace paths receive lexical PurePosixPath checks, not real filesystem/symlink
  confinement proof. JSON must reproduce Python's sorted compact serialization,
  including its default ASCII escaping and acceptance of nonfinite numeric constants.
  Truthy whitespace command arguments are accepted. Validation preserves input text.
- Graph/plan constructors validate selected identity relationships without proving
  connectivity, topological order, recipe exactness, positive non-root IDs, or the
  truth of availability evidence. Plan warnings do not affect its predicates;
  already_available and requires_replay can both be true for supplied values.
- Registration resolves the result and sources, checks complete source coverage and
  comparable input/artefact digests, validates exact output evidence, and rejects
  cycles including managed artefacts. These checks occur before the metadata lock
  and transaction, without a final in-transaction recheck. Equivalent declarations
  receive distinct records. No physical reads, URI probes, or recipe execution occur.
- Iteration captures records eagerly in sorted mapping-key order. Filters are
  conjunctive, workflow labels match exactly, and atomic-source filtering examines
  direct provenance only, without expanding Composites or recipe-only inputs.
- Graph traversal expands all current Composite members and recipe inputs, excluding
  managed executor/dependency Assets. All matching records are indexed before walking,
  so a disconnected broken Composite reference can fail even a depth-zero request.
- BOTH combines separate ancestor and descendant walks from the root, with ancestor
  evidence first. It does not discover the entire undirected component. Descendant
  records can retain co-inputs absent from the atomic node inventory. Any adjacency
  at the depth cutoff marks truncation, including records already encountered.
- Ancestor/descendant convenience methods materialize the graph before returning an
  iterator and discard its truncation flag. Replay planning starts with a fresh memo,
  compares viable branch proposals by step count and derivation ID, and orders
  prerequisites before consumers without claiming a global cost optimum or reservation.
- Forgetting checks an existing record's supplied revision, then removes only the
  provenance assertion. Absence returns False before revision checking. Policies
  that depended on the removed recipe are not revalidated by this method.

## Verification

- Strict structural audit and normalizer passed for all three targets and the full
  **500-file reviewed set**, with counts derived from source ASTs.
- Every executable AST matches HEAD after removing only leading literal docstrings.
  Signatures, annotations, assertions, runtime strings, and limits are unchanged;
  no runtime-doc exception was added. Ruff multisets match their baselines: one
  existing finding each in model/API files, none in the implementation.
- Selected source/test doctests and Store/API/manager/ingest/filesystem/archive/
  database regressions: **461 passed, 280 explicitly skipped integration examples,
  1 failed**, 137.26s. The sole failure remains
  `test_storage_manager_composition.py::test_manager_module_stays_a_small_composition_root`,
  because `_policy_support.py` is 1,213 lines against its unchanged 900-line ceiling.
  The newly documented derivation mixin is **625 lines**, within that ceiling.
- Full quality runner passed: 159 formatted files, annotations across 456 modules,
  221 protected dependency modules, selected lint/complexity, both production type
  checkers, 188 strict-mypy files, and 37 rejected invalid examples per checker.
- Migration/public-documentation/developer-link contracts: **38 passed**, 19.85s.
- **27 isolated observations** passed for constructor/validator boundaries, graph
  direction and depth, exact filter spelling, eager snapshots, registration identity,
  recipe-input versus tool expansion, disconnected lookup failure, revision ordering,
  and removal without policy revalidation. These used model values and transient
  memory Store operations, with explicit corrupt-index/validation-hook doubles.
  No durable, remote-service, or actual recipe-execution coverage is claimed.
  Manager cleanup ran in finally; no harness corrections were needed.
- Every launched verification process completed with an observed terminal exit code.
  The seven known docstring-sensitive guard failures remain unresolved: six earlier
  Core/CLI/terminal ownership failures plus the manager composition failure above.
  The earlier six were not rerun, and no guard was changed or deselected.
- Root and data-submodule whitespace checks passed. All local links resolve in the
  index (113), main ledger (3), policy checkpoint (3), and this note (3). Existing
  data-generator edits and untracked bytecode remain retained. Final AST recount
  confirms the 500/715/5,954 totals and the remaining package counts below.

Durable results:
`working-memory/test-results/docstrings-derivations-2026-09-11-{regression,quality,contracts}.{log,done}`
and `docstrings-derivations-2026-09-11-observations.json` in that directory.
Static reports: `/tmp/liuxin-docstring-derivations-batch-2026-09-11.json`,
`/tmp/liuxin-derivations-ast-lint-2026-09-11.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-11.json`.

Whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**, no parse
failures. Missing/blank docs remain **1,076 modules, 1,908 classes, 20,579 functions
= 23,563 declarations**. This batch completed existing incomplete documentation;
missing-docstring counts alone do not reflect that work. Overlapping findings are
3,756 delimiter layout, 10,093 examples, 2,860 parameter fields, 3,767 return fields,
3,427 empty parameter descriptions, 4,472 empty return descriptions, and 28 summaries.

## Next

Storage has **20 unreviewed API modules** and **12 unreviewed storage_manager
implementation modules**, plus application-manager, registry, repository, and workflow
owners elsewhere. Continue through reconciliation, ingest, persistence, and remaining
shared composition/support contracts. Preserve the larger metadata, catalogue/cache,
file-format, test/script/example, inherited-code, and tracked data-submodule scope.
Retain the runtime-doc consumer cautions and three narrow exceptions in the
[main ledger](project-docstrings-2026-09-08.md).
