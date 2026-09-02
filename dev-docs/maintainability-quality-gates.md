# Maintainability quality gates

Status: enforced for the modern ratchet, 2026-08-31.

## Purpose

LiuXin contains modern application code alongside inherited compatibility
surfaces. Treating the entire tree as one strict typing target produced a
permanently red check: on 2026-08-31 the former configuration reported 2,639
basedpyright errors and 498 mypy errors. A check that cannot pass cannot prevent
regressions.

The default gate is therefore a zero-error ratchet. It covers the modern
storage API, the Core program facade and its endpoint providers, the mixed
ingest application seam, and the packaged storage CLI. Newly extracted leaf
protocols are checked strictly. Existing orchestration remains on
basedpyright's standard mode until its dynamic subsystem boundaries are made
more precise. Mypy uses strict checking within the selected files while
treating imported compatibility modules as external dependencies.

Run the same gate locally with:

```bash
bash scripts/run_type_checks.sh
```

The command also checks callable annotations in `file_formats`, runs Ruff over
newly ratcheted modules, and rejects cycles in the protected Catalog writer/API
and Calibre metadata API seams.
The CI quality job additionally guards documentation at the reviewed
first-party boundaries: every module and top-level public class in `caches`,
`catalog`, `core`, `databases`, `ingest`, `jobs`, `storage`, and `surfaces`, plus
every definition explicitly published through a literal `__all__`, must have a
real docstring. Maintained top-level Python tools must document their module and
public classes as well. The same boundary now covers metadata main-table row
and self-relation APIs, their concrete non-WEMI rows, and the public WEMI API
and implementation container families.
It is offline and non-mutating by default; pass `--install` only when the
repo-local environment needs the `typing` extra. A separate Ruff C901 pass
holds the modern CLI/application orchestration seams to a maximum complexity
of 10.

The composed storage-manager implementation is fully linted and typed as a
unit. Its extracted legacy workflows have a separate maximum complexity of 15,
down from 19 at extraction time. This is a ratchet, not a preferred target:
new and materially rewritten methods should remain at or below 10, and the
storage-manager ceiling should only move downward.

Application-surface ownership is enforced by
`tests/surfaces/test_core_boundary_enforcement.py`. CI runs both checks in the
`Modern Architecture and Typing` job.

## Ratchet rules

1. The configured gate must remain at zero errors; do not add an error-count
   baseline or blanket suppressions.
2. New application services and leaf protocols enter the strict list when
   introduced.
3. Existing files move from standard to strict only after they pass locally.
4. A compatibility subtree is added only after its current errors are fixed;
   expanding the list must never make the normal command red.
5. Imports inside the protected cycle graph must point toward leaf protocols,
   not back through package-level facade exports.
6. CLI and other presentation modules must use Core or an application service;
   file-wide boundary exceptions are not accepted for new command families.
7. Functions in the protected modern orchestration set must remain at or below
   the configured complexity ceiling; extract named policy or presentation
   helpers instead of raising the ceiling.
8. Storage-manager behaviour belongs in the API-shaped implementation mixins;
   `storage_manager/manager.py` remains a small composition and compatibility
   root. Cross-cutting mechanics belong in explicitly private support mixins,
   not whichever public component happens to call them first.
9. The documentation ratchet protects architectural and exported boundaries.
   Do not weaken it to accommodate a new public API, and do not widen it by
   generating tautological prose for inherited private helpers. Move legacy
   areas into the ratchet only after a human review can describe them honestly.

## Current ownership seams

- `core/program_endpoints` owns transport descriptions and registration by
  command family; `CoreProgramAPI` continues to own handler implementation and
  public compatibility.
- `ingest/mixed_application.py` owns database, Store-manager, and mixed-ingest
  coordinator composition. CLI code owns parsing, operator interaction,
  process signals, logs, locks, and report presentation.
- `catalog/write/host_api.py` is the dependency leaf used by Catalog writers.
  Writers do not import the high-level Catalog API package that constructs
  them.
- Mutually-referential metadata-tool protocols are co-located in
  `metadata_tools_api/facades.py`; the historical modules are compatibility
  exports only.
- `storage/storage_manager/mixins` mirrors the ordered `StorageManagerAPI`
  components. Shared mutable state, durable ingest wire types, and
  cross-component mechanics are isolated in private implementation modules.

The excluded legacy and renderer areas are still valid maintenance work. Their
absence from the green ratchet is explicit debt, not evidence that they pass
strict checking.

## Repository-wide documentation audit

The 2026-09-01 pass parsed all production and maintained-tooling Python files,
not only the ratcheted packages. The initial snapshot covered 1,770 files,
3,300 classes, and 25,098 functions without a syntax failure. It found 17,335
undocumented class or function definitions and 9,408 existing definition
docstrings without examples. Those totals are useful as a map, but they are not
a sensible pass/fail target: most of the count is concentrated in inherited
Calibre format, metadata, library-compatibility, and utility internals.
After the initial modern-boundary work, the corresponding totals were 17,080
missing definition docstrings and 9,663 documented definitions awaiting
examples. Extending the ratchet through the public metadata row and container
model moved those totals to 16,909 and 9,834 respectively. The increase in the
second number is intentional: honest summaries were added without manufacturing
example blocks merely to improve an aggregate count.

The pass therefore completed and now enforces the boundaries that readers and
plugins actually use:

- every module and top-level public class in the modern first-party packages;
- every locally defined callable or class explicitly published through a
  literal `__all__` in those packages;
- module and public-class documentation in maintained top-level Python tools;
- complete definition and field coverage for the composed storage-manager
  API and implementation.
- public row, relation, identity, metadata-bundle, typed-value, per-kind, and
  target-wide container boundaries in the metadata model.

Empty `:param:` and `:return:` descriptions remain valid structural markers
under the project style guide. `scripts/normalize_docstrings.py` can normalize
reviewed files, but it deliberately does not invent summaries or examples.
The next useful documentation work is human review of public metadata row and
container families, followed by format-specific compatibility seams as those
areas receive functional maintenance. Raw missing-docstring totals must not be
reduced with generated restatements of symbol names.

## Whole-project baseline and priority order

The 2026-09-02 whole-project review rates readability at 6/10 and
maintainability at 6.5/10. The modern application spine is materially stronger
(about 8/10), as is the test system (about 8.5/10), but those areas coexist with
a very large inherited compatibility tree. The review counted roughly 1,730
production Python files and 1,005,000 raw lines; about 45% is generated or
resource-lookup-style code. It also found seven modern import-cycle components
covering 81 modules, about 1,267 TODO-like markers, and several 2,800–4,300-line
modern orchestration files.

The numbers are a navigation aid rather than new red gates. The order for
improving them is:

1. keep built/installable artifacts operational, with package discovery and
   runtime data verified outside the checkout;
2. cut modern cycles at leaf protocols and registries;
3. split the largest modern orchestration modules along existing command and
   service ownership seams;
4. widen the zero-error typing, Ruff, and complexity ratchets only after each
   selected package is green;
5. consolidate duplicated CI and developer-documentation navigation.

The first item now has an installed-catalogue wheel gate. See
`dev-docs/packaging.md` and
`working-memory/maintainability-and-packaging-2026-09-02.md` for the artifact
contract, evidence, and remaining external-resource limitation.
