# Maintainability quality gates

Status: enforced for the modern ratchet; updated 2026-09-07 through the
internal-contract, workflow-ownership, dependency-direction, and
failure-visibility tranches.

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
newly ratcheted modules, and rejects cycles in the protected Catalog writer/API,
Calibre metadata API, cache writer, and shared/web surface seams. The dependency
gate includes import-time, deferred, and type-only imports; see the dependency
direction section below for its scope and limits.
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
10. Calls between maintained components require named, typed contracts.
    Storage helpers and Core endpoint providers must not use catch-all
    `__getattr__`, unrestricted callable signatures, or casts from `object`
    to bypass implementation conformance. Extend the positive and negative
    contract examples when adding a new kind of internal call.

## Current ownership seams

- `core/program_endpoints` owns transport descriptions and registration by
  command family. `core/program_services` owns execution; `CoreProgramAPI`
  retains installation and explicit compatibility delegates only.
- `surfaces/cli/storage_commands` separates administration, Store options and
  guided setup, parser construction, and ingest process/reporting concerns.
  `surfaces/cli/storage.py` retains explicit compatibility aliases only.
- `ingest/mixed_application.py` owns database, Store-manager, and mixed-ingest
  coordinator composition. CLI code owns parsing, operator interaction,
  process signals, logs, locks, and report presentation.
- `catalog/write/host_api.py` is the dependency leaf used by Catalog writers.
  Writers do not import the high-level Catalog API package that constructs
  them.
- Mutually-referential metadata-tool protocols are co-located in
  `metadata_tools_api/facades.py`; the historical modules are compatibility
  exports only.
- `caches/write` assembles and exports writers; its implementations import
  base classes directly from their defining modules, never through that
  assembling package.
- `surfaces/presentation.py` owns shared text, integer-option, and row-display
  helpers. `surfaces/acquisition_types.py` owns portable delivery targets and
  the narrow byte-reader contract. Neither imports another LiuXin module;
  reusable backends and web applications depend on them independently.
- `storage/storage_manager/mixins` mirrors the ordered `StorageManagerAPI`
  components. Shared mutable state, durable ingest wire types, and
  cross-component mechanics are isolated in private implementation modules.

The excluded legacy and renderer areas are still valid maintenance work. Their
absence from the green ratchet is explicit debt, not evidence that they pass
strict checking.

## Internal callable contracts

The 2026-09-06 tranche makes the storage and Core registration boundaries
checkable at both the caller and implementation:

- `storage/storage_manager/mixins/_contracts.py` declares the 39 helpers
  shared between storage components, including Store attachment during
  construction. The state base inherits these explicit protocols. Abstract
  declarations reject a composed manager that omits a required support
  component; helpers used only within their own component remain local.
- `core/program_endpoints/handlers.py` describes each provider's named
  handlers and the aggregate surface implemented by `CoreProgramAPI`.
  Providers receive these contracts directly. Their registrar specifies
  query versus command handlers, required arguments, and allowed keywords.
  Handler results retain mapping contracts where the implementation promises
  a record; the generic schema-column result remains opaque.
- Core command and query envelopes are explicit targets of both checkers.
  This matters for mypy: its existing `follow_imports=skip` policy must not
  erase their distinct types at registration.

`scripts/run_type_checks.sh` now runs
`scripts/check_internal_type_contracts.py` for each selected checker after its
production check succeeds. The static-only fixture
`tests/typing/internal_contracts.py` contains valid calls against real
implementations and 25 deliberately invalid examples covering names, argument
types, return types, signatures, provider conformance, and typed evacuation
plans/limits, acquisition-reader calls, and row lookups. Each invalid line
must report its expected diagnostic rule; all other lines must pass. An
unrelated import error or checker failure cannot satisfy the test.

Run these checks separately with:

```bash
.venv/bin/python scripts/check_internal_type_contracts.py --checker basedpyright
.venv/bin/python scripts/check_internal_type_contracts.py --checker mypy
```

The fixture is never executed. Production checks still require zero errors;
the intentionally invalid examples are checked in a separate invocation.

## Workflow implementation ratchet

The 2026-09-06 workflow extraction includes both complete implementation trees
in typing, Ruff, and the complexity ceiling of 10. Before extraction,
`core/program_api.py` was outside that complexity gate and had seven violations
with complexity up to 19. None remain in the extracted implementation.

New evacuation models, planning, execution, and placement-policy helpers are
strict basedpyright targets. Moved legacy envelope adapters and CLI workflows
retain their existing standard basedpyright mode; their dynamic subsystem
boundaries have not become strict merely because their files moved. At this
tranche's completion, all 145 selected source files passed the existing strict
mypy configuration; the subsequent shared-leaf extraction expanded the current
scope to 147.

`tests/scripts/test_workflow_ownership.py` prevents implementation from flowing
back into compatibility facades, rejects cycles within the extracted trees,
and bounds the reviewed owners to 450 module lines and 160 function lines.
The two compatibility files stay below 250 lines and contain no workflow
bodies. These are growth ceilings, not recommended sizes: most functions are
substantially shorter, and long wire projections/parser declarations remain
visible debt rather than a reason to raise limits.

See `dev-docs/core-program-workflows.md` for ownership and change guidance.

## Dependency direction and import contexts

The 2026-09-06 dependency tranche expands
`scripts/check_modern_import_cycles.py` from 58 to 105 protected modules,
including the complete cache-writer package, shared surface backends/contracts,
and the five maintained web/API/OPDS application packages. Previous protected
Catalog and metadata seams remain protected. The exact scope lives in the
script's named prefix tuples; the separate workflow-ownership test continues
to protect the stage-2 implementation trees.

The gate rejects multi-module strongly connected components in the **combined**
graph. It also rejects three directions even without a cycle:

- cache writer implementations importing through `caches.write`;
- shared surface backends/contracts importing a web application package;
- the new presentation/acquisition leaves importing another LiuXin module.

Failure output lists actual dependency edges, source paths, lines, and contexts:

- `import-time`: imports outside function bodies and recognized type-only
  branches; class bodies execute in their enclosing context;
- `deferred`: imports in ordinary or async function bodies;
- `type-only`: imports under recognized `typing.TYPE_CHECKING` guards,
  including aliases and negated guards' `else` branches.

These labels describe syntax, not a proof of Python's complete initialization
order. For example, a function can be called during module initialization.
Guard recognition is syntactic rather than a scope-aware symbol resolver;
unrecognized conditions are treated conservatively. Both branches remain in
the combined graph. Dynamic import calls and implicit parent-package execution
are not modeled, so fresh-process import tests complement the static check.
No context is silently excluded from the architecture gate. The combined
`build_graph` API retains its previous semantics for existing callers.

`tests/scripts/test_check_modern_import_cycles.py` tests context classification,
relative imports, combined-graph rejection, acyclic direction violations, and
missing-source failure. CI runs it alongside
`tests/surfaces/test_shared_surface_dependencies.py` and
`tests/databases/caches/test_writer_dependencies.py`, which exercise isolated
imports, compatibility-export identity, helper/byte-reader behavior, and writer
dispatch. Both new leaf modules enter strict typing, lint, and complexity-10
checking; strict mypy now covers 147 selected source files.

The old private names in `surfaces.web_readonly.app` remain compatibility
aliases, not duplicate implementations. Shared read-model, image, catalogue,
and OPDS backends import their owners directly. Helper fallback behavior is
unchanged by the dependency extraction; the subsequent failure-visibility
tranche narrows missing-column fallback and removes broad query-error catches,
as described below.
This is a scoped dependency ratchet, not a whole-project acyclicity claim.

## Read-model failure visibility

The stage-4 failure contract distinguishes successful empty/missing results and
explicit incomplete-query fallbacks from failed reads. Read-model and image
backends may not reintroduce catch-all handlers. API category routes, OPDS
related-data collection, home counts, and file/image resolution must not hide
query failures in an outer adapter. WSGI owns generic HTTP 500 responses and
server-side traceback logging; public responses do not contain exception detail.

CI runs the read-model failure, surface HTTP error, real direct/RPC error, and
Core application contracts. The normal quality helper lints the new standalone
test modules. See [read-model-failures.md](read-model-failures.md) for the exact
fallback rules, diagnostic ownership, and the count-only Core query repair
uncovered by removing silent catches.

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
