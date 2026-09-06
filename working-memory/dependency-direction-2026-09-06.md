# Shared dependency direction — 2026-09-06

## Scope and ownership

Stage 3 of the readability/maintainability work repairs the cache-writer and
shared-surface dependency directions, preserving behavior and compatibility
exports. It is included in the [stages 1–4 checkpoint](maintainability-checkpoint-2026-09-07.md)
on `codex/package-calibre-resources`.

- Seven cache writers now import their bases from implementation owners instead
  of `caches.write`, the package assembling them. Public package exports and
  `get_writer` dispatch remain unchanged; duplicate package imports are removed.
- `surfaces/presentation.py` owns escaping, text shortening, integer coercion,
  and row-value lookup, with a narrow `RowLookup` protocol.
- `surfaces/acquisition_types.py` owns immutable `ResolvedFileTarget` and
  `CoreStoredFile` values and the injected `AcquisitionReader` protocol. It does
  not require a concrete Core model or web application.
- Read-model, image, catalogue, and OPDS backends import those owners directly.
  `web_readonly.app` retains the historical private helper/type names as aliases
  to the same objects. Both new owners depend only on the standard library.
- Both leaf modules enter strict typing, Ruff, and complexity-10 checking.
  Three additional negative typing examples protect reader and row-lookup
  calls: both checkers reject all 25 examples. The fixture also passes real
  `CoreSurfaceModel`/`CoreRow` values through the new protocols; basedpyright
  resolves those imported concrete implementations. Mypy's existing skipped
  compatibility-import policy is unchanged; this is not a claim that the whole
  surface/Core model is strictly checked.

## Dependency enforcement

`scripts/check_modern_import_cycles.py` now protects 105 modules (previously 58),
including complete cache writers, shared surface backends/contracts, and the five
web/API/OPDS application packages. It retains the existing combined `build_graph`
contract and previous Catalog/metadata protections.

Imports are classified as import-time, deferred/function-body, or type-only.
The combined graph still rejects cycles involving **any** context. Separate
rules reject writer-to-assembling-package, shared-backend-to-web-application,
and shared-leaf-to-LiuXin dependencies even without a cycle. Diagnostics show
real edges with paths, lines, and contexts, not sorted SCC members presented as
a traversal path. Empty/missing protected sources cannot report success.

The classifier recognizes direct/negated `TYPE_CHECKING` guards and typing
aliases; it is syntactic, not a scope-aware symbol resolver or full Python
initialization simulator. Dynamic imports and implicit parent initialization
are not modeled. Fresh-process import tests complement the static gate.
CI now includes the scanner, independent surface-import/compatibility tests,
and writer import/dispatch tests.

Canonical policy: [maintainability quality gates](../dev-docs/maintainability-quality-gates.md), especially
“Dependency direction and import contexts.”

## Verification at stage-3 completion

- `bash scripts/run_type_checks.sh`: passed; zero-error production typing,
  strict mypy in 147 selected source files, all 25 negative examples rejected
  by each checker, plus annotations, lint, complexity, and dependency gates.
- Initial combined scanner/shared-surface/writer tests: **93 passed**. A further
  scanner test covers context restoration after branches/nested bodies and
  wildcard imports; the final scanner rerun was **49 passed**.
- `tests/databases/caches`, Catalog repository invariants, and link updates:
  **417 passed, 12 skipped**. Skips are the six opt-in legacy Calibre-cache
  harnesses and six backend-specific snapshot cases, not new exclusions.
- Read-model/parity, images/contracts, acquisition, OPDS, read-only/Calibre web,
  documentation, workflow ownership, Core boundaries, and quality-runner
  contracts: **103 passed**.
- API-readonly and read-write web regressions: **20 passed**.
- `tests/surfaces/test_core_surface_acceptance.py`: **1 passed**, exercising
  direct and HTTP Core across application surfaces. Its first sandboxed run
  failed to create a socket; the explicitly permitted local-socket rerun passed.
- Read-only AST comparison against pre-extraction commit `9d86fedb`: bodies of
  all four extracted presentation helpers, the acquisition byte reader, all
  four migrated backend definitions, and every cache-writer definition were
  unchanged at stage-3 completion.
- `git diff --check` and formatter checks of the new owners/tests/checker: clean.

The selected database backend is SQLite; no PostgreSQL or full-project suite
claim is made. Test counts above describe separate runs and overlap.
The final full quality-gate rerun passed after all code and test edits.
Stage 3 is complete for the dependency-direction scope above.

## Remaining work

The broader static import scan of the eight modern packages no longer finds
cache-writer or shared-surface executable cycles. With type-only dependencies
excluded, it reports no module-level-only cycles and two components when
function-body imports are included: CLI package/app/completion/SquashFS and
terminal text-browser/windowed-UI. Those existing deferred cycles remain
outside this protected repair; this is not a whole-project acyclicity sign-off.

Stage 4 subsequently completed unexpected read-model error visibility,
narrowing the broad row-lookup fallback and backend exception handling left
unchanged by this extraction. See [read-model failure visibility](read-model-failure-visibility-2026-09-06.md).
Incremental formatter enforcement remains next.
