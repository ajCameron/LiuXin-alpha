# Read-model failure visibility — 2026-09-06

## Scope and changes

Stage 4 of the readability/maintainability programme, included alongside
stages 1–3 in the [current checkpoint](maintainability-checkpoint-2026-09-07.md)
on `codex/package-calibre-resources`.

- `ReadModelBackend` no longer converts arbitrary query/schema/lookup/count/
  relationship failures into empty lists, `None`, zero, or a retry. Missing
  optional tables are checked explicitly. An optimized query's explicit
  `complete=False`, or absence of a sortable field, retains the existing
  materialized fallback through Core; fallback failures propagate too.
- Numeric URL/value conversion handles only expected conversion errors, with
  query calls outside those handlers. `presentation.row_value` treats only
  `KeyError` as a missing optional column; other row-access failures propagate.
- Removed outer masking in image discovery, author-route selection, API
  category detail, OPDS related-data collection, relationship schema discovery,
  and file/image acquisition-resolution metadata. Explicit unavailable targets
  and malformed IDs remain normal outcomes; byte-serving fallback policy is
  separate and unchanged.
- API category detail now checks a missing row explicitly instead of relying
  on rendering to fail. Missing/invalid routes retain their usual 400/404;
  query and presentation failures are no longer mislabeled as 404.
- Unexpected exceptions reach the existing WSGI server's generic 500/logging
  boundary. Tests verify traceback detail goes to the server error stream, not
  the public response. Existing direct/HTTP Core error codes/details survive.

## Issues exposed by removing silent catches

1. Database-backed home counts failed on text migration-ledger IDs. Core's
   `rows.query(limit=0)` was ordering/projecting IDs despite returning no rows.
   It now applies filtering and counts without ordering or row projection;
   non-count queries retain their existing integer-identity contract.
2. Cache-only home counts encountered schema views not served by the cache.
   Core now maps only `UnknownCacheTableError` and `UnsupportedCacheQueryError`
   from structured queries to `read_query_unavailable`, with `table`/`reason`
   details. Home counts recognize only that code and display “count unavailable,”
   not zero. Every other error is re-raised; required reads still fail for
   unsupported queries. No database fallback or retry is introduced, and
   unknown-field/arbitrary failures retain their normal error path.

## Tests and ratchets

- New `test_read_model_failure_contracts.py`: failure identity, schema, lazy
  iteration, supported absence/fallback, malformed responses, row/value access,
  and an AST guard against catch-all handlers in read-model/image backends.
- New `test_surface_read_errors.py`: route 400/404 distinctions, outer-adapter
  propagation, known unavailable counts, and generic WSGI 500/private logging.
- New `test_read_model_transport_errors.py`: real direct/HTTP propagation and
  precise Core classification of known versus unexpected cache failures.
- Updated existing shared-helper/image contracts to require error visibility.
  Added Core count-only regression cases and a real cache-only home assertion.
- CI includes all three new suites and Core application contracts. The normal
  quality runner lints the new standalone tests; existing stage-3 dependency
  and strict-leaf typing scopes remain intact.

Canonical policy: [read-model failure boundaries](../dev-docs/read-model-failures.md);
also linked from [Core API](../dev-docs/core-api.md) and
[maintainability quality gates](../dev-docs/maintainability-quality-gates.md).

## Verification

- Final quality gate passed: zero-error production checks, strict mypy in 147
  source files, 25 negative examples rejected by both checkers, annotations, lint,
  complexity, and 105-module dependency protection.
- Combined failure contracts, shared helpers, images, dependency/architecture,
  documentation, and quality-runner tests: **279 passed**.
- The initially failing database home test passed after the count-only repair.
  The initially failing cache home check passed after explicit capability
  classification. No catch-all empty-result fallback was restored.
- Final Core application, direct/RPC failure classification, and cross-surface
  acceptance rerun: **22 passed** with explicitly permitted local sockets.
- Final affected real-database regression rerun, including the added cache-only
  home assertion: **80 passed**.
- Final read-model, image, and HTTP failure contracts: **199 passed**. This
  includes the last absence edge case: unsaved rows return no relationships
  without consulting the source.
- Final modified-production syntax/undefined-name checks, new-test Ruff, and
  `git diff --check`: clean.

Stage 4 is complete. Tests use SQLite; no full-project or PostgreSQL claim is
made. The runs above overlap and should not be summed as distinct tests.

## Remaining programme

Incremental formatter enforcement is stage 5. The previously identified deferred
CLI/terminal cycles and inherited lower-level adapter recovery policies remain
separate work; this is not a whole-codebase exception-handling rewrite.
