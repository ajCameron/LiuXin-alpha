# Core integration review, 2026-11-23

Objective: fork the reviewed cache work and review Core composition, metadata
routing, lifecycle ownership and the public search/select/edit/save/readback
workflow. This is a bounded integration review, not the wider jobs/configuration/
ingest migration programme. Canonical findings and route map:
`dev-docs/core/integration-review-2026-11-23.md`.

Branch `core-review`, base `215390832224a33c6ac22a9efab16debad6a9442` (metadata
PR #214; still open at start). Core repair PR #215 is also open. The separate
original checkout remains on `cache-review` at `510c0a82`, with its dirty data
submodule and six stashes. No merges, force pushes or original-checkout changes.
No subagents. Python 3.14.10; `PYTHONPATH=src`. Run quality and pytest sequentially
because the repository artifact guard observes newly created root caches.

## Repairs and contracts

- Validate injected database identities before cache preparation and avoid eager
  access to a legacy `db` getter when `database` is present.
- Retain ownership of a created cache before initial load. Rollback closes it on
  load/binding failure and restores prior storage bindings. Carry that policy
  through LibraryServices Catalog installation and factory runtime installation.
  Borrowed collaborators remain open. Stop a newly created durable job service
  before unwinding the library graph; retain both startup and cleanup errors.
- Guard lazy construction and explicit refresh against closure. Take the database
  mutex before the Core lifecycle lock; a waiting reader must not block the SQL
  owner's access to services. Owned maintenance is stopped and briefly joined.
- Attempt each closer independently, retaining all failures. Repeated close does
  not retry. An old session cannot detach a replacement session's cache binding.
- Preserve committed receipts when post-write cache state/generation reporting
  fails, as well as when reload fails. Native metadata receipts bypass the older
  Core reconciliation adapter and must never be refreshed twice.
- Propagate required global-search reads through local and RPC structured errors
  instead of returning successful incomplete results. Unicode matching and
  paging semantics are retained.
- Preserve explicit injected readers/writers. Native metadata uses its Catalog/
  cache composition; a legacy injected reader controls row/Item projection routes.

## Final validation

Artifacts use the `/tmp/liuxin-core-review-` prefix and matching `.xml`/`.log`
where pytest ran:

- `lifecycle-before`: 20 failed / 2 passed, reproduced boundary defects.
- `lifecycle-after`: 22 passed after initial repairs.
- `lifecycle-wide`: 66 passed in 168.49 s (library owner, field routing, waits).
- `public-workflow-final`: 70 passed in 192.67 s. Covers SQLite/APSW, uncached and
  all three caches, local/RPC, stale saves, durable receipts and recovered reads.
- `graph-before`: 4 failed, reproducing displaced binding loss during later
  Catalog/runtime installation with either a borrowed or created cache.
- `lifecycle-complete`: 31 passed in 1.51 s after graph rollback and closed-refresh
  guards. Includes construction/close concurrency and database lock ordering.
- `quality`: full repository gate passed before the last LibraryServices/factory
  rollback refinement (0 type errors, 571 mypy files, 127/123 negative contracts).
- `regressions`: all Core tests, surface boundary enforcement and existing native
  metadata workflow regressions passed **405 tests**, with **one skip**, in
  **1,021.46 s**. The skipped durable-maintenance test requires
  `LIUXIN_TEST_POSTGRES_URL`. No live PostgreSQL run was made for this review.
- `quality-final`: full gate passed after the last graph changes. Formatting,
  lint, complexity, architecture/import checks, basedpyright, mypy and negative
  contracts are green (571 mypy files; 127/123 invalid examples rejected).
- `docs`: **22 passed in 7.69 s**, developer navigation and public boundaries.
- Extra Ruff check/format of new tests and lifecycle owners passed. Unrestricted
  Ruff on runtime/services reports 41/2 existing style findings respectively,
  exactly matching the base; no added findings. These owners are typed, but not
  included in the repository's modern lint scope.

All launched validation sessions completed. The final source-only changes after
the broad regression were docstring corrections; the subsequent quality gate
checked those final files. The review, API/workflow docs and indexes record the
same ownership contracts and validation scope.

## Delivery and next scope

The bounded review and repairs are complete on the isolated `core-review` branch.
Commit code, tests, documentation and this handoff together. No PR, push or merge
was requested for this new review. The original checkout remains at `510c0a82`
with its existing dirty data submodule and all six stashes.

Keep the existing ownership split. Follow Core's `next_tasks.md` for lower-layer
lifecycle caller migration, configuration capture and capability discovery.
Expose further native picker/shared-record/tree operations when consumers need
them, retaining explicit injected-service and wire-result contracts. Jobs and
transport remain separate review scopes; this is not whole-Core sign-off.
