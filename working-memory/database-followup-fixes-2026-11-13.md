# Database follow-up repairs — 2026-11-13

Completed objective: fix DR01–DR03 from the review of `7780b7eb`, clarify database
docstrings throughout, and prepare a verified handoff for an independent reviewer.
No commit or push was requested. The prior review note is historical evidence.

## Implemented

- **DR01:** `CustomFieldService.write()` retains ordinary managed composition.
  Nested PostgreSQL units acquire the same schema/database advisory writer key
  used by outer serialized macros before reading native revisions or plans.
  `databases.transactions.lock_postgres_writer()` owns the shared SQL primitive.
  Reservations are reacquired after savepoint rollback; SQLite retains its
  native deferred-snapshot conflict behavior.
- **DR02:** metadata-only label/name/editability changes update in place. Rebuilds
  capture historical allocation bounds and reserve them before generating new
  identities, preserving deleted handles across empty fields and normalized /
  inline datatype round trips. Entry/vocabulary meanings stay distinct; allocation
  may have gaps. No schema-format version, compatibility or cache owner was added.
- **DR03:** drivers attempt all handle closes and report failures as an exception
  group while retaining failed handles. Database close stays `closing` until retry
  succeeds. Published driver replacement retains the old cleanup owner and new
  active driver, reports that publication happened, and requires `close()` to
  finish both lifetimes. Constructor/context errors keep cleanup failure notes.

Docstrings now explain all previously undocumented database implementations
(abstract/overload stubs excluded), with expanded native contracts and documented
native protocol methods. Documentation-only edits were checked for unchanged
executable ASTs by the local authoring scripts. Canonical behavior and review
guidance live in `dev-docs/databases/module-boundary-and-lifecycle-review-2026-11-13.md`
under “Follow-up repairs and reviewer handoff”, plus `native-custom-fields.md`
and `transactions.md` in the same directory.

## Regression requirements

53 new mandatory cases, preserving all 2,206 prior cases and invariant memberships:

- `database-handle-shutdown-ownership`: 21 SQLite/PostgreSQL cases.
- `native-custom-field-identity-lifetimes`: 24 SQLite/PostgreSQL cases.
- `native-custom-field-writer-reservations`: eight real PostgreSQL concurrency cases.

Current mandatory total: **2,259**. New test modules are included in lint and
formatting scopes. Existing driver close/rebuild tests keep their IDs and now
require explicit failure reporting.

## Validation state

Evidence directory: `.tmp/database-followup-fixes-2026-11-13/`.

- `initial-sqlite.log`: 34 passed (30 new plus four existing checks).
- `initial-postgres.log`: 23 passed (all new PostgreSQL cases).
- `tooling.log`: 134 passed for regression-gate, quality-workflow and docstring tooling.
- `docstrings-after.json`: no undocumented implementation definitions remain.
- `baseline-manifest.json`: preserved original manifest; current adds exactly 53 cases.
- `initial-state.json`: HEAD and real-index fingerprint before edits.
- `gate-source-fingerprints.json`: implementation/test/config snapshot for final validation.
- `quality-final.log`: full formatter/lint/architecture/import/typing runner passed;
  371 formatted files, 463 annotation checks, 25 cold-import budgets, zero
  basedpyright errors/warnings, 470 mypy sources and 95/93 negative contracts.
- `docs.log`: 22 passed, with a disposable Git index containing the five proposed
  new test targets; the real index was unchanged. `docs-index-validation.json`
  records that scope. Minor subsequent prose corrections add no new links.
- `core-integration.log`: eight Core custom-field and Library lifecycle cases passed.
- `scope-audit.json`: eight files contain executable changes; 31 source files have
  docstring-only changes relative to HEAD. `behavior-only.diff` isolates the logic.
- `database-gate-final.log`: **2,370 passed**, two existing logging deprecation
  warnings, in 3,218.59 seconds. The final report records **2,259/2,259 required
  cases fully passed**, no failures and pytest exit zero.
- `completion-audit.json`: final preservation checks passed. All 49 frozen
  implementation/test/config inputs match the validated snapshot; HEAD, real
  index and pre-existing data-submodule status are unchanged.
- `postgres-stop-proof.json`: private test server stopped successfully; subsequent
  `pg_ctl status` returned exit three, “no server running”.

The first quality pass caught a constant-getattr lint issue. It was corrected
with explicit schema validation. The preliminary full gate was interrupted before
that source correction, so `preliminary-gate-interrupted.*` is not validation of
the finished change. An interruption-time pytest teardown error belongs only to
that incomplete run. The final full gate and final quality runner passed.
Gate artifacts and telemetry are retained in
`/tmp/liuxin-database-followup-fixes-final-2026-11-13`; session **10112** completed
with exit zero. No validation job from this task remains running.

The private PostgreSQL cluster at `/tmp/liuxin-cc-followup-pg/data`, formerly on
port 55439 with database `liuxin_followup_utf8`, is **stopped**. The process audit
confirmed that this task's controller and server exited.

Ready for independent review. Focus on lock acquisition after savepoint rollback,
historical identity bounds through storage-shape changes, and cleanup ownership
after a replacement binding has already been published. The wider architectural
follow-ups remain documented separately. No commit or push was requested or made.

Subsequent checkpoint, **2026-11-14**: the user authorized a combined local commit
with the whole-package DBR repairs. This earlier uncommitted status is historical;
see [the final handoff](database-review-fixes-2026-11-14.md) for the combined
validation and commit boundary. No push was requested.
