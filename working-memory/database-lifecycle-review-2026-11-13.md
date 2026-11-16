# Database module boundary and lifecycle review — 2026-11-13

Subsequent authorized repairs are recorded in the [repair handoff](database-lifecycle-fixes-2026-11-13.md).
The status and observations below describe the earlier review checkpoint.

Status: review, requested dev-docs and validation complete.
The four lifecycle findings remain open. This request authorized a review, not a
production refactor or repair implementation.

## User intent and decision

The user agreed with the package assessment and requested that it be preserved
in dev-docs, followed by a lifecycle review ahead of module sign-off. They added
that efficient low-level SQL can reasonably belong to either Catalog or databases,
with only a slight preference for databases. The docs preserve that nuance while
describing the current database-owned executable-SQL convention and guard.

Read the [review and ownership decision](../dev-docs/databases/module-boundary-and-lifecycle-review-2026-11-13.md)
and [Catalog/database guide](../dev-docs/catalog/catalog-database-ownership.md).
The database README links the new review. No skill or subagent was used.

## Findings and evidence

- LC01 (P2): strict storage-bootstrap failure after maintenance starts leaves the
  failed constructor's worker alive and primary connection usable.
- LC02 (P2): Database.close suppresses maintenance join timeout, clears the
  driver's/macro owner's bindings, and returns with a live worker. Event-controlled
  resumed dispatch then fails against the closed database.
- LC03 (P2): Database-created StorageManager has no matching shutdown call.
  A real bootstrapped single_file_sqlite Store remains started after database close.
  This is evidence of missing lifecycle shutdown, not a claim of a persistent
  SQLite BLOB-handle leak; that Store opens per-operation handles.
- LC04 (P2): public set_macros/set_driver_wrapper allow foreign collaborators.
  Writes through the first facade persist in the second catalogue; wrapper
  replacement also leaves first.execute pointing at the first wrapper. Setter
  docstrings already describe shallow assignment; this is an existing public
  ownership hazard. Recommend removal/private assembly or complete guarded binding.

Evidence root `.tmp/database-lifecycle-review-2026-11-13/`:
- `probes.py`, `probes.log`, `probe-results.json`: all 10 expected defect
  observations reproduced (five routes per SQLite driver name). Real workers,
  drivers, Store bootstrap and independent committed-value readers; controlled
  failure/event injection. All temporary catalogues, workers and Stores cleaned up.
- `baseline-tests.log`: 76 lifecycle/rebinding/transaction cases passed.
- `creation-tests.log`: 38 creation/replacement cases passed.
- Total existing checks: 114 passed. No live PostgreSQL run. No new permanent
  tests assert these defects; convert the probes into proper regressions when fixed.
- `baseline.json` and `state.json` retain incoming work/index/submodule state.
  Baseline capture overlapped initial doc edits; compare source/test preservation
  with the preceding custom-column task's final fingerprints as an extra control.

Sign-off recommendation: fix LC01–LC04 with permanent failure-path regressions.
Open/create/repair API separation, broad Calibre extraction and internal module
splitting are recorded separately from those blockers. Current implicit file
creation and bootstrap repairs are documented/tested behaviour, not newly claimed
correctness defects. The previous full 2,273-test gate remains earlier evidence.

## Final validation / repository state

Repo `/mnt/c/dev/LiuXin-alpha`, branch main, HEAD
`a88a5230e819c8de40518228b34e70584283f4cb`. Use `.venv/bin/python` and
`PYTHONPATH="$PWD/src"`. Many earlier custom-column changes are uncommitted;
preserve them, the real index and dirty `LiuXin_alpha_data` submodule.

Documentation/navigation checks passed all 22 cases using the disposable alternate
index (`docs.log`). Whitespace checks passed. `preservation.json` confirms HEAD,
real index and the dirty data submodule are unchanged; source and test fingerprints
match both incoming controls. `validation.json` records the completed review.
No production code or permanent test changes belong to this review. No commit,
push, PR, publication or production migration was requested or performed.
