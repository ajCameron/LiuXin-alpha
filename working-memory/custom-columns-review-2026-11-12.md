# Custom-column review handoff — 2026-11-12

The user asked to commit the completed database-facade repairs, then review
`databases.custom_columns`.

## Current repair status

The subsequent user request authorizes fixing the six findings. The user
explicitly confirmed direct database values without a Calibre results cache,
while preserving optional cache integration. All six are implemented locally;
no commit, push or PR was requested for this repair turn.

Read [the service contract](../dev-docs/databases/custom-columns.md) and the
repair follow-up in [the review](../dev-docs/databases/custom-columns-review-2026-11-12.md).
Persistent per-column cleanup covers every write connection; constructor and
refresh preserve borrowed transactions; field metadata prepares before commit
and publishes afterward; value reads/writes use attachment storage; enum
choices persist atomically. Native boolean adaptation and generated link UPDATE
integrity were also repaired after integration exposed their defects.

Final validation: broad integration **585 passed, one legacy module skip**;
final lifecycle/transaction/definition run **121 passed**. All **106 new cases**
passed setup/call/teardown and are added to the manifest, preserving the old
1,622 requirements (**1,728 total**). The final full quality script exited 0:
basedpyright reports zero errors; mypy covers 464 source files; static contracts
reject 95/93 invalid examples respectively. All **79 docs/tooling checks** pass.
The final cache-access diagnostic rerun passes all 11 service contracts.
All checks are finished. The entire 1,728-case ratchet and hosted CI were not run.
Evidence: `.tmp/custom-columns-repairs-2026-11-12/`, especially
`comprehensive.{log,json}`, `final-contracts.{log,json}`, `quality.log` and
`quality-final.log`. The documentation check used a disposable index/object
store; the real index remains empty. `documentation.log`, `service-final.log`
and `added-ratchet-evidence.json` contain final supporting checks.
Historical failed runs are retained; they do not describe
the repaired final behavior.

Remaining boundaries: SQLite-specific compatibility DDL, incremental cleanup
repair on attachment load/refresh, explicit separate orphan repair, deferred
purge-on-load, caller-owned public writes refused even without a cache,
multiple-Series extra compatibility and external composite evaluation. Hosts
serialize cache readers/writers and honor dirty-hook `commit=False`. Refresh
in-memory descriptors again after rolling back caller-owned definition edits.
Existing stored value-link UPDATE triggers are not migrated by owner cleanup.

Preserve the dirty data submodule and real Git index. The baseline review below
is historical and records the original evidence, not the current repair status.

## Delivery

- Committed the 36 validated repair/test/note files on `main` as
  `a88a5230e819c8de40518228b34e70584283f4cb`:
  `Fix database facade transactions, driver rebinding and query contracts`.
- Git identity was absent after restart. The first commit attempt failed;
  the successful command used per-command `user.name=ajCameron` and
  `user.email=alexanderjfcameron@gmail.com`, matching recent repository commits.
  No global or local identity setting was changed.
- No push or PR was requested for this turn. The index was empty after commit.
- Preserve the pre-existing dirty `LiuXin_alpha_data` submodule.

## Review outcome

Read [the complete review](../dev-docs/databases/custom-columns-review-2026-11-12.md).
The original review identified six P2 findings, before the subsequent repair
authorization above:

1. CC01: cleanup trigger uses nonexistent `work` instead of the generated
   target-table owner column; direct-connection parent deletion fails.
2. CC02: creation never installs refreshed triggers; normal macro transaction
   connections bypass the constructor's TEMP trigger and leave orphan links,
   even after reconstruction.
3. CC03: constructor trigger installation commits unrelated pending primary
   connection work; a later rollback cannot undo it.
4. CC04: create/edit/refresh updates definition maps but leaves FieldMetadata
   and aliases absent or stale; value dirtying raises on the renamed key.
5. CC05: generalized FRBR cached writers still require `meta2`; projection SQL
   and owner dirtying also hardcode Books. A correctly supplied cache does not
   make a freshly created Work column writable.
6. CC06: enum rename changes values and in-memory choices but never persists
   `custom_column_display`; reloaded metadata rejects the new stored value.

## Evidence

All checks finished:

- Focused existing suite: 215 passed, 109.05 seconds.
- Additional compatibility/cache suite: 34 passed, 1 explicit legacy module
  skip, 15.72 seconds.
- Final reproduction suite: 16 passed, 22.27 seconds, both SQLite backends.
  These assert defects and one supported clear operation, not repaired code.
- Documentation/navigation: 16 passed, 60.68 seconds, using an alternate index
  and object store in a temporary directory; real index unchanged. Tracked
  diff contains only the two review-index edits, plus the two new review docs.
- No PostgreSQL or fresh CI run for this review.

Ignored evidence: `.tmp/custom-columns-review-2026-11-12/`. Final probes are
`test_review_probes.py`; `probes.log` contains captured SQL/errors and passed
case IDs. Earlier probe logs retain harness corrections and the discovery that
macro deletes bypass the trigger. Read the final log for conclusions.

At the end of the original review, only its documentation was uncommitted.
The subsequent repair now includes custom-column source and tests as above. Prior facade validation
remains documented in the preceding review/handoff and its ignored evidence.
