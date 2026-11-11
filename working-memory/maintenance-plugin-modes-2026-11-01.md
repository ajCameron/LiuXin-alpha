# Maintenance modes sketch — 2026-11-01

Historical declaration-only checkpoint. Superseded by the completed
[full implementation](maintenance-implementation-2026-11-01.md), which supplies
transactional prompt effects and durable lazy delivery. The notes below record
the earlier design stage, not the current implementation status.

User requested a sketch of lazy and prompt maintenance modes after agreeing the
consistency distinction. This task delivers declarations, a design and guards;
it does not implement the complete durable worker or prompt write integration.

Local branch: `maintenance-plugin-modes`, based on `b2343096` from PR #153.
Leave the sketch for review; no commit, push or new PR is requested in this turn.

## Delivered shape

- `MaintenanceMode` in `databases.db_types.maintenance_types`, exported through
  database API and maintenance namespaces. Both plugin bases default to LAZY.
- Existing lazy/undeclared duck plugins retain current behavior and priority.
  The queue engine rejects prompt/unknown modes before registration or startup,
  and rechecks before draining/dispatch to prevent silent lazy fallback.
- `dev-docs/databases/maintenance-modes.md` defines transactional prompt effects,
  batch/dependency/read boundaries, nested rollback, durable lazy obligations,
  idempotence/revisions/deletion, restart/retry and implementation acceptance tests.
- Existing jobs owns a separate database, so durable lazy work needs a catalogue
  obligation/outbox and reliable handoff. Existing retry declarations alone do
  not supply automatic retry behavior.
- Validation and normalized keys remain with their existing domain/schema owners.
  Legacy creator plugins require caller migration/retirement; no WEMI claim is made.

## Verification

Nine focused mode/dispatch regressions pass. The final selection repeats those
alongside public documentation, links and hygiene: 32 passed in 75.50 seconds.
Maintenance integration and API signature parity passed 30 cases, with two existing
expected failures for DIRTY_INTERLINK_RECORD's argument-count mismatch. Together
these are 62 distinct passing tests, plus the two expected failures.

All 25 cold-import budgets and the layer-direction check pass. New declaration
and test formatting/lint pass; touched engine/plugin/API lint passes with the
pre-existing quote-style annotation rule excluded. Scoped whitespace checks pass.
Logs and telemetry remain under `/tmp/liuxin-maintenance-mode-*`.

Completion audit: the enum, both plugin declarations and public exports agree;
the real engine accepts lazy/legacy plugins and refuses prompt/unknown modes at
its execution boundaries; current delivery limits and future transaction/outbox
work are explicit in the dated guide, navigation and DB07. The sketch is complete
and uncommitted for review. Full prompt/lazy guarantees remain future work as
requested by the sketch's scope, rather than claimed implemented behavior.

## Preserved work

Do not overwrite or stage the user's edits in maintenance/builtin_plugins.py or
maintenance/dummy_maintenance_bot.py. The dirty LiuXin_alpha_data submodule is
also unrelated. New sketch files can be marked intent-to-add for the existing
tracked-documentation-link check; that does not commit the sketch.
