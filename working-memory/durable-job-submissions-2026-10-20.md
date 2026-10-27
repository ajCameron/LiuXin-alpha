# Durable submission checkpoint, 2026-10-20

Branch: `durable-job-submissions`, based on lease prerequisite
[PR #139](https://github.com/ajCameron/LiuXin-alpha/pull/139) at `ac3a6728`.
The full A01–A16 programme and A09 remain open. No PR has been merged.
The six unrelated staged storage files and dirty data submodule remain protected
by `/tmp/liuxin-docs-pr-preserved-storage.json`.

## Change

One repository transaction now owns initial definition/run/event/request mapping.
Repeated delivery returns the original run; changed intent under the same key
raises JobRequestConflict. Existing-definition enqueue optionally accepts a key,
preserving unkeyed callers. Ledger UUIDs survive reopening and result retention
does not erase request identity.

Explicit retries retain captured kind/label/payload, increment attempt number,
and record their immediate parent. Each parent has at most one successor.
Started attempts require acknowledgement of possible effects; this is not domain
idempotency and does not stop a stale external worker. Active/successful attempts
cannot be retried. Schema migration captures legacy labels once and leaves unknown
ancestry unset. See `dev-docs/jobs/submission-identity.md`.

## Validation

- Six new delivery/retry contracts failed against unchanged `ac3a6728`.
- First implementation: 38 durable-job tests passed.
- Expanded contracts: 67 passed, including concurrency, rollback, input conflicts,
  ancestry, retention and legacy migration.
- Final jobs/utility/Core/docs selection: **157 passed**, with one failure because
  new documentation targets were not yet staged in the isolated checkout.
  After staging the intended files, all **49 documentation/tooling checks passed**.
  All **69 durable-job cases**, including **37 new submission cases**, passed.
- Complete quality gates passed: 205 formatted files, 244 protected modules,
  1,802 layer modules, 25 cold-import budgets, lint/complexity and both type
  checkers. Both rejected all 38 negative contracts; mypy checked 200 source files.

The selections overlap. Final tests add a real publishing handler proving that
duplicate delivery does not execute it again, plus interrupted-run retry and
stale-owner refusal. Test runs are serial with telemetry and immediate scratch
cleanup. Full-suite validation belongs to PR CI.
Logs are archived under ignored
`working-memory/test-results/durable-job-submissions-20261020/`.

Isolated checkout: `/tmp/liuxin-a09-review`, detached at `ac3a6728` with this
overlay. Sync helper `/tmp/liuxin-sync-submissions.py`; commit manifest
`/tmp/liuxin-submission-files.json`. Main contains unrelated staged work, so never
run a broad commit/reset/stash there. Helpers verify the protected files.

Parent JOB01 PR #138 is fully green: 7,540 passed, 290 skipped, 17 xfailed,
run `39486510953`. Parent rating PR #137 is fully green: 7,525 passed, 289
skipped, 17 xfailed, run `39484843469`. Both PR descriptions carry that evidence.

## Continue A09

The durable repository is still separate from the default Core utility manager.
Connect one production workflow through a Core adapter with stable namespaced IDs,
bounded progress/events, receipts, cancellation and restart inspection/recovery.
Avoid a third state owner. Preserve injected/borrowed utility-manager lifetime.

Storage audit is not read-only: `storage_integrity.storage_audit` calls
`verify_replica`, which persists observations through `_update_replica_observation`.
That helper checks current state/revision inside the metadata unit of work.
The durable mapping reloads records rather than caching them
(`storage/storage_manager/database_mappings.py`); inspect the repository's actual
transaction/update guarantees before putting it on another connection.
A10 owns broader storage writer migration.

Persisted backup is another candidate, but `run_persisted_backup_job` has a
side-effect/checkpoint gap and currently submits only a mutable workflow ID.
Do not claim that job leases or keys alone fix domain publication/recovery.

No production integration code has been changed in this slice.
