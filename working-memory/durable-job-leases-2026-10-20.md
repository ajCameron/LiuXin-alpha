# Durable job lease checkpoint, 2026-10-20

Branch: `durable-job-leases`, based on JOB01 [PR #138](https://github.com/ajCameron/LiuXin-alpha/pull/138)
at `1e98ecf5`. **A09 and the full A01–A16 programme remain open. No PR is merged.**
The six staged storage files and dirty data submodule remain protected by
`/tmp/liuxin-docs-pr-preserved-storage.json`.

## Scope

Before connecting the durable ledger to a production Core workflow, fix its
claim/state contract. SQLite immediate transactions now coordinate enqueue
policy, claim selection, state and corresponding events. Every internal
connection is closed. A fresh token fences worker updates even when a worker
name is reused; expired running attempts become explicitly aborted with
uncertain effects and are not replayed.

Runs capture kind/payload at enqueue. A bounded schema migration adds token and
payload columns, capturing legacy inputs from the current definition once.
Workers renew long handlers independently of progress callbacks and persist
setup/validation/serialization failures while their claim remains valid.
Cancellation before execution prevents entry; cancellation acknowledged after
return preserves the completed-effect receipt. Retention prunes result payloads
without deleting live runs, identities, status or event cursors.

Worker repository calls and manually constructed JobRunContext now require a
claim token. Repository, worker and retained backup-handler callers are migrated.
Old worker binaries must be stopped before schema/API upgrade.
Definition timeout/retry fields remain stored declarations, not active enforcement.

This does not wire the ledger into Core, provide idempotent submissions or add
retry ancestry. Storage audit is not a purely read-only candidate: verification
updates replica state. Consider the existing persisted-backup workflow owner or
an explicitly scoped audit contract for JOB02, preserving domain recovery evidence.

## Validation and continuation

- `/tmp/liuxin-durable-jobs-red.log`: five failures on the unchanged ledger:
  stale start, lost running state, missing-handler setup, queued cancellation
  entering the handler, and leaked owned connections.
- `/tmp/liuxin-durable-jobs-initial.log`: 11 passed, including all existing
  repository/worker/backup-handler tests.
- `/tmp/liuxin-durable-jobs-contracts.log`: 28 passed, including concurrent
  claims/enqueue, same-name fencing, process restart, event rollback, renewal,
  payload snapshots, retention and the fixed legacy-schema fixture.
- `/tmp/liuxin-durable-jobs-final.log`: **154 passed in 42.05s**, including all
  32 durable-job cases, utility execution, Core job clients/concurrency,
  documentation, formatting-runner and ownership contracts. New controls cover
  late success/failure after recovery, renewal-thread startup failure and
  cooperative cancellation.
- `/tmp/liuxin-durable-jobs-quality.log`: complete quality gates passed:
  204 formatted files, 244 protected import modules, 1,802 layer modules,
  25 cold-import budgets, lint/complexity, both type checkers and both 38-example
  negative contracts. Mypy checked 200 source files.

Selections overlap. Logs are archived under ignored
`working-memory/test-results/durable-job-leases-20261020/`.
Only prose/handoff evidence changes follow the final validation.

Parent rating PR #137 is fully green at `875065b7`: **7,525 passed,
289 skipped, 17 xfailed**. JOB01 PR #138 is also fully green at `1e98ecf5`:
**7,540 passed, 290 skipped, 17 xfailed** (run `39486510953`).

The isolated checkout `/tmp/liuxin-a09-review` is detached at `1e98ecf5` with this
overlay. `/tmp/liuxin-sync-durable.py` copies scoped changes and verifies
protected files; `/tmp/liuxin-durable-files.json` is the commit manifest.
Tests are serial with telemetry and immediate scratch cleanup.

Published as [PR #139](https://github.com/ajCameron/LiuXin-alpha/pull/139)
at `ac3a6728`, based on `core-job-wait-dispatch`; its quick CI lanes are green.
Continue with the [submission checkpoint](durable-job-submissions-2026-10-20.md)
and then JOB02/A09's production workflow and recovery contract.
Do not mark A09 complete on these prerequisites.
