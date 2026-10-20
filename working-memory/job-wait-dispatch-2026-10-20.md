# Job wait dispatch checkpoint, 2026-10-20

Branch: `core-job-wait-dispatch`, based on rating PR #137 at `875065b7`.
Published as [PR #138](https://github.com/ajCameron/LiuXin-alpha/pull/138) at
`1e98ecf5`; quick CI lanes are green and the full suite is pending.
This is JOB01, the first part of A09. **The full A01–A16 programme remains open,
and A09 still requires durable workflow integration. No PR has been merged.**

The six staged storage files and dirty data submodule are unrelated work.
`/tmp/liuxin-docs-pr-preserved-storage.json` verifies their contents/index.
Never include those paths in this branch's commits.

## Parent CI correction

PR #136's PostgreSQL prerequisite is green. Rating PR #137 initially failed
because standalone drivers had no macro facade binding before transactional
rating bootstrap. Commit `875065b7` binds them before discovery/repair.
All 56 lifecycle contracts and the local confidence stream (304 passed, three
skipped) passed. Updated push confidence, quality and packaging CI lanes are
green. Full PR #137 CI subsequently passed **7,525 tests, 289 skipped,
17 xfailed**. See the [rating handoff](rating-validation-2026-10-20.md).

## Implementation and limits

Core's query registration now captures handler, description/permissions,
serialization and result policy in one immutable record. Normal and replacement
registrations default to serialized execution. Only built-in `jobs.wait` and
`jobs.result` opt out; both use the independently synchronized utility manager.
Preference-context resolution remains under the shared handler lock before
releasing it for either wait.

Queued commands/queries recheck shutdown before entering their handlers.
A refused queued command emits `command.failed` after its earlier `command.started`.
An admitted wait may finish after runtime shutdown; borrowed manager ownership,
timeout outcomes and backend-specific cancellation are unchanged.
Injected managers must support concurrent wait/result/state calls.

The docs explain the contract and mark A09 partial. Other synchronous long
operations can still monopolize dispatch. `jobs.repository`, workers/scheduler,
utility execution and domain journals remain distinct owners; no durable
workflow migration or restart/idempotency guarantee is claimed.

## Validation

- `/tmp/liuxin-a09-red.log`: unchanged dispatch reproduced **nine failures,
  five passing controls**. Failures covered local/HTTP responsiveness, shutdown
  behind waits, queued handlers running after closure and mixed replacement
  result policy.
- `/tmp/liuxin-a09-fixed.log`: initial correction passed **14 tests in 19.09s**.
- `/tmp/liuxin-a09-regressions.log`: **234 passed in 202.55s**, covering broader
  Core/HTTP/job/lifecycle/docs contracts and two additional disconnect cases.
- `/tmp/liuxin-a09-final.log`: **86 passed in 35.59s** on the final preference
  setup policy, including all 16 new dispatch cases, configuration propagation,
  developer/public documentation, docstring tools and workflow/type contracts.
- `/tmp/liuxin-a09-quality.log`: complete quality gates passed: 196 formatted
  files, 244 protected import modules, 1,802 layer modules, 25 cold-import
  budgets, lint/complexity, both type checkers and both 38-example negative
  contracts. Mypy checked 200 source files.

These selections overlap. Logs are also archived under ignored
`working-memory/test-results/job-wait-dispatch-20261020/`.
The final follow-up after those runs changes only prose and handoff evidence.

Tests use serial execution, telemetry and immediate scratch cleanup in
`/tmp/liuxin-a09-review`, detached at `875065b7` with the scoped overlay.
`/tmp/liuxin-sync-a09.py` copies the scoped files and verifies protected edits;
`/tmp/liuxin-a09-files.json` is the explicit commit manifest.
The new concurrency tests are enrolled in formatting/lint checks.

Publication target: focused PR based on `catalog-rating-validation`; query
`gh pr view core-job-wait-dispatch` for its current review/CI status.
Next implementation: JOB02/A09 with one real durable workflow and its recovery
contract. Full suites remain delegated to CI.
