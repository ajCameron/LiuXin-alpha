# Durable Core audit checkpoint, 2026-10-20

Branch `core-durable-storage-audit` is now based on A10 prerequisite PR #141,
`7f67a4d1` (`storage-owned-record-mutations`). The audit integration is verified and being committed/published. The full A01–A16 programme remains open; no PR is merged.

## Completed prerequisites

- #139 durable leases: full CI 7,566 passed, 290 skipped, 17 xfailed.
- #140 delivery identity/retry ancestry: full CI 7,603 passed, 290 skipped,
  17 xfailed (run 39493435860, job 118872230803).
- #141 storage record ownership: published, full CI pending. Generic Asset/Replica
  writes are refused; policy assignment and FRBR backup indexing use storage.
  Local checks: 80 passed/9 optional PostgreSQL skips, 37 docs/tooling passes,
  scanner/prototype cases and complete quality gates. See its handoff.

## Audit result

The [contract](../dev-docs/jobs/core-storage-audit.md) describes stable namespaced
IDs, lazy library-bound ledger, delivery deduplication, explicit retries, bounded
progress/event cursors, partial cancellation receipts and interrupted-run state.
Only the storage audit is migrated. Its worker owns a separate LibraryServices
graph; utility IDs/managers remain borrowed. The CLI submits and uses bounded
waits. Synchronous storage.audit remains supported.

Review also fixes relative ledger paths at composition, refuses populated
unowned ledgers, and checks both SQLite drivers during actual worker execution.
Strict typing enrolls the new modules and their actual dependency APIs, without
permissive overrides; utils/jobs/api.py gains the concrete pipe annotation.

## Validation and next action

Earlier Core/lifecycle/CLI regression selection: 84 passed. Expanded strict mypy
passed 205 sources. Final integration/repository selection: 77 passed, one test
fixture missing JobDefinition.payload_json; corrected. Final combined Core/lifecycle/CLI/docs checks: **91 passed**. Final existing
utility-job/Core application/repository/docs checks: **70 passed**. All 15 audit
cases pass, including the corrected fixture. Counts overlap. Complete quality
gates passed: 211 formatted files, 244 protected imports, 1,804 layer modules,
25 import budgets, both type checkers (205 mypy sources) and both 38-example
negative contracts. The dynamic-import baseline changes only the existing
utils/jobs/api.py location from line 191 to 194 after its pipe annotation.
Logs: /tmp/liuxin-audit-{final,regressions-final,compatibility,quality-complete}.log.

Run tests and quality checks sequentially. Concurrent pytest invocations collided
through shared scratch cleanup; concurrent mypy creation triggered the repository
leak guard. Use a unique --basetemp, -n 0, --telemetry-dir and immediate cleanup.
Do not run a full suite locally; use PR CI. C: headroom is around 7 GiB, Linux /tmp
has ample space. No PostgreSQL server is currently running.

## Worktrees and preservation

Main: /mnt/c/dev/liuxin-alpha, branch core-durable-storage-audit.
Review: /tmp/liuxin-a09-review, detached at 7f67a4d1 with audit overlay.
Prerequisite: /tmp/liuxin-storage-record-boundary, branch storage-owned-record-mutations.
Sync helper /tmp/liuxin-sync-audit.py uses git diff HEAD (staged and unstaged),
manifest /tmp/liuxin-audit-files.json. Format in the review checkout and copy only
owned files back. Both branches' quality enrollment was preserved on advancement.
Backup of the pre-advancement audit: /tmp/liuxin-audit-before-owner.

Main contains six unrelated staged storage edits plus a dirty data submodule.
Their file hashes and exact index entries are in
/tmp/liuxin-docs-pr-preserved-storage.json. Preserve them; commit only the audit
manifest paths. Main's audit changes are partly staged after three-way application.
Publish the focused PR on storage-owned-record-mutations, inspect CI, and continue
remaining A10 and A11/A12/A13/A16 work. No local full-suite rerun is needed.
# CI correction, 2026-10-20

The initial full PR #142 suite reported 3 failures with 7,627 passes: optional
APSW was not installed on CI, the new audit/event endpoint protocol docstrings
missed the maintained service links, and the surface factory expectation omitted
`durable_job_path=None`. Added the optional-backend guard, completed both endpoint
and service parameter/result docs, updated the endpoint count to 82 and retained
the surface's exact forwarding assertion with the new argument.

The complete affected audit/service/surface selection now passes 63 tests.
The parent PR #141 full suite is green: 7,615 passed, 290 skipped, 17 xfailed.
Composite work remains isolated on `storage-owned-composite-mutations`; propagate
this correction through policy PR #143 before publishing the next slice.
