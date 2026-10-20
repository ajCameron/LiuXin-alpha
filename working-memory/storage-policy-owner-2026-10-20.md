# Storage policy mutation checkpoint, 2026-10-20

Branch `storage-owned-policy-mutations`, worktree
`/tmp/liuxin-storage-policy-boundary`, based on audit PR #142 at `8e57d575`.
Main remains on `core-durable-storage-audit` with six unrelated staged storage
edits and a dirty data submodule. Preserve their contents/index using
`/tmp/liuxin-docs-pr-preserved-storage.json`. No PR is merged; the full A programme
remains open.

## Changes

`core/storage_policy_mutations.py` translates policy wire fields to existing
storage domain values and delegates create/update/delete to the manager.
Generic policy CRUD preserves owned envelopes/revisions and reference guards.
Partial updates fence against the source record even without a client token.
Unsupported bookkeeping fields and duplicate aliases are rejected; other
resource families do not silently accept a revision token they cannot enforce.
Post-commit resource readback/cache failures retain the canonical receipt.

Storage continues to own policy constraints, recreation checks and transactions.
The adapter adds no second policy registry. Both domain field names and existing
backup/JSON aliases are admitted. Numeric strings and ambiguous boolean values
are rejected at the wire boundary. See the
[mutation contract](../dev-docs/storage_and_backup/record-mutations.md).

## Validation

All **14 initial integration cases failed on the parent**, across SQLite and APSW.
After migration, **41 Core/storage cases passed**. Further cases cover interleaved
updates, committed-readback failure and wire aliases/types: **32 contract cases
passed**. Documentation/tooling checks passed **36**. Counts overlap. Complete
quality gates passed: 213 formatted files, 244 protected imports, 1,805 layer
modules, 25 import budgets, both type checkers (206 mypy sources) and both
38-example negative contracts. Logs: `/tmp/liuxin-policy-owner-*`.

Run test invocations and quality checks sequentially, with unique --basetemp,
serial -n 0, telemetry and immediate scratch cleanup. Full suites belong in CI.
The storage-owner PR #141 and audit PR #142 have green quick checks; their full
suites were still running at the last check.

## Next

The slice is verified; commit/push and publish this
focused PR on `core-durable-storage-audit`. Remaining A10 families include
Composite/Item links, retained files reconciliation and broader recovery receipts.
A11 lifecycle migration, A12 scratch/startup ownership, A13 provider/conversion
outcomes and A16 compatibility retirement also remain.
