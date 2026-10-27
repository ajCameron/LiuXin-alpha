# Storage record owner checkpoint, 2026-10-20

Branch `storage-owned-record-mutations`, based on submission PR #140 at
`5cbd7a6b`. This is A10's first family and a prerequisite for A09's unpublished
durable audit. The full A01–A16 programme remains open; no PR is merged.

## Result

- Core generic Asset/Replica CRUD now refuses mutation. Inspection remains.
- Asset policy assignment uses the manager's serialized unit of work, preserving
  omitted fields, validating current policies/recreation rules, honoring
  `if_revision` and returning the committed revision.
- Existing-drive FRBR backup indexing adopts/verifies through storage. New
  identities use real SHA-256. Source changes persist unhealthy observations
  and refuse planning without redefining the old Asset.
- Maintained Core fixtures declare Assets and adopt actual files through the
  owner. Documentation records compatibility changes and partial effects.
- Historical SHA-512-plus-size identities are not silently rewritten.
- Other generic resource families and legacy `files` reconciliation remain open.

See [the mutation contract](../dev-docs/storage_and_backup/record-mutations.md)
for exact scope and caller migration.

## Validation

Eight Core ownership tests failed against the parent. Three scanner tests
separately reproduced incorrect hashing and silent Asset redefinition.
The first combined run passed 25 cases; six HTTP cases were denied localhost
sockets by the sandbox. The first socket-enabled run collided with concurrent quality/test scratch cleanup;
no production change was made for those harness errors. The isolated rerun passed
**80 tests, with nine optional PostgreSQL cases skipped**. Documentation/tooling
checks passed **37**. Scanner ownership and both prototype cases passed in the
initial selection. Counts overlap.

Complete quality gates passed: 208 formatted files, 244 protected imports, 1,802
layer modules, 25 import budgets, lint/complexity, both type checkers (200 mypy
sources) and both 38-example negative contracts. Logs are under
`/tmp/liuxin-record-owner-*`. Keep future tests and quality runs sequential, and
give test invocations a dedicated basetemp to avoid cleanup collisions.

Local tests run serially with telemetry and immediate scratch cleanup, using
Linux temporary storage. Full-suite validation belongs in PR CI.

Parent PR #139 full CI: 7,566 passed, 290 skipped, 17 xfailed.
Parent PR #140 full CI: 7,603 passed, 290 skipped, 17 xfailed
(run `39493435860`, job `118872230803`).

## Next

After publishing this slice, advance `core-durable-storage-audit` onto it while
preserving its uncommitted integration. Both branches change quality enrollment;
merge both file lists. Main also has six unrelated staged storage edits and a
dirty data submodule: preserve their contents/index using the snapshot at
`/tmp/liuxin-docs-pr-preserved-storage.json`.

The audit review worktree is `/tmp/liuxin-a09-review`; the independent prerequisite
worktree is `/tmp/liuxin-storage-record-boundary`. Audit tests previously passed
84 cases before this prerequisite. Finish audit docs and final combined checks,
then publish its PR. Broader A10 and A11/A12/A13/A16 remain programme work.
