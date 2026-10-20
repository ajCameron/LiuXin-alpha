# Managed deletion checkpoint, 2026-10-20

The full A01–A16 programme remains open. A05 is implemented on
`storage-deletion-policy`, based on A04's `e9efedc4`. Implementation commit `938f9a22` is published as
[PR #133](https://github.com/ajCameron/LiuXin-alpha/pull/133).
Preserve the six unrelated staged storage files and dirty data submodule exactly;
the hash/index snapshot is `/tmp/liuxin-docs-pr-preserved-storage.json`.

## Implemented contract

One StorageDeletionMixin owns both public byte-deletion routes. It refuses
read-only, unregistered and protected UNMANAGED/ARCHIVE/BACKUP removal, requires
fresh distinct survivors, and enforces applicable minimum/separation policy.
Durable metadata units serialize through the existing macro transaction owner
(SQLite BEGIN IMMEDIATE / PostgreSQL advisory transaction lock), reading past
stale local caches. Deletion requires outermost units and commits DELETING
reservations before external I/O. Competing operations cannot count those claims
as survivors. Verification/reconciliation cannot clear a pending reservation;
revision checks also stop late verification reviving a completed tombstone.

StorageDeletionError carries phase, attempted reservation revisions and a
three-valued byte outcome. Core evacuation and CLI receipts preserve it.
Operational status reports manual inspection, not automatic retry. Metadata-only
removal remains distinct; raw Store/driver operations are privileged. See
[the contract](../dev-docs/storage_and_backup/deletion_policy.md).

No automatic recovery, destructive override, zero-copy eviction, native APSW
connection certification or OS-crash simulation is claimed. A10 still owns the
remaining direct storage-record writers. Manual repair requires the original
operation to be stopped, fresh byte inspection and exact revision checks.

## Validation

The final isolated candidate run passed **110 tests** with live PostgreSQL,
SQLite, independent managers, stale Asset/Replica cache, fault injection, Core,
CLI, late-verification concurrency, read-only builders and API/doc examples.
Complete quality gates passed: formatting, annotation coverage, import cycles,
layer direction, import budgets, lint/complexity, basedpyright and mypy including
both negative contract suites. The final documentation/link/workflow follow-up passed 58 tests; later edits only clarify documentation.

Earlier broad caller run: 403 passed, five failed and five skipped. Failures were
one missing public Example section and four stale tests expecting unrestricted
manager deletion; all were corrected and passed the final selection. Earlier
boundary/live runs found test setup defects, an inherited PostgreSQL schema
issue and a concurrent quality cache artifact; the final candidate avoids those
fixture/artifact problems. Do not add overlapping selections into a fabricated
unique-test total. The initial nine-failure red run included two UNMANAGED setup
errors; the corrected fixture exercises the intended refusal.

Full-schema PostgreSQL cache discovery encounters a generated link column longer
than PostgreSQL's identifier limit. Reproduced on unchanged A04 parent e9efedc4.
The competing-cache regression uses the real schema-backed cache over its
Asset/Replica tables, not a fake repository; it does not certify full-schema
PostgreSQL cache loading. The separate schema contract needs a compatibility fix.

Logs are copied into ignored `working-memory/test-results/deletion-policy-20261020`.
Telemetry is under `/tmp/liuxin-a05-telemetry`. All local runs are serial with
immediate pytest scratch cleanup. Do not run another full suite on this VM;
use PR CI. Quality caches must be outside the candidate checkout.

## Predecessors and remaining work

[A04 PR #132](https://github.com/ajCameron/LiuXin-alpha/pull/132) is green on
`e9efedc4`, full CI run 39450272043: **7,403 passed, 259 skipped, 17 xfailed**,
plus green quality, wheel, format and confidence lanes. Its optional APSW fixture
passes 19 tests with APSW, or 15 with four explicit skips when absent.
[A15 PR #131](https://github.com/ajCameron/LiuXin-alpha/pull/131) is green on
`a0e4d2ed`, full CI: 7,388 passed, 249 skipped and 17 xfailed. No PR was merged.

PR #133 is based on `database-transaction-ownership`; check its separate remote CI. Next runtime work
is A06 server-enforced transport capabilities, followed by the remaining tasks
in the canonical architecture plan. Do not mark the whole programme complete.

The isolated candidate is `/tmp/liuxin-a05-review`; its HEAD predates the overlay.
The disposable PostgreSQL cluster uses `/tmp/liuxin-a05-postgres/data`, socket
`/tmp/liuxin-a05-postgres/socket`, port 55435. It was stopped after local validation; no local tests remain running.
The sync script `/tmp/liuxin-sync-a05.py` excludes and verifies the preserved files.

## Confidence-suite caller migration

PR #133's confidence run exposed one remaining old Library-facade test that
expected deletion of its only copy. It now asserts unchanged bytes/metadata on
refusal, creates a verified survivor, and then verifies deletion, tombstone and
surviving content through the Library facade. All **10 Library facade checks**
passed on the isolated A05 follow-up. No runtime policy was weakened. The updated
parent requires its own remote confidence/full CI result. A06 transport work is
on a separate branch and is not part of this follow-up.
