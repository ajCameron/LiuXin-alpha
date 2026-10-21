# Composite mutation ownership — 2026-10-20

Continues A10 on `storage-owned-composite-mutations`, based on policy PR #143
at `527301bf`. The full A01–A16 programme remains open. No PR is merged.

## Implemented boundary

`core/storage_composite_mutations.py` translates wire values into the existing
storage declaration/replacement/forget APIs. Generic Composite creation requires
complete nonempty membership; updates preserve omitted values and check the
revision used to construct the replacement, even without a client token. Name,
attributes and complete member lists are the only writable fields.

The storage owner persists the record envelope, scalar/member rows and revision
together. Deletion protects Item and derivation references and retains atomic
Assets and bytes. Core readback/cache errors preserve committed receipts.
`composite-member-link` is now inspectable only; the maintained Core acquisition
fixture creates a complete Composite instead of staging a parent and raw members.
The adapter and regressions join the existing formatting/lint/type scopes.

The [mutation contract](../dev-docs/storage_and_backup/record-mutations.md)
documents payloads, a novel/introduction example, caller migration and limitations.
Architecture and storage task owners reflect this selected family.

## Evidence and discovered follow-up

Initial parent run: 24 failures / 14 passes, including two failures in the new
Item fixture (empty macro insert, corrected to explicit Item fields). The first
implementation run passed 54 cases and exposed two existing SQLite constraint
failures. Final combined Composite, policy and Core selection passed 90 tests.
The final creation-refusal/documentation/tooling run passed 38 tests; selections
overlap. Complete quality gates passed: 215 formatted files, 244 protected imports,
1,806 layer modules, 25 cold-import budgets, lint/complexity, both type checkers
(207 mypy sources), and both 38-example negative contracts.

SQLite's generic role-style link constraint is unique on `(Composite, Asset,
stored role)`, while Composite membership is ordered by sequence. Consequently
two occurrences of one Asset with the same projected role fail despite distinct
positions. Distinct stored roles work. The new tests verify both supported
replacement and complete rollback on the conflicting case; no SQL bypass or
schema change is hidden in this adapter.

Repair that schema in a focused follow-up with existing-catalogue preservation
and PostgreSQL parity. The inline constraint comes from
`databases/database_driver_plugins/SQL/utility_mixins.py`; the generator also adds
a unique parent/sequence index. Removing only the extra pair/role restriction
requires handling existing SQLite tables, their indexes, triggers and dependent
views, not just changing fresh database generation.

Other A10 work includes Item-target links, backup workflow definition/source
adapters, retained `files` reconciliation and broader partial-effect receipts.

## CI repairs carried forward

Audit PR #142's first full suite exposed three failures: missing optional APSW
guard, new endpoint/service documentation contracts, and a surface factory
expectation missing `durable_job_path`. Correction `63c75729` passed 63 affected
checks and all quality gates. Policy PR #143 carries it forward plus its own
optional APSW guard; 34 policy/documentation checks passed at `527301bf`.
Their full suites rerun remotely. PR #141 is fully green: 7,615 passed,
290 skipped, 17 xfailed.

## Workspace and test discipline

Main's six unrelated staged storage edits and dirty data submodule are preserved.
Their hashes and index entries are recorded in
`/tmp/liuxin-docs-pr-preserved-storage.json`; do not reset, stash or commit them.
The isolated worktree is `/tmp/liuxin-storage-composite-boundary`.

Run tests and quality gates sequentially. Use `-n 0`, a unique Linux `/tmp`
basetemp and telemetry directory, and `-o tmp_path_retention_policy=none`.
Leave full suites to CI; C: had about 7 GiB free during this work.
