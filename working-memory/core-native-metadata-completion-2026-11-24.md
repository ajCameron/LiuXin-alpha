# Core native metadata completion — 2026-11-24

## Completion and workspace

All three requested native metadata groups are implemented and locally verified:
WEMI creation/deletion and connected construction; shared records, relationships
and trees; bounded atomic batches and search/discovery. This handoff accompanies
the completion commit on `core-review` (base `bfb2c208`). No push or PR was requested.

Worktree: `/tmp/liuxin-core-review-20261123`. The original checkout at
`/mnt/c/dev/LiuXin-alpha` stays on `cache-review`, with its preexisting dirty
`LiuXin_alpha_data` and six stashes untouched. No subagents or skills were used.
Python is the shared `/tmp/liuxin-pr-192-venv` (3.14) through `.venv/bin/python`.
The disposable PostgreSQL 16 server at `/tmp/liuxin-native-metadata-pg`, port 55440,
was stopped after acceptance. The unrelated system PostgreSQL cluster and other
projects' workers were left alone.

## Implementation and ownership

The former `core/program_services/native_metadata.py` is now a package at the same
import path, split into common validation, entities, shared records, relationships,
trees and batches. It installs 51 operations: 19 queries and 32 commands.

- Named WEMI CRUD, connected stack creation, attachment/replacement and links.
- Shared family reads/CRUD, exact identity ensures and literal suggestions; Agent
  subtypes, contextual text/identifiers, credits and Work Tag/Series memberships.
- Genre, Subject and Series trees: roots/children/ancestors/subtree, create/ensure,
  move/reorder and explicit constrained deletion.
- Atomic batches allow up to 100 native writes, backward JSON result references
  and initial revisions. Child result projection happens before commit; the
  aggregate receipt owns persistence and cache publication.
- Strict finite JSON controls, depth 48 / 100,000 values, page/identity/tree bounds
  of 1,000. Search `limit=null` is intentionally rejected.

Catalog retains domain policy and transactions, the metadata service owns cache
reconciliation, and Core adapts transport. No new top-level authority was added.
See `dev-docs/core/native-metadata.md` and the native completion follow-up in
`dev-docs/core/review-and-completion-plan-2026-11-23.md` for contracts and evidence.

## Defects repaired during acceptance

Reserved Series ID zero entered default-bootstrap root lists and prevented full
sibling reorder. Catalog now supplies exclusions before physical SQL pagination;
cached tree queries apply the same exclusion. The stored sentinel remains readable
as a reserved shared record. Mutation IDs stay positive. Portable macro exclusions
are optional; the existing hydrator fake was updated to match the new argument.

PostgreSQL cache publication repeatedly probed physical schema. One diagnostic
create took 21.155s (20.689s in refresh); a standalone reload took 19.334s / 216
physical probes, versus 2.188s / two probes inside the existing driver scope.
`ReadCache._run_full_refresh` now shares that scope for READY publications, verifies
exit before readiness and retains DIRTY on entry/read/exit failure. Caller SQL
transactions retain ownership. Clear/close never probe. SQLite's scope is a no-op.
The profile is diagnostic evidence, not a latency guarantee.

## Validation evidence

Log prefix: `/tmp/liuxin-native-metadata-`. Selections overlap; do not add totals.

- All 80 new SQLite-family workflow cases have passing partition evidence:
  32 WEMI/batch cases across SQLite/APSW, four cache modes and local/HTTP;
  32 shared-record/tree cases across SQLite, four modes and local/HTTP;
  16 reserved-Series cases across both SQLite drivers and all modes/transports.
  `completion` supplied 16 workflow passes; `repairs` supplied 14 batch passes;
  `shared-3` supplied 28 shared/tree passes. `boundary-verified` passed all 24
  selected cases (2,611.65s), covering earlier HTTP/numpy timeouts and all sentinels.
- All 40 atomicity cases passed across `atomicity-verified` (34, 947.73s) and
  `refresh-core` (14, 275.82s; eight repeated caller-transaction cases plus six
  new schema-exit failure cases). Serialization rollback, revision competition,
  caller transactions, full 100-operation batches and committed/dirty receipts
  are exercised.
- `validation-final`: 135 passed (119 native validation + 16 quality-runner checks).
- `refresh-unit-final`: 104 passed, six plugin-inapplicable skips, including 42 new
  refresh-scope lifecycle cases. `refresh-pg`: 15 passed, including three new real
  PostgreSQL probe-count/transaction-ownership cases and 12 discovery regressions.
- `existing-fixed`: 41 passed (hydrator fake and Core metadata commands).
  `existing`: 128 prior workflow/contract cases passed.
- `broad`: 884 passed, 13 failed, one inapplicable pg_temp harness skip, 4,397.48s.
  Nine failures used the old imported Series predicate, two the old fake signature,
  and two exceeded the 10s HTTP timeout. `broad-rerun` passed all 11 repaired cases;
  `workflow-timeout` passed the remaining two with the explicit 120s functional
  allowance (83.47s). A JUnit audit confirmed all 13 original failures have passing
  follow-up evidence. The broad run included real PostgreSQL portable macro tests.
- All 20 PostgreSQL native acceptance cases passed. Audit script:
  `/tmp/liuxin-native-metadata-audit.py`. Its exact named-case audit reads these XMLs:
  `postgres-scoped-workflows` (four local workflows), `postgres-scoped-http`
  (three HTTP workflows), `postgres-scoped-http-numpy` (one HTTP workflow),
  `postgres-scoped-batches` (eight), `postgres-scoped-shared` (three shared/tree),
  `postgres-scoped-shared-numpy` (one shared/tree, 945.97s).
  Superseded partition tails were deliberately interrupted only after their
  assigned cases passed; remaining cases ran independently. Nameless interrupted
  setup placeholders are excluded from the exact named-case audit.
- `quality-complete`: full quality gate green: 493 formatted files, 482 annotation
  targets, 268 import modules, 2,098 direction modules, 31 cold-import budgets,
  lint/complexity, basedpyright zero errors/warnings, mypy 581 files, and 127/123
  rejected negative type examples. The subsequent test-timeout call-only edit
  passed targeted lint/format checks.
- Documentation link/public-boundary selection: 22 passed. Diff whitespace check
  passed. Final documentation verification is recorded in `docs-complete.log`.

Earlier superseded PostgreSQL runs exposed actual 120s request failures and led to
the schema-scope fix. They are diagnostic history, not current passing acceptance.
Tests use PYTHONPATH=src and
LIUXIN_PLUGIN_CACHE_PATH=/tmp/liuxin-core-review-plugin-cache.json. Live PostgreSQL
acceptance uses LIUXIN_TEST_POSTGRES_URL with disposable schemas. The private test
server is stopped; restart/provision an isolated server before rerunning live tests.

## Explicit limits and next work

Fresh PostgreSQL schemas currently leave `languages` empty, unlike SQLite's seeded
constants. Acceptance first verifies that empty result, then loads an explicit
English fixture for read-only interface tests. Database creation and an explicit
existing-catalogue upgrade/repair policy must preserve IDs, references and restore
semantics. This is recorded in both Core and database next_tasks; queries must not
silently bootstrap data. Native PostgreSQL unmanaged-disk ingestion's separate
legacy `files` dependency remains open too.

Heavy functional HTTP fixtures allow 120s per request. Production defaults remain
10s; this matrix does not certify that deadline under load. Broader performance
and resource budgets remain Core completion slice 6. No whole-Core sign-off is
claimed by native metadata completion.

Next recommendation: slice 4, recoverable command identities and durable outcome
lookup. CoreCommand already has an ID, but reusing it currently repeats effects.
Start with metadata/database writes: bind ID to operation/payload, atomically retain
outcome where possible, reject conflicting reuse, and test disconnect/crash/replay
boundaries. External effects require their domain recovery contracts. That next
slice has been discussed, not implemented here.
