# Next database tasks

Date: **2026-10-20**. DB01–DB03 are implemented for PR review; the remaining entries are
proposed work grounded in the driver/schema review. Cross-project order belongs
to the [architecture tasks](../architectural_reviews/next_tasks.md).

**2026-11-06 fifth driver review:** [DDP20–DDP22](database-driver-plugins-fifth-review-2026-11-06.md)
have local mitigations and 156 new required regressions, taking the manifest to
808. Completion guards share backend tokenization; SQLite closes owned readers
and checks old journals before publication; explicit PostgreSQL operators retain
their role. The review records validation. Published head 47e089d4's green CI
predates these changes: require fresh checks after publication. Exclusive offline
access remains required for rebuilds; broader connection setup consolidation and
measured inspection performance remain follow-ups.

**2026-11-06 fourth driver review:** [DDP17–DDP19](database-driver-plugins-fourth-review-2026-11-06.md)
have local repairs with 104 new mandatory cases, taking the gate to 652.
SQLite recognizes legal non-ASCII CTE names and leading empty statements;
PostgreSQL resolves temporal bindings by parameter count and rejects remaining
ambiguity explicitly. The physical-index fixture and optional-APSW test guard
address both failures from the old head's full CI run. Final validation is
recorded in the review. Require fresh CI before merging; structural consolidation
and measured inspection performance remain subsequent work.

**2026-11-06 third driver review:** [DDP11–DDP16](database-driver-plugins-third-review-2026-11-06.md)
have local repairs and 148 additional mandatory cases, increasing the gate from
400 to 548. Explicit CTE-batch transactions, native PostgreSQL value preservation,
completed RETURNING results, exception-safe group cleanup, shared SQLite schema
facts and contextual SQL keyword handling address all six findings. The review
records validation and the write-result buffering tradeoff. Structural consolidation
and measured PostgreSQL inspection performance remain the next driver tasks.

**2026-11-05 driver follow-up:** all six findings in the
[database driver plugins review](database-driver-plugins-review-2026-11-05.md)
now have local repairs and 110 added mandatory regression cases: WAL-safe
backup, explicit recovery, bounded handle ownership, lexical SQL adaptation,
transaction-preserving dump and validated populated restore. Remaining structural
work is to consolidate duplicated stdlib connection setup and split the largest
macro/driver implementation modules without changing their public contracts.
Also profile the full-schema PostgreSQL inspection walk: its existing integration
test passed but took 140.2 seconds; the review records the measurement and limits.

**2026-11-05 second review:** [DDP07–DDP10](database-driver-plugins-follow-up-review-2026-11-05.md)
now have local repairs and 50 additional mandatory cases. Direct batches execute
once and roll back failures, qualified SQL identifiers retain their operand role,
and uniqueness shares the physical reader's index facts. The obsolete scratch
redirection API is retired; use a verified backup with a separate owner as
documented in [working copies](creation-and-replacement.md#independent-sqlite-working-copies).
The gate now requires 400 cases. Structural consolidation and measured PostgreSQL
inspection performance remain the next driver tasks; these fixes do not replace
the planned production import trial.

## DB01. Make creation safe for an existing destination

**Implemented for PR review, 2026-10-20; architecture A03.** Ordinary SQLite/APSW
creation builds privately and publishes create-only, including the registry,
Database, Core and terminal paths. Explicit offline replacement requires a new
verified recovery backup; preparation failures preserve the original. PostgreSQL
initialization accepts absent/empty schemas, serializes creators and rejects
populated namespaces. See the [contract and regressions](creation-and-replacement.md).

## DB02. Specify transactions independently of connection lifetime

**Architecture A04; implemented for PR review, 2026-10-20.** The
[transaction contract](transactions.md) now defines owned units, nested/borrowed
savepoints, resource rollback and the reentrant facade mutex. It inventories
remaining direct commit points and explains the compatibility transition.
SQLite/APSW-named, live PostgreSQL, Catalog/Row/Agent, storage and lifecycle
checks pass. The [handoff](../../working-memory/transaction-ownership-2026-10-20.md)
records counts, optional legacy limitations and separate full-CI status.

## DB03. Align default WEMI selection

**Implemented for PR review, 2026-10-20; architecture A07.** Catalog's
[selection policy](../catalog/catalog-wemi-selection.md) now matches the existing
SQL complete-ray ranking. Partial bundles remain available when SQL has no
complete row. Bilingual, novel/introduction and omnibus fixtures retain every
reachable Work. Unlinking removes the edge without promoting flags; the next
read ranks remaining candidates. Existing SQL views need no migration.

## DB04. Keep schema declarations and actual write enforcement distinct

**Next; coordinate with Catalog A08 and storage A10.** Column metadata now
declares normalization/validation and the field guide matches all 67 explicit
tables. Add enforcement only through the domain owner. Add bounded coverage for
new declarations, uniqueness scopes and migration rollback on collisions.
Do not treat nullable placeholder rows or declared profiles as validated records.

**2026-10-30 implementation note:** [table SQL batches](table-sql-batches.md)
now provide declaration-driven key audit/repair with selected-row collision
checks, NULL scope handling and rollback. Repair is data-only and does not add
constraints or validate bibliographic meaning. DB04's broader enforcement work
remains with the relevant domain owners.

## DB05. Retire legacy connection/custom-column assumptions through callers

**Following.** Inventory `books`-based category/custom-column consumers before
changing them. Recreate connection-local cleanup after connection replacement
and metadata changes, and verify unrelated permanent integrity triggers survive.
Done when supported callers have an owner and tests cover two independent
connections; remove only compatibility code whose callers have migrated.

## DB06. Make generated PostgreSQL identifiers discoverable

**Implemented for PR review, 2026-10-20.** Full-schema cache discovery expected
`composite_digital_asset_digital_asset_link_composite_digital_asset_id`, but
PostgreSQL stores its truncated identifier. A05's live testing exposed this
before deletion; the same discovery error reproduces on unchanged A04 parent
`e9efedc4`. A08 subsequently exposed the same defect in schema-based writers.
The [discovery fix](postgres-identifier-discovery.md) resolves generated names
through PostgreSQL's own identifier representation, then requires membership in
the discovered headings. Existing physical names stay unchanged; there is no
schema migration. Live regressions cover full schema/cache discovery and public
schema writes, missing columns, multibyte clipping and DDL collisions.
Both snapshot cache backends also use native driver index metadata, replacing
SQLite-only probes that silently aborted PostgreSQL's shared transaction.

This does not provide SQLite's compatibility views or full legacy Library parity.

## DB07. Implement the maintenance consistency contracts

**Implemented and locally validated, 2026-11-01.**
[Maintenance modes](maintenance-modes.md) now has transaction-bound prompt
dispatch, persistent table subscriptions and a catalogue outbox, jobs handoff,
retry/backoff, explicit installation and status/drain/retry controls. Catalog owns
the prompt Work revision receipt and lazy expression-count summary. Legacy
creator plugins are no longer default registrations. Normalized keys and
validation remain in their existing write paths. The
[completion work](maintenance-completion-plan-2026-11-02.md) adds resumable Work
projection rebuilds and explicit implementation-authored plugin upgrades, then
measures scale and addresses additional domain-owned repair operations.

**2026-11-02 retirement:** removed the obsolete maintenance module, creator
plugins and title-aggregate callbacks. Catalog now owns bounded, scoped duplicate
inspection; destructive automatic duplicate repair is retired. The
[capability inventory](legacy-maintenance-retirement-2026-11-02.md) records what
was removed, which functions were only placeholders, and requirements for any
future semantic merge, aggregate, Agent sort or orphan-cleanup implementation.

**2026-11-02 restoration:** Agent rename and bounded sort repair now live in
Catalog, with Core/CLI previews and explicit apply. They preserve manual sorts by
default, support deliberate regeneration, and use atomic current-table writes.
See the [capability decision](legacy-maintenance-retirement-2026-11-02.md#2-creator-sorting-and-rename-maintenance)
and [operator examples](../../docs/maintenance.md#agent-names-and-sort-repair).

## DB08. Correct driver-wrapper boundaries and keep its API declarative

**Implemented and locally verified, 2026-11-04.** Native relation
inspection and SQLite schema refresh preserve the primary transaction and TEMP
objects. Single-column updates submit a targeted normalized write, preserving
other writers' unrelated fields. Custom-column definition insertion and DDL now
share an owned transaction; public creation refuses nested macro units so facade
bookkeeping follows a real commit. The generated legacy DDL remains SQLite-only.

Parent lookup delegates to the driver's current `*_parent`/`*_parent_id` rule, and
table-spec iteration filters views by actual relation kind. The wrapper API and
its schema-introspection base contain abstract declarations only: concrete
mixins own initialization and case-sensitivity aliases. New regressions guard
transaction ownership, interleaved writes, early/late DDL failures, real folder
traversal, view exclusion and API bodies. See
[transaction ownership](transactions.md) and
[connection lifetime](<02 - Connection ownership and stale connections.md>).

Validation: the final SQLite/APSW boundary selection passed **73 tests**; cache,
custom-column and maintenance integration passed **94**, with **nine PostgreSQL
cases skipped in that SQLite lane**. The separate live PostgreSQL wrapper
selection passed **nine tests**, including atomic rollback when its unsupported
SQLite custom-column helper is requested. PostgreSQL's existing direct-writer
and transaction cases also passed. These selections overlap; their counts are
not a unique-test total. Logs are under `/tmp/liuxin-wrapper-fixes-*.log`.

The final API/declaration selection passed **nine checks**. The full scoped
quality runner passed formatting, lint, complexity, import/layer/footprint checks,
basedpyright, mypy and both 38-invalid-call contract probes. All 94 local links in
the four edited guides resolve to versioned content. The private PostgreSQL
server was stopped and removed, and protected preference hashes are unchanged.

The regression owners are `tests/databases/test_driver_wrapper_review.py` and
`tests/databases/test_driver_wrapper_review_postgres_live.py`. They also verify
schema-query failures preserve the caller's transaction, native column discovery
notices schema changes, and the wrapper API has no operational method bodies.
Custom-column DDL remains SQLite-specific; adding PostgreSQL compatibility DDL
is separate work. No new full-repository suite run is claimed.

## DB09. Correct remaining wrapper metadata boundaries and type the wrapper

**Implemented and locally verified, 2026-11-04.** The
[follow-up review](driver-wrapper-review-2026-11-04.md) records five reproduced
faults and their repairs: preserving WEMI custom-column attachments, atomic
metadata/deletion operations, primary-connection view inspection, working
schema-version cache invalidation and restored declared types in table specs.

The whole wrapper package and its abstract API are included in both static
checkers and the formatting/lint gates. Portable query protocols and actual
missing-row sentinels are checked through valid and rejected-call examples.
The review records regression evidence. DB10 below implements physical
constraint introspection, view-row resource handling and cycle detection;
custom-column placeholders remain unresolved. The monitored real import remains
the next operational exercise.

Validation: **119** final boundary/API/tooling passes, **137** integration passes
(three skips/four existing xfails) and **25** live PostgreSQL passes. Selections
overlap. All scoped quality gates and both 44-invalid-call contract probes pass;
105 edited-guide links resolve. The private server is removed and protected
preferences are unchanged. See the review for logs and evidence limits.

## DB10. Add physical schema facts, bounded reads and optional diagnostics

**Implemented for review, 2026-11-04.** The
[feature guide](driver-wrapper-features-2026-11-04.md) records native constraint
facts, connection-aware fetch helpers, projected/filtered scans with resumable
IDs, live backend capability reports, and opt-in query observations. View-row
reads now preserve borrowed transactions and close resources on errors; hierarchy
traversal detects cycles. API declarations, concrete signatures and typed caller
examples cover the new surface.

Scans share the portable macros' existing pagination primitive. Callers own
snapshot lifetime and checkpoint durability; observations have no default timer
or SQLite progress handler. Remaining custom-column placeholders, compatibility
DDL portability and historical direct commit points remain separately scoped.
The guide records final validation evidence and limits.

## Maintenance operational follow-ups

Recorded **2026-11-03**. These are proposed additions after the
[completed maintenance tranche](maintenance-review-fixes-2026-11-03.md), with no
new signoff blockers. Preserve the prompt/lazy contracts and existing ownership:
Databases owns capture and transactional delivery, Catalog owns projection and
repair meaning, and Core/jobs owns worker orchestration and durable execution.

**Next step: a monitored SquashFS collection import.** Record import and drain
rates, pending counts and oldest age per plugin, catalogue/job-ledger sizes,
scratch usage, free disk space and process memory. Check final projections and
that eligible work drains; account for paused or failed work explicitly. Save
the workload, configuration and observations alongside the
[synthetic scale evidence](maintenance-scale-2026-11-02.md). Use these results to
prioritize the following additions and separate Catalog write costs from
maintenance delivery costs.

### 1. Explain why maintenance work is waiting

Extend the existing status surface, which already exposes pending counts, ages
and implementation availability. Distinguish pause, unsatisfied dependencies,
retry backoff, exhausted retries and missing/incompatible implementations. Add
last success/failure and recent processing rate, with a defined sampling window.
Report worker liveness separately from handler progress.

Keep status read-only and bounded. Core should combine runtime and jobs evidence
for CLI/other surfaces through one shared result contract. Done when an operator
can identify the reason for a stalled backlog and the appropriate existing
resume, retry or implementation action without examining SQL tables.

### 2. Add an opt-in retention policy

Build on the existing bounded journal pruning and age-based
`maintenance prune-jobs` command. Make enablement, cadence, age threshold and
per-run work budget explicit and visible in status. Reuse Core/jobs ownership
and suitable host scheduling facilities; keep installation and plugin upgrades
explicit.

Preserve pending/active work, retry ancestry and the existing proof of catalogue
completion before retiring delivery keys. Retention of unrelated storage/audit
history needs its own policy. Done when repeated scheduled sweeps safely reuse
database space, resume across protected pages and report what was retained.
Row retirement makes pages reusable; file compaction remains a separate action.

### 3. Provide plugin examples and reusable contract checks

Add a worked prompt plugin and lazy projection using the public contracts in
[maintenance modes](maintenance-modes.md). Include declarations, dependencies,
failure/retry behavior, replay, rebuilds and version upgrades. Extract reusable
author-facing fixtures from the existing failure and recovery coverage.

Done when a new plugin can run focused checks for transactional rollback,
acknowledgement, replay safety and lifecycle behavior on its supported backends.
Keep the trusted synchronous Python contract explicit; database rollback does
not undo filesystem/network side effects.

### 4. Document and validate worker supervision

Provide a supported Linux service setup for a long-lived maintenance worker,
first confirming the actual entry point and lifecycle. Document logs, graceful
stop, stalled-handler diagnosis, external termination and restart/recovery.
Lease renewal proves ownership and must not be presented as handler progress.

Core/jobs owns the execution lifecycle; deployment supervision handles stuck
synchronous Python. Done when the documented setup survives process termination
and resumes eligible work without stale workers acknowledging a new attempt.
Use the existing hung-handler/process-loss checks as the starting evidence;
any automatic termination threshold must be explicit and configurable.

### 5. Improve lazy batching when measurements justify it

Publication already batches ledger lookups, but each obligation still receives
its own job. Profile repeated changes to the same Work during the real import
before changing delivery granularity. Evaluate the existing coalescing hooks
and bounded grouped delivery for projections that can safely recompute from
current state.

Catalog must declare which projection events may coalesce. Preserve obligations'
acknowledgement, dependencies, version/generation fences and retry recovery;
distinct actions and deletion events need explicit handling. Done when a
representative measured workload improves while final projections, rollback and
process-recovery checks retain their current guarantees.

## Review evidence and checks

**2026-11-03 follow-up implemented and locally verified:** Work rebuilds and plugin upgrades
are implemented. [Measured page publication](maintenance-scale-2026-11-02.md)
substantially improves durable delivery; a 2,200-Work SQLite run verifies every
projection and drains/prunes the complete backlog. Catalog now supplies attributed
WEMI tags and previewed merge/selected-disposal operations with audit receipts;
[the repair policy](../catalog/maintenance-repair-policies.md) lists supported and
refused cases. A 660-Work PostgreSQL soak also verified every projection and
drained/pruned its journal. Final failure-boundary and snapshot checks passed;
the [completion record](maintenance-completion-plan-2026-11-02.md) links evidence.
The [latest review fixes](maintenance-review-fixes-2026-11-03.md) add coordinated
retry/pause controls, effective lazy priority and explicit maintenance job-history
retirement. The [operational follow-ups](#maintenance-operational-follow-ups)
start with a monitored real-file import. Catalog scalar-write profiling and
retention policy for unrelated storage/audit histories remain separate work.

The 2026-10-20 review built a fresh SQLite schema and compared explicit table
names, columns and types with the field guide. Historical SQL models remain
labelled as proposals. Useful checks are
[WEMI schema smoke tests](../../tests/databases/test_frbr_wemi_smoke.py),
the database test stream and the
[PostgreSQL runbook](postgresql-backend.md). Use disposable databases for creation,
migration and failure injection; do not rerun live provisioning merely to check docs.

Return to [Databases](README.md).

## Table SQL ownership follow-up — 2026-10-30

**Implemented for review.** All 36 physical main tables now have a nonempty SQL
mixin; feeds and shared normalization bring the total to 38 mixins and 65 public
operations. [The ownership guide](table-sql-ownership.md) records helper families,
callers, batch/paging contracts and test evidence. SQL targets stored rows and
references; Catalog, Core and storage retain validation, selection and policy.
DB04's broader semantic enforcement and the other open tasks above remain open.

## Measured SQL follow-up — 2026-10-31

The [SQL efficiency measurements](sql-efficiency-2026-10-31.md) established these
priorities. PR #152 subsequently passed its full-suite gates; the 2026-11-01
follow-up addresses the first two measured sources of overhead:

1. **Implemented for review, 2026-11-01:** resolve normalized identity declarations
   and column metadata once per bulk operation. Snapshots belong to one operation;
   the next operation reloads persisted policy. Portable inserts/updates and direct
   SQLite/PostgreSQL batches preserve normalization and atomic failure behavior.
2. **Profiled and addressed for review, 2026-11-01:** millions of default progress
   callbacks dominated shared WEMI search and its integrity preflight.
   [Explicit hooks](sqlite-progress.md) retain observation/cancellation while leaving
   the handler unset by default. Further query-plan work can follow the remaining
   measured cost; the integrity checks remain enabled.
3. Provide an explicit structural repair for older PostgreSQL schemas missing
   backup-workflow tables or using the old Catalog priority direction. New-schema
   creation is corrected; index migration deliberately does not perform this DDL.
4. Consider native indexed normalized search on PostgreSQL, preserving the exact
   shared text/identifier semantics before advertising the capability.
