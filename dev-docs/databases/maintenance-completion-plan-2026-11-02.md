# Maintenance completion work

Started: **2026-11-02**. Completed: **2026-11-03**. Status: **implemented and locally verified**.

This implements the four follow-ups from the maintenance assessment. The existing
transaction contract, domain ownership and jobs worker remain the foundation.

## 1. Projection backfill and rebuild

Provide an explicit Work projection rebuild through Catalog, Core and CLI. Scan
in bounded pages with a persistent cursor and a fixed upper identity bound.
Include existing projection identities so stale entries become tombstones.
Checkpoint scheduling and prompt effects atomically; use the normal durable jobs
path for lazy effects. Repeating after interruption must resume without losing or
duplicating a committed page. Report scheduling separately from completed work.
Concurrent inserts, updates and deletes remain covered by normal capture.

Verify old catalogues, missing/corrupt projections, empty sources, deletion,
restart, prompt failure, lazy failure, concurrent writes, pruning, and explicit
new rebuilds. Opening a Library or querying status must remain read-only.

## 2. Plugin lifecycle upgrades

Provide previewable, explicit declaration/version upgrades under the catalogue
writer lock, with an expected old version/declaration and implementation-owned
migration policy. Pending work must be drained or deliberately migrated; it must
never disappear implicitly. Fence old jobs, validate the resulting dependency
graph and subscriptions, invalidate affected projection freshness, and make
failure roll back declarations, data and obligations together. Document the
deployment and rebuild sequence, including old-process behavior.

Verify pending and completed history, stale workers, old writers, migration
failure, dependency changes and source capture after upgrading on both engines.

## 3. Scale and operational evidence

Add a reproducible, bounded workload using actual catalogue writes and the jobs
delivery path. Measure import throughput, writer latency, backlog/age, drain
throughput, process memory, database/journal/jobs sizes and pruning behavior.
Exercise interrupted/resumed rebuilding and continued writes during delivery.
Run SQLite and PostgreSQL trials, preserve machine-readable results and a dated
analysis, and address bottlenecks demonstrated by those measurements. Use /tmp
for scratch, monitor host free space, and clean disposable databases afterward.
Synthetic catalogue trials must be labelled as such, not presented as a real
SquashFS collection import or a host power-loss test.

## 4. Domain repair policies and operations

Specify Catalog-owned policies for semantic merge, inherited metadata and unused
record cleanup, then implement usable operations under those policies. Merge
needs a preview, scalar and relationship conflicts, explicit resolution, atomic
rechecks and preserved provenance. Aggregation needs WEMI-level attribution,
structured provenance, deterministic ordering and clearing on empty results;
multi-Work Manifestations must retain separate Work contributions. Cleanup needs
explicit candidates and retention decisions, complete reference checks, a
bounded preview/apply flow and an audit receipt. An unlinked bibliographic record
alone is not evidence that it should be deleted.

Cover the legacy failures (scoped Genres, typed Work relationships, partial link
transfer and failed deletion), multi-Work examples, new references after preview,
transaction rollback and user preservation choices. Document any domain cases
that require a separately selected policy instead of silently guessing.

## Completion evidence

Rebuild and upgrade implementation is in place, with operator/CLI documentation.
The broad regression selection passed **324 tests with 28 backend-specific skips**
on SQLite/APSW/live PostgreSQL. An additional upgrade selection passed **22 tests**,
including process/version boundaries and concurrent writer rechecks. These
selections overlap and are not a full repository run.

The reproducible [benchmark](../../scripts/benchmark_maintenance.py) now verifies
synthetic Catalog imports, restart, concurrent writes, delivery and pruning.
Equivalent 220-Work baselines completed on SQLite and PostgreSQL. Page publication
and bounded ledger lookups reduced SQLite's 440-obligation drain from **132.70s to
37.69s**, with all values/revisions verified. The publication regression selection
passed **162 tests with 28 backend-specific skips**. PostgreSQL's equivalent drain
improved from **209.46s to 67.40s**. The 2,200-Work SQLite soak completed in
699.01s, verified every projection and drained/pruned all work at 104.1 MiB peak
application RSS. The [dated analysis](maintenance-scale-2026-11-02.md) links raw
reports and explains their limits. The 660-Work PostgreSQL soak completed in
774.45s, with 257.13s in concurrent import/delivery and 116.2 MiB peak application
RSS. Every count/revision matched, the rebuild completed, and all journal work
was drained/pruned. Both larger runs used synthetic metadata, not source files.

[Domain repair policies and operations](../catalog/maintenance-repair-policies.md)
are implemented. `CatalogRepairs` owns merge and selected disposal; the old WEMI
merge entrypoint delegates to it. Physical FK/polymorphic inspection, PostgreSQL
write fencing, preview fingerprints and audit persistence belong to database
helpers. Inherited Tags are read in an enforced read-only snapshot along every
WEMI path. Core/CLI expose all three operations and receipt retrieval.

The broader repair/Core/CLI selection passed **53 tests with four backend skips**.
Its three prompt-test failures expected the wrong injected exception type; those
cases passed after correction in the final focused run. That run passed **51
checks**, with one API-documentation failure fixed and verified by **28 passing
follow-up checks**. Counts overlap. Covered boundaries include typed/symmetric
Work links, scoped Genre refusal, multi-Work Manifestations, new references after
preview, a raw concurrent PostgreSQL FK writer, prompt rollback, rollback after
audit insertion, retained rows, unknown ownership and migrated old merge callers.
These are targeted selections, not a full repository test run.

The final snapshot selection passed **nine tests**, including native SQLite
completion-hook write rejection, persistent connection restoration and production
SQLite/APSW/PostgreSQL snapshot and multi-Work inheritance cases. Read-only
protection remains active through transaction completion, including an empty body.

Formatting passes for all **258** ratcheted files, scoped lint is clean, and the
import-cycle, layer-direction, all **25** cold-import budgets, documentation links,
API usage examples, Catalog SQL ownership and man rendering checks pass.
The final documentation-link/format-contract selection passed **43 tests**.
All disposable benchmark/test schemas were removed; the private PostgreSQL
cluster was stopped and removed after verification.

Implementation/test evidence: [repair owner](../../src/LiuXin_alpha/catalog/mutations/repairs.py),
[inherited retrieval](../../src/LiuXin_alpha/catalog/retrieval/inherited.py),
[physical storage/locks](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/macros/repair_storage.py),
[repair regressions](../../tests/catalog/test_catalog_repairs.py),
[inheritance/snapshot regressions](../../tests/catalog/test_inherited_metadata.py),
[completion-hook regressions](../../tests/databases/test_read_transaction.py).

The initial repair policies deliberately refuse unsupported direct owners,
hierarchy changes, self-links and incomplete scans. Agent/series/genre tag
propagation, automatic duplicate consolidation and recursive resource deletion
are not implied. The compatibility queue remains transient; the older physical
`maintenance merge` has its narrower documented contract. Use `catalog repair
merge` for semantic consolidation. Completed jobs and audit history have retention
lifecycles separate from maintenance journal pruning.

All four areas now have implementation and targeted local evidence. Further work
is a monitored real collection import, Catalog scalar-write profiling and explicit
jobs/audit retention policy. The larger trials demonstrate these bounded workloads;
they do not establish production capacity, arbitrary-size memory bounds or host
power-loss durability.

Related: [maintenance contract](maintenance-modes.md),
[retired capabilities](legacy-maintenance-retirement-2026-11-02.md),
[operator guide](../../docs/maintenance.md).
