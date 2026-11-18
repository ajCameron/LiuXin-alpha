# Small invalidation and cache lifecycle

Date: **2026-11-18**. Follow-up to the
[cache review](../architectural_reviews/caches-review-2026-11-17.md): the fourth
performance target, CACHE01 write-route review and CACHE03 backend registration.

## Small changes stay small

The native query engine repairs eligible row-local membership by checking only
the changed IDs. It no longer enumerates the table to intersect those IDs or
copies an entire exact-value index. Retained membership sets are private to the
engine and transfer through the facade's locked invalidation/refresh boundary;
repair edits them in place after matching succeeds. Public results remain copied
records and independent ID tuples. They cannot mutate the retained sets.

The existing budget of fifty changed-ID/query pairs remains. Larger work is
evicted, and relationship, provider and indexed-text queries still take their
conservative rebuild paths. Failed repair discards query state and uses a fresh
query. Several invalidations before the next read accumulate their IDs.

This removes population-sized work from membership repair, not from every
invalidation operation. The schema backend supports targeted row reload; NumPy
may reload a complete table. Insert/delete ordering, relationship fan-out,
database I/O and later arbitrary-field sorting can still depend on population.
Python set growth is amortized, not a constant-allocation guarantee per insertion.

## Write owners and failure boundaries

| Route | Invalidation owner | Transaction and failure behaviour |
| --- | --- | --- |
| Cache column writer | Cache facade: table and affected field | Catalog-owned repositories or portable row batching join the caller's transaction. Immediate refresh failure raises `CacheReconciliationError` with the write receipt and leaves recovery pending. |
| Cache shared-value/link writer | Cache facade: destination and both link directions | Value resolution owns a transaction around the bridge. Snapshot refresh is deferred until it closes; the write returns its receipt, and a failed later read raises `CacheDirtyError`. |
| Cache owned-row writer | Cache facade: destination and both link directions | Owned-row persistence is atomic. An enclosing transaction defers refresh; rollback reloads the durable pre-write state. |
| Direct Catalog, repository or SQL write outside a cache-aware owner | Calling application/workflow | Explicitly invalidate affected rows, fields and links, or reload. Arbitrary delegated Catalog methods do not acquire reconciliation automatically. |
| Storage record upsert/delete | Storage metadata repository: affected IDs | Serialized units read authoritative transaction state. Snapshots stay dirty until the outer transaction ends. An invalidation error can follow a committed write; a successful full recovery is required before serving it. |
| Storage composite membership and Item targets | Storage metadata repository: affected record plus relationship directions | Replacements invalidate removed and newly added relationships. Item-role deletion shares one transaction across both target kinds. |
| Storage envelope migration | Storage metadata repository: covered envelope tables | Committed rewrites invalidate even if subsequent migration bookkeeping raises. |
| Native custom-field descriptors | Custom-field coordinator and descriptor registry | Prepared metadata publishes after transaction completion; rollback discards publication and tickets reject obsolete completions. Existing descriptor regressions remain applicable. |

The real-database audit repaired three defects and hardened one deletion boundary:

- Extension-table column updates used an independent connection and could lock
  against an outer transaction. `CatalogColumnUpdate.write` now calls portable
  `update_rows`; physical normalization remains in databases.
- Replacing an Item's atomic target with a composite invalidated only the new
  relationship. Both target kinds now invalidate in both directions.
- Migration-recording failure skipped invalidation after committed envelope
  rewrites. Invalidation now runs in the completion `finally` block.
- Item-role deletion now joins one transaction, so a later deletion failure
  cannot commit only part of the removal before invalidation.

Storage's journal and ID-reservation helpers remain authoritative internal
operations, not automatically published application snapshots. Their owning unit
of work completes the record publication. Direct low-level writes, trigger side
effects beyond a declared update, schema mutation and other cache instances still
need explicit invalidation by their owner. No global observer or Catalog-to-cache
callback was added.

The facade lock covers persistence plus reconciliation, reads, refresh and close.
Readers cannot see an intermediate refresh; close waits for a writer to finish.
Retained public records remain independent, and old writers reject a closed
owner or changed database attachment before persistence. Successful clear leaves
EMPTY; successful close is terminal and idempotent. Failed destructive lifecycle
operations leave DIRTY and require full recovery. Raw backend child objects keep
their backend-specific lifetime contract; mutating or rebinding a borrowed
backend behind a facade is outside facade synchronization.

## Backend registration policy

Canonical names and aliases have immutable bindings for the life of the process.
Names are stripped and casefolded; aliases are deduplicated. Repeating an identical
normalized registration is harmless. Retargeting a name, changing its alias set,
or colliding with another binding raises `CachePluginError` before publishing any
new key. This deliberately replaces silent overwriting; use a new name or a new
process when changing backend meaning.

Register before lookup. New distinct backends may be registered while others are
in use. A registry lock protects publication and lookup, while immutable
registration objects allow loading to proceed outside that lock. Imports and
constructors can register dependencies without deadlocking registry readers.
Import, missing-attribute, constructor and capability failures preserve the
intended binding; they do not substitute another backend. Retrying after repairing
the same import target remains possible.

## Addition and removal follow-up

The subsequent membership pass adds **78 mandatory cases**, without changing
runtime code. The [72 source-model cases](../../tests/databases/caches/test_membership_mutations.py)
run across schema, live database and NumPy backends. They cover:

- Deleting first, interior and last IDs; draining to empty, deleting missing IDs,
  and refilling an empty table under both row-ID and whole-table invalidation.
- Inserting below, between and above existing IDs; warming known misses; deleting
  and reusing identities with different values; retaining old public results.
- Accumulating insert/delete/reinsert notifications before a read, including
  batches beyond the small-repair budget.
- Forward and reverse relationships, removing one shared edge, endpoint deletion
  with explicitly modeled cascade cleanup, reusing endpoint/link identities,
  and emptying/refilling relationships without resurrecting old edges.
- Invalidation or refresh errors after partial backend work, blocked reads while
  full recovery is unavailable, and further changes before recovery.
- Two deterministic seeds of bounded add/remove/reuse churn with repeated drains.

Independent Python scans check IDs, complete lookups, records, exact and null
predicates, Unicode text searches, warm results, counts, sorted/projected pages
and refreshed field indexes. Source-model cases use small fixtures; they do not
claim large-scale performance or prove database cascade behavior.

The [six real transaction cases](../../tests/databases/caches/test_write_route_recovery.py)
cross SQLite/APSW with all three backends. An independent SQL connection checks
committed state through insert/delete/reuse commit and rollback, constraint
failure after an earlier insert, complete deletion and refill. Snapshot reads
must remain blocked inside dirty outer transactions. These tests capture the
single extension table involved; existing full-catalogue long lanes remain
separate. External mutations explicitly publish invalidation as before.

All 78 cases join `native-cache-membership-add-remove-reuse` in the regression
manifest, taking the required total from **2,748 to 2,826** without removing or
moving any earlier case. The new module joins formatting and lint enforcement.

The final focused selection passed **174 tests**, with no skips; all **174 selected
mandatory cases** passed setup, call and teardown. This includes the 78 additions
plus existing small-invalidation, paging and search-stress regressions. Regression
runner and quality-tooling tests passed **64 cases**. This follow-up did not rerun
the full database ratchet, PostgreSQL or the gated long workloads.

The full quality runner also passed: formatting for 420 Python files, lint,
annotation/import/layer/footprint checks, both type checkers and their internal
contracts. Final acceptance source hashes match the tested files; changed
working-tree documentation destinations resolve.

## Evidence

The [invalidation benchmark](../../scripts/benchmark_cache_invalidation.py)
measures 1,000/10,000/100,000 rows and one/ten/fifty warm queries, both unchanged
and changing membership. Each scale uses a fresh process, five untraced latency
samples and a separate allocation sample. It includes facade invalidation, schema
row refresh and membership repair over an indexed database double. Initial load,
database I/O and independent result checks are outside timing.

The [raw measurements](invalidation-measurements-2026-11-18.json) retain every
sample, platform information and hashes of the benchmark and measured runtime
sources. Before and after used the identical benchmark, Python 3.12.3 and Linux/WSL,
without concurrent repository test suites. The baseline used the saved query/read
implementation immediately before this pass, including the earlier pagination
optimization. The after hashes match the final working sources.

```sh
.venv/bin/python scripts/benchmark_cache_invalidation.py \
  --scales 1000 10000 100000 --repeats 5
```

At **100,000 rows**, median milliseconds for one changed row were:

| Warm queries | Membership unchanged: before | After | Membership changed: before | After |
| --- | ---: | ---: | ---: | ---: |
| 1 | 13.599 | 0.157 | 12.773 | 0.152 |
| 10 | 134.381 | 0.487 | 198.667 | 0.443 |
| 50 | 619.510 | 2.019 | 793.307 | 3.833 |

For fifty queries, peak additional traced allocation fell from 216,839,176 bytes
to 22,768 bytes: approximately **206.8 MiB to 22.2 KiB**. This is temporary/request
allocation after warming, not total process memory or the size of retained match
sets. One-query unchanged-membership latency was 0.161/0.175/0.157 ms at
1k/10k/100k rows. Small populations show smaller gains, and five samples are not
portable timing guarantees. These figures do not include real database I/O,
NumPy's whole-table fallback or subsequent sorting/materialization.

The initial invalidation/lifecycle acceptance selections passed **572 tests**, with **16 expected
skips**: 542 in the broader cache/Catalog selection and thirty real write-route
cases in a separate selection. All **280 selected mandatory cases** passed setup,
call and teardown, including all **73 new cases**. The complete mandatory manifest
then named **2,748** cases and preserved every previous entry. These are selected
acceptance results, not a rerun of the complete database ratchet.

The new real-database matrix uses SQLite and APSW across schema, live and NumPy
backends. It retains actual database transactions and independent SQL durability
checks while limiting cache capture to the route families under test. The broader
selection includes existing full-catalogue backend and SQL differential tests.

Both selected **full snapshot soaks** passed their mandatory phase gates: 10,000
rows and 1,000 mutation/recovery steps each for schema and NumPy. They include
sorted pages, bounded retained query state and destructive refresh recovery.
Their source hashes match the final runtime. These validation suites overlapped;
their wall times are not matched performance measurements. The real-database
long lane and Calibre soak were not repeated in this pass.

The full quality runner passed formatting for 419 files, lint, annotation,
import/layer/footprint checks, both type checkers and their internal contracts.
Tooling passed **87 tests** with one explicit deselection: the tracked-content
documentation-link check while the new files and existing review draft remain
untracked. A separate working-tree audit confirmed all documentation destinations.
The [working handoff](../../working-memory/cache-invalidation-lifecycle-2026-11-18.md)
retains the local run artifacts.

Physical CACHE Replica leases remain CACHE02 with storage/ingest. Content FTS,
notes, annotations and application providers remain SEARCH02–SEARCH05. This pass
does not claim those features or native Windows/PostgreSQL validation.

Return to the [cache guide](README.md).
