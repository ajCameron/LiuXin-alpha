# Cache architecture and optimization readiness review — 2026-11-17

Status: **metadata-search core ready for measured optimization; whole-cache
sign-off remains open.** The architecture is coherent, and the completed stress
pass provides useful regression protection for the next stage of work.

This assessment covers the current working tree based on
`2d2a9cc304b5394ab90003b0567c4d06130337f1`, including the uncommitted compatibility
migration, metadata-search implementation and stress-test repairs. It records the
assessment after that work; it is not a fresh exhaustive source audit or a new
execution of the test suites. Implementation, validation and publication remain
separate facts.

Read the [implemented structure](../caches/structure.md),
[metadata-search contract](../caches/metadata-search.md) and
[stress-test report](../caches/metadata-search-testing-2026-11-17.md) for detailed
contracts, reproducible commands and evidence.

## Architectural assessment

Preserve the current ownership boundaries:

| Owner | Responsibility |
| --- | --- |
| Catalog | Bibliographic meaning, search semantics and mutation policy. |
| Databases | Persistence, transactions and physical SQL execution. Efficient low-level SQL fits here when it belongs to database access. |
| Native caches | Derived state, query reuse, freshness, invalidation, publication and recovery. |
| `utils.calibre_compat` | Calibre field conventions, retained legacy cache behavior and the explicit metadata-search adapter. |

The `ReadCache`/`Cache` split remains useful: reads and lifecycle have their own
owner, while the writable facade bridges Catalog mutations and reconciles derived
state. Native custom-field descriptors and their publication belong with caches;
the native database service should stay free of cache responsibilities.

The compatibility boundary supports the LiuXin-first design. Native code has its
own descriptors and predicates; callers opt into Calibre projection semantics.
The migration removed old forwarding paths, and import and packaging checks
protect that separation. Copied upstream code retains its licence notices and
provenance. Compatibility relocation does not by itself retire every legacy
consumer or establish whole-application parity.

There is no architectural reason from this pass to reorganize these owners before
optimizing. Improve the implementation within the established contracts.

## Correctness assessment

Confidence is materially stronger because tests compare results with independent
reference implementations and exercise state changes and failures. The stress
pass covers generated Boolean queries, nullable values, sorting and pagination,
405 mutation steps across all three native backends, partial lifecycle failures,
concurrent access, snapshot isolation and real database integration.

Six defects were exposed and repaired: lifecycle failure recovery, null membership,
two mutable-snapshot leaks, alias handling and series-index zero. The earlier
[publication and lifecycle review](caches-review-2026-11-16.md) findings are also
resolved at their named boundaries: CR01 by ordered descriptor publication, and
CR02/CR03 by invalidation validation and dirty full-recovery semantics. Historical
open statuses in that review describe earlier checkpoints.

The validation evidence is:

- The original full database gate passed **5,121 tests**, with **94 skips**; all
  **2,534 original mandatory cases** passed. That run began before the stress
  fixes and new test files.
- The subsequent cache/Catalog run on corrected code passed **459 tests**, with
  **16 skips**. All **101 new regression cases** passed across focused runs,
  including **89 new mandatory cases**. These counts overlap and should not be
  added together as a unique-test total.
- Quality, explicit native typing, documentation, tooling and installed-package
  checks passed. All **2,623 currently required cases** have passing evidence
  across runs; the updated manifest was not executed in one full-suite invocation.

This supports optimizing the metadata-search core with the existing regressions
as acceptance criteria. It does not establish every write route, backend lifetime
or concurrency arrangement. PostgreSQL differential tests use Catalog's semantic
fallback; they do not establish a PostgreSQL normalized SQL search compiler.

## Performance assessment and recommended order

Performance is the clearest current weakness. Several small requests still incur
work proportional to the full collection. The
[recorded baseline](../caches/metadata-search-baseline-2026-11-17.json) makes these
costs measurable through 100,000 records.

| Order | Target | Preserve while optimizing |
| --- | --- | --- |
| 1 | Repeated PostgreSQL schema discovery during cache loading and metadata refresh. | Independent-connection schema-change detection and transaction ownership; avoid repeated discovery within one complete schema read. |
| 2 | Candidate identity construction and repeated projection copying. | Explicit identities, snapshot isolation, field meaning and definition invalidation. Reduce redundant work without exposing mutable snapshot state. |
| 3 | Sorting all matching native IDs before pagination. | Stable ordering, missing-value behavior, series index zero and correct total counts. |
| 4 | Whole-table ID-set construction during small invalidations. | Membership correctness, bounded predicate repair and safe full invalidation when targeted repair is unavailable. |

For scale, the 100k-record samples measured a warm Calibre Boolean query at about
46 ms and a native sorted 20-ID page at about 470 ms. These measurements were taken
on Linux/WSL while other regression suites were active. They are directional
baselines, not production latency promises or universal CI thresholds. The
synthetic benchmark excludes the expensive real-database schema discovery.

Optimize one measured cost at a time. Compare equivalent workloads under matching
conditions and retain the independent result checks, fault cases and work-count
bounds. The 50-pair repair budget bounds predicate visits; it does not currently
bound every ID-set copy or backend refresh operation.

## Remaining sign-off work

The existing [cache backlog](../caches/next_tasks.md) remains the implementation
owner for these follow-ups:

- **CACHE01 — write-route invalidation and lifecycle.** Complete the audit of
  direct Catalog writes, cache writer bridges and storage repository writes.
  Establish affected-ID/link publication, rollback and failed reconciliation
  behavior for each selected route. Broaden retained-object and concurrent
  lifecycle coverage beyond the repaired cases. Arbitrary delegated Catalog
  methods do not automatically gain cache reconciliation.
- **CACHE03 — backend registry lifetime and concurrency.** Define registration,
  duplicate-name/alias policy and concurrent lookup behavior, including failed
  loads. Resolving search-field aliases does not resolve backend registry policy.
- **CACHE02 — physical replica leases, shared with storage and ingest.** Define
  retention during active use, bounded crash recovery and eviction behavior.
  These leases protect retained bytes; they are a separate cross-module contract
  from catalogue snapshot freshness or metadata-search readiness.

The [search parity ledger](../caches/metadata-search.md#parity-ledger-and-next-milestones)
separately tracks content FTS, notes, annotations and remaining application
providers as SEARCH02–SEARCH05. Native Windows execution, the full PyICU locale
matrix and whole-Calibre-application differential validation remain unverified.
These limits must remain explicit in any broader compatibility claim.

Recommendation: proceed with measured optimization of the metadata-search core,
preserve the current ownership boundaries, and keep whole-cache sign-off open
until the remaining lifecycle and registry contracts have been reviewed.

Return to the [cache guide](../caches/README.md) or
[architecture review index](README.md).
