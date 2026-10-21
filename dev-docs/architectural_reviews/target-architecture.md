# Target architecture and decision status

Decision history begins **2026-09-26**. Implementation reviewed **2026-10-20**
against the documentation pass and A01–A08 implementation slices (PR review).

Status: **Accepted ownership decisions with explicit implementation limits.**
This is the decision record and rationale. The [ownership guide](<../04 - Seperation of Concerns.md>)
is the current rule for new code; the [package overview](<../02 - Top Level Structure.md>)
and subsystem contracts describe the supported implementation. Accepted direction
can precede complete migration. A completed migration does not make every broader
proposal in the same section implemented.

## Decision map

| Area | Current implementation | Remaining decision or migration |
| --- | --- | --- |
| Lifecycle | `core.library_services.LibraryServices` owns the graph it opens; Library borrows collaborators; Database defaults to persistence-only startup. A03's create-only publication and explicit backed-up replacement are implemented for PR review. | Lower-layer path wrappers, maintenance scheduling and residual direct commit points; A04 defines transaction/close semantics. |
| Bibliographic writes and identity | Metadata supplies pure rules; Catalog repositories/mutations and compatibility adapters share preparation and relationships. | Validators for uncovered fields, explicit repair contracts and residual raw-write contracts. |
| Persistent reads and caches | Catalog owns hydration; `ReadCache` is independent of Catalog, while writable `Cache` delegates to it. Core owns application category policy. | Other projection/search adapters, query cost and snapshot consistency; A07 now defines preferred-path selection. |
| Configuration | Core resolves deployment settings and supplies one application preference store. | Legacy scratch/startup consumers and component-specific lifetimes; database preferences remain a separate scope. |
| Formats and plugin composition | `plugins` unifies shared mechanics and application assembly, with internal dependency rules; formats retain standalone composition. | Unsupported providers, availability reporting and separate domain registries. |
| Dependency enforcement | Protected acyclic scope, exact whole-package debt and cold-import budgets are enforced. | Existing exceptions and unmodelled/dynamic behaviour; no universal package ranking for cache sources. |
| Storage | Manager → configured Store → raw driver; passive Locations, durable metadata adapters and ingest recovery exist. Backup runs own private staging workspaces with recovery/cleanup checks (A01). The four affected local builders preserve read-only intent through drivers and durable reload (A02). A05 adds enforced deletion policy and durable recovery receipts. These are implemented for PR review. | Other storage writer migration, legacy registrations and cross-workflow resource budgets. |
| Jobs and ingest | Durable jobs, Core execution and utility workers coexist; mixed ingest consumes an injected Library. | One coherent restart/cancellation/progress contract and durable maintenance signals. |
| Transport and permissions | A06 enforces server-owned capabilities, authenticated writable roles and restricted read-only composition. | Fixed deployment roles are not per-user accounts; remote operators provide TLS or tunnels. |

## 1. Lifecycle ownership

### Problem and accepted decision

The September review found application composition spread across Database,
Library, Core and path-opening helpers. Storage teardown and failed-startup
cleanup had no consistent owner. Those findings describe the September source,
not the current defaults.

**Core owns application composition and resource lifetime.** `LibraryServices`
separates ownership of one open library graph from the Library facade that uses
it. A worker or one-shot command can open that graph without constructing a full
Core runtime. Lower-level workflows should receive their database, Library or
storage handles rather than select a deployment and open another graph.

This choice keeps configuration and lifecycle together without making a borrowed
Library responsible for shutting down collaborators. It also avoids introducing
a new top-level application package. Surfaces still own their presentation,
transport and session resources; the decision does not mean that a surface may
never construct an object.

### Implemented contract

[`LibraryServices.open`](../../src/LiuXin_alpha/core/library_services.py) has
explicit keyword arguments, including `database_path` or a supplied `database`,
backend selection, optional collaborators and lifecycle policy. It is not a
positional `open(settings)` API. Core resolves application settings before
composition; lower-level standalone consumers must supply the needed values.

| Resource | Ownership now |
| --- | --- |
| Database created from a path/connection selection | Owned by LibraryServices; implicit storage and maintenance construction are disabled on that Database call. |
| Supplied database or existing storage | Borrowed. New resources created around them have separate ownership. |
| Storage created for the graph | Owned; teardown closes its Store facades and removes the owned back-reference. |
| Supplied cache versus constructed cache | CoreServices borrows the former and closes the latter. |
| Catalog | Composed by CoreServices; it currently has no independent close handle. |
| Library | A facade over supplied collaborators, not another lifecycle root. |
| Default job manager | Process-shared; CoreRuntime does not shut it down by default. Explicit ownership can request shutdown. |

ExitStack callbacks attempt every registered owned closer. Startup failure
unwinds those registrations; a failure inside a collaborator's own constructor
still needs that constructor's cleanup contract. Cleanup errors remain visible.
`closed` means teardown has been attempted: the owner marks itself closed before
running callbacks, so a repeated close does not retry failed closers.

CoreRuntime currently hosts one Library facade and, when applicable, one owned
LibraryServices graph. A registry of independently selected libraries remains a
future design. Shutdown synchronization is not a distributed transaction and
cannot undo every earlier file operation or database mutation.

### Remaining work and completion evidence

- Remove/migrate the `*_with_database_path` wrappers in ingest and storage after
  moving their callers to the appropriate Core/surface boundary. They delegate
  cleanup correctly but still import upward into Core.
- Decide how maintenance becomes durable scheduled work. LibraryServices can
  still attach the existing Database maintenance service; Core job scheduling
  has not replaced its thread/queues.
- Specify transaction rollback and existing-database creation safety separately.
  `Database.__exit__` closes resources and discards pending work on either exit.
  Explicit macro transactions own commit/rollback; nested and borrowed units use
  savepoints. See the [transaction contract](../databases/transactions.md) for the
  compatibility transition and remaining direct commit points.
- Retire legacy opt-ins/back-references only after their supported consumers have
  migrated. `db.storage` is still used by current integration paths.
- Design multi-library selection if a concrete caller needs it; do not describe
  an unimplemented runtime registry as today's lifecycle.

Use the [opening-path review](lifecycle-opening-path-review-2026-10-09.md) and
[lifecycle regressions](../../tests/core/test_library_services_lifecycle.py).
Completion requires normal shutdown, failed startup, failing closers and borrowed
resources to behave correctly, plus caller and dependency checks for removed
opening paths. A passing close test alone does not establish safe rollback.

### Alternatives and historical migration plan

The initial proposal put lifecycle in `Library.open`; the September 28 decision
moved it into Core. Putting everything into CoreRuntime was also rejected because
one-shot workflows should not need a transport/runtime host. Leaving composition
in Database would keep application policy below its owner.

The former seven-step plan at this path was a September migration sketch.
LibraryServices, Core delegation, settings composition and several caller moves
are implemented. Durable maintenance jobs, universal wrapper removal and a
multi-library registry are not. The remaining items above replace that sketch
as the actionable lifecycle list; original defect evidence remains in the
[September review](architecture-review-2026-09-26.md).

## 2. Bibliographic writes and normalized identity

Metadata owns value meaning, pure normalization and validation. Catalog owns
persistent identity, matching, relationships and semantic mutations. Databases
owns SQL, transactions and structural/concurrency constraints. Neither a prepared
update object nor an earlier surface validation is sufficient evidence that the
Catalog boundary established its own preconditions.

Supported repository/mutation APIs and the advanced schema-writer trio now share
preparation and relationship routes. Row-oriented compatibility helpers adapt
Catalog-owned tables to those owners; non-Catalog tables retain lower-level
contracts. Caches delegate semantic writes and reconcile derived state afterward.
Stored identity profiles are declared by the schema and evaluated through pure
metadata rules, rather than several unrelated cleaners writing the same key.

A08 extends this boundary to [Rating values and scales](../catalog/catalog-rating-validation.md).
Reference reuse validates the stored pair, while unrelated edits and reads leave
historical values alone. The retained Library import coordinates metadata in one
transaction; optional file and cover publication keeps a separate effect boundary.
Its route matrix records the unsupported container-write field and PostgreSQL
compatibility-view exclusions.

This does not implement every field validator or retrospectively repair old
records. An old-schema migration/backfill project was explicitly excluded from
this consolidation; no user database migration is implied. Direct SQL/driver/
private writer-host operations remain outside the
public semantic guarantee. Approximate matching, display sorting and search
normalization have different purposes from stored identity.

The [write contract](../catalog/catalog-write-validation-contract.md) specifies
actual coverage; the [Catalog work list](../catalog/catalog-roadmap.md) owns its
remaining tasks. Extend public-boundary and real-backend rollback checks when
adding a validator or moving a caller. Historical reads must remain inspectable;
repair/merge decisions belong to explicit operations.

## 3. Read ownership and cache composition

Catalog owns database/cache-backed hydration and persistent bibliographic
retrieval. Metadata containers stay passive. `caches.read_cache.ReadCache` supplies
structured reads without importing Catalog; writable `caches.cache.Cache` adds
the mutation bridge. Direct Catalog mutations do not refresh every independent
snapshot cache.

Core owns application read-source selection and tag/label category counts,
ordering and membership. Surface adapters preserve those results and failures.
That consolidation does not unify Work/search/acquisition projections or provide
atomic snapshots across successive reads.

A07 aligns SQL complete rays and Catalog bundles under the
[Catalog preferred-path policy](../catalog/catalog-wemi-selection.md), with partial
bundle and relation-local hydration contracts explicitly distinct. Full membership
remains separate from selection. Query bounds and snapshot consistency remain open. See the [cache boundary](../catalog/catalog-cache-boundary.md)
and [read-failure rules](../surfaces/read-model-failures.md).

## 4. Configuration ownership

Core selects deployment/profile settings and binds the application preference
store before bootstrap and dispatch. `ConfigurationPaths` supplies explicit paths;
ordinary application writes go to the Preferences INI. Compatibility preference
and tweak views resolve the same application store. Database-local preferences,
plugin component namespaces and conversion options keep their separate purposes.

Workers receive the chosen configuration, but that propagation does not give all
job systems one durable lifecycle. Legacy startup hooks, scratch defaults and
component singleton lifetimes remain separate work. The
[configuration contract](../core/configuration-ownership.md) owns precedence,
reload, old-file inputs and worker semantics; this record should not duplicate
its option matrix.

## 5. Formats, plugins and dependencies

`plugins` is the single shared package for generic mechanics and application
composition. `customize` and `plugin_system` are retired without forwarding
namespaces. Neutral plugin leaves cannot import the application host or concrete
domain providers. Formats own conversion contracts and standalone assembly;
application startup binds its initialized providers into that runtime.

The merger established common loading/priority policy, not a universal registry
for storage, database, cache and job capabilities. Unsupported providers and
availability/error reporting still need domain-specific decisions. See
[plugin composition](../plugins/plugin-composition.md).

Dependency direction is an accepted, partly migrated contract. The
[layer ratchet](layer-direction-ratchet.md) checks an exact whole-package debt
baseline, while the protected scope must remain acyclic. The
[cold-import ratchet](import-footprint-ratchet.md) observes initializer effects in
fresh interpreters. Passing these gates does not prove absence of all runtime
cycles, side effects or hidden service construction.

## 6. Storage, execution and trust boundaries

Storage's manager/Store/driver split is implemented; Location values remain
passive. Asset/Replica identity and recovery are separate from bibliographic
matching. Database commits, backend publication and cleanup have distinct failure
boundaries. A05 now enforces retention/last-copy policy at both manager deletion
entries; A10 still owns other storage mutation callers and recovery coordination. See the [storage overview](<../06 - Storage.md>).

Jobs invoke domain workflows rather than acquire their validation or placement
policy. Consolidating Core execution, durable jobs and utility workers requires
an agreed run identity, restart/cancellation behaviour and result/log contract.
Mixed ingest already consumes a Library, but this does not settle every acquisition
coordinator or make cross-domain effects atomic.

A06 implements the independent [transport authority boundary](../core/transport_permissions.md):
server-owned capabilities authorize invocation, administration and ordinary writes
before dispatch. Read-only composition uses a restricted reader. A permitted
endpoint still obeys its domain policy, including A05
[last-copy deletion rules](../storage_and_backup/deletion_policy.md). The
[annotated review](architecture-review-2026-09-26.md) records the concrete CO-4/SU-6/ST
implementation evidence and remaining limits.

## Decision log

- **2026-09-26:** Initial lifecycle proposal placed ownership in Library.
- **2026-09-28:** Core ownership adopted, separating LibraryServices from Library;
  durable maintenance and future multi-library support were directions to design.
- **2026-10-09:** LibraryServices and principal startup/teardown migrations landed.
- **2026-10-11:** Catalog semantic-write boundary adopted; pure value rules remain
  in metadata and raw persistence keeps its structural responsibilities.
- **2026-10-12–13:** Whole-package dependency/cold-import enforcement and hydration,
  value-policy and standalone-conversion containment progressed.
- **2026-10-18:** Write/identity follow-ups, category-read policy and application
  configuration consolidation are recorded in their implementation handoffs.
- **2026-10-19:** Plugin composition merged and renamed to `plugins`, preserving
  internal boundaries and retiring both former namespaces.
- **2026-10-20:** This record reconciled with current source; implemented decisions,
  remaining migrations and future proposals are separated. No runtime change.

## Promoting the next decision

Record the concrete caller/problem, owner, current alternatives and chosen
boundary. State which behaviour changes, which callers migrate, and how existing
data/wire contracts survive. Define normal and partial-failure evidence, resource
limits and rollback/recovery before calling a migration complete.

Detailed tasks belong in subsystem plans, linked from [cross-project work](../global_todo.md).
[Next architecture tasks](next_tasks.md) supplies the proposed sequence and
acceptance criteria for this review; update the owning plan when a task lands.
Unselected ideas belong in [experiments](../ideas.md). Keep this decision record
focused on rationale/status, and do not treat a historical recommendation as an
instruction to restart completed work.
