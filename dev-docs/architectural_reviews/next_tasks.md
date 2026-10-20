# Next architecture tasks

Date: **2026-10-20**. Basis: documentation review at `a5406cf8`, A01 at `75b266ed`,
A02 at `d5956d6e`, and A03's implementation on `database-create-safety`.

Status: **A01–A08 are implemented for PR review; later runtime fixes remain open.
Documentation progress is recorded in A14; A15's repaired full CI baseline is verified.** Recheck the named behaviour before starting each task, especially
where evidence comes from an older review. The first safety slices were checked
against their current source; A15 records fresh CI and branch-rule observations.

Keep the current high-level package structure. The useful next changes enforce
ownership at the places where data, resources or authority cross a boundary.
Further package renaming or a broad rewrite would add migration work before
settling those contracts.

This document owns the suggested order and acceptance criteria for architecture
follow-up. [Cross-project work](../global_todo.md) remains the routing index;
subsystem plans own implementation detail. The
[decision record](target-architecture.md) owns rationale and status. Update those
owners when a task lands instead of maintaining independent copies of its state.
Finding IDs below refer to the
[annotated September review](architecture-review-2026-09-26.md); DS IDs refer to the
[documentation structural review](documentation-structural-review-2026-10-18.md).

## Recommended order

With **A15's repaired CI baseline verified** and A01–A08 implemented for review,
continue the remaining **A10 storage writer migration** after reviewing A09's
first [durable Core audit](../jobs/core-storage-audit.md). The
[responsive wait-dispatch slice](../jobs/core-wait-dispatch.md) is implemented
for review; the selected audit now supplies durable workflow integration.
A08's [route matrix](../catalog/catalog-rating-validation.md)
records the selected field family and remaining exclusions. A09/A10 continue
execution and storage ownership.

The order is a recommendation, not a requirement to finish every earlier item
before starting an independent one. Dependencies are stated in each task.

| Task | Priority | Concrete result | Main owner |
| --- | --- | --- | --- |
| [A01](#a01-preserve-caller-owned-backup-staging) | Implemented; PR review | Cleanup removes only scratch owned by the backup run. | Storage backup |
| [A02](#a02-enforce-store-read-only-intent) | Implemented; PR review | Four affected builders preserve and enforce read-only intent through durable reload. | Storage registry and Stores |
| [A03](#a03-make-database-creation-nondestructive-by-default) | Implemented; PR review | Creation refuses collisions; explicit offline replacement retains a verified backup. | Databases and Core bootstrap |
| [A04](#a04-separate-resource-lifetime-from-transaction-lifetime) | Implemented; PR review | Exceptions and nested mutations have one explicit transaction contract. | Databases, with Catalog/storage callers |
| [A05](#a05-enforce-deletion-policy-at-the-mutation-boundary) | Implemented; PR review | Removing bytes cannot bypass retention/last-copy policy. | Storage manager |
| [A06](#a06-enforce-transport-capabilities-on-the-server) | Implemented; PR review | Remote/read-only clients cannot invoke privileged operations by name. | Core transport and surfaces |
| [A07](#a07-choose-one-default-wemi-selection-policy) | Implemented; PR review | SQL projections and Catalog bundles apply an agreed path-selection policy. | Catalog and schema projections |
| [A08](#a08-complete-semantic-write-coverage) | First family implemented; PR review | Rating values/scales share preparation, checked reuse and transactional import. Other fields remain on the Catalog work list. | Metadata and Catalog |
| [A09](#a09-converge-job-state-and-release-long-waits-from-core-dispatch) | Selected workflow implemented; PR review | Storage audit has durable delivery, progress/events, cancellation receipts and explicit interruption/retry. | Jobs and Core |
| [A10](#a10-give-storage-record-mutations-one-owner) | Selected families implemented; PR review | Asset/Replica, policy and Composite writes use storage ownership; Item/workflow adapters, schema parity and broader receipts remain open. | Storage, using database transactions |
| [A11](#a11-finish-lifecycle-caller-migration) | Following | Lower-layer path wrappers and obsolete opt-ins retire through caller migration. | Core, ingest and storage |
| [A12](#a12-finish-operational-path-and-startup-ownership) | Following | Scratch/startup helpers no longer pull process-wide hooks into foundation imports. | Configuration paths and utility owners |
| [A13](#a13-make-provider-and-conversion-outcomes-explicit) | Following | Unsupported providers and conversion losses become structured results. | Plugins, file formats and jobs |
| [A14](#a14-continue-documentation-with-concrete-source-backed-examples) | Documentation pass complete; PR review | Schema inventory, conventions, subsystem ownership and next-task routing reviewed. | Developer documentation |
| [A15](#a15-refresh-repository-hygiene-and-ci-evidence) | Verified; PR review | Repair shared failures, remove generated scratch and validate a clean baseline. | Packaging and test tooling |
| [A16](#a16-reduce-compatibility-debt-one-live-boundary-at-a-time) | Following | A measured dependency or duplicate implementation is removed with caller evidence. | Relevant subsystem owner |

## A01. Preserve caller-owned backup staging

**Findings:** ST-6. **Implemented for PR review, 2026-10-20.** **Dependencies:** none.

[`SquashfsBackupWorkflow`](../../src/LiuXin_alpha/storage/backup/squashfs_backup_workflow.py)
now allocates an exclusive workspace beneath the supplied staging parent. It
persists ownership outside archive members, validates that identity during
reconstruction/execution/cleanup, and removes only its workspace after successful
publication when requested. Routed intermediate images are included in cleanup.
Legacy checkpoint staging remains borrowed; missing ownership is not recreated.

[Regression tests](../../tests/storage/backup/test_backup_staging_ownership.py)
exercise caller sentinels through success, build/verification/publication failure,
cancellation, persisted recovery, shared parents and cleanup errors. See the
[staging contract](../storage_and_backup/storage_api.md#backup-staging-ownership)
for diagnostic retention and legacy behaviour. Concurrent use of one checkpoint
remains outside this workflow's synchronization contract.

## A02. Enforce Store read-only intent

**Findings:** ST-2. **Implemented for PR review, 2026-10-20.**
**Dependencies:** none; use this result in A05.

The compatibility builders in
[`storage.utils.backend_registry`](../../src/LiuXin_alpha/storage/utils/backend_registry.py)
now preserve `StoreConfiguration.read_only` through managed, flat, Calibre-like
and SQLite construction, including their raw drivers. SQLite opens existing
containers with `mode=ro`, validates the object schema without initialization
writes, and closes each operation's connection. Intrinsic read-only restrictions
remain intact.

[Regression tests](../../tests/storage/test_read_only_backend_construction.py)
exercise Store and manager mutations, driver refusal, writable controls and
catalogue reopen/reload on both database drivers. Refusals preserve stored bytes
and metadata. SQLite cases also cover missing storage/schema, engine-level write
refusal and live WAL commits. See the
[construction contract](../storage_and_backup/storage_api.md#read-only-store-construction)
for WAL sidecar behaviour and scope. A05 adds retention/last-copy enforcement.

## A03. Make database creation nondestructive by default

**Findings:** DB-1. **Implemented for PR review, 2026-10-20.**
**Dependencies:** none; A04 handles transaction lifetime separately.

[`Database`](../../src/LiuXin_alpha/databases/database/__init__.py) now initializes
new SQLite/APSW databases privately and publishes create-only before opening its
runtime connections. Registry, Core and terminal callers share that contract.
Explicit offline replacement prepares a verified, create-only recovery backup
before the final atomic rename. PostgreSQL creation accepts absent/empty schemas,
serializes initializers and rejects populated namespaces.

The [creation contract](../databases/creation-and-replacement.md) states backup,
failure, recovery and offline-exclusion requirements. Real file regressions cover
collisions, partial failure and writable reopen; live PostgreSQL regressions cover
refusal, concurrent creation and rollback. No user database or old schema was
migrated. A04 defines the general transaction/close semantics.

## A04. Separate resource lifetime from transaction lifetime

**Findings:** DB-1/2. **Implemented for PR review, 2026-10-20.**
**Dependencies:** shares Catalog's existing transaction owner.

Database close, resource exit and driver replacement now discard pending work.
Portable macros explicitly own fresh transactions and use savepoints for nested
or borrowed work. Temporary tables share that contract; their cleanup cannot
commit the caller's writes. `DriverWrapper.lock` is a reentrant mutex, with SQL
callers migrated to explicitly acquired connections.

The [transaction contract](../databases/transactions.md) records the compatibility
transition, source-backed commit-point inventory and privileged direct helpers.
Catalog/Row/Agent and storage database units retain the existing macro owner;
legacy direct APIs are not advertised as universally atomic writes.

**Acceptance:** injected late failures roll back the complete semantic operation;
nested units cannot commit their caller's pending changes; resource cleanup does
not accidentally commit failed work. The main SQLite/APSW-named caller/resource selection passed 426 checks;
62 PostgreSQL checks and 52 documentation/API/workflow follow-ups passed. Quality
gates passed. Windows-only checks and disabled legacy Calibre-cache features
remain explicitly outside that evidence; the [handoff](../../working-memory/transaction-ownership-2026-10-20.md) records their limits.
Full PR CI passed on `e9efedc4`, including the corrected optional APSW test setup.

## A05. Enforce deletion policy at the mutation boundary

**Findings:** ST-1/3. **Implemented for PR review, 2026-10-20.**
**Dependencies:** A02 and A04; A10 still owns additional storage writer migration.

[`StorageDeletionMixin`](../../src/LiuXin_alpha/storage/storage_manager/mixins/deletion.py)
owns `remove_replica` and routed deletion. It checks read-only/retention/last-copy
policy before reserving, uses fresh surviving-byte evidence and configured joint
separation, then commits DELETING claims before external I/O. SQLite/PostgreSQL
metadata owners serialize decisions using A04's transaction owner and read past
stale local caches. A caller-owned transaction cannot hide deletion reservations.

[The contract](../storage_and_backup/deletion_policy.md) defines permitted modes,
raw privileged escapes, recovery evidence and manual reconciliation. Core
removal receipts preserve structured errors for CLI output. The final focused
selection passed 110 tests, including SQLite/PostgreSQL fault/concurrency and real
Core/CLI cases; complete quality gates passed. A late verification cannot revive
a deleted claim. The linked contract records the inherited PostgreSQL full-schema
cache-discovery limitation; these checks do not claim general cache compatibility.

Automated pending-deletion recovery, zero-copy eviction and destructive overrides
remain absent. Recipe/loss policies alone do not authorize deleting a last copy.
A SQL rollback never promises restoration of erased external bytes.

## A06. Enforce transport capabilities on the server

**Findings:** CO-4, SU-2/6. **Implemented for PR review, 2026-10-20.**
**Dependencies:** A05 continues to enforce destructive storage policy. Published
as [PR #134](https://github.com/ajCameron/LiuXin-alpha/pull/134); its follow-up
centralizes role lookup in Core (41 boundary/HTTP checks and full quality passed).

Registrations now declare remote authority independently of `transport_stable`.
HTTP defaults to declared reads; writable/privileged roles require authentication.
The runtime checks the selected handler and declaration together before effects.
A failed registration cannot pair a new handler with the old route's permission.
Generic invocation, administrative mutations and executable configuration require
explicit stronger grants; claimed request permissions do not affect the role.

The [transport contract](../core/transport_permissions.md) defines bearer-token
roles, Host/origin/media-type checks, protected deployment modes and credential
handling. Read-only web/API/OPDS composition binds a restricted client and model.
Local readers use the same runtime check; direct HTTP requests remain subject to
the server's role. Refresh, maintenance and global event access are privileged.
Ordinary trusted in-process calls retain their separate authority.

**Evidence:** the final focused selection passed 67 tests, including raw HTTP
bypass attempts, explicit read/write parity, credentials and redirect refusal,
CLI token files, failure-atomic registration, restricted surfaces and the A05
last-copy rule through authenticated HTTP. Complete quality gates passed, with
38 rejected negative examples in each type checker. The
[handoff](../../working-memory/transport-permissions-2026-10-20.md) records broader
caller selections and their corrected fixtures. Full PR CI is separate.

Authentication identifies one deployment role. Per-user/row authorization and
in-process plugin isolation are not claimed; TLS remains a deployment boundary.

## A07. Choose one default WEMI selection policy

**Findings:** remaining read-model work under SU-1. **Implemented for PR review,
2026-10-20**, on `catalog-wemi-selection`, based on A06 PR #134 (`13d6e5aa`).

Catalog owns the [preferred-path policy](../catalog/catalog-wemi-selection.md).
Bundles now match the existing SQL complete-ray rank: primary flags, ascending
priorities and IDs, with Item availability excluded. If no complete WEM path
exists, bundles retain the fullest partial path. Repository lists and full graph
membership keep all related Works and their existing order.

The real SQLite/APSW fixtures cover translations, competing primary flags, nulls,
ties, incomplete paths, introductions, bilingual editions, omnibuses and public
primary-change/unlink operations. Worked examples now show matching outputs.
Metadata relation-local primary accessors explicitly answer a different question;
a contrasting-output test preserves that distinction and both hydrated Works.
Core's category/tag/label semantics are unchanged.

**Evidence:** 22 final SQLite/APSW parity cases and 55 documentation/contract
checks passed; the broader selection passed 52 cases before correcting two
worked-example test API calls. Complete quality gates passed, including both
38-example negative contracts. Counts overlap. The
[handoff](../../working-memory/wemi-selection-2026-10-20.md) records scope and
publication evidence. Query cost, multi-read snapshot consistency and broader
search/acquisition projections remain separate work.

## A08. Complete semantic write coverage

**Findings:** remaining coverage after CM-1/2/3, plus CM-6/9 and DB-3/7.
**First PR:** one identified field or relationship family through all supported
routes. **Dependencies:** preserve A04's transaction rules; do not wait for a
wholesale cache redesign.

**Rating slice implemented for PR review, 2026-10-20.**
The [value/scale contract and coverage matrix](../catalog/catalog-rating-validation.md)
place the pure rule in metadata and its enforcement in repository preparation.
Schema writers, Row/Core adapters and legacy rating helpers share it; fractions
are preserved, omitted fields stay unchanged, and historical reads remain tolerant.
Retained Library metadata imports now use one transaction and read their newly
written rows on that connection. Optional file/cover publication remains a
separate effect boundary. Modern container write-back does not accept ratings;
PostgreSQL lacks the retained importer's SQLite compatibility views.

[PR #136](https://github.com/ajCameron/LiuXin-alpha/pull/136) fixes the prerequisite
PostgreSQL link-name discovery and snapshot-cache index probes exposed by live
testing. Rating bootstrap inserts now use the macro identity/transaction owner.
The [aggregate-link inventory](../catalog/catalog-rating-validation.md#remaining-aggregate-link-operations)
keeps coordinated WEMI/merge operations with their current domain owners;
remaining cover/file links route to A10/A11. Other field families stay on the
Catalog coverage work list.

Use the [Catalog work list](../catalog/catalog-roadmap.md) and
[write contract](../catalog/catalog-write-validation-contract.md) to make a concise
coverage matrix: operation, pure rule owner, shared preparation entry point and
public callers. Add missing value rules in metadata and enforce them at Catalog's
existing boundary. Inventory remaining direct aggregate link operations and decide
whether they should use CatalogRelationships; do not add another parallel writer
or force every aggregate through LinkWriter solely for uniformity.

**Done when:** invalid input and partial-update semantics agree across repositories,
container writes, schema writers and Row/Core compatibility adapters for the
selected family. Late failures leave no partial mutation. Stored normalized
identity, display sorting and approximate matching retain their distinct meanings.
Tolerant reads and collision-aware current-schema repair remain explicit; an
old-schema import/backfill project stays outside this programme.

## A09. Converge job state and release long waits from Core dispatch

**Findings:** CO-5/8; related ST-5 and CM-5. **Selected workflow implemented for review:**
JOB01 releases `jobs.wait`/`jobs.result` from the shared handler lock while
preserving database serialization and shutdown checks. See the
[waiting contract](../jobs/core-wait-dispatch.md). The
[Core storage audit](../jobs/core-storage-audit.md) now uses the durable run-state
contract. **Dependencies:** A06 for remote permissions;
A10 for workflows that mutate storage records. Pure execution contracts can start
independently.

The next prerequisite implements [durable claim ownership](../jobs/durable-claims.md):
exclusive claims, atomic state/events, fenced callbacks, input snapshots and
explicit interrupted-run recovery. It remains separate from Core's default
manager. Core routes the audit's namespaced IDs through this owner and preserves
utility job IDs for the other workflows.

A further [submission prerequisite](../jobs/submission-identity.md) now provides
ledger identity, atomic delivery deduplication and explicit retry ancestry.
Started attempts require acknowledgement of possible effects; this does not
replace domain recovery. The audit integration supplies direct/remote status,
progress/event cursors, completed observation receipts, cooperative cancellation
and explicit aborted state after interruption. It owns a separate worker library
graph and uses the A10 Asset/Replica write boundary. It does not migrate
conversion/ingest/backup scheduling or automatically replay interrupted work.

Map the live Core utility manager, durable `jobs` repository/worker/scheduler and
ingest journals by responsibility. Recommend durable jobs as the owner of
restartable run state, with utility workers as execution mechanisms where useful.
Define stable run identity, progress/log cursors, result/receipt, cancellation,
retry/idempotency and recovery after process death. Do not force ephemeral work
into persistent storage without a requirement.

Migrate one long operation first, such as a storage audit or reconciliation run.
Submit briefly, then poll/wait outside Core's database-handler lock. Preserve
connection serialization: shortening a lock is not permission to share a mutable
database connection concurrently. Adopt maintenance scheduling only after this
contract works for a real workflow.

**Done when:** a fresh process can inspect/resume or explicitly fail an interrupted
run; cancellation has a documented effect boundary; duplicate delivery cannot
silently repeat publication. While a client waits or disconnects, unrelated
queries and cancellation remain responsive. Direct and remote clients observe
the same status, progress and terminal receipt.

## A10. Give storage record mutations one owner

**Findings:** ST-3/5 and parts of CO-8. **First PR:** one reconcile/admin mutation
family through a storage-owned service. **Dependencies:** A04/A05 contracts;
coordinate interfaces with A09.

**First family implemented for PR review, 2026-10-20.** Generic Asset/Replica
CRUD is refused. Explicit Asset policy assignment uses the manager's serialized
record/revision mutation, and FRBR backup indexing adopts/verifies through that
owner instead of changing identities and scalar rows directly. The
[mutation contract](../storage_and_backup/record-mutations.md) records the
compatibility change, historical fingerprint limitation and remaining families.
This prerequisite lets A09 verification rely on those normal writers' revisions.

The following policy-definition slice moves generic replication/backup policy
CRUD through storage as well. Partial updates preserve the current definition,
check its revision and retain committed receipts on readback/cache failure.
Assigned-policy deletion uses the owner. The Composite slice likewise routes
complete declarations and revision-checked patches through storage and refuses
raw member-row mutations. Its regression tests cover durable reopening, repeated
Assets, member-insert rollback, stale patches, reference guards and committed
receipts. Item-link, workflow-definition/source and legacy file adapters remain open.
The slice exposed a separate SQLite schema mismatch: its `(Composite, Asset,
stored role)` uniqueness rejects repeated same-role Assets despite distinct
sequence positions. Repair the current-schema constraint and its existing-library
migration before claiming full ordered-membership parity; failures currently
roll back the whole owned mutation.

Choose the storage-owned boundary for Asset/Replica and retained files/link-table
writes, building on the durable manager/repository adapters already present.
Move reconciliation and Core storage-graph admin calls through it. Catalog keeps
bibliographic semantics; storage does not become a second bibliographic writer.
Document mutation receipts, revision/cache updates and byte-publication recovery.

**Done when:** equivalent reconcile, manager and admin operations enforce the same
storage invariants and report partial effects consistently. Tests cover a late
metadata failure, published bytes awaiting adoption, retries and derived-state
refresh after commit. The migrated caller no longer writes storage tables behind
the selected owner's back.

## A11. Finish lifecycle caller migration

**Findings:** CO-7, DB-1/4 and the
[opening-path review](lifecycle-opening-path-review-2026-10-09.md).
**First PR:** one path-wrapper family and its callers. **Dependencies:** handle-based
operations already exist; durable maintenance waits for A09.

Move the `*_with_database_path` entry points from ingest/storage into the Core or
surface boundary that selects deployment settings. Migrate callers and tests,
then remove the upward imports and tighten the layer baseline. Retire Database
opt-ins/back-references only after their live consumers have moved. Review
constructor-internal partial cleanup where a resource can fail before registration
with LibraryServices.

**Done when:** the chosen lower-level module accepts borrowed handles only; normal
and failing calls preserve borrowed resources and close newly owned resources.
LibraryServices remains the graph owner. Keep the removed memory-SQLite factory
and broken Library facades retired; no replacement lifecycle package is needed.

## A12. Finish operational path and startup ownership

**Findings:** UT-1/10, RP-6 and residual DB-8 scope. **First PR:** remove the
`ptempfiles` dependency on the hook-bearing startup package. **Dependencies:**
use the existing [configuration contract](../core/configuration-ownership.md).

Extract the needed path helper to a passive owner and migrate its callers before
retiring `startup_scripts` hooks. Give temporary, debug and operational directories
explicit lifetimes and configuration, avoiding implicit checkout/CWD state. Keep
application preferences, library DBPrefs and component options in their intended
scopes; this work does not require another settings store.

**Done when:** targeted fresh-process imports do not create directories or install
those hooks; scratch is created on demand under the selected owner. Different
profiles and worker processes use the intended paths. Recheck remaining out-of-
dispatch preference/SQL-UDF consumers rather than assuming every global read is gone.

## A13. Make provider and conversion outcomes explicit

**Findings:** UT-3/4/5, FF-2/3/4/7/8. **First PR:** report one unsupported provider
family accurately. **Dependencies:** `plugins` is already unified; receipt delivery
can later use A09.

Expose structured availability with the provider, missing capability/dependency
and reason. Start with retained device registrations and the DOCX EMF/HTML2ZIP
cases: implement support with real inputs or mark it unavailable explicitly.
Refresh the relevant missing-import inventory instead of reusing the September
counts or blindly importing every module.

Follow with separate PRs for ConversionReport propagation through jobs/Core/API,
and for conversion scratch isolation that removes concurrent process-CWD changes.
Keep discovery/loading mechanics shared without merging unrelated domain
registries just because they all contain names and factories.

**Done when:** unsupported conversion is distinguishable from success, empty
output and runtime failure; structured loss/unsupported-feature receipts survive
transport; concurrent conversions cannot change each other's working directory
or remove caller files. Validate each slice with real format fixtures and installed
package checks where discovery/import paths change.

## A14. Continue documentation with concrete source-backed examples

**Findings:** DS-02/03/06/07/09. **Documentation pass completed, 2026-10-20;
review in [PR #127](https://github.com/ajCameron/LiuXin-alpha/pull/127).**

The detailed field guide now matches all 67 explicit tables in a fresh generated
SQLite schema. Coding and Style guides have distinct authority; metadata value,
Catalog hydration and provider routes are clarified. The remaining subsystem
folders were reviewed, historical/proposal status made explicit, and focused task
lists added. Jobs, ingest and caches received dedicated documentation indexes
because their ownership was previously scattered.

The [developer index](../README.md#subsystem-next-tasks) routes each folder's
follow-up. Keep examples aligned when A07 changes default path selection, and add
executable contract coverage when implementing the remaining runtime tasks. This
documentation pass does not close those implementation findings or certify old
benchmark/test counts as fresh results.

## A15. Refresh repository hygiene and CI evidence

**Findings:** RP-2/4/5/7/8. **Implemented for PR review, 2026-10-20.**
**Dependencies:** none.

The [dated CI baseline review](ci-baseline-2026-10-20.md) records the dependency,
surface-import and headless-image repairs on `ci-baseline-repair`. A clean
environment passes the complete quality gates and installed-wheel checks.
Thirty generated benchmark scratch files (about 98 MiB) are removed and guarded;
historical reports, source fixtures and compatibility configuration are classified.
The full run on `a0e4d2ed` now passes: **7,388 passed, 249 skipped and
17 expected failures**. [Run #39441669661](https://github.com/ajCameron/LiuXin-alpha/actions/runs/39441669661)
also passed the wheel, quality, both format lanes and aggregate gate. Optional
integration/GUI skips remain separate evidence.

**Fresh evidence, 2026-10-20:** [PR #128's full run](https://github.com/ajCameron/LiuXin-alpha/actions/runs/39366322310)
and [PR #129's full run](https://github.com/ajCameron/LiuXin-alpha/actions/runs/39368762975)
have the same 40 failing test IDs. The latter passed 7,308 tests, with 242 skips
and 17 expected failures. Failure groups include missing Ruff/encryption test
dependencies, surface package state and image fallback behaviour. The repair
addresses those groups; the subsequent current-head full run establishes the
green baseline separately from focused, architecture and wheel checks.

Classify tracked generated outputs, real fixtures and instance configuration from
a fresh inventory. Remove only confirmed generated material from future commits,
with a small guard against recurrence. This does not reclaim old Git objects or
authorize history rewriting. Reproduce current CI dependency/environment failures
before treating the October 18 Ruff/encryption failures as still present.

Check the installed wheel and optional-dependency contract, including the retained
`past` package. Establish a green reproducible baseline before recommending which
checks should be required. Keep the general full-suite owner and storage-aware
telemetry/cleanup workflow; preserve portable summaries of ignored evidence.

**Done when:** a clean environment can run the declared gates, wheel contents
match policy and fixtures still provision. Record fresh branch-rule/CI observations
with dates. Report focused tests, collection and full runs separately; do not
repeat the full suite just to validate a documentation-only edit.

## A16. Reduce compatibility debt one live boundary at a time

**Findings:** DB-3/4/5/6/7/9, CM-4/5/6/7/8, SU-3/4/5, FF-5/6, UT-6/7/9.
**First PR:** a caller-backed leaf extraction, preferably the field-metadata
contract behind reviewed upward imports. **Dependencies:** choose only the
contract needed by the selected callers; use A09/A10 before moving acquisition
or storage orchestration wholesale.

Inventory actual consumers, persisted/wire names and behaviour before deleting
or relocating a compatibility implementation. Separate schema descriptors from
bibliographic/display policy, then extract the appropriate lower-level contract.
Later slices can migrate MetadataSQL/legacy caches, consolidate specified
normalization or OPF implementations, and replace obsolete direct macro calls.
Keep intentional legacy behaviour differences explicit until a policy replaces them.

Acquisition deserves a separate decision: ingest should own HTTP/worker workflow,
metadata should own value contracts, and storage should supply byte access.
Define provider-to-candidate-to-write/receipt conversion before moving
`metadata.web_sources` or crawler backends. Likewise, split remaining large
facades/web composition only around demonstrated shared responsibilities.

**Done when:** live callers use the selected owner, supported behaviour and wire
contracts remain covered, removed names stay absent from installed artifacts,
and the exact dependency baseline tightens where an edge disappears. A module's
absence from a static reachability graph is insufficient evidence for deletion.

## Delivery and completion rules

Keep PRs focused on one observable contract or caller family. Each should state
before/after behaviour, owner, affected callers and compatibility limits; include
normal, refused and partial-failure cases that matter for that change. Select
backend/integration checks by the affected contract, preserving the existing
resource-safe test workflow.

Run the [quality gates](maintainability-quality-gates.md) relevant to changed code;
when scope/imports change, run the configured runner and architecture checks.
Never relax a baseline simply to make a migration green. Keep 2026-10-20's passing
checks as dated evidence, not a substitute for testing a later implementation.

On completion, add the implementation commit/PR, date and exact validation scope
here, update the corresponding finding and subsystem work list, and remove the
item from active sequencing. A task is complete when the behaviour and callers
have migrated, not merely when a new interface or document exists.
