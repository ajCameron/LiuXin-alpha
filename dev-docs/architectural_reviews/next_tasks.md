# Next architecture tasks

Date: **2026-10-20**. Basis: documentation review at `a5406cf8`, A01 at `75b266ed`,
A02 at `d5956d6e`, and A03's implementation on `database-create-safety`.

Status: **A01–A03 are implemented for PR review; remaining runtime fixes are open.
Documentation progress is recorded in A14; A15's CI repair is under verification.** Recheck the named behaviour before starting each task, especially
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

First repair the shared full-suite failures recorded under **A15** so subsequent
runtime slices have a useful CI baseline. With A01–A03 implemented for review,
finish **A04's transaction verification**,
then enforce A05's deletion policy. A06's transport contract is independent of
job consolidation and can proceed in the same safety group. A07 is the next bounded read-model
change; A08 continues the existing write-consolidation programme.

The order is a recommendation, not a requirement to finish every earlier item
before starting an independent one. Dependencies are stated in each task.

| Task | Priority | Concrete result | Main owner |
| --- | --- | --- | --- |
| [A01](#a01-preserve-caller-owned-backup-staging) | Implemented; PR review | Cleanup removes only scratch owned by the backup run. | Storage backup |
| [A02](#a02-enforce-store-read-only-intent) | Implemented; PR review | Four affected builders preserve and enforce read-only intent through durable reload. | Storage registry and Stores |
| [A03](#a03-make-database-creation-nondestructive-by-default) | Implemented; PR review | Creation refuses collisions; explicit offline replacement retains a verified backup. | Databases and Core bootstrap |
| [A04](#a04-separate-resource-lifetime-from-transaction-lifetime) | Verification in progress | Exceptions and nested mutations have one explicit transaction contract. | Databases, with Catalog/storage callers |
| [A05](#a05-enforce-deletion-policy-at-the-mutation-boundary) | First | Removing bytes cannot bypass retention/last-copy policy. | Storage manager |
| [A06](#a06-enforce-transport-capabilities-on-the-server) | First | Remote/read-only clients cannot invoke privileged operations by name. | Core transport and surfaces |
| [A07](#a07-choose-one-default-wemi-selection-policy) | Next | SQL projections and Catalog bundles apply an agreed path-selection policy. | Catalog and schema projections |
| [A08](#a08-complete-semantic-write-coverage) | Next | Missing validators enter the existing shared write boundary. | Metadata and Catalog |
| [A09](#a09-converge-job-state-and-release-long-waits-from-core-dispatch) | Next | One real workflow has durable, observable execution without monopolizing dispatch. | Jobs and Core |
| [A10](#a10-give-storage-record-mutations-one-owner) | Next | Reconcile and admin adapters share storage record invariants and receipts. | Storage, using database transactions |
| [A11](#a11-finish-lifecycle-caller-migration) | Following | Lower-layer path wrappers and obsolete opt-ins retire through caller migration. | Core, ingest and storage |
| [A12](#a12-finish-operational-path-and-startup-ownership) | Following | Scratch/startup helpers no longer pull process-wide hooks into foundation imports. | Configuration paths and utility owners |
| [A13](#a13-make-provider-and-conversion-outcomes-explicit) | Following | Unsupported providers and conversion losses become structured results. | Plugins, file formats and jobs |
| [A14](#a14-continue-documentation-with-concrete-source-backed-examples) | Documentation pass complete; PR review | Schema inventory, conventions, subsystem ownership and next-task routing reviewed. | Developer documentation |
| [A15](#a15-refresh-repository-hygiene-and-ci-evidence) | Verification in progress | Repair shared failures, remove generated scratch and validate a clean baseline. | Packaging and test tooling |
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
for WAL sidecar behaviour and scope. A05's retention/last-copy policy remains open.

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
migrated. A04's general transaction/close semantics remain open.

## A04. Separate resource lifetime from transaction lifetime

**Findings:** DB-1/2. **Implementation under verification, 2026-10-20.**
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
not accidentally commit failed work. Live PostgreSQL checks pass; final SQLite/
APSW caller and resource checks are running. Preserve the already-tested
Catalog/Row/Agent rollback guarantees before publishing this slice.

## A05. Enforce deletion policy at the mutation boundary

**Findings:** ST-1/3. **First PR:** one policy boundary around replica byte removal.
**Dependencies:** A02; use A04's transaction contract for related metadata changes.
A10 can migrate additional storage writers afterward.

[`remove_replica`](../../src/LiuXin_alpha/storage/storage_manager/mixins/replicas.py)
and [`router.delete`](../../src/LiuXin_alpha/storage/storage_manager/mixins/router.py)
currently reach byte removal without one enforced retention/last-copy decision.
Define the permitted destructive operations and put policy immediately before
effects in the manager, where Asset/Replica knowledge exists. Keep raw drivers
as byte mechanisms. A direct driver escape must be clearly scoped as privileged
low-level access, never advertised as a policy-enforcing deletion API.

**Done when:** direct manager, Core and CLI paths agree on refusals; read-only,
retention and last-copy cases fail before mutation. Concurrent deletion requests
cannot both rely on the same remaining copy. Authorized deletion and backend
failure produce honest results, including recovery information when byte and
metadata effects cannot be atomic. Do not pretend a database rollback restores
already-deleted external bytes.

## A06. Enforce transport capabilities on the server

**Findings:** CO-4, SU-2/6. **First PR:** explicit remote endpoint permissions and
read-only dispatch policy. **Dependencies:** none; integrate A05's destructive
policy instead of duplicating it in HTTP.

[`HTTP dispatch`](../../src/LiuXin_alpha/core/transport/http.py) accepts registered
operation names; `transport_stable` describes encoding, not authorization. Define
remote exposure and required capabilities explicitly. Deny generic `invoke` and
privileged admin/executable-configuration operations unless an explicit trusted
policy permits them. Bind capabilities to a server-established client identity or
a fixed deployment policy; never trust a capability list in the request payload.

Give read-only surfaces a narrow client contract and enforce the same restriction
on the server. Move refresh/maintenance side effects to an explicitly authorized
path. Define authentication, request media-type and browser-origin handling for
the supported deployment modes; loopback binding alone is not the policy.

**Done when:** direct HTTP requests cannot bypass restrictions by naming handlers,
using the generic invoke route, setting executable paths or claiming permissions.
Known allowed reads/writes still work under the intended policy, and rejections
cause no side effects. Use real direct/HTTP parity tests, not just hidden UI controls.

## A07. Choose one default WEMI selection policy

**Findings:** remaining read-model work under SU-1, with the
[worked SQL/Catalog discrepancy](../databases/wemi-link-examples.md#default-sql-view-versus-catalog-bundles).
**First PR:** decision, shared selection semantics and parity cases.
**Dependencies:** none of the execution refactors.

Specify how explicit primary links, priority, missing relationships and ties
select an expression/manifestation/item path. Choose the owner of that policy in
Catalog, then align SQL projections and retrieval adapters. If two views
intentionally answer different questions, name that difference in their contracts
instead of making both claim to return the same default. Keep full bundle/list
APIs distinct from a preferred-path projection.

**Done when:** shared fixtures cover multiple translations, competing primary
links, ties, incomplete paths and a manifestation embodying both an introduction
and its novel. SQL and Catalog agree when answering the same question; neither
silently loses the other Work. Update the worked examples with actual outputs,
and retain Core's already-consolidated tag/label semantics.

## A08. Complete semantic write coverage

**Findings:** remaining coverage after CM-1/2/3, plus CM-6/9 and DB-3/7.
**First PR:** one identified field or relationship family through all supported
routes. **Dependencies:** preserve A04's transaction rules; do not wait for a
wholesale cache redesign.

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

**Findings:** CO-5/8; related ST-5 and CM-5. **First PR:** one workflow through a
specified run-state contract. **Dependencies:** A06 for remote permissions;
A10 for workflows that mutate storage records. Pure execution contracts can start
independently.

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

**Findings:** RP-2/4/5/7/8. **Implementation under verification, 2026-10-20.**
**Dependencies:** none.

The [dated CI baseline review](ci-baseline-2026-10-20.md) records the dependency,
surface-import and headless-image repairs on `ci-baseline-repair`. A clean
environment passes the complete quality gates and installed-wheel checks.
Thirty generated benchmark scratch files (about 98 MiB) are removed and guarded;
historical reports, source fixtures and compatibility configuration are classified.
Full-suite acceptance remains pending; focused results do not close this task.

**Fresh evidence, 2026-10-20:** [PR #128's full run](https://github.com/ajCameron/LiuXin-alpha/actions/runs/39366322310)
and [PR #129's full run](https://github.com/ajCameron/LiuXin-alpha/actions/runs/39368762975)
have the same 40 failing test IDs. The latter passed 7,308 tests, with 242 skips
and 17 expected failures. Failure groups include missing Ruff/encryption test
dependencies, surface package state and image fallback behaviour. The repair
addresses those groups; the green focused, architecture and wheel checks do not
establish a green full-suite baseline.

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
