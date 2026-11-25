# Core workflows and storage CLI ownership

Reviewed **2026-10-20** against the working tree based on `fc5de7af`.

**Durable execution update, 2026-11-25:** named long operations now use the
catalogue outbox and the existing top-level jobs ledger. Their public result is an
acceptance receipt; domain adapters still return completion receipts to workers.
`core.workflow_operations` selects the explicit adapters and `workflow_delivery`
binds captured settings, execution claims and cancellation. Storage restoration
and exact recreation live in `storage.workflows.recreation`. See the
[recovery contract](recovery-operations.md) for the operation families, retries,
held publication and caller migration.

The program API and storage CLI retain their public entry points, but their
compatibility files no longer implement workflows.

**Registration caller follow-up, 2026-11-23:**
[`core.store_registration`](../../src/LiuXin_alpha/core/store_registration.py)
owns the four synchronous path helpers through LibraryServices; the standalone
local registration command lives in `surfaces.cli.store_registration`. Domain
registration functions borrow handles, and existing Core workflow jobs continue
to use those functions. See the [migration contract](../architectural_reviews/lifecycle-opening-path-review-2026-10-09.md#registration-caller-migration).

## Core

`core/program_endpoints` defines operation names, payload descriptions, handler
contracts, and registration order. `core/program_api.py` installs those
providers and explicitly delegates to `core/program_services`. Static handler
aliases and small instance-method adapters preserve existing bound and unbound
call signatures. Subclass implementations must satisfy the endpoint contract;
service code never calls back through the compatibility class.

Implementation ownership is by responsibility:

| Owner in `core/program_services` | Responsibility |
| --- | --- |
| `payloads` | Envelope validation and subsystem-to-wire conversion |
| `discovery` | Capability inventory and job results/logs |
| `database`, `schema`, `preferences` | Database administration, schema/custom fields, preferences |
| `catalog` | Catalogue identity, hierarchy, fields, and search |
| `native_metadata` | Native WEMI search/read/edit envelopes through `CoreServices.metadata`; Catalog/MetadataAPI/CatalogCache own domain rules, revisions and cache publication |
| `metadata`, `conversion`, `ingest` | Metadata read/write and serializable content-job submission |
| `backup`, `maintenance` | Durable backup workflows and maintenance operations |
| `store_resolution`, `stores` | Store identity/configuration lookup and administration |
| `storage_status` | Durable/live inventory, per-Store projections, aggregate health |
| `storage_integrity`, `storage_repair`, `storage_recovery` | Verification, bounded repair, interrupted-ingest recovery |
| `storage_evacuation` | Core envelope adapter for evacuation |
| `evacuation_models`, `evacuation_planning`, `evacuation_execution`, `storage_placement` | Typed evacuation workflow and shared placement rules |

The [2026-11-23 integration review](integration-review-2026-11-23.md) distinguishes
native metadata operations from the older Catalog and Item/file-shaped routes.
Native metadata receipts already describe reconciliation and must not be passed
through Core's legacy post-operation refresh a second time.

Evacuation planning returns immutable values containing Asset/Replica/Store
identities, replacement destinations, copy targets, and transfer estimates.
Wire dictionaries are produced at the boundary, not passed around as workflow
state. Execution checks action and byte limits before starting an entry,
creates verified replacement copies, and checks current policy/topology and
verified capacity before removing source replicas. A failed replacement or
insufficient verified capacity retains source claims and bytes. Operator
retention, read-only sources, and unmanaged replicas retain source bytes even
when their claims can be removed. Receipts and completion fields preserve the
existing Core response shape. These are evacuation-workflow guarantees, not
universal protection of every raw manager deletion path; the storage mutation
boundary still needs the policy enforcement described in the architecture tasks.

The evacuation implementation depends on `StorageManagerAPI`, not a database,
CLI, or Core runtime. The adapter resolves user-facing references and handles
the final before/after snapshots. Planning and execution share policy
eligibility/capacity logic so the safety check cannot drift into a separate
interpretation of placement rules.

## Storage CLI

`surfaces/cli/storage_commands` contains the concrete CLI owners:

- `parsers` and `parser_*`: command-family registration, split into individual
  command builders while preserving help and option order;
- `administration`, `integrity`, `resources`: thin named Core operations;
- `store_options`, `store_add`, `store_wizard`, `prompts`: non-secret option
  validation, persistence, and guided operator interaction;
- `ingest_options`, `ingest_config`, `ingest_preflight`, `ingest_paths`:
  arguments, budgets/profile defaults, readiness, path validation, and locks;
- `ingest`: run lifecycle, cancellation scope, and terminal completion;
- `ingest_run`: invocation of the existing mixed-ingest application service;
- `ingest_reporting`, `signals`, `filesystem`: durable reports/logs, signal
  handling, and filesystem mechanics.

The CLI still does not construct catalogue or storage subsystems. Core-owned
`LibraryServices` composes the durable service graph; `ingest/mixed_application.py`
consumes an injected Library for its application workflow. Parser modules
depend on command owners, and helper modules do not import the public facade.
Tests that substitute Core sessions or command handlers patch the consuming
owner module, not a facade alias; no dynamic module proxy is used.

## Adding or changing an operation

1. Place behaviour in its responsibility owner. Introduce typed request/result
   values for new workflows; do not use wire dictionaries as mutable state.
2. Declare the named endpoint in the relevant provider and handler protocol,
   then add its explicit compatibility delegate if it belongs to that facade.
3. Add CLI parsing and presentation to the corresponding command owner when
   needed. Keep the Core/application-service boundary intact.
4. Test failure and safety conditions as well as the successful response.
   Extend the static contract examples for new kinds of internal calls.
5. Run `bash scripts/run_type_checks.sh`, the workflow ownership tests, and the
   relevant Core/CLI behaviour tests. RPC tests need local socket access.

Both extracted trees enter the zero-error typing/lint/complexity gate. Newly
written evacuation services are strict basedpyright targets; migrated dynamic
adapters retain standard mode until those legacy contracts can be made precise.
