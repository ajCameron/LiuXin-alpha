# Core next tasks

Updated **2026-11-23** after the lifecycle repairs and caller migration.
The [whole-Core review and completion plan](review-and-completion-plan-2026-11-23.md)
defines the full functionality target, ten findings and nine ordered completion
slices. The architecture below remains the foundation; whole-module sign-off is
still pending. Follow the [architecture sequence](../architectural_reviews/next_tasks.md)
for cross-package dependencies.
The named API, LibraryServices lifecycle, category policy and application
configuration store already exist. Preserve them while closing these gaps.

**Integration follow-up, 2026-11-23:** the
[metadata/lifecycle review](integration-review-2026-11-23.md) maps the native and
existing Catalog routes and repairs Core startup, cleanup, closed-service and
required-read failure boundaries. It also exercises a public native metadata
workflow across local/RPC clients and all cache modes. Validation passed 405
regressions (one PostgreSQL maintenance case skipped without a test server URL),
22 documentation checks and the complete quality gate. This addresses composition
within CORE-03. The caller migration below is a separate follow-up commit;
configuration remains the next independent task.

## CORE-01 — Enforce endpoint permissions

**Implemented for PR review, 2026-10-20; architecture A06.** The
[transport contract](transport_permissions.md) separates exposure from
serialization, gives HTTP a server-owned authenticated role, and binds read-only
surfaces to a restricted client. Generic invocation, admin writes, external
execution and refresh/maintenance require explicit authority. Host/origin/media
checks and credential redirect refusal happen before effects.

The final focused selection passed 67 checks; complete quality gates and both
38-example negative type contracts passed. Earlier full-API parity fixtures now
choose their trusted role explicitly. Per-user authorization, TLS termination
and plugin sandboxing remain separate deployment/design work.

## CORE-02 — Separate waiting from database dispatch

**Wait, durable audit and durable maintenance slices implemented.** The built-in
`jobs.wait` and `jobs.result` queries use concurrent dispatch while ordinary
handlers retain serialization. Registration replacement and queued shutdown
have explicit contracts; see [job waiting](../jobs/core-wait-dispatch.md).
Core's durable service also delivers maintenance outbox work through explicit
drains or opt-in polling. Other synchronous long operations and ordinary utility
submissions still need migration.

Concurrent local/HTTP tests cover responsive reads/cancellation, wait timeouts and
shutdown while work remains active. The [durable audit](../jobs/core-storage-audit.md)
uses jobs' repository for run identity and receipts, with independently owned
worker services. Other workflow migrations remain open under the jobs plan.

## CORE-03 — Complete lifecycle caller migration

**Selected caller migration implemented, 2026-11-23; architecture A11.** All four
local-disk/rclone/wget/native-HTML path helpers now live in
[`core.store_registration`](../../src/LiuXin_alpha/core/store_registration.py).
The local-disk CLI lives in `surfaces.cli.store_registration`; repository callers use the
new imports. Ingest/storage retain borrowed-handle operations without upward
Core imports or compatibility reexports. LibraryServices remains the graph owner.
The earlier integration review covers partial startup/cleanup; the follow-up
covers helper failure paths, real registration callers and CLI behavior.
The direction baseline drops the storage-to-Core allowance and explicitly bars
ingest from Core/surfaces. Resolved settings propagation, Database opt-ins and
maintenance scheduling remain separately scoped work.

Done when the selected lower-layer module has no upward Core import, failures
close newly owned resources, borrowed services survive, and the dependency
baseline tightens. Durable maintenance scheduling depends on the jobs contract;
transaction rollback and safe database creation remain database responsibilities.

## CORE-04 — Finish configuration and operational-path scope

**Architecture A12.** Inventory startup/scratch consumers and component singletons
that capture a profile at first use. Remove the `ptempfiles` startup-package edge,
using the existing passive path owner. Preserve distinct application, library and
component setting scopes.

Done when targeted fresh imports install no legacy hooks or directories, explicit
profiles remain isolated through worker submission, and components document their
capture/reload behaviour. Do not describe process-copy worker settings as live
cross-process synchronization.

## CORE-05 — Keep the public client contract honest

The local factory currently returns CoreRuntime directly. Decide whether a
restricted local adapter is needed by a concrete consumer; until then document
protocol discipline without claiming object isolation. Inventory new endpoints
through the existing provider/handler/service owners and expose real conditional
availability through discovery.

Done when client-facing docs and examples match factory identity, shutdown
ownership, wire values and errors. Add a restricted adapter only with caller
migration and direct/remote contract tests; it would not replace CORE-01's server
policy. Move domain policy out of Core only after its receiving owner is specified.

## Whole functionality completion order

The [detailed plan](review-and-completion-plan-2026-11-23.md#ordered-implementation-plan)
extends CORE-01 through CORE-05 with the remaining public and operational work:

1. Correct discovery, strict primitive validation, command registration capture
   and diagnostic redaction before extending those contracts.
2. Complete CORE-04 configuration/connection propagation.
3. Expose the full native metadata workflow through named operations, including
   shared records, trees, relationships and bounded batches.
4. Add recoverable command identities, effect receipts and post-disconnect lookup.
5. Extend durable jobs across ingestion, conversion, backup/restoration and
   remaining maintenance workflows using the existing job owners.
6. Bound scan/content/HTTP work and measure whole-Core mixed-load behavior.
7. Add event gap/restart recovery and operational observability.
8. Complete missing domain operations and consolidate adapters with caller migration.
9. Validate the backend/cache/local-RPC matrix, recovery scenarios and runbooks.

The review distinguishes implemented work from proposals. Its sign-off criteria
require actual supported workflow implementations; truthful unavailable labels
are necessary but do not replace completion.

## Validation and close-out

Use [Core regressions](../../tests/core/), the
[surface boundary tests](../../tests/surfaces/test_core_boundary_enforcement.py),
[configuration regressions](../../tests/preferences/test_configuration_ownership.py)
and the quality gates for affected contracts. Record exact backend/socket access
and test scope. Update [Core API](core-api.md), [workflow ownership](core-program-workflows.md)
and the corresponding architecture finding when each focused PR lands.
