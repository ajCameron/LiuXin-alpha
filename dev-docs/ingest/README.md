# Ingest

Reviewed **2026-10-22**. Ingest discovers sources and coordinates acquisition;
metadata rules, Catalog writes and storage publication retain their own owners.
See [next tasks](next_tasks.md).

## Choose the workflow

| Entry point | What it does | Important distinction |
| --- | --- | --- |
| [ingest.stores.ingest_store](../../src/LiuXin_alpha/ingest/stores.py) | Enumerates a Store and copies or adopts objects through the storage manager. | Enumeration cursors and per-object staged-prefix checkpoints solve different retry problems. |
| [ingest.mixed_application](../../src/LiuXin_alpha/ingest/mixed_application.py) | Composes transient discovery or durable mixed-tree ingestion using an injected Library. | The caller owns lifecycle, process locking, signals and report publication. |
| [storage.ingest.mixed_format](../../src/LiuXin_alpha/storage/ingest/mixed_format.py) | Walks nested local containers, applies budgets, materializes and records assets/replicas. | Storage-facing traversal and recovery, not arbitrary bibliographic matching. |
| [ingest.remote_html](../../src/LiuXin_alpha/ingest/remote_html.py) | Composes remote discovery and registers discovered addresses/files. | Registration is not a byte download or an all-crawl transaction. |
| [ingest.web](../../src/LiuXin_alpha/ingest/web) | Records remote candidates and optionally acquires verified copies through managed storage. | Registration creates no placeholder Assets; durable capture receipts distinguish observations from retained bytes. |
| [ingest.sources](../../src/LiuXin_alpha/ingest/sources) | Native/wget discovery adapters and shared crawl policy. | Discovered hints remain untrusted input. |
| [ingest.adding](../../src/LiuXin_alpha/ingest/adding.py) | Retained filename grouping and legacy Calibre-style helpers. | Some integration paths reference absent metadata APIs; importability is not operational support. |

## Boundaries and recovery

For a bibliographic import, turn extracted facts into an explicit metadata
candidate, match it with provenance, then invoke Catalog's validated mutation.
For bytes, use the storage manager's ingest/publication contract. A parser result
does not bypass either boundary.

The supported workflows can commit incremental progress. An exception near the
end does not undo all prior objects, Store rows or published bytes. Reports,
checkpoints and durable operation identities must explain what completed and
what can resume. `continue_on_error` generally covers selected object failures,
not all setup or enumeration errors.

The [mixed-ingest runbook](../storage_and_backup/mixed_ingest_operations.md)
describes CLI discovery, preflight, run/resume, telemetry and exact budget defaults.
Global wall-time checks are cooperative; an active parser can return after the
deadline. Nested materialized inputs currently lack a live cache pin/lease
contract, so automatic eviction cannot safely race an active ingest.

Catalogue registration, materialization and bibliographic import are different
results. Keep them distinguishable in receipts and operator output. In
particular, the [Calibre compatibility plan](../calibre_compat/README.md) does not
represent an already implemented complete Calibre-to-WEMI import profile.

## Application entry points

The [web-ingest runbook](web_ingest_operations.md) covers native/wget discovery,
registration without mirroring, selective acquisition, budgets and saved-run
replay through `liuxin ingest web`.

Use the [operational CLI](../surfaces/operational-cli.md) and Core workflows for
application work. Existing path-opening helpers remain migration boundaries;
new code should receive the composed Library/storage services rather than
secretly opening another application graph.

Validation should cover repeated input, changed source versions, resume after
publication, malformed inventory and bounded resource use. Keep network
discovery fixture-backed unless an explicit live probe is the task.

Return to the [developer index](../README.md).
