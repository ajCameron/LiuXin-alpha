# Cross-project work

Reviewed: **2026-10-20**, including A01–A07 implementation slices for PR review.

This is a routing list for work that crosses owners. Detailed implementation
steps belong in subsystem plans; speculative additions belong in [ideas](ideas.md).
A retained review finding must be checked against source before being scheduled.
The [architecture next-task list](architectural_reviews/next_tasks.md) proposes
the implementation sequence and acceptance criteria for the reviewed architecture
work; this index keeps the routes to the owning plans.

## Current priorities

| Work | Next useful boundary | Owner and detail |
| --- | --- | --- |
| Enforce remote capabilities | A06 is implemented for review: server-owned grants, authenticated roles and restricted readers. Inspect PR evidence before further account/deployment work. | Core transport/surfaces: [A06](architectural_reviews/next_tasks.md#a06-enforce-transport-capabilities-on-the-server). |
| Complete semantic write coverage | Add missing field/relationship rules and explicit historical-data repair without another write path. | Catalog and metadata: [work list](catalog/catalog-roadmap.md), [write contract](catalog/catalog-write-validation-contract.md). |
| Align remaining read behaviour | A07 aligns SQL complete rays and Catalog bundles. Address remaining projection/search and bounded-query gaps. | Catalog/Core: [selection contract](catalog/catalog-wemi-selection.md), [Core API](core/core-api.md). |
| Harden destructive storage operations | Enforce retention/last-copy decisions at mutation boundaries and make partial effects recoverable. | Storage and application workflows: [storage overview](<06 - Storage.md>), [annotated findings](architectural_reviews/architecture-review-2026-09-26.md). |
| Finish lifecycle and transaction ownership | Review remaining path-opening wrappers, database creation and failure semantics using the actual owner of each resource. | Core/databases: [lifecycle review](architectural_reviews/lifecycle-opening-path-review-2026-10-09.md), [configuration](core/configuration-ownership.md). |
| Consolidate execution and acquisition | Define durable jobs versus Core execution, restart/cancellation and ingest receipts; retain domain policy with its owner. | Jobs/ingest/Core/storage: [ownership rules](<04 - Seperation of Concerns.md#ingest-jobs-and-execution-helpers>), [mixed ingest](storage_and_backup/mixed_ingest_operations.md). |
| Continue registry and compatibility cleanup | Reuse shared loading where contracts match; remove unsupported providers and retired adapters after caller migration. | Plugins and domain registries: [plugin composition](plugins/plugin-composition.md). |
| Bound resource use | Coordinate active cache users, scratch ownership and acquisition/conversion budgets, informed by telemetry. | Storage/jobs/test tooling: [test resources](<07 - Test Databases.md>), ideas I15/I16 in [experiments](ideas.md). |
| Reconcile deeper documentation | Review older database field inventories, storage design notes and subsystem claims against source; update callers when moving guides. | Subsystem owners: [structural review](architectural_reviews/documentation-structural-review-2026-10-18.md), [developer index](README.md). |

Keep extending the enforced dependency/typing/complexity scopes through reviewed
changes. The former seven modern import-cycle components and monolithic Core/CLI
extraction tasks are historical starting points, not the current backlog.
The [layer ratchet](architectural_reviews/layer-direction-ratchet.md) distinguishes
the protected zero-cycle scope from remaining whole-package exceptions.

## Existing campaigns and open choices

- Continue format work through the [conversion work list](conversion_pipeline/conversion_pipeline_todo.md)
  and [dated sign-off matrix](conversion_pipeline/conversion_pipeline_signoff.md).
  Check each format's actual status before treating an old campaign item as open.
- Extend the [semantic fixture families](<07 - Test Databases.md>) only for new
  shapes. A dedicated compatibility-projection fixture is a possible addition;
  compatibility-view tests already exist.
- The [benchmark summarizer](../scripts/summarize_benchmark_report.py) already
  emits text/Markdown. Decide whether scheduled benchmark runs should publish
  that summary and whether a condensed machine-readable slowest-path report is
  useful; neither requires a second benchmark harness.
- Re-profile the remaining OPDS/global-search assembly when that path is changed;
  the [earlier optimization checkpoint](../working-memory/optimization-pass-driver-wrapper-opds-2026-03-19.md)
  supplies history, not a current bottleneck measurement.
- Evaluate an event mechanism only after identifying producers, consumers,
  ordering, lifetime and failure requirements. `python-event-bus` remains an
  unselected candidate, not an architecture dependency.

## Completed foundations to preserve

The current checkout already has shared Catalog write/normalization routes,
Core category selection, application preference ownership, a merged `plugins`
package, extracted Core/CLI services, a developer-documentation index and one
general full-suite CI owner. Keep their regressions and ownership contracts.
The separately named syntax check is not a replacement for lint/type checking;
see [CI ownership](hacking/testing/continuous-integration.md).

Completion of these foundations does not settle the remaining validation,
selection, storage or execution boundaries above. Record new completion evidence
with its scope and date, then remove or rewrite the corresponding active task.

## Subsystem follow-up review, 2026-10-20

The [developer index](README.md#subsystem-next-tasks) now routes focused `next_tasks.md` lists for every top-level documentation folder. Jobs, ingest and caches have dedicated indexes. Keep detailed task status with the owning folder and cross-project order in the architecture list.
