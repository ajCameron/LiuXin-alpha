# Developer documentation

Top-level content reviewed: **2026-10-20**, against the working tree based on
`fc5de7af`. This review does not relabel every linked historical note as current.

Start here to find the owner of a change, its contract and the relevant checks.
Subsystem indexes distinguish maintained guidance from proposals, historical
reviews and completed campaigns. A link to a design note does not mean its
implementation is complete.

## Start here

- [Local setup](../README.md): environment, entry points and common commands.
- [Top-level structure](<02 - Top Level Structure.md>): package summaries,
  responsibilities and application relationships.
- [Separation of concerns](<04 - Seperation of Concerns.md>): the ownership rules
  for new code and remaining migration boundaries.
- [Project goals](project-level-goals.md), [coding guide](<hacking/Coding Guide.md>) and
  [style guide](<hacking/Style Guide.md>): principles and contributing conventions.
- [Contributor tools and testing](hacking/README.md): packaging, quality gates,
  test streams and documentation checks.

## Read the top-level guides

The numbered guides form an orientation route. Subsystem contracts remain the
place for detailed API guarantees and operational flags.

| Guide | What it answers |
| --- | --- |
| [00: About these documents](<00 - This Document.md>) | Where current guidance, ideas, decisions and evidence belong. |
| [01: Introduction](<01 - Introduction.md>) | What the project aims to preserve, what exists, and which claims remain goals. |
| [02: Top-level structure](<02 - Top Level Structure.md>) | How the sixteen packages fit together and who owns a change. |
| [03: Database schema](<03 - Database Schema.md>) | How WEMI, assets, replicas and generated relations are persisted. |
| [04: Separation of concerns](<04 - Seperation of Concerns.md>) | The current ownership policy and remaining migrations. |
| [05: Calibre compatibility](<05 - Calibre compat.md>) | Supported readers/projections and the limits of source identity and imports. |
| [06: Storage](<06 - Storage.md>) | Managers, configured Stores, drivers, recovery and retention boundaries. |
| [07: Test databases](<07 - Test Databases.md>) | Fixture selection, provisioning, telemetry, cleanup and explicit benchmarks. |
| [08: Storage cache backends](<08 - Storage Cache Backends.md>) | Derived catalogue caches, freshness, mutation reconciliation and backend choice. |

## Find the subsystem

| Topic | Start here |
| --- | --- |
| Application operations, lifecycle and configuration | [Core](core/README.md) |
| Durable execution, job state and worker lifetimes | [Jobs](jobs/README.md) |
| Discovery, acquisition and resumable ingest | [Ingest](ingest/README.md) |
| Derived catalogue state, freshness and backend choice | [Caches](caches/README.md) |
| Bibliographic writes, identity, matching and persistent reads | [Catalog](catalog/README.md) |
| Metadata sources, projections and file-writing behaviour | [Metadata](metadata/README.md) |
| Passive metadata values, containers and hydration boundaries | [Metadata containers](metadata_container/README.md) |
| Schema, database drivers and transaction mechanics | [Databases](databases/README.md) |
| Files, replicas, stores, backup and recovery | [Storage and backup](storage_and_backup/README.md) |
| CLI, terminal, GUI and operational entry points | [Surfaces](surfaces/README.md) |
| Plugin contracts, loading, application composition and import migration | [Plugins](plugins/README.md) |
| Format implementations and standalone conversion | [File formats](file_formats/README.md) |
| Shared conversion pipeline and verification records | [Conversion](conversion_pipeline/README.md) |
| Inherited behaviour and supported compatibility | [Calibre compatibility](calibre_compat/README.md) |
| Decisions, enforced boundaries and dated findings | [Architecture and reviews](architectural_reviews/README.md) |

## Subsystem next tasks

Folder-level guidance reviewed **2026-10-20**, including historical/proposal
classification and current source boundaries. Each list proposes changes and
completion criteria; it does not claim those changes have shipped. New jobs,
ingest and caches indexes collect ownership that was previously scattered.

| Area | Follow-up |
| --- | --- |
| Cross-project architecture | [Next tasks](architectural_reviews/next_tasks.md) |
| Core | [Next tasks](core/next_tasks.md) |
| Catalog | [Next tasks](catalog/next_tasks.md) |
| Databases | [Next tasks](databases/next_tasks.md) |
| Metadata | [Next tasks](metadata/next_tasks.md) |
| Metadata containers | [Next tasks](metadata_container/next_tasks.md) |
| Storage and backup | [Next tasks](storage_and_backup/next_tasks.md) |
| Jobs | [Next tasks](jobs/next_tasks.md) |
| Ingest | [Next tasks](ingest/next_tasks.md) |
| Caches | [Next tasks](caches/next_tasks.md) |
| Plugins | [Next tasks](plugins/next_tasks.md) |
| File formats | [Next tasks](file_formats/next_tasks.md) |
| Conversion | [Next tasks](conversion_pipeline/next_tasks.md) |
| Surfaces | [Next tasks](surfaces/next_tasks.md) |
| Calibre compatibility | [Next tasks](calibre_compat/next_tasks.md) |
| Contributor tools | [Next tasks](hacking/next_tasks.md) |
| Historical SQL sketches | [Next tasks](random_sql/next_tasks.md) |

## Common tasks

- Add an application operation: [Core API](core/core-api.md) and
  [workflow ownership](core/core-program-workflows.md).
- Change bibliographic persistence: [Catalog API](catalog/catalog-api-usage.md)
  and [write contract](catalog/catalog-write-validation-contract.md).
- Configure a deployment: [configuration ownership](core/configuration-ownership.md)
  and [operational CLI](surfaces/operational-cli.md).
- Add or migrate a plugin: [plugin ownership](plugins/README.md) and
  [import migration](plugins/plugin-composition.md#import-migration).
- Inspect a Calibre library or change a compatibility adapter:
  [current compatibility boundaries](<05 - Calibre compat.md>) and
  [identity mappings](<calibre_compat/mapping calibre to LiuXin.md>).
- Operate acquisition or recovery: [mixed ingest](storage_and_backup/mixed_ingest_operations.md)
  and the [storage API](storage_and_backup/storage_api.md).
- Select a test lane or investigate resource use: [testing](hacking/testing/README.md)
  and [test streams](hacking/test-streams.md).
- Build an installable artifact: [packaging](hacking/packaging.md).

## Background and history

The [architecture index](architectural_reviews/README.md) separates accepted
decisions and active enforcement from dated reviews. The [working-memory index](../working-memory/index.md)
holds handoffs and detailed evidence; current contracts belong in the subsystem
guides rather than only in a sequence of session notes.

The [earlier cache rationale](top-level-modules-notes.md) and
[earlier separation model](seperation_of_concerns.md) are explicitly superseded;
their proposed owners/layouts must not guide new code. [SQL sketches](random_sql/README.md)
also retain design history rather than the installed schema.

[Ideas and experiments](ideas.md) collects proposals with first experiments and
likely owners. [Cross-project work](global_todo.md) routes current gaps to the
subsystem plans. Neither is a release commitment or a substitute for those
contracts.

## Maintaining navigation

Link each new guide from its subsystem index. Use relative Markdown links, with
angle brackets around destinations containing spaces. A move must update incoming
links, the moved document's outgoing links, source signposts, scripts, tests and
deployment-bundle paths together.

The [documentation checks](../tests/scripts/test_developer_documentation_links.py)
parse the root README and every Markdown document under `dev-docs`, require local
targets to exist in tracked content, and require every developer guide to be
reachable from this index. They do not fetch external websites or certify every
heading fragment. Run them with the
[public-boundary checks](../tests/scripts/test_public_documentation_boundaries.py):

```bash
python -m pytest -q tests/scripts/test_developer_documentation_links.py tests/scripts/test_public_documentation_boundaries.py
```

Keep decisive historical results in tracked text. Describe ignored local outputs
as local evidence paths in code text, rather than publishing links that work only
in one checkout. Preserve useful history while making each document's status clear.
