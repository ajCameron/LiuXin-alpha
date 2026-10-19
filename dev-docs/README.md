# Developer documentation

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
- [Project goals](project-level-goals.md), [coding guide](<Coding Guide.md>) and
  [style guide](<Style Guide.md>): principles and contributing conventions.
- [Contributor tools and testing](hacking/README.md): packaging, quality gates,
  test streams and documentation checks.

## Find the subsystem

| Topic | Start here |
| --- | --- |
| Application operations, lifecycle and configuration | [Core](core/README.md) |
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

Earlier exploratory writing remains available: [about these notes](<00 - This Document.md>),
[project motivation](<01 - Introduction.md>), [module notes](top-level-modules-notes.md),
[earlier separation model](seperation_of_concerns.md), [ideas](thoughts.md) and
[cross-project TODOs](global_todo.md). Check their dates and current owners before
treating them as implemented behaviour. [SQL sketches](random_sql/README.md) are
design history, not the installed schema.

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
