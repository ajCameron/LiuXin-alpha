# Developer documentation

Start here to find the owner of a change, the relevant contract, and the checks
to run. These are maintained guides alongside older design notes; linking a
roadmap or proposal does not mean that its implementation is complete.
Use the [working-memory index](../working-memory/index.md) for dated evidence
and outstanding work, not as a replacement for the subsystem guides.

## Start here

- [Local setup](../README.md): environment, entry points, and common commands.
- [Style guide](<00 - Style Guide.md>): naming and useful docstrings.
- [Project-level goals](project-level-goals.md): safety, usability,
  verifiability, and explicit human/AI readability goals.
- [Top-level structure](<02 - Top Level Structure.md>) and
  [responsibility boundaries](<04 - Seperation of Concerns.md>): where code belongs.
- [Maintainability quality gates](maintainability-quality-gates.md): enforced
  formatting, typing, complexity, documentation, and dependency scopes.
- [Project docstring completion plan](project-docstrings-completion-plan.md):
  bounded work modules, exact remaining inventory, and verification checkpoints.
- [Import shim removal](shim-removal-plan.md): owner migrations, retained adapters,
  verification and documentation-inventory reconciliation.
- [CI ownership](continuous-integration.md) and [test streams](test-streams.md):
  automated checks and local feedback loops.
- [Packaging](packaging.md): installed-artifact and runtime-resource contracts.

## Core and application surfaces

- [Core API](core-api.md) and [workflow ownership](core-program-workflows.md).
- [CLI composition](cli-composition.md) and [terminal composition](terminal-composition.md).
- [Operational CLI](operational-cli.md) and [metadata CLI](metadata-cli.md).
- [Read-model failure boundaries](read-model-failures.md) and
  [read-only surface startup](read-only-surface-appliance-startup.md).
- [Tkinter architecture](tkinter-gui-architecture.md) and
  [implementation plan](tkinter-gui-implementation-plan.md).

## Catalog and metadata

- [Catalog API usage](catalog-api-usage.md), [matching policy](catalog-matching-policy.md),
  and [cache boundary](catalog-cache-boundary.md).
- [Link writers](catalog-link-writer-architecture.md),
  [legacy mutation migration](catalog-legacy-mutation-migration.md),
  [fitness review](catalog-fitness-review.md), and [roadmap](catalog-roadmap.md).
- [Metadata container system guide](metadata_container/metadata_container_system_guide.md)
  and [architecture](<metadata_container/00 - metadata_container_architecture.md>).
- Container [boundaries](metadata_container/metadata_container_boundaries.md),
  [family matrix](metadata_container/metadata_container_family_matrix.md),
  [naming](metadata_container/metadata_container_naming_conventions.md), and
  [vocabularies](metadata_container/metadata_container_vocabularies.md).
- Container [constraint alignment](metadata_container/metadata_container_db_constraint_alignment.md),
  [dynamic convenience policy](metadata_container/metadata_container_dynamic_convenience_policy.md),
  and [test surface](metadata_container/metadata_container_test_surface.md).
- [Database source layer](metadata_container/metadata_db_source_layer.md),
  [facade workflows](metadata_container/metadata_facade_workflows.md),
  [projection views](metadata-projection-views.md), and [dirtied queue](metadata_dirtied_queue.md).
- [Local metadata sources](metadata-local-sources.md),
  [web sources](metadata-web-sources.md), [WEMI hardening](metadata-wemi-container-hardening.md),
  and [writer coverage](metadata-writer-coverage-contract.md).
- [Column metadata](column-metadata.md), [fields](what-are-fields.md), and
  [link capabilities](link-capabilities.md).

## Storage and databases

- [Storage developer guide](storage/README.md),
  [storage terminology](storage/liuxin_terminology_and_conceptual_model.md),
  [assets, files, and replicas](storage/assets_files_and_replicas.md),
  [storage API](storage/storage_api.md), [storage design patterns](storage/design-patterns.md),
  and [component status](storage/storage_component_status.md).
- [Historical storage overview](<06 - Storage.md>) (deprecated design material),
  [cache backends](<08 - Storage Cache Backends.md>),
  [cache benchmark](storage/storage_cache_benchmark.md), and
  [mixed-ingest operations](storage/mixed_ingest_operations.md).
- [Live storage CI](storage/live_storage_ci.md) and
  [archive recoverability](<Archive Recoverability, Redundancy, and Self-Describing Packs.md>).
- [Database field guide](LiuXin_database_field_guide.md),
  [schema overview](<03 - Database Schema.md>), [backend strategy](database/liuxin_database_backend_strategy.md),
  and [PostgreSQL runbook](postgresql-backend.md).
- [Connection ownership](<database/02 - Connection ownership and stale connections.md>),
  [TEMP trigger cleanup](<database/03 - TEMP triggers for custom-column cleanup.md>),
  and [asset/replica policy](<database/04 - Digital assets, replicas and storage policy.md>).
- [Test databases](<07 - Test Databases.md>), [data artifacts](data-artifacts.md),
  and [malformed-input fuzzing](malformed-input-fuzzing.md).

## File formats and Calibre compatibility

- [Format guide index](file-formats/README.md): per-format behavior and sign-off notes.
- [Conversion pipeline](<conversion_pipeline/00 - Conversion Pipeline.md>),
  [sign-off](conversion_pipeline/conversion_pipeline_signoff.md), and
  [remaining work](conversion_pipeline/conversion_pipeline_todo.md).
- [Format typing](file-formats-typing.md) and [Unicode conversion](file-format-unicode-conversion.md).
- [Calibre compatibility policy](<calibre_compat/00 - Calibre Compatibility Policy.md>),
  [compatibility overview](<05 - Calibre compat.md>), and [mapping notes](<mapping calibre to LiuXin.md>).

## Design background and planning

These documents provide context and proposals; check the current owner, tests,
and dated handoff before treating a sketch as a shipped contract.

- [Target architecture](target-architecture.md): planned target decisions and
  rationale, starting with lifecycle ownership; not yet implemented.
- [Top-level architecture review](top-level-architecture-review.md): dated findings
  on subsystem ownership, configuration and plugin naming.
- [Whole-repository architecture review](architecture-review-2026-09-26.md):
  dated 2026-09-26 findings on lifecycle, layering, duplicate ownership, safety
  invariants and legacy containment, with a prioritised action list.
- [Project motivation](<01 - Introduction.md>), [top-level module notes](top-level-modules-notes.md),
  [earlier separation model](seperation_of_concerns.md),
  [ideas](thoughts.md), and [cross-project TODOs](global_todo.md).
- [Schema draft](<database/00 - Schema Draft.md>),
  [category terminology](<database/01 - Categories and the Tag Browser Misnomer.md>),
  and [WEMI graph](database/WEMI_as_a_Graph_ASCII.md).
- [Schema notes](liuXin_schema_notes.md), [metadata tables](liuXin_metadata_tables_notes.md),
  [triggers](liuXin_triggers_notes.md), and [tightened triggers](liuXin_triggers_tightened_notes.md).
- [SQL design sketches](random_sql/): not the installed schema's source of truth;
  see [packaging](packaging.md) for production schema ownership.

## Maintaining these guides

Add new primary guides to the relevant section or an existing subsystem index.
Use links relative to the containing document; wrap destinations containing
spaces in angle brackets. Do not link to a developer's absolute checkout path.
Label unavailable local reports as historical outputs instead of publishing
broken links. Preserve useful design history without presenting it as current
verification.

The [documentation link contracts](../tests/scripts/test_developer_documentation_links.py)
check this index, reviewed close-out guides, and portable link destinations
throughout developer documentation. They do not validate every historical
relative link, heading anchor, or external website.
