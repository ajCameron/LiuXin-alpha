# Metadata containers

Reviewed **2026-10-20**. Value containers remain passive. Hydration belongs to Catalog; current API roots and the follow-up work are linked in [next tasks](next_tasks.md). Stage numbers and historical test totals below describe their original campaign.

Start with the [system guide](metadata_container_system_guide.md) and
[architecture](<00 - metadata_container_architecture.md>). Containers describe
values; database/cache reads and writes have separate Catalog owners.

## Value contracts

- [Boundaries](metadata_container_boundaries.md) and [family matrix](metadata_container_family_matrix.md).
- [Naming](metadata_container_naming_conventions.md) and [vocabularies](metadata_container_vocabularies.md).
- [Constraint alignment](metadata_container_db_constraint_alignment.md).
- [Dynamic convenience policy](metadata_container_dynamic_convenience_policy.md).
- [Test surface](metadata_container_test_surface.md).

## Retrieval and conversion

- [Facade workflows](metadata_facade_workflows.md): Catalog hydration versus pure
  metadata/OPF conversion.
- [DB source layer](metadata_db_source_layer.md): historical source-layer rationale
  with its current Catalog ownership identified.

Use the [Catalog write contract](../catalog/catalog-write-validation-contract.md)
for persistence, and the [metadata index](../metadata/README.md) for providers.
Return to the [developer index](../README.md).
