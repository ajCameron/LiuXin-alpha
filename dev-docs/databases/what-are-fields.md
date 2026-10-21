# Fields, columns and projections

Reviewed **2026-10-20**.

A physical **column** belongs to a database table. A **field** is the value an
API or user interface exposes: it may be one column, a relationship, an ordered
set, or a calculated projection. A title field, an author list and a format
summary therefore need different read and write contracts.

[Column metadata](column-metadata.md) declares policies for physical columns.
[Catalog field metadata](../../src/LiuXin_alpha/catalog/field_metadata.py)
describes inherited display/search fields and custom fields. These records are
not interchangeable, and declaring a validator or formatter does not execute it.

Catalog owns persistent bibliographic reads and semantic writes. Metadata
containers hold values; a field assignment on a container does not persist it.
Use the [Catalog API](../catalog/catalog-api-usage.md) and
[write contract](../catalog/catalog-write-validation-contract.md) to select the
operation, then the [schema overview](<../03 - Database Schema.md>) to understand
its physical rows and links.

For example, a Work's title can be a scalar column, its authors are typed
Agent–Work links, and its available formats are derived from reachable payloads.
Editing a display string cannot safely stand in for all three mutations.

See [next tasks](next_tasks.md) for remaining database contracts.
