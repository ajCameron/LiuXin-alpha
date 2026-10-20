# Historical SQL sketches

Reviewed **2026-10-20**, working tree based on `fc5de7af`.

These files preserve design work. They are not the installed schema and should
not be used as a current bootstrap or migration procedure.

- [FRBR/storage tables](liuXin_main_tables_frbr_storage.sql).
- [Metadata tables](liuXin_metadata_tables.sql).
- [Triggers](liuXin_triggers.sql).

The sketches use older direct file/store links and polymorphic metadata pointers.
Their trigger assertions must not be assumed to enforce the current Asset/Replica
model or runtime byte-operation policy. No application/bootstrap reference to this
folder was found in the inspected scripts/database code; production generation
uses the [packaged schema tree](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/).
The original SQL bodies are preserved, with explicit historical headers.

[Next tasks](next_tasks.md) describes how to recover useful design rationale
without maintaining a competing schema.

Use the [database guides](../databases/README.md) for persistence contracts and
[packaging](../hacking/packaging.md) for the production schema/resource owner.
Return to the [developer index](../README.md).
