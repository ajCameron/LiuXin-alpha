# Calibre compatibility

Updated **2026-10-29** for the implemented Expression conversion policy.

Compatibility includes source-library inspection, metadata adapters, generated
SQL views and selected web routes. Each boundary has its own owner and IDs.
The new Catalog compatibility adapter uses an explicit Expression reference;
it does not reinterpret existing SQL, source or web IDs.

## Current behaviour

- [Expression conversion policy](expression-conversion-policy.md): versioned
  field owners, validated ray conversion, atomic writes and migrated cache hooks.
- [Compatibility overview](<../05 - Calibre compat.md>): implemented adapters,
  resource ownership, source-reading example, diagnostics and relevant checks.
- [Mapping guide](<mapping calibre to LiuXin.md>): source IDs, SQL ray IDs and web
  Work IDs, including a novel and introduction sharing one owned volume.

## Proposed canonical import

- [Import-policy draft](<00 - Calibre Compatibility Policy.md>): proposed
  Manifestation anchor, source identity preservation, write ownership and work
  still required. Its durable compatibility records and canonical importer do
  not exist yet. The implemented Catalog projection is separate from that import
  workflow. Current scan/job APIs classify payloads without executing an import.

Use the [format guides](../file_formats/README.md) for format implementations,
[plugin composition](../plugins/plugin-composition.md) for provider ownership,
and [surface startup](../surfaces/read-only-surface-appliance-startup.md) for serving commands.
[Next tasks](next_tasks.md) records proposed changes, dependencies and completion
checks and separates the landed projection from proposed source import work.

Return to the [developer index](../README.md).
