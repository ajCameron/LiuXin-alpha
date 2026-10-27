# Calibre compatibility

Reviewed **2026-10-20** against the working tree based on `fc5de7af`.

Compatibility includes source-library inspection, metadata adapters, generated
SQL views and selected web routes. Each boundary has its own owner and IDs.

## Current behaviour

- [Compatibility overview](<../05 - Calibre compat.md>): implemented adapters,
  resource ownership, source-reading example, diagnostics and relevant checks.
- [Mapping guide](<mapping calibre to LiuXin.md>): source IDs, SQL ray IDs and web
  Work IDs, including a novel and introduction sharing one owned volume.

## Proposed canonical import

- [Import-policy draft](<00 - Calibre Compatibility Policy.md>): proposed
  Manifestation anchor, source identity preservation, write ownership and work
  still required. Its compatibility-record schema and Catalog adapter do not
  exist yet. Current scan/job APIs classify payloads without executing that import.

Use the [format guides](../file_formats/README.md) for format implementations,
[plugin composition](../plugins/plugin-composition.md) for provider ownership,
and [surface startup](../surfaces/read-only-surface-appliance-startup.md) for serving commands.
[Next tasks](next_tasks.md) records proposed changes, dependencies and completion
checks. No runtime change is implied by this documentation review.

Return to the [developer index](../README.md).
