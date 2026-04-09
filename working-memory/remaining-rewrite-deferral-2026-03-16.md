# Remaining Rewrite Deferral

Date: 2026-03-16

## Decision

Do not force the remaining `rewrite: 16` rows through fake compatibility work.

They are intentionally parked as follows:

### Deferred

- `core_xmlrpc_compat`
  - row count: `1`
  - reason: only worth doing if LiuXin-alpha makes an explicit XML-RPC or equivalent remote-introspection compatibility claim

### Blocked

- `folder_store_runtime`
  - row count: `4`
  - reason: these target the removed legacy folder-store runtime
  - do not port until a real replacement implementation seam exists

### Rewrite Boundary

- `db_property_secondary_uuid_cluster`
  - row count: `3`
- `db_property_identifier_cluster`
  - row count: `1`
- `db_property_compatibility_projection_cluster`
  - row count: `5`
- `db_property_rich_content_cluster`
  - row count: `2`

Reason:

- the old specialized builders behind those DB-property rows are no longer in the live provisioning path
- the narrow current compatibility guards are worth pinning, but they are not the old builder outputs

## Practical Effect

- no further legacy-test rewrite work should happen by default
- there is no remaining `salvage_existing` bucket
- future migration work should focus on:
  - explicitly scoped `rewrite` seams
  - explicitly covered/retired rows
  - or genuinely new alpha-native tests

## Resume Conditions

Resume only if one of these becomes true:
1. a real replacement runtime for the old folder-store behavior exists
2. XML-RPC or equivalent legacy remote-compat becomes a product goal
3. a real replacement builder/test seam is defined for one of the remaining DB-property rewrite families
