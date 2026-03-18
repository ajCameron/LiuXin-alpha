# Remaining Rewrite Deferral

Date: 2026-03-16

## Decision

Do not force the remaining `rewrite: 5` rows through fake compatibility work.

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

## Practical Effect

- No further legacy-test rewrite work should happen by default.
- Future migration work should focus on:
  - `salvage_existing`
  - explicitly covered/retired rows
  - or genuinely new alpha-native tests

## Resume Conditions

Resume only if one of these becomes true:
1. a real replacement runtime for the old folder-store behavior exists
2. XML-RPC or equivalent legacy remote-compat becomes a product goal
