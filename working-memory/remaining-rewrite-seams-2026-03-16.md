# Remaining Rewrite Seams

Date: 2026-03-16

## Summary

After closing the `legacy_support_harness` seam, the manifest rewrite set is down to `5` rows.

Current manifest totals:
- `covered`: `53`
- `rewrite`: `5`
- `salvage_existing`: `26`
- `retire`: `24`
- `integration_frozen`: `9`
- `vendor_frozen`: `7`

## Remaining Seams

### 1. `core_xmlrpc_compat`

Rows: `1`

- `src/LiuXin_tests/core/self_test.py`

Reason it stays `rewrite`:
- the old test is XML-RPC `system.listMethods()` compatibility, not just generic core health
- alpha now has a different core/runtime/proxy/HTTP model
- this should only move when there is an explicit compatibility claim to make

Recommendation:
- leave it alone for now
- revisit only if XML-RPC compatibility or equivalent remote introspection becomes a deliberate product goal

### 2. `folder_store_runtime`

Rows: `4`

- `src/LiuXin_tests/folder_stores/cover_cache/on_disk/utils_test.py`
- `src/LiuXin/folder_stores/drivers/on_disk/test_store.py`
- `src/LiuXin/folder_stores/drivers/on_disk_flat/test_store.py`
- `src/LiuXin/folder_stores/drivers/zip/test_store.py`

Reason it stays `rewrite`:
- this is the old folder-store runtime surface, not a utility-dependency problem
- parts of the old behavior are already retired
- cover-cache still lacks a live alpha seam

Recommendation:
- do not touch this until there is a concrete replacement implementation seam
- keep the focus on current storage backends, not fake compatibility

## What Closed

The old `legacy_support_harness` seam is no longer part of the rewrite backlog.

That closure is recorded in:
- [legacy-support-harness-closure-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/legacy-support-harness-closure-2026-03-16.md)

## Recommended Next Step

Do not invent a third seam.

The remaining rewrite work is now explicitly one of:
1. defer `core_xmlrpc_compat` until there is a deliberate compatibility goal
2. keep `folder_store_runtime` blocked until a real replacement implementation seam exists
