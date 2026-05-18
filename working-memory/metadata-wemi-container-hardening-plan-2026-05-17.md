# Metadata WEMI Container Hardening Plan - 2026-05-17

Branch: `metadata-wemi-container-hardening-plan`

Durable doc: `docs/development/metadata-wemi-container-hardening.md`

## Context

The next metadata priority is the WEMI container/conversion stack. These
containers are likely to be heavily used, and conversion is a high-risk path
because OPF/Calibre-shaped metadata cannot preserve the full WEMI graph.

I audited:

- `src/LiuXin_alpha/metadata/__init__.py`
- `src/LiuXin_alpha/metadata/opf_tools.py`
- `src/LiuXin_alpha/metadata/containers/calibre_like_book_metadata/calibre_to_and_from_mixin.py`
- `src/LiuXin_alpha/metadata/containers/metadata_containers/liuxin_wemi_metadata.py`
- `src/LiuXin_alpha/metadata/containers/metadata_containers/lazy_liuxin_wemi_metadata.py`
- `src/LiuXin_alpha/metadata/containers/metadata_containers/liuxin_wemi_metadata_hydrator.py`
- `src/LiuXin_alpha/metadata/containers/metadata_containers/liuxin_wemi_lazy_metadata_hydrator.py`
- `src/LiuXin_alpha/metadata/containers/metadata_containers/liuxin_wemi_metadata_writer.py`
- `src/LiuXin_alpha/metadata/containers/metadata_containers/wemi_containers/projection_views.py`
- Existing container/conversion tests under `tests/metadata/containers` and
  `tests/metadata/test_opf_tools.py`.

## Findings

- The public surface is in decent shape, but the hard edge is conversion:
  `to_calibre()` previously mostly read legacy Calibre-like fields, while WEMI
  relation state was synced into those fields during hydration.
- `LiuXinWEMIMetadata` stores one identity bundle per WEMI level. That identity
  spine must be treated as selected/primary context only. The full graph must be
  preserved in relation links.
- The eager hydrator and lazy hydrator both select a first/primary parent when
  building the identity spine, while relation links can carry the rest of the
  graph. Tests should lock that down.
- Projection views already have strong coverage, but they are still a key
  contract boundary: they flatten values and must error on unloaded lazy data.
- The latest full coverage XML shows `opf_tools`, the WEMI writer, the WEMI
  container, lazy metadata, expression metadata, and relation value containers
  as the most useful next coverage targets.

## Proposed Work Order

1. Add conversion contract tests for database/cache/lazy hydration, WEMI to
   Calibre, WEMI to OPF, OPF back to WEMI, and sidecar mapping round trips.
   Initial slice is implemented in
   `tests/metadata/containers/test_wemi_conversion_contracts.py`.
2. Calibre/OPF conversion now reads a projection-synced defensive copy for
   supported flat fields. Direct WEMI edits are visible in conversion without
   mutating the source container, and OPF remains explicitly lossy for relation
   link ids/provenance/priority/graph shape.
3. Multi-parent graph tests now cover eager and lazy central hydrators. The
   identity spine follows structural relation links using the shared selector:
   explicit primary flag, lower priority, lower index, then original order.
   Direct foreign-key/source-row hints are fallback identity data only when no
   relation target is available. Non-primary graph links remain in relation
   links, and item-manifestation link ids survive sidecar mapping.
4. Lazy/eager projection parity tests are in progress. Hydrated lazy stack
   projections now have tests proving unloaded reads raise without materializing
   loaders, `load("tags")` unlocks only the requested projection, selected
   `force_hydrate(...)` unlocks legacy-backed projections, and full `load()`
   matches eager values across repeated access.
5. Add writer append/replace/idempotence tests with unicode and failure-report
   coverage.
6. Build a relation-container torture matrix, starting with expression metadata,
   identifiers, agents, and titles.

## Latest Progress

- Branch `metadata-wemi-multiparent-contracts` adds a fixture where the item row
  still hints at manifestation `10`, but primary graph links select
  manifestation `11`, expression `21`, and work `31`.
- `LiuXinWEMIMetadataHydrator` and `LazyLiuXinWEMIMetadataHydrator` now agree on
  that selected spine while preserving the full item-manifestation graph.
- Added a no-primary fixture proving relation priority selects manifestation
  `11`, expression `22`, and work `32` ahead of direct fallback ids.
- Added hydrated lazy/eager projection parity tests and fixed rating projection
  parity so a lazy legacy Calibre tag-viewer rating does not duplicate the WEMI
  graph rating when graph rating values are present.
- Focused validation passed:
  `.venv/bin/python -m pytest tests/metadata/containers/test_item_metadata_hydrator.py tests/metadata/containers/test_hydrator_edge_cases.py tests/metadata/containers/test_liuxin_wemi_metadata.py tests/metadata/containers/test_wemi_conversion_contracts.py tests/metadata/containers/test_metadata_real_backend_parity.py -q`
  (`80 passed`).
- Projection parity validation passed:
  `.venv/bin/python -m pytest tests/metadata/containers/test_item_metadata_hydrator.py tests/metadata/containers/test_metadata_projection_views.py tests/metadata/containers/test_liuxin_wemi_metadata.py tests/metadata/containers/test_wemi_conversion_contracts.py tests/metadata/containers/test_metadata_real_backend_parity.py tests/metadata/test_metadata_top_level_facade.py -q`
  (`59 passed`).

## Next Step

Widen validation for the projection parity slice, then move to writer
append/replace/idempotence tests if it stays green.

## Validation

- `.venv/bin/python -m pytest tests/metadata/containers/test_wemi_conversion_contracts.py -q`
- `.venv/bin/python -m pytest tests/metadata/containers/test_wemi_conversion_contracts.py tests/metadata/containers/test_liuxin_wemi_metadata.py tests/metadata/test_opf_tools.py -q`
- `.venv/bin/python -m pytest tests/metadata/containers/test_metadata_real_backend_parity.py -q`
- `.venv/bin/python -m pytest tests/metadata/containers/test_item_metadata_hydrator.py -q`
- `.venv/bin/python -m pytest tests/metadata/containers/test_item_metadata_hydrator.py tests/metadata/containers/test_metadata_projection_views.py -q`
- `.venv/bin/python -m pytest tests/metadata/containers/test_item_metadata_hydrator.py tests/metadata/containers/test_metadata_projection_views.py tests/metadata/containers/test_liuxin_wemi_metadata.py tests/metadata/containers/test_wemi_conversion_contracts.py tests/metadata/containers/test_metadata_real_backend_parity.py tests/metadata/test_metadata_top_level_facade.py -q`
- `.venv/bin/python -m pytest tests/metadata/containers/test_item_metadata_hydrator.py tests/metadata/containers/test_hydrator_edge_cases.py tests/metadata/containers/test_liuxin_wemi_metadata.py tests/metadata/containers/test_wemi_conversion_contracts.py tests/metadata/containers/test_metadata_real_backend_parity.py -q`
