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
3. Add multi-parent graph tests proving selected identities follow primary links
   while all relation links and link ids survive.
4. Add lazy/eager projection parity tests, including unloaded projection errors
   and explicit load/force-hydrate behaviour.
5. Add writer append/replace/idempotence tests with unicode and failure-report
   coverage.
6. Build a relation-container torture matrix, starting with expression metadata,
   identifiers, agents, and titles.

## Next Step

Move to the multi-parent graph tests. They should prove that selected identities
follow primary links while non-primary graph links and link ids remain preserved
in the WEMI container and sidecar mapping.

## Validation

- `.venv/bin/python -m pytest tests/metadata/containers/test_wemi_conversion_contracts.py -q`
- `.venv/bin/python -m pytest tests/metadata/containers/test_wemi_conversion_contracts.py tests/metadata/containers/test_liuxin_wemi_metadata.py tests/metadata/test_opf_tools.py -q`
- `.venv/bin/python -m pytest tests/metadata/containers/test_metadata_real_backend_parity.py -q`
