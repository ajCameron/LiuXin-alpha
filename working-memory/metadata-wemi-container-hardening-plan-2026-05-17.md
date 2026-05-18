# Metadata WEMI Container Hardening Plan - 2026-05-17

Branch: `metadata-wemi-multiparent-contracts`

PR: `#56` - https://github.com/ajCameron/LiuXin-alpha/pull/56

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
5. Writer append/replace/idempotence tests are in progress. Current coverage
   includes append/idempotence, replace link removal, identifier replacement,
   existing identifier primary repair, explicit target-row mappings,
   existing-term link-only appends, relation-link metadata passthrough, and
   dirty marking suppression.
   Latest slice also covers missing relation tables, missing identifier
   columns, unsupported relation pairs, failed link/unlink/delete/update
   operations, and unsafe relation/identifier text rejection while keeping valid
   unicode writable.
6. Build a relation-container torture matrix, starting with expression metadata,
   identifiers, agents, and titles.
   First slice is implemented in
   `tests/metadata/containers/test_relation_container_contracts.py` and covers
   expression metadata mapping/helpers, expression titles, expression
   identifiers, and expression agent credits.
   Second slice extends the same file to expression notes, labels, subjects,
   languages, series, ratings, resources, and dates.

## Latest Progress

- PR `#56` is open against `main` and includes three commits:
  - `5006a07` `Harden WEMI multi-parent spine selection`
  - `7dc2655` `Cover lazy eager WEMI projection parity`
  - `be4f59f` `Cover WEMI metadata writer edge contracts`
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
- Added writer contract tests for existing identifier primary repair,
  link-only appends to existing relation term rows, relation-link metadata
  passthrough, dirty suppression, and explicit target-row mappings. Fixed the
  identifier primary repair report so the identifier value remains available
  and the changed column value is reported as `new_value`.
- Focused validation passed:
  `.venv/bin/python -m pytest tests/metadata/containers/test_item_metadata_hydrator.py tests/metadata/containers/test_hydrator_edge_cases.py tests/metadata/containers/test_liuxin_wemi_metadata.py tests/metadata/containers/test_wemi_conversion_contracts.py tests/metadata/containers/test_metadata_real_backend_parity.py -q`
  (`80 passed`).
- Projection parity validation passed:
  `.venv/bin/python -m pytest tests/metadata/containers/test_item_metadata_hydrator.py tests/metadata/containers/test_metadata_projection_views.py tests/metadata/containers/test_liuxin_wemi_metadata.py tests/metadata/containers/test_wemi_conversion_contracts.py tests/metadata/containers/test_metadata_real_backend_parity.py tests/metadata/test_metadata_top_level_facade.py -q`
  (`59 passed`).
- Writer slice validation passed:
  `.venv/bin/python -m pytest tests/metadata/containers/test_item_metadata_hydrator.py tests/metadata/containers/test_hydrator_edge_cases.py tests/metadata/containers/test_liuxin_wemi_metadata.py tests/metadata/containers/test_wemi_conversion_contracts.py tests/metadata/containers/test_metadata_real_backend_parity.py tests/metadata/test_metadata_top_level_facade.py -q`
  (`93 passed`).
- Writer failure-contract slice added:
  - missing relation-table and identifier-column skip reports;
  - unsupported relation-pair skip reports;
  - failed link, unlink, delete, and identifier-primary update errors;
  - valid unicode relation/identifier writes;
  - rejection of unsafe C0 control characters and surrogate code points before
    relation text or identifier values are persisted.
- Writer failure-contract validation passed:
  `.venv/bin/python -m pytest tests/metadata/containers/test_item_metadata_hydrator.py -q`
  (`32 passed`).
- Widened metadata validation passed:
  `.venv/bin/python -m pytest tests/metadata/containers/test_item_metadata_hydrator.py tests/metadata/containers/test_hydrator_edge_cases.py tests/metadata/containers/test_liuxin_wemi_metadata.py tests/metadata/containers/test_wemi_conversion_contracts.py tests/metadata/containers/test_metadata_real_backend_parity.py tests/metadata/test_metadata_top_level_facade.py -q`
  (`98 passed`).
- Relation-container first slice added:
  - expression metadata relation aliases, primary helper selection, link-id
    lookup/removal, and grouped related-target access;
  - expression/manifestation `to_mapping()` serialization of live `Row`
    relation targets as plain mappings rather than leaking row objects into
    sidecar payloads;
  - expression title container ordering, primary selection, write payloads,
    unicode text, and invalid shape/value checks;
  - expression identifier container scheme validation, primary selection,
    normalized values, write payloads, unicode values, and invalid
    shape/value checks;
  - expression agent-credit container role grouping, dynamic role text helpers,
    unicode credited names, write payloads, and invalid role/target/confidence
    checks.
- Fixed two bugs exposed by the relation-container tests:
  expression/manifestation metadata sidecar mapping no longer emits raw `Row`
  targets, and slotted dataclass agent-credit validation no longer fails on
  zero-argument `super()`.
- Relation-container validation passed:
  `.venv/bin/python -m pytest tests/metadata/containers/test_relation_container_contracts.py -q`
  (`5 passed`).
- Widened container validation passed:
  `.venv/bin/python -m pytest tests/metadata/containers/test_relation_container_contracts.py tests/metadata/containers/test_work_metadata_container.py tests/metadata/containers/test_item_metadata_container.py tests/metadata/containers/test_agent_profiles.py tests/metadata/containers/test_metadata_container_string_representations.py tests/metadata/containers/test_expression_metadata_hydrator.py tests/metadata/containers/test_manifestation_metadata_hydrator.py tests/metadata/containers/test_item_metadata_hydrator.py tests/metadata/containers/test_hydrator_edge_cases.py -q`
  (`91 passed`).
- Full metadata-container validation passed:
  `.venv/bin/python -m pytest tests/metadata/containers -q`
  (`223 passed, 1 warning`).
- Relation-container second slice added:
  - expression note ordering, primary selection, write payloads, dynamic
    description text helpers, unicode bodies/titles, blank-body rejection, and
    bad association range rejection;
  - expression label and subject ordering, primary selection, write payloads,
    dynamic text helpers, unicode values, wrong-target-kind rejection, and
    blank-text rejection;
  - expression language ordering, write payloads, dynamic text helpers,
    unicode language names, missing-language-value rejection, and duplicate
    primary rejection;
  - expression series, rating, resource, and date value contracts, including
    unicode display text, write payloads, dynamic text helpers, wrong target
    kinds, bad authority pairs, out-of-range ratings, blank URIs, empty dates,
    inverted date ranges, and duplicate-primary rejection.
- Relation-container validation passed after the second slice:
  `.venv/bin/python -m pytest tests/metadata/containers/test_relation_container_contracts.py -q`
  (`9 passed`).
- Widened container validation passed after the second slice:
  `.venv/bin/python -m pytest tests/metadata/containers/test_relation_container_contracts.py tests/metadata/containers/test_metadata_container_string_representations.py tests/metadata/containers/test_expression_metadata_hydrator.py tests/metadata/containers/test_item_metadata_hydrator.py tests/metadata/containers/test_hydrator_edge_cases.py -q`
  (`88 passed`).
- Full metadata-container validation passed after the second slice:
  `.venv/bin/python -m pytest tests/metadata/containers -q`
  (`227 passed, 1 warning`).

## Next Step

PR `#56` is open and this branch now includes the writer failure-contract slice
plus the first two relation-container torture slices. The next useful coverage
work is either a branch-coverage sweep over these relation families or the
remaining metadata black spots outside the WEMI relation containers.

## Validation

- `.venv/bin/python -m pytest tests/metadata/containers/test_wemi_conversion_contracts.py -q`
- `.venv/bin/python -m pytest tests/metadata/containers/test_wemi_conversion_contracts.py tests/metadata/containers/test_liuxin_wemi_metadata.py tests/metadata/test_opf_tools.py -q`
- `.venv/bin/python -m pytest tests/metadata/containers/test_metadata_real_backend_parity.py -q`
- `.venv/bin/python -m pytest tests/metadata/containers/test_item_metadata_hydrator.py -q`
- `.venv/bin/python -m pytest tests/metadata/containers/test_item_metadata_hydrator.py tests/metadata/containers/test_metadata_projection_views.py -q`
- `.venv/bin/python -m pytest tests/metadata/containers/test_item_metadata_hydrator.py tests/metadata/containers/test_metadata_projection_views.py tests/metadata/containers/test_liuxin_wemi_metadata.py tests/metadata/containers/test_wemi_conversion_contracts.py tests/metadata/containers/test_metadata_real_backend_parity.py tests/metadata/test_metadata_top_level_facade.py -q`
- `.venv/bin/python -m pytest tests/metadata/containers/test_item_metadata_hydrator.py tests/metadata/containers/test_hydrator_edge_cases.py tests/metadata/containers/test_liuxin_wemi_metadata.py tests/metadata/containers/test_wemi_conversion_contracts.py tests/metadata/containers/test_metadata_real_backend_parity.py tests/metadata/test_metadata_top_level_facade.py -q`
- `.venv/bin/python -m pytest tests/metadata/containers/test_item_metadata_hydrator.py tests/metadata/containers/test_hydrator_edge_cases.py tests/metadata/containers/test_liuxin_wemi_metadata.py tests/metadata/containers/test_wemi_conversion_contracts.py tests/metadata/containers/test_metadata_real_backend_parity.py -q`
- `.venv/bin/python -m pytest tests/metadata/containers/test_item_metadata_hydrator.py -q`
- `.venv/bin/python -m pytest tests/metadata/containers/test_relation_container_contracts.py -q`
- `.venv/bin/python -m pytest tests/metadata/containers/test_relation_container_contracts.py tests/metadata/containers/test_work_metadata_container.py tests/metadata/containers/test_item_metadata_container.py tests/metadata/containers/test_agent_profiles.py tests/metadata/containers/test_metadata_container_string_representations.py tests/metadata/containers/test_expression_metadata_hydrator.py tests/metadata/containers/test_manifestation_metadata_hydrator.py tests/metadata/containers/test_item_metadata_hydrator.py tests/metadata/containers/test_hydrator_edge_cases.py -q`
- `.venv/bin/python -m pytest tests/metadata/containers -q`
- `.venv/bin/python -m pytest tests/metadata/containers/test_relation_container_contracts.py tests/metadata/containers/test_metadata_container_string_representations.py tests/metadata/containers/test_expression_metadata_hydrator.py tests/metadata/containers/test_item_metadata_hydrator.py tests/metadata/containers/test_hydrator_edge_cases.py -q`
