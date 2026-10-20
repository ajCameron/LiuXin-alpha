# Next Catalog tasks

Date: **2026-10-20**. CAT02 is implemented for PR review; other entries remain proposed work.

Keep the shared repository/relationship owner. The [roadmap](catalog-roadmap.md) records implemented consolidation; this list makes the next slices reviewable.

## CAT01. Complete validator coverage by public route

**Next; architecture A08.** Inventory domain fields without pure value rules, then add one rule through repository preparation and the existing relationship owner. Include generic CRUD, schema writers, container writes and retained Row adapters.

Done when invalid input is rejected consistently, valid historical reads remain possible, and a failure late in a multi-attachment operation rolls back on supported transactional handles.

## CAT02. Unify default WEMI path selection

**Implemented for PR review, 2026-10-20; architecture A07.** Catalog owns the
[preferred-path contract](catalog-wemi-selection.md). Bundles match SQL's complete
ray ranking and retain the fullest partial path when no complete one exists.
Item availability does not rank editions; full relationship lists retain their order.

Regression fixtures cover bilingual editions, introductions, omnibuses, ties and
unlinking. Hydration retains all related Works; its relation-local primary accessor
has an explicit distinct contract and a contrasting-output test. It is not a
complete-ray selector. Pagination/truncation contracts are unchanged.

## CAT03. Make transaction capability explicit

**First for callers requiring atomic writes.** Audit optional `macros.transaction` fallbacks in metadata attachment, merging and container workflows. Define whether unsupported handles are rejected or expose a deliberately weaker contract; do not imply atomicity from the Catalog name alone.

Done when missing transaction capability and late identifier/title failures have documented outcomes, and supported operations share the driver transaction contract from DB02.

## CAT04. Consolidate remaining read adapters with measurements

**Following.** Core categories share semantics, but Work/search/acquisition adapters and legacy field metadata still overlap. Measure server-side scans before changing the owner. Keep cache freshness and unavailable-versus-empty distinctions explicit.

Done when one selected adapter delegates to the agreed read owner, counts/results/failures agree across consumers, and the measured workload improves without hiding truncation.

## Validation and routing

Use [write parity](../../tests/catalog/test_write_boundary_parity.py), [normalization](../../tests/catalog/test_write_path_normalization.py), [legacy import boundaries](../../tests/catalog/test_legacy_mutation_boundary.py) and focused repository/retrieval tests. Coordinate cache state with [Caches](../caches/README.md), not a second persistence implementation.

Return to [Catalog](README.md).
