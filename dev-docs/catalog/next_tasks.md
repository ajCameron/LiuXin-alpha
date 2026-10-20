# Next Catalog tasks

Date: **2026-10-20**. Proposed work; this list does not implement the changes.

Keep the shared repository/relationship owner. The [roadmap](catalog-roadmap.md) records implemented consolidation; this list makes the next slices reviewable.

## CAT01. Complete validator coverage by public route

**Next; architecture A08.** Inventory domain fields without pure value rules, then add one rule through repository preparation and the existing relationship owner. Include generic CRUD, schema writers, container writes and retained Row adapters.

Done when invalid input is rejected consistently, valid historical reads remain possible, and a failure late in a multi-attachment operation rolls back on supported transactional handles.

## CAT02. Unify default WEMI path selection

**Next; architecture A07.** Choose a policy for primary flags, priorities, ties and no-Item paths with the database projection owner. Preserve graph reachability separately from default selection.

Done when SQL primary rays, bundles and hydration defaults have explicit parity tests using bilingual editions, introductions and omnibuses; pagination/truncation remain visible.

## CAT03. Make transaction capability explicit

**First for callers requiring atomic writes.** Audit optional `macros.transaction` fallbacks in metadata attachment, merging and container workflows. Define whether unsupported handles are rejected or expose a deliberately weaker contract; do not imply atomicity from the Catalog name alone.

Done when missing transaction capability and late identifier/title failures have documented outcomes, and supported operations share the driver transaction contract from DB02.

## CAT04. Consolidate remaining read adapters with measurements

**Following.** Core categories share semantics, but Work/search/acquisition adapters and legacy field metadata still overlap. Measure server-side scans before changing the owner. Keep cache freshness and unavailable-versus-empty distinctions explicit.

Done when one selected adapter delegates to the agreed read owner, counts/results/failures agree across consumers, and the measured workload improves without hiding truncation.

## Validation and routing

Use [write parity](../../tests/catalog/test_write_boundary_parity.py), [normalization](../../tests/catalog/test_write_path_normalization.py), [legacy import boundaries](../../tests/catalog/test_legacy_mutation_boundary.py) and focused repository/retrieval tests. Coordinate cache state with [Caches](../caches/README.md), not a second persistence implementation.

Return to [Catalog](README.md).
