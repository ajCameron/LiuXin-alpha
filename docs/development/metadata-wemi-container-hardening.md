# Metadata WEMI Container Hardening Plan

Date: 2026-05-17

Current PR: `#56` (`metadata-wemi-multiparent-contracts`) against `main`.
This PR packages the first hardening batch after the conversion-contract PR:
multi-parent identity-spine selection, hydrated lazy/eager projection parity,
and WEMI metadata writer edge contracts.

This plan covers the next focused pass over the WEMI metadata containers and
conversion paths. The goal is not line coverage for its own sake. The goal is to
make graph fidelity, projection behaviour, conversion loss, and write-back
safety explicit before these containers become heavier production surfaces.

## Current Shape

The public workflow surface is `LiuXin_alpha.metadata`:

- `metadata_from_database(...)` hydrates WEMI metadata from a database or cache,
  optionally with lazy loading.
- `metadata_to_opf_bytes(...)`, `metadata_from_opf(...)`, and friends route OPF
  conversion through `metadata/opf_tools.py`.
- `LiuXinWEMIMetadata.as_calibre_metadata()` and `.calibre` route to
  `to_calibre()` on the Calibre-like base.

The item-centred WEMI object lives in
`metadata/containers/metadata_containers/liuxin_wemi_metadata.py`. It owns one
metadata bundle per WEMI level: work, expression, manifestation, and item. Each
bundle exposes identities plus relation links. The important invariant is that
the identity spine is the selected context for the item, not the whole graph.
Additional graph edges must remain in relation links.

Eager and lazy hydration both prefer explicit primary structural links when
selecting that identity spine. Direct foreign-key/source-row hints remain useful
fallbacks, but a primary item-to-manifestation, manifestation-to-expression, or
expression-to-work link is the authoritative selected traversal. When relation
links exist without an explicit primary flag, the shared relation selector uses
lower priority, then lower index, then original order. Direct foreign-key and
source-row hints are fallback identity data only when there is no relation
target to select. Non-primary parents stay available through relation links and
retain their link ids.

Projection views live in
`metadata/containers/metadata_containers/wemi_containers/projection_views.py`.
They deliberately flatten relation targets into read-only `values` and `text`
views. Lazy projections must not half-render partial state: eager projections
only read loaded data, and unloaded lazy dependencies raise
`UnloadedMetadataProjectionError`.
Hydrated lazy projections follow the same rule. Reading `md.values.*` or
`md.text.*` before the relevant lazy legacy fields and relation loaders are
materialized raises without invoking the loaders. Calling `md.load("tags")`
hydrates the projection dependencies for that relation only; calling `md.load()`
hydrates every pending lazy field and relation loader. `force_hydrate(...)`
remains a legacy-field materialization API and is suitable for selected
legacy-backed projections such as tags, labels, and identifiers.

Write-back lives in
`metadata/containers/metadata_containers/liuxin_wemi_metadata_writer.py`. It
currently handles selected legacy relation-backed fields and entity identifiers.
Append mode adds missing terms and links; replace mode treats requested fields
as authoritative for the target row.
Writer reports distinguish created rows, added links, updated rows, removed
links, skipped fields, and errors. Existing relation term rows can be linked
without being reported as new rows, relation-link metadata is carried into link
rows where the database supports those columns, and `mark_dirty=False` suppresses
dirty-record writes even when a link is added.
The writer now also reports missing relation tables, missing identifier columns,
unsupported relation pairs, failed link/unlink/delete/update operations, and
unsafe text inputs. Valid unicode remains writable, but C0 control characters
other than tab/newline/carriage-return and surrogate code points are rejected
before they can be persisted into relation text or entity identifier rows.

The latest full coverage XML in `working-memory/test-results` shows the main
remaining risk areas:

- `metadata/opf_tools.py`: 72.8% line, 46.0% branch.
- `liuxin_wemi_metadata_writer.py`: 75.8% line, 67.1% branch.
- `liuxin_wemi_metadata.py`: 82.6% line, 61.1% branch.
- `lazy_liuxin_wemi_metadata.py`: 80.6% line, 62.2% branch.
- `expression_metadata_container.py`: 60.5% line, 30.8% branch.
- Relation value containers such as agents, identifiers, titles, labels,
  subjects, notes, languages, series, ratings, resources, and dates are mostly
  in the 75-80% line coverage range.
- Projection views are already near complete by line coverage, but should stay
  in the invariant suite because they are a contract boundary.

## Core Invariants

The hardening pass should make these behaviours executable:

1. Graph fidelity is preserved. Multiple works, expressions, manifestations, or
   items may be linked. The single identity bundle for each level is only the
   selected primary context.
2. Primary selection is deterministic. If a primary link exists it wins. If no
   primary link exists, the fallback order must be stable and documented.
3. Relation links are authoritative for graph/provenance data. Projections are
   views over values only and must not be used as a graph source of truth.
4. Conversion is explicit about loss. OPF and Calibre-shaped metadata cannot
   preserve WEMI link ids, relation provenance, link priority, or structural
   graph shape. They must preserve every supported flat field we claim to
   convert.
5. Lazy and eager hydration agree once the same fields are loaded.
6. Calling `md.values.*` or `md.text.*` on unloaded lazy dependencies raises the
   specific projection error. Calling `md.load(...)` or `force_hydrate(...)`
   then exposes the same values as eager hydration.
7. `to_mapping()`, `from_mapping()`, `deepcopy_metadata()`, and sidecar mapping
   preserve identities, relation links, link ids, and legacy data within the
   WEMI sidecar contract.
8. Write-back is idempotent for no-op writes, reports all changes, respects
   append versus replace semantics, and does not silently corrupt text,
   identifiers, or relation links when given hostile unicode.

## Fixture Strategy

Build small deterministic fixtures rather than relying only on large artifacts.
The fixture set should include:

- A simple single-spine item for baseline parity.
- A multi-parent graph where an item has more than one manifestation link, a
  manifestation has more than one expression link, and an expression has more
  than one work link. Mark one link primary at each level.
- A no-primary multi-parent graph to lock down deterministic fallback order.
- Duplicate relation values across levels, with different link ids/provenance,
  to prove value projections dedupe while relation links do not collapse.
- Mixed identifier sources: legacy identifiers, item identifiers, and entity
  identifiers at more than one WEMI level.
- Unicode torture values in titles, agents, identifiers, series, tags, labels,
  subjects, notes, comments, and OPF output.
- Missing-table and missing-column variants for writer/report failure paths.

Prefer local test builders under `tests/metadata/containers` unless a fixture
needs to exercise the real database builder contract. Any randomness must be
seeded or avoided.

## Work Order

1. Add conversion contract tests first.
   Cover `metadata_from_database` kind/source/lazy combinations,
   `as_calibre_metadata()`, `to_calibre()`, OPF write/read round trips, and
   sidecar mapping round trips. These tests should make any currently implicit
   loss boundary visible.

2. Enforce the Calibre/OPF source of truth.
   Calibre/OPF conversion reads a projection-synced copy of the WEMI container
   for supported flat fields. Direct WEMI relation edits for tags, subjects,
   genres, labels, series, languages, identifiers, and title fallback are
   therefore visible in conversion without mutating the source container's
   legacy fields. OPF and Calibre-shaped metadata remain lossy graph formats:
   relation link ids, provenance, priority, and structural graph shape do not
   round-trip through them.

3. Multi-parent graph tests now cover eager and lazy central hydrators. Selected
   identities follow primary structural links, then relation priority fallback,
   even when direct foreign-key hints point elsewhere. Non-primary relation
   links remain present and link ids survive sidecar mapping.

4. Add lazy/eager projection parity tests.
   Exercise unloaded projection errors, `load(...)`, `force_hydrate(...)`,
   conversion materialization, cache read sources, and repeated access.
   Current hydrated-path tests prove unloaded stack projections raise without
   materializing loaders, single-field `load(...)` keeps unrelated fields
   guarded, selected `force_hydrate(...)` unlocks legacy-backed projections,
   and full lazy `load()` matches eager values on repeated access.

5. Add writer append/replace/idempotence tests.
   Cover target resolution, explicit `target_row`, primary relation-link
   metadata passed through to link rows, identifier primary marking, stale-link
   removal in replace mode, dirty marking, and clean report entries for skipped
   or failed writes.
   Current writer tests cover append/idempotence, replace link removal,
   identifier replacement, existing identifier primary repair, explicit
   target-row mappings, existing-term link-only appends, relation-link metadata
   passthrough, dirty marking suppression, missing schema skips, unsupported
   relation-pair skips, failed operation errors, and hostile text filtering.

6. Build a relation-container torture matrix.
   Prioritize expression metadata, identifiers, agents, titles, notes, labels,
   subjects, languages, series, ratings, resources, and dates. Cover
   `from_mapping`, `to_mapping`, relation-key validation, primary helpers,
   duplicate handling, invalid values, repr/string output, and unicode.

7. Re-run focused coverage after each slice.
   Use coverage to find missed branches, but do not add tests that only assert
   implementation details unless those details are part of the contract above.

## Acceptance Criteria

- Focused tests cover conversion, lazy/eager parity, multi-parent graph
  preservation, sidecar mapping, and writer safety.
- WEMI container and writer files move materially above their current coverage,
  with branch coverage improved on the failure paths that can corrupt metadata.
- Supported Calibre/OPF flat fields have explicit round-trip tests, including
  unicode and XML-hostile values.
- Known lossy boundaries are documented in tests and docs instead of being
  accidental.
- The identity-spine versus full-graph distinction is visible in both test names
  and docs.

## Current Landing State

PR `#56` includes three committed slices:

- Multi-parent WEMI spine selection. Eager and lazy hydrators select the identity
  spine from primary/priority structural relation links while preserving the full
  graph in relation links.
- Lazy/eager projection parity. Hydrated lazy projections raise before loading,
  single-field `load(...)` materializes only the requested projection
  dependencies, full `load()` matches eager projections, and rating projection
  parity avoids duplicate legacy Calibre tag-viewer ratings when graph ratings
  are present.
- Writer edge contracts. Tests cover explicit target rows, link-only appends to
  existing relation term rows, relation-link metadata passthrough, dirty marking
  suppression, identifier replacement, existing identifier primary repair,
  missing relation tables and identifier columns, unsupported relation pairs,
  link/unlink/delete/update failures, valid unicode writes, and unsafe
  relation/identifier text rejection.

The next useful pass is the relation-container torture matrix: expression
metadata, identifiers, agents, titles, notes, labels, subjects, languages,
series, ratings, resources, and dates.

## Non-Goals

- Do not add write-back through `values` or `text` projections in this pass.
- Do not redesign the WEMI schema unless a failing invariant proves the current
  shape cannot represent the required graph.
- Do not treat OPF or Calibre-shaped metadata as a complete WEMI sidecar format.
