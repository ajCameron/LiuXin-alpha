# Metadata Interaction Surfaces Review - 2026-05-08

Branch: `metadata-interaction-surfaces-review`

Base: stacked on `metadata-interface-review-plan`, because the public
metadata/cache import-root cleanup in that branch is directly relevant to this
review.

## Scope

Reviewed the current user-facing and machine-facing interaction surfaces under
`src/LiuXin_alpha/surfaces`, with emphasis on where they meet metadata objects,
tags/labels, caches, and write-back paths.

Primary areas inspected:
- `surfaces/read_model`
- `surfaces/catalog`
- `surfaces/api_readonly`
- `surfaces/opds` and `surfaces/opds_readonly`
- `surfaces/web_readonly`
- `surfaces/web_calibre_readonly`
- `surfaces/web_readwrite`
- `surfaces/terminal` commands for show/on/off/new_tag/new_series-style flows

## Current Signal

Command attempted:

```bash
.venv/bin/python -m pytest -q tests/surfaces
```

Result:
- The run reached visible progress around `29%` with only passing dots and two
  skips shown.
- After a long quiet interval, the PTY still reported the command as running
  even though no matching Python/pytest process was visible from the workspace.
- The stale session was closed with `pkill -f ".venv/bin/python -m pytest -q tests/surfaces"`.

Treat this as an incomplete smoke signal, not a passing surface-suite result.

Follow-up focused validation after the first implementation slice:

```bash
.venv/bin/python -m pytest -q tests/surfaces/test_surface_package_api.py
.venv/bin/python -m compileall -q src/LiuXin_alpha/surfaces
.venv/bin/python -m pytest -q tests/surfaces/test_read_model_api.py tests/surfaces/test_catalog_api.py tests/surfaces/test_images_api.py tests/surfaces/test_acquisition_api.py tests/surfaces/test_opds_api.py tests/surfaces/test_opds_readonly.py tests/surfaces/test_api_readonly.py
.venv/bin/python -m pytest -q tests/surfaces/test_web_readonly.py tests/surfaces/test_web_calibre_readonly.py tests/surfaces/test_web_readwrite.py
```

Results:
- surface API import contract: `3 passed`
- compileall over `src/LiuXin_alpha/surfaces`: passed
- affected read/API/OPDS/acquisition/image slice: `19 passed`
- web read-only/Calibre/read-write outer slice: `30 passed`

Follow-up focused validation after the second implementation slice:

```bash
.venv/bin/python -m pytest -q tests/surfaces/test_metadata_facets.py tests/surfaces/test_read_model_api.py tests/surfaces/test_text_browser.py -k "tag or tags or table_token or show_tags or new_tag or on_tag or off_tag"
.venv/bin/python -m compileall -q src/LiuXin_alpha/surfaces
.venv/bin/python -m pytest -q tests/surfaces/test_read_model_api.py tests/surfaces/test_catalog_api.py tests/surfaces/test_api_readonly.py tests/surfaces/test_web_readonly.py tests/surfaces/test_web_calibre_readonly.py tests/surfaces/test_opds_readonly.py
.venv/bin/python -m pytest -q tests/surfaces/test_surface_package_api.py tests/surfaces/test_metadata_facets.py
```

Results:
- focused metadata-facet/read-model/terminal tag slice: `32 passed, 158 deselected`
- compileall over `src/LiuXin_alpha/surfaces`: passed
- read/API/web/OPDS consumers: `29 passed`
- surface package/facet import tests: `16 passed`

Follow-up focused validation after the third implementation slice:

```bash
python3 -m pytest tests/surfaces/test_read_model_metadata_parity.py
python3 -m pytest tests/surfaces/test_read_model_api.py tests/surfaces/test_metadata_facets.py tests/surfaces/test_read_model_metadata_parity.py
```

Results:
- metadata-object parity tests across configured DB drivers: `6 passed`
- read-model/facet/parity slice: `27 passed`

## Progress This Branch

Completed:
- Added `LiuXin_alpha.surfaces.api` as a shared public contract root for
  surface host protocols, response/file-target protocols, and presentation DTO
  aliases.
- Added `api` to the lazy top-level `LiuXin_alpha.surfaces` package surface.
- Replaced duplicated host Protocol definitions in `read_model`, `catalog`,
  `images`, `acquisition`, and `opds` with imports from the shared contract
  root.
- Added surface package tests proving the public contract names exist and the
  previous per-surface import paths still re-export the same protocol objects.
- Added `surfaces.metadata_facets` as the shared tag/label resolver helper for
  interaction surfaces.
- Replaced duplicated tag-vs-label table selection in the read model, terminal
  table resolution, `show tags`, `on tag`, and `add tag` with the shared helper.
- Added focused tests for tag-table preference, tag/label token resolution,
  normalized search-column selection, tag row payloads, and legacy row text
  precedence.
- Added read-model metadata-object parity tests for a complete WEMI fixture,
  checking the shared title/id/tag/series/file projection against
  `WorkMetadata` and item-centered `LiuXinWEMIMetadata`.
- Pinned the transitional fallback where the read-model `tags` projection comes
  from WEMI labels when the real `tags` table exists but is empty.

## Findings

1. The shared browse/API/OPDS read path is still row/dict based.

   `ReadModelBackend` builds Calibre-shaped metadata dictionaries directly from
   database rows and related-row scans. `ApiReadOnlyApplication`,
   `CalibreCatalogBackend`, `web_calibre_readonly`, and OPDS all consume that
   dictionary shape. None of these paths currently call
   `metadata_from_database`, `LazyLiuXinWEMIMetadata`, cache-backed metadata
   read sources, or the metadata object APIs.

2. Cache integration is absent from the interaction surfaces.

   Category counts, category rows, work metadata payloads, OPDS feed entries,
   and related-row lookups all go straight through the live database/host
   helpers. That keeps the surfaces simple, but it means the performance work
   on cache-backed metadata reads is not yet available to web/API/OPDS users.

3. Tags and labels are still transitional in several places.

   `ReadModelBackend` chooses `tags` if the table exists and has rows,
   otherwise falls back to `labels`. The terminal commands have their own
   independent tag/label resolution logic, and `web_readwrite` still exposes
   both descriptive tags and operational labels as separate work-link managers.
   The split is sensible, but the resolution rules should live in one shared
   helper before more surfaces start depending on them.

4. Surface host contracts are local and duplicated.

   `ReadModelHostApi`, `CalibreCatalogHostApi`, `ImageHostApi`,
   `AcquisitionHostApi`, and `OpdsHostApi` are useful Protocols, but they are
   spread across individual modules rather than exported as one deliberate
   surface contract. The actual host methods mostly live on
   `ReadOnlyWebApplication` as private helpers.

5. Write surfaces bypass the metadata writer/report layer.

   `web_readwrite` and terminal mutation commands create rows, interlink rows,
   sync rows, and delete links directly through the database. That means there
   is no shared metadata write report, no common validation surface, and no
   cache freshness behavior after metadata changes.

6. The interaction model is work-centered while WEMI hydration is item-centered.

   Most current surface routes treat a work row as the book/catalog unit.
   Metadata hydration can produce work/expression/manifestation/item objects and
   WEMI containers, but the first-class public hydrator path is still oriented
   around item/source rows. Before swapping surfaces to metadata objects, we
   need an explicit work-centered projection strategy.

7. Calibre compatibility payloads are presentation DTOs and should stay that
   way.

   The Calibre-like fields (`authors`, `tags`, `series`, `formats`,
   `format_metadata`, `uuid`, `thumbnail`, `cover`) are useful for API/OPDS/web
   compatibility. The issue is not their existence; it is that the adapter from
   real metadata objects to those payloads is implicit and scattered.

## Action Plan

1. Pin the current surface import roots and route contracts with lightweight
   tests.

   Keep `LiuXin_alpha.surfaces` lazy, but add explicit package/root tests for
   the real public entry points so later API movement is deliberate.

2. Define a shared surface contract module.

   Add a `surfaces.api` or tightened `surfaces.read_model.api` contract for the
   shared read DTOs and host requirements: work metadata payloads, file payloads,
   category items, related-entity summaries, and surface host protocols.

3. Extract shared metadata/category/tag helpers.

   Move tag-vs-label resolution, category normalization, primary display
   extraction, and work-centered related-row selection behind a common helper
   used by read model, terminal commands, and write surfaces.

4. Add metadata-object parity tests for the read model. Completed in this
   branch.

   For a fixture database, compare surface payload values for title, authors,
   tags, labels, series, formats, and identifiers against the relevant
   LiuXin/WEMI metadata objects. This should guard the behavior before replacing
   internals.

5. Add optional cache/read-source injection to the read model.

   Keep direct database access as the default, but allow surface apps to build
   from a metadata read source or loaded storage cache. Start with reads only;
   make cache freshness on writes explicit before using it in write surfaces.

6. Align terminal and web tag behavior.

   Use the shared resolver for `show tags`, `on tag`, `off tag`, `new tag`, API
   tag categories, OPDS tag categories, and web read/write tag pages. Reserve
   labels for operational labels, while keeping compatibility fallbacks for old
   databases.

7. Wire write flows through metadata write reports where appropriate.

   Leave row-level generic editing alone where it is truly generic. For
   metadata-specific edits, route through the metadata writer/report layer so
   web and terminal users get the same validation, changed-row reporting, and
   cache invalidation/reload story.

8. Expose OPF and pretty metadata flows after the object bridge is in place.

   Once surface payloads can reliably derive from metadata objects, add terminal
   and API affordances for pretty printing, OPF export/import, and round-trip
   diagnostics without duplicating formatting logic.

## Suggested PR Order

1. Review note plus current surface import/root tests.
2. Shared surface DTO/protocol contract extraction with no behavior change.
3. Shared tag/label/category resolver and terminal/web/API alignment.
4. Metadata-object parity tests for the read model.
5. Optional cache-backed read path for surface reads.
6. Metadata write/report integration for metadata-specific surface writes.
