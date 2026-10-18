# Catalogue read ownership — 2026-10-18

Branch: `catalog-read-model-consolidation`, based on `ownership-consolidation`
commit `2d8bcaeb` (PR #120). This is the next focused ownership PR requested by
the user, including docstring and comment updates. Work remains in the isolated
`/tmp/liuxin-test-vm-telemetry` checkout; the main checkout's user edits are untouched.

## Behaviour and ownership

Core browse queries own category source selection, entity totals, category-entry
ordering and linked-work counts. The surface read model adapts those receipts
into CoreRow objects, display names and URLs instead of rebuilding entity lists.
CLI already uses these semantic queries; API, Calibre web and OPDS now consume
the same category decisions.

- Populated tags win catalogue-wide. When tags are empty, available labels supply
  the category. Tags-only catalogues retain an empty source; neither source means
  an empty category. Tags and labels are never summed or merged.
- Work tags and category-ID lookup use that same selection. A Work linked only to
  labels does not silently switch families when the catalogue selects tags.
  Colliding numeric IDs cannot redirect a tag query into the labels table.
- `browse.categories` accepts an optional category selector. Totals use native
  source counts or zero-limit structured cache queries, without materializing
  entity rows or following links. The full surface summary is one Core query.
- Entity pages use structured source reads. Failed or incomplete category reads
  remain errors; relationship reads use their known endpoint rather than the
  older heuristic that guesses a table and suppresses errors.
- Surface requests use transport pages capped at 1,000 records. Full lists and
  larger requested windows follow subsequent pages without silent truncation;
  a nonfinal empty page raises instead of looping forever.
- API/web/OPDS preserve Core's absent source instead of substituting `tags`.
  The unused populated-tags branch in the write-target helper is removed. Writes
  continue to prefer any existing tags table regardless of population.
- Core category labels recognize `label_text` and canonical Agent names.

This slice consolidates browse category semantics. Generic Work, file/image,
search and acquisition projections retain their existing adapters and policies.
Core still materializes and sorts entity listings on the server before paging;
this change bounds transport pages, not server-side catalogue scanning. Multiple
reads are not an atomic snapshot. Author categories still include the advertised
Agent families; this change does not redefine credit types or merge those tables.

## Validation

Serial runs use the existing telemetry plugin and immediate scratch cleanup.
Artifacts: main checkout `working-memory/test-results/catalog-reads-20261018`.
The first run caught a missing Core label-text display field and mocks targeting
the retired surface selection boundary. The initial parity probe also caught an
incorrect test assumption: relationship order is link order, not category label
order. The regression now checks membership and equality of actual Core/surface
link order without changing that policy.

Latest outcomes cover **331 distinct passing cases**:

- The main parity/read-model/facet run passed all **171** cases.
- The expanded adapter run passed **159**, with one stale WSGI test injection:
  AJAX now obtains its independent catalogue total from `browse.categories`,
  so injecting failure into the old row-query path no longer exercises that
  operation. The updated error-contract file passed all **62** cases, including
  new category-summary and category-page HTTP 500 checks. No production change
  was needed after the main run.
- Real SQLite and APSW fixtures cover tags-only, labels-only, both populated,
  empty-tag, both-empty and absent-source cases through direct and real loopback
  HTTP clients. Overlapping IDs and a label-only Work distinguish catalogue-wide
  source policy from per-Work fallback. The final absent-source case hides populated
  tables and checks that API/web/OPDS do not resurrect them.
- Category count, entity and relationship failures retain their structured codes
  over direct/HTTP transport; unsupported and incomplete cache queries remain
  unavailable. Existing cache-snapshot and surface acceptance tests pass.
- A larger requested window and a complete 2,000-entry listing use bounded pages;
  a retained model observes tag population changes instead of caching selection.

`validation-summary.json` records the latest outcome per case, replacing the
superseded error-contract parameters and the earlier empty absent-source fixture.
The three final runs peaked at **162.8 MiB** process-tree RSS, kept at least
**6.41 GiB** RAM available and **14.83 GiB** C: headroom, and observed no swap use
or OOM kills. All five task scratch directories were removed after their runs;
`cleanup.json` records them. Durable logs and older full-suite evidence remain.

This was targeted validation, not another full-suite run. The **11 known baseline
failures** from the previous handoff remain outside this PR's scope.

`scripts/run_type_checks.sh` passed separately after pytest: formatting,
annotation coverage, lint/complexity, import cycles, layer direction, import
footprint, basedpyright, mypy and internal typing contracts. `quality.log` retains
the full output. Scoped Pyflakes checks for every changed Python file and
`git diff --check` also pass.

Publication: prepared as a ready PR against `ownership-consolidation` (#120).
