# Standalone OPDS and catalogue fixture docstrings — 2026-09-09

Verification completed on 2026-09-10 in Europe/London (2026-09-09 UTC).

Continuation of the active [whole-project descriptive reST goal](project-docstrings-2026-09-08.md),
following the [shared compatibility backends](project-docstrings-compatibility-surfaces-2026-09-09.md).
The intervening PR-status response did not advance documentation; this continuation
revalidated the worktree and resumed the next available source-review work. All
eight edited files were clean before this batch and were read completely.

Work stays local on `codex/project-docstrings`, based on `edf6bf05`. No commit,
push, submodule publication, or PR mutation is part of this documentation batch.
The maintainability PR remains separate from the unfinished documentation goal.

## Reviewed scope and contracts

- [opds_readonly/app.py](../src/LiuXin_alpha/surfaces/opds_readonly/app.py): fully
  document the frozen configuration, application constructor, routing, response
  builders, legacy acquisition hooks, limited feed relationship projection, parser,
  and server runner. The application borrows the generic web base, not the Calibre
  HTML UI. Database compatibility-session ownership differs from a borrowed Core
  client. Catalogue composition shares the host's read model and image adapter.
- Route documentation covers method rejection, dot-segment normalization before
  component decoding, dropped blank query values, ignored extra path components,
  and the legacy acquisition route's minimum component count. HEAD follows GET
  without removing the body. Backend exceptions are not converted into successful
  empty results. Icons always return the built-in PNG without size processing.
- The current compatibility acquisition adapter reads directly through Core.
  Retained file/image host hooks do not imply that adapter calls them. In
  particular, `enable_file_downloads=False`/`--no-file-downloads` is not enforced
  on this direct acquisition path and must not be described as authorization.
  Generic local-file hooks add no path confinement; filename handling removes
  quotes, not every possible header control character. No policy was changed.
- Category-item lookup recognizes exact authors/tags/series only; author results
  use the first nonempty preferred table rather than a union. Feed metadata reads
  the limited expression/file/selected-tag/series relationship groups and bypasses
  catalogue category-URL augmentation. Underlying query failures remain visible.
- CLI docs distinguish direct config's 50-item page default from the parser's 25.
  Main clamps page sizes to one and grouping threshold to zero, owns Core/server
  context lifetimes, and prints the requested URL before binding. Port zero is
  printed as zero, not the assigned port. It does not catch interrupts or startup
  errors and returns zero only after normal server/context completion.
- [opds_readonly/__init__.py](../src/LiuXin_alpha/surfaces/opds_readonly/__init__.py)
  and [__main__.py](../src/LiuXin_alpha/surfaces/opds_readonly/__main__.py): explain
  package exports, import effects, the guarded runner, and exit propagation.
- [scripts/run_opds_readonly.py](../scripts/run_opds_readonly.py): document platform
  interpreter-path construction, POSIX diagnostic quoting versus list execution,
  required explicit source selection, option forwarding, and child lifecycle.
  The wrapper checks interpreter existence, not executability; relative database
  paths use the child's repository-root cwd. PYTHONPATH is copied/prepended without
  mutating the parent's environment. The child has no wrapper timeout and its
  nonzero return code is preserved. No environment or dependency installation ran.
- [test_catalog_api.py](../tests/surfaces/test_catalog_api.py): document all fixture
  helpers and the three database regressions. They cover linked category counts,
  metadata/search/token projections, WEMI file/image discovery, Core byte reads,
  aliases, and placeholder SVG. The image is a short PNG-like prefix and the ebook
  is opaque bytes; neither is a media-validity test. Fixture helpers register
  metadata, while only the asset helpers stat existing files.
- [test_opds_readonly.py](../tests/surfaces/test_opds_readonly.py): document the WSGI
  collector and nested callback, all fixture insertion helpers, inheritance/CLI
  checks, cache-snapshot behavior, routing, and two acquisition URL forms. Header
  duplicates collapse to dict entries; exc_info is ignored and no write callable
  is supplied. Response iterables close even when joining fails. An optional item
  ID is omitted when None, rather than inserting an explicit null.
- [test_readonly_surface_cli_help.py](../tests/surfaces/test_readonly_surface_cli_help.py):
  document substring-based help contracts and four-surface parser/Python/Bash
  invocation coverage. Help exits before virtualenv validation or server startup;
  shell wrappers may use a different interpreter and skip when Bash is unavailable.
- [tests/support/_surface_storage_tables.py](../tests/support/_surface_storage_tables.py):
  document the SQLite-specific fixture schema, three helpers, and literal-table
  ownership. Existing names are not schema-validated or migrated. The raw helper
  neither commits nor rolls back; the Database wrapper commits the connection's
  pending transaction and refreshes six table inventories only after creation.
  Partial DDL/refresh effects can remain on failure. In-memory doctests cover
  creation, idempotent name detection, and optional image-table addition.

Batch: **8 modules, 2 classes, 71 functions = 81 declarations**. Cumulative
reviewed Python scope: **195 modules, 248 classes, 2,064 functions = 2,507 declarations**,
plus the separately reviewed native C and runtime-created vacuum adapter.

## Verification

Overlapping test selections must not be summed into a distinct-test total.

- Initial standalone-OPDS source plus catalogue/OPDS tests and doctests:
  **23 passed, 47 explicitly skipped integration examples** in 80.94 seconds.
- Strict structural audit and normalizer --check pass across all **195 reviewed
  Python files**, including the separately tracked data-submodule generator.
- All **eight edited existing Python files** retain identical executable ASTs to
  HEAD after genuine leading docstrings are removed. No runtime-doc exception is
  needed in this batch. Runtime CLI help literals, SQL definitions, and assertions
  were not edited.
- No new Ruff findings relative to HEAD. Existing findings: catalogue tests
  I001/two UP032; OPDS tests I001/two UP032; help tests I001/two UP022; fixture helper
  I001; OPDS application I001/five UP045/two UP032. The launcher and both package
  bootstrap modules are clean. No unrelated import, typing, or style repair ran.
- Full `bash scripts/run_type_checks.sh`: **passed** with unchanged scopes of
  159 formatter files, 456 annotated modules, 221 protected dependencies, 188
  strict-mypy files, and 37 invalid examples rejected by each checker. Selected
  lint, complexity, dependency, and production type checks remain green.
- Fresh integrated source/test/doctest, import-boundary, migration/public-doc,
  surface read-error, and ownership selection: **304 passed, 103 explicitly
  skipped examples, 5 failed** in 220.16 seconds. All failures are the previously
  recorded physical-line ownership conflicts: Core services, Core program facade,
  browser components, windowed components, and the windowed root. No new failure
  appeared. This is not a green overall pytest result.
- Isolated checks with the existing acquisition Core double confirm actual GET
  and HEAD format requests both return bytes even when the host download flag is
  false. Mocked runner checks confirm numeric clamping, Core feature/cache flags,
  requested-port-zero output, normal return, and both context exits. A mocked
  launcher child confirms option forwarding, repo cwd/copied PYTHONPATH, unchanged
  parent environment, and nonzero exit propagation. These checks bind no socket,
  start no server, and launch no real child server; they are not live-I/O proof.
- Final public-documentation and developer-link regression selection:
  **23 passed** in 34.76 seconds. Root and data-submodule diffs are whitespace-clean,
  and the staging area is empty. All **180 local link destinations** across the
  index and twelve project-docstring working-memory notes resolve.

The previous batch's handle **9174** is authoritatively missing. Its final output
was not observed, so its completion is not claimed as a pass. The fresh combined
selection includes that batch's relevant suites plus this batch's modules,
launcher/help/fixture doctests, and surface read-error contracts. The five known
physical-line ownership conflicts remain unmodified; no guard logic, limits, or
default quality scopes were changed. All replacement/verification handles have
completed; none remains pending. No APSW/live PostgreSQL, displayed UI,
installed wheel, real HTTP listener, or remote-CI success is claimed here.

## Whole-project inventory and next work

The current full audit covers **2,730 modules, 4,400 classes, 35,119 functions**,
with no parse failures. Missing/blank docs remain on **1,108 modules, 2,026 classes,
23,001 functions = 26,135 declarations**. Other overlapping findings: 4,265
delimiter-layout, 10,535 missing-example, 3,054 parameter-field, 3,995 return-field,
4,139 empty-parameter, 5,575 empty-return, and 28 missing-summary findings.

Reports remain `/tmp/liuxin-docstring-current-2026-09-09.json` and
`/tmp/liuxin-docstring-reviewed-2026-09-09.json`. The whole-project goal remains
active and unfinished. Next: the complete `surfaces/web_calibre_readonly` package
(app.py is 1,677 pre-migration lines, plus initializer and entrypoint), then the
remaining CLI/HTTP/web source and the rest of the source/scripts/examples/tests.
Do not repeat the completed shared backends, standalone OPDS package, launcher,
or eight files in this batch. Preserve the main ledger's configuration-exec
compatibility warning, runtime doc consumers, and data-submodule scope.
