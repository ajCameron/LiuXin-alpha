# Read-only JSON API docstrings — 2026-09-10

Continuation of the active [whole-project descriptive reST goal](project-docstrings-2026-09-08.md),
following [generic read-only web](project-docstrings-generic-web-2026-09-10.md).
The previous goal turn made verified progress. This turn checked the clean target
paths and read the complete API application (461 pre-migration lines), both
bootstrap modules, launcher (113), main regression module (363), and adjacent
direct/RPC acceptance module (154). All six files are now source-reviewed and
documented; no executable implementation or test assertion changed.

Work remains local on `codex/project-docstrings`, based on `edf6bf05`. No commit,
push, submodule publication, or PR change accompanies this batch.

## Reviewed contracts

- [app.py](../src/LiuXin_alpha/surfaces/api_readonly/app.py): both classes and all
  24 functions now have descriptive contracts and examples. Configuration retains
  unvalidated inherited settings, while changing service title and default port.
  Construction delegates ownership/coercion to the generic host and binds its
  catalogue collaborator without starting a server. The Core-client annotation
  does not erase the inherited legacy-database compatibility path.
- Module/class/routing docs distinguish a read-oriented interface from access
  control and content isolation. The API does not mount generic HTML table routes;
  projected HTML/image hints are not guaranteed to resolve on this dispatcher.
  HEAD retains GET bodies, robots.txt returns text, and file delivery retains
  the inherited preview/redirect/error behavior. Author routes accept a table
  component without an author-only allowlist.
- Dispatch normalizes dot segments before decoding parts and drops blank query
  values. Works/authors/tags/series use prefix dispatch, and their handlers largely
  select by component count rather than rechecking prefix names. Consequently
  /api/works-extra reaches the collection handler. Unknown routes and unsupported
  methods receive JSON errors, while backend and serialization errors propagate.
  JSON output is Unicode UTF-8 with sorted keys and default nonfinite-float behavior.
- The index performs independent category/materialized counts and an optional
  files-table count, not an atomic snapshot. Confirmed table absence gives zero;
  explicit count unavailability is not suppressed as it is in generic HTML home.
  Paging copies filters, replaces limit/offset, always supplies a self query,
  floors only Previous's offset, and does not clamp oversized offsets or validate
  nonpositive direct-call limits. Route callers perform their own coercion.
- URL docs distinguish percent-quoted generic entity IDs from direct interpolation
  in work/file summaries. Missing IDs can become the literal None; URLs do not
  validate existence. Contributor URLs retain table, but tags and labels share a
  tag route without source-table identity. Unknown tables produce HTML fallback
  paths not handled by this API. Required projected keys and coercion failures
  remain visible rather than silently dropping malformed entries.
- Payload ownership is explicit: entity and file summaries shallow-copy their
  outer dicts; related and work-detail adapters mutate backend payloads in place.
  File-detail copies its outer mapping but mutates shared related entries. Search
  retains backend results/group_counts and inserts API links into those results.
  A malformed later entry can fail after earlier nested mutations. Work-detail
  augments credits/files/related, not its nested work metadata itself.
- Work collection sorting accepts normalized recent and otherwise chooses title.
  Work/file invalid IDs return 400 and missing rows 404. Category details instead
  map ordinary invalid IDs to 404; linked-work routes can return successful empty
  lists for missing/invalid entities through the shared backend. Category detail
  uses a numeric ID for lookup while preserving original spelling for linked-work
  discovery and URL encoding. Author routes pass the provided table through.
- Category collection entries retain raw metadata identity while quoting links;
  unexpected kinds in the item helper select series URLs rather than raising.
  Name-ascending category listing belongs to the shared backend. Linked works are
  sliced locally without sorting/deduplication. Search q-list precedence can
  suppress global_q for direct helper calls with a blank first q item; HTTP query
  parsing ordinarily removes those blank values first.
- Parser/runner docs distinguish option declaration from Core/cache composition.
  Page sizes are independently clamped in main, storage is enabled, maintenance
  disabled, and Core/server are context-managed. The printed /api URL precedes
  binding and retains the requested port zero. Unlike generic web, the API parser
  does not expose database-path display. Runtime failures and interrupts propagate.
- [__init__.py](../src/LiuXin_alpha/surfaces/api_readonly/__init__.py) and
  [__main__.py](../src/LiuXin_alpha/surfaces/api_readonly/__main__.py): package and
  entrypoint imports are inert; guarded main-name execution forwards the runner's
  return code through SystemExit. No production guard was changed.
- [run_api_readonly.py](../scripts/run_api_readonly.py): platform interpreter paths,
  existence-only validation, POSIX diagnostic quoting versus list execution,
  explicit database/endpoint selection, endpoint-only Core timeout, copied
  PYTHONPATH, repository-root cwd, inherited streams, no subprocess timeout, and
  preserved child failure codes. Application page-size flags are not exposed.
- [test_api_readonly.py](../tests/surfaces/test_api_readonly.py): all sixteen
  functions, including the nested WSGI callback, are documented. The harness
  splits path/query, collapses duplicate headers, ignores exc_info, has no write
  callable, and closes on body-join failure. JSON decoding does not enforce status
  or mapping shape at runtime. Fixtures distinguish metadata-only insertion,
  explicit graph links, statted local file metadata, and arbitrary EPUB-named
  bytes from media validation. Four existing cases cover parser configuration,
  cache exclusion, index/work graph projections, and category/search/file download.
  The last case checks preview metadata but does not request the preview route.
- [test_core_surface_acceptance.py](../tests/surfaces/test_core_surface_acceptance.py):
  all four functions are now documented after complete source review. This WSGI
  harness intentionally differs: path is copied without query splitting and its
  callback accepts no exc_info. Optional form data is encoded independently of
  request method. Direct then RPC clients exercise six adapters and each creates
  a work through writable web, sharing evolving catalogue state. Tk backend reads
  do not display a Tk window. Daemon startup occurs before the cleanup try block;
  once started, the finally block stops it and shuts down Core. The real loopback
  path requires socket permission and is not a mocked-transport success claim.

Batch: **6 modules, 2 classes, 47 functions = 55 declarations**. Cumulative:
**211 modules, 257 classes, 2,336 functions = 2,804 reviewed Python declarations**,
plus the separately reviewed native C and runtime-created vacuum adapter.

## Verification

Overlapping selections must not be added into a distinct-test total.

- Strict structural audit and normalizer --check pass for all **211 reviewed
  Python files**, including the tracked data-submodule generator. The five
  original API targets also pass individually; all six retain executable ASTs
  identical to HEAD after stripping genuine leading docstrings. Imports,
  signatures, route conditions, JSON/CLI strings, fixtures, and assertions remain.
- No new Ruff findings relative to HEAD. Existing findings: app one I001, three
  UP045, seven UP032; entrypoint one I001; API tests one I001/one UP032; acceptance
  tests one I001/four UP032. Initializer and launcher are clean. No baseline
  warning was waived or incidentally fixed.
- API source/launcher/test doctests plus API regression, surface read-error,
  CLI-help, and Core-boundary suites: **86 passed, 32 explicitly skipped examples**
  in 70.83 seconds. No socket escalation was needed for this in-process selection.
- With explicitly permitted local sockets, documented direct/RPC acceptance:
  **1 passed, 3 explicitly skipped examples** in 32.91 seconds. All six surface
  adapters are exercised on both transports against the temporary catalogue.
  No displayed browser, curses UI, or Tk window is claimed.
- Full `bash scripts/run_type_checks.sh`: **passed**, retaining 159 formatter
  files, 456 annotated modules, 221 protected dependency modules, 188 strict-mypy
  files, and 37 rejected invalid examples per checker. Selected formatting, lint,
  complexity, annotation, dependency, and both production type checks are green.
- Isolated mocked checks confirm prefix dispatch, HEAD bytes and robot headers,
  work/file/category identity status differences, arbitrary author tables,
  preserved raw ID spelling, unmounted HTML links, robots text, nonfinite JSON,
  visible serialization/read failures, outer-copy versus nested mutation, quoted
  versus directly interpolated IDs, q/global_q precedence, shared search entries,
  unclamped empty pagination, and confirmed-absent file-table count behavior.
- Mocked runner checks verify page-size clamps, exact composition flags, both
  context exits, requested-port-zero output, and guarded import/execution. Mocked
  launcher checks cover module selection, endpoint/database option forwarding,
  endpoint-only timeout, copied environment, cwd, child failure code, and rejected
  paging flags. These isolated checks open no listener or real child server.
- Adjacent dependency/import, migration, public-doc/link, and ownership selection:
  **85 passed, 5 failed** in 114.56 seconds. All failures are the same known
  physical-line conflicts for Core services, program facade, browser components,
  windowed components, and windowed root. No new failure appeared. Guard logic
  and limits remain unmodified; this selection is not green.
- Collection confirms **four existing API tests**: three SQLite-parametrized
  integration cases and one parser-only case, plus the single shared direct/RPC
  acceptance test. An absent driver is not silently counted as tested.
- All **202 local link destinations across sixteen working-memory files** resolve.
  All verification processes completed; no running test handle remains.

Root and data-submodule whitespace checks pass, and the staging area is empty.
No APSW/live PostgreSQL, installed-wheel, or remote-CI verification is claimed.

## Inventory and next work

Full audit: **2,730 modules, 4,400 classes, 35,119 functions**, with no parse
failures. Missing/blank docs remain on **1,104 modules, 2,023 classes, and
22,739 functions = 25,866 declarations**. Additional overlapping findings:
4,247 delimiter-layout, 10,519 missing-example, 3,054 parameter-field, 3,995
return-field, 4,132 empty-parameter, 5,565 empty-return, and 28 missing-summary.
Reports: `/tmp/liuxin-docstring-current-2026-09-10.json` and
`/tmp/liuxin-docstring-reviewed-2026-09-10.json`.

Next: complete `surfaces/web_readwrite` (app 2,798 pre-migration lines, initializer
12, entrypoint 9), its 94-line Python launcher, and 1,007-line main regression
module. These files have not yet been source-reviewed in full. Then continue
remaining CLI/HTTP owners and all source/scripts/examples/tests. Do not repeat
the completed API package, launcher, or these two test modules. Preserve the
configuration-exec warning, runtime documentation consumers, native source, and
data-submodule scope in the main ledger. The whole-project goal remains active.
