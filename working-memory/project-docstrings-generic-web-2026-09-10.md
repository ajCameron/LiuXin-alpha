# Generic read-only web docstrings — 2026-09-10

Continuation of the active [whole-project descriptive reST goal](project-docstrings-2026-09-08.md),
following [Calibre-style web](project-docstrings-calibre-web-2026-09-10.md).
The intervening PR-status turn did not advance the documentation goal; this turn
revalidated the clean target files and resumed the available implementation work.
The complete 2,852-line application was source-reviewed, followed by both bootstrap
modules, the 125-line launcher, and the full 442-line existing regression module.

Work remains local on `codex/project-docstrings`, based on `edf6bf05`. No commit,
push, submodule publication, or PR change accompanies this batch. Preserve all
earlier documentation work and the separate data-generator changes.

## Reviewed scope and contracts

- [app.py](../src/LiuXin_alpha/surfaces/web_readonly/app.py): all four named classes
  and 88 functions, including the nested cleanup class/methods and both source
  definitions of the store-detail renderer. Module/class descriptions no longer
  make an unsupported blanket public-safety promise. This is a read-oriented
  interface, not authentication, authorization, or active-content isolation.
- Lifecycle docs distinguish borrowed Core from owned compatibility sessions,
  an unchecked injected model, and exact cache-selection spelling. WSGI docs
  retain GET bodies for HEAD, copied headers with appended robot guidance,
  repeated closer invocation, and lack of cleanup wrapping if start_response
  itself fails. The nested wrapper does not automatically call the underlying
  iterable's close method. Response construction performs no validation.
- Routing preserves dot normalization before component decoding, discarded blank
  query values, method rejection, and exact path shapes. Missing/invalid table or
  row pages remain explanatory HTML with 200; unknown routes are 404. Required
  read failures propagate. Optional database-path hint errors are suppressed in
  this base layout, unlike the Calibre override. Home counts suppress only the
  explicit read_query_unavailable capability outcome, not arbitrary errors.
- Visibility docs explain lowercase name heuristics, unchanged configuration
  token case, visible-ID preference, eight-column browse and ten-column search
  limits, and preferred-summary fields bypassing visibility rules. Summary limits
  are checked after appending, so nonpositive values can still return one part.
  Default public search uses only preferred tables when any exist, rather than
  appending all other main tables. Table browsing still includes operational and
  relationship tables. ID links quote components but do not validate integer IDs.
- Search docs cover NFKC/case-folded additive relevance, first matching column as
  snippet source, row identity retention, and shared full-search delegation.
  Highlighting uses escaped literal regex terms with IGNORECASE, whereas snippets
  use lowercase matching; neither promises ranking's normalization. Snippet
  ellipses can exceed width, and pager offsets remain unclamped. Exact results
  are materialized before local slicing; global and exact sections can coexist.
  The q-to-global alias is applied after the form is rendered.
- Value renderers distinguish escaped text from trusted body/action markup, JSON
  pretty-printing from repr shortening, name-derived timestamp kinds, float/int
  millisecond conversion, and raw timestamp retention. URI/path styling is not
  URL validation. Invalid JSON retains original whitespace. Credits preserve
  provider order rather than implementing the priority-order caption themselves.
  Projected format links are not reacquired or checked against download policy.
- Relationship helpers preserve group/row order and do not paginate linked lists.
  An explicit empty group mapping triggers a fresh query. Source store-renderer
  duplication is documented, not removed: the later definition is runtime-active,
  while the earlier body remains source-audited. Their executable ASTs are equal.
  Work/file/store methods return body fragments, not complete layouts. File hero
  relationship counts and rendered row groups come from separate inputs.
- Acquisition docs separate target-first capability display from reader-first
  delivery. The built-in target resolver only constructs redirects; local paths
  remain an override-compatible branch. Resolution does not prove byte delivery.
  Stored payloads are fully buffered, strings are UTF-8 encoded, and other values
  use bytes(). Initial discovery and final fallback exceptions are outside the
  501/502 conversion handler. NotImplementedError with a target resolves that
  fallback again; FileNotFoundError falls through. Disabled download resolution
  normally ends in 404 rather than 403. Filename quote removal is not general
  header sanitization, and streaming has no path confinement or range handling.
- Preview selection is suffix-only. HTML/SVG remain unsanitized active content;
  declaring UTF-8 does not transcode bytes. Unsupported suffixes return 415 before
  download-policy resolution, and external redirects retain external content
  policy. Existing runtime strings and behavior were not changed.
- Shared CLI docs distinguish parser construction from Core/cache loading.
  build_metadata_read_source validates aliases but ignores its cache/fallback
  compatibility arguments; Core composition owns the actual selection. The runner
  independently clamps page sizes, enables storage, disables maintenance, uses
  Core/server contexts, and prints the requested URL before binding, including
  port zero rather than the eventual assigned port.
- [__init__.py](../src/LiuXin_alpha/surfaces/web_readonly/__init__.py) and
  [__main__.py](../src/LiuXin_alpha/surfaces/web_readonly/__main__.py): exports and
  guarded execution are explicit. Importing this entrypoint is inert, unlike the
  Calibre entrypoint; main-name execution forwards the runner result to SystemExit.
- [run_web_readonly.py](../scripts/run_web_readonly.py): platform interpreter paths,
  existence-only validation, display-only POSIX quoting, explicit database versus
  endpoint selection, forwarded cache flags, copied PYTHONPATH, repository-root
  cwd, inherited streams, and preserved child return codes. Core timeout is not
  a subprocess timeout. The wrapper does not expose application page-size flags.
- [test_web_readonly.py](../tests/surfaces/test_web_readonly.py): document the storage
  double and all seventeen functions, including the nested WSGI callback. Harness
  docs record collapsed duplicate headers, ignored exc_info, absent write callable,
  and close-on-join-failure. Fixture docs distinguish metadata insertion, statted
  local files, blob-store keys, and explicit links. Eight existing tests cover
  grouping/search/detail, cache snapshot exclusion, filesystem/blob downloads,
  unsupported delivery, selected sensitive-column hiding, and linked entities.
  No browser rendering, valid-EPUB parsing, or complete privacy audit is implied.

Batch: **5 modules, 5 classes, 108 functions = 118 declarations**. Cumulative:
**205 modules, 255 classes, 2,289 functions = 2,749 reviewed Python declarations**,
plus the separately reviewed native C and runtime-created vacuum adapter.

## Verification

Overlapping selections are not additive distinct-test totals.

- All five changed Python files have identical executable ASTs to HEAD after
  stripping genuine leading docstrings. Signatures, imports, code, HTML/CSS,
  CLI strings, fixture bytes, and existing test assertions are unchanged.
- Strict audit and normalizer --check pass for all **205 reviewed Python files**,
  including the separately tracked data generator. No parse/unsafe-literal
  exception was added. The full audit remains intentionally incomplete.
- No new Ruff findings relative to HEAD. Existing findings remain: application
  two I001, one UP035, one F401, one UP028, fourteen UP045, eighty-five UP032;
  entrypoint one I001; launcher one F401; tests one I001, two UP034, seventeen
  UP032. Initializer is clean. No baseline warning was waived or fixed incidentally.
- Full `bash scripts/run_type_checks.sh`: **passed**, retaining 159 formatter
  files, 456 annotated modules, 221 protected dependencies, 188 strict-mypy files,
  and 37 rejected invalid examples per checker. Selected formatting, lint,
  complexity, annotation, dependencies, and both production type checks pass.
- Initial generic-web source/launcher/test doctests, web regressions, Core
  transport failures, and CLI help: **56 passed, 69 explicitly skipped examples,
  5 failed** in 126.10 seconds. All five failures were PermissionError at test
  socket creation; no source assertions or doctests failed. The explicitly
  permitted full rerun passed: **61 passed, 69 explicitly skipped examples** in
  89.19 seconds, including all five direct/loopback-HTTP transport checks.
- Adjacent surface read-error, dependency/import, migration, public-doc/link,
  and ownership selection: **145 passed, 5 failed** in 98.08 seconds. Failures are
  the same known physical-line guards for Core services, program facade, browser
  components, windowed components, and windowed root. No new owner failure
  appeared. Guard logic and limits remain unchanged; this result is not green.
- Four directly discovered nested cleanup class/method doctest examples passed
  separately, because ordinary module doctest discovery does not reach that
  nested class. The two store-renderer bodies were compared structurally and
  proved executable-equivalent; only the later one is runtime-bound. Its earlier
  skipped example is source documentation, not claimed runtime exercise.
- Isolated mocked checks verify HTML statuses, optional path-hint failure
  suppression, HEAD retaining bytes, copied headers, file closure, raw active
  HTML preview delivery, 415 precedence, acquisition error boundaries, repeated
  fallback resolution, preferred-summary visibility bypass, nonpositive summary
  limits, preferred public-table selection, empty-group rediscovery, and oversized
  pager ranges. These checks did not bind sockets or contact remote services.
- Mocked CLI contexts verify configuration clamps, exact composition flags,
  both context exits, requested-port output, ignored selector compatibility
  settings, and invalid selector rejection. Mocked launcher checks cover option
  forwarding, copied environment, cwd, child failure propagation, and rejected
  page-size flags. A runpy check confirms inert import versus guarded execution.
  No real child web server was spawned by these checks.
- Test collection confirms **eight existing generic-web tests**: seven
  SQLite-parametrized integration cases and one pure classifier case. No absent
  driver is silently counted as exercised. Root and data-submodule whitespace
  checks pass, and the staging area is empty.
- All **194 local link destinations across fifteen working-memory files** resolve.
  All verification processes completed; no running test handle is left behind.

No APSW/live PostgreSQL, displayed UI, installed-wheel, or remote-CI success is
claimed. Known docstring-sensitive ownership conflicts are not waived, and useful
documentation has not been compressed to satisfy physical-line guards.

## Inventory and next work

The whole-project inventory still covers **2,730 modules, 4,400 classes, and
35,119 functions**, with no parse failures. Missing/blank docs remain on
**1,106 modules, 2,023 classes, and 22,784 functions = 25,913 declarations**.
Additional overlapping findings: 4,253 delimiter-layout, 10,523 missing-example,
3,054 parameter-field, 3,995 return-field, 4,133 empty-parameter, 5,567 empty-return,
and 28 missing-summary findings. Reports are
`/tmp/liuxin-docstring-current-2026-09-10.json` and
`/tmp/liuxin-docstring-reviewed-2026-09-10.json`.

Next: the complete `surfaces/api_readonly` package (app 461 pre-migration lines,
initializer 7, entrypoint 9), its 113-line launcher and 363-line regression
module. These files have not yet been source-reviewed in full. Then the 2,798-line
read/write web application, remaining CLI/HTTP owners, and the rest of the full
source/scripts/examples/tests inventory. Do not repeat completed generic-web work.
Preserve the main ledger's configuration-exec warning, runtime documentation
consumers, native-code scope, and initialized data-submodule coverage. The full
goal remains active and far from complete.
