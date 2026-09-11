# Calibre-style web docstrings — 2026-09-10

Continuation of the active [whole-project descriptive reST goal](project-docstrings-2026-09-08.md),
following [standalone OPDS](project-docstrings-standalone-opds-2026-09-09.md).
The previous goal turn made verified progress. All five targets were clean before
this batch and read completely: the 1,677-line application, both bootstrap modules,
the launcher, and the 793-line existing integration test module.

Work remains local on `codex/project-docstrings`, based on `edf6bf05`. No commit,
push, data-submodule publication, or PR change accompanies this documentation batch.

## Reviewed files and behavior

- [web_calibre_readonly/app.py](../src/LiuXin_alpha/surfaces/web_calibre_readonly/app.py):
  fully document both classes and all 88 functions. The module composes generic
  web, catalogue, image, OPDS, and acquisition owners; it is partial compatibility,
  not the upstream Calibre content server. Constructor docs distinguish borrowed
  Core clients from database-compatibility session ownership and explain that an
  injected model is not checked against the supplied Core client.
- Routing docs cover method rejection, dot-segment normalization before component
  decoding, dropped blank query values, exact versus prefix routes, tolerated
  extra components, and fallback to the generic host with original environ.
  Bare mobile renders home unless a recognized nonblank query survives parsing.
  Every stanza-prefixed route redirects; interface-data-prefixed unknown paths
  stay in that handler. HEAD retains GET bodies. HTML missing/invalid book or
  category pages can be wrapped with 200, unlike explicit JSON 400/404 responses.
- Rendering docs separate escaped scalar text from trusted HTML fragments and
  embedded CSS. Optional database-path exposure queries database.info on each
  layout; nonmapping receipts yield an empty hint, but query errors propagate.
  Navigation/overview counts are separate reads, not an atomic snapshot. Home
  enumerates the full recent list before taking eight. Existing footer safety
  text is not claimed to create an acquisition authorization boundary.
- Catalogue delegates document encoded token ambiguity, normalization boundaries,
  preferred author sources, row-identity/order preservation, URL mutation, and
  augmented versus unaugmented metadata. Category links can cause additional
  reads. Positional tag-tree editability flags are presentation, not permission.
  OPDS category-item delegation here is broader than standalone OPDS's exact-token
  hook: it uses catalogue normalization and whole-work categories.
- AJAX docs record the fixed main library, unused library suffixes, first query
  values, category sorting overrides, unbounded books-without-IDs enumeration,
  and explicit identity errors. Interface-data init/books-init/get-books always
  request offset zero; IDs take precedence over search while search text remains
  echoed. Metadata selection uses input-order ID membership, not page rank.
  update echoes a translations hash into settings without mutating or refreshing
  catalogue data. JSON retains standard json.dumps nonfinite-float behavior.
- Mobile docs distinguish one-based start from zero-based slices, complete sorting
  before pagination, exact ascending versus reverse ordering, fixed form options,
  and unclamped oversized starts. Navigation can show an inverted range and its
  Last offset is total-num+1, not page-aligned. Browse and linked-work pages have
  distinct pagination behavior; linked-work rendering is unbounded.
- Asset/response hooks document current direct-Core acquisition versus retained
  legacy calls. The enable_file_downloads flag is not enforced by the compatibility
  adapter's direct Core path. File response hooks do not add path confinement;
  filename quote stripping is not general header sanitization. Images retain
  shared discovery/resolution semantics. Thumbnail/format IDs are HTML-escaped,
  not percent-quoted. Format labels derive from filenames and do not validate media.
- Book detail docs cover integer-only IDs, four-credit bylines, twelve tag pills,
  unbounded series pills, and first-note selection from synopses/comments/notes,
  shortened to 600 characters. A cover link is emitted without an availability
  or download-policy check. None of these existing behaviors was changed.
- [__init__.py](../src/LiuXin_alpha/surfaces/web_calibre_readonly/__init__.py) and
  [__main__.py](../src/LiuXin_alpha/surfaces/web_calibre_readonly/__main__.py): package
  import does not start a server, but importing __main__ invokes the runner
  unconditionally. This differs from standalone OPDS's guarded entrypoint and is
  now explicit. Library users should import the package or app module.
- [run_web_calibre_readonly.py](../scripts/run_web_calibre_readonly.py): document
  platform interpreter paths, POSIX display quoting versus list execution, required
  explicit database/endpoint selection, existence-only interpreter checks, option
  forwarding, copied PYTHONPATH, repository-root cwd, inherited streams, no wrapper
  timeout, and preserved child return code. Unlike the application parser, this
  wrapper exposes neither paging nor OPDS grouping options. No installation ran.
- [test_web_calibre_readonly.py](../tests/surfaces/test_web_calibre_readonly.py):
  document all 26 helpers/tests, including the nested WSGI callback. Explain
  collapsed duplicate headers, ignored exc_info, missing write callable, and
  response closure on join failure. Fixtures distinguish metadata insertion,
  optional item IDs, statted assets, and explicit relationship creation. Tests
  cover cache snapshots, HTML/facet/search routes, byte-identical cover/thumbnail
  delivery, legacy acquisition, static assets, OPDS grouping/paging, and JSON/tree
  projections. Assertions do not imply browser rendering, media parsing/resizing,
  XML schema validation, or every malformed-route edge case.

Batch: **5 modules, 2 classes, 117 functions = 124 declarations**. Cumulative
reviewed Python scope: **200 modules, 250 classes, 2,181 functions = 2,631 declarations**,
plus the separately reviewed native C and runtime-created vacuum adapter.

## Verification

Overlapping selections must not be summed into a distinct-test total.

- Initial application/initializer doctests plus existing Calibre, surface read-error,
  and four-surface CLI-help regressions: **104 passed, 65 explicitly skipped examples**
  in 212.72 seconds. This run collected the Calibre tests before their own docstring
  additions; the final selection reruns them with those doctests included.
- Strict structural audit and normalizer --check pass across all **200 reviewed
  files**, including the separately tracked data generator. All five new targets
  pass individually. No parse or unsafe-literal exception was introduced.
- All **five edited existing Python files** retain identical executable ASTs to
  HEAD after removing genuine leading docstrings. Embedded CSS/HTML, JSON behavior,
  CLI help strings, signatures, fixture bytes, and test assertions are unchanged.
- No new Ruff findings relative to HEAD. Existing findings remain: app I001/one
  F401/nine UP045/28 UP032; initializer I001; existing tests I001/61 UP032/one F841.
  The entrypoint and launcher are clean. No warning was waived and no incidental
  import, unused-variable, typing, or formatting repair was made.
- Full `bash scripts/run_type_checks.sh`: **passed**, retaining scopes of 159
  formatter files, 456 annotated modules, 221 protected dependencies, 188 strict
  mypy files, and 37 rejected invalid examples per checker. Selected lint,
  complexity, dependency, annotation, formatting, and type checks are green.
- Isolated checks with simulated reads confirm actual HTML 200 versus JSON
  400/404 behavior, prefix routing, main-library suffix handling, NaN serialization,
  and GET/HEAD byte delivery despite the disabled host download setting. Mocked
  projections confirm ignored interface-data offsets, ID precedence, unbounded
  AJAX books selection, and mobile Last offset arithmetic.
- A mocked runner verifies __main__ invokes it and raises SystemExit even under
  a non-main run name. Mocked application contexts verify config clamps, cache/
  feature flags, requested-port-zero output, normal return, and both context exits.
  Mocked launcher checks verify forwarded flags, repo cwd/copied environment,
  child failure propagation, and rejection of the unsupported page-size option.
  No listener, real child server, or database was opened by these isolated checks.
- Final combined application/launcher/test doctests, Calibre/read-error/help
  regressions, dependency/import boundaries, migration/public-doc/developer-link
  contracts, and ownership selection: **215 passed, 95 explicitly skipped examples,
  5 failed** in 337.78 seconds. All failures are the already recorded physical-line
  conflicts: Core services, program facade, browser components, windowed components,
  and windowed root. No new failure appeared; the overall pytest result is not green.
- Collection confirms **12 existing Calibre tests**: eleven SQLite-parametrized
  integration cases and one parser-only test. No absent driver is silently counted
  as tested. All **187 local link destinations** across the index and thirteen
  project-docstring notes resolve. Root/data-submodule diffs are whitespace-clean
  and the staging area is empty.
- After the final class-field-spacing correction, application doctests:
  **27 passed, 63 explicitly skipped examples** in 26.41 seconds. The executable
  AST still matches HEAD, and strict audit/normalizer checks pass again for all
  **200 reviewed files**. Final root/data-submodule whitespace checks pass and the
  staging area remains empty. All verification handles have completed.

The unguarded __main__ is audited structurally and exercised with a mocked runner,
not imported by pytest's doctest collector, which would invoke the real CLI. No
production guard was added and no default test selection changed. The five known
physical-line ownership conflicts are not waived or repaired by this batch.
No APSW/live PostgreSQL, displayed UI, installed-wheel, or remote-CI success is
claimed. The whole-project goal remains active; useful docs are not compressed
to satisfy physical-line guards that include docstrings.

## Inventory and next work

The full audit still covers **2,730 modules, 4,400 classes, 35,119 functions**,
with no parse failures. Missing/blank docs remain on **1,107 modules, 2,026 classes,
22,886 functions = 26,019 declarations**. Additional overlapping findings: 4,259
delimiter-layout, 10,531 missing-example, 3,054 parameter-field, 3,995 return-field,
4,138 empty-parameter, 5,573 empty-return, and 28 missing-summary findings.

Current reports are `/tmp/liuxin-docstring-current-2026-09-10.json` and
`/tmp/liuxin-docstring-reviewed-2026-09-10.json`. Next: the complete generic
`surfaces/web_readonly` package (app.py is 2,852 pre-migration lines, initializer
25, entrypoint 9), its 125-line launcher and 442-line main regression module,
then API/HTTP/CLI and the remaining source/scripts/examples/tests. The generic
web base has only been inspected at selected delegation points, not reviewed
in full. Do not repeat this completed Calibre package/launcher/test batch.
Preserve the main ledger's configuration-exec warning, runtime documentation
consumers, native source, and data-submodule scope.
