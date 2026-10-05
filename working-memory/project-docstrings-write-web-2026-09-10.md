# Read/write web docstrings — 2026-09-10

Continuation of the active [whole-project descriptive reST goal](project-docstrings-2026-09-08.md),
following [read-only API documentation](project-docstrings-readonly-api-2026-09-10.md).
The preceding PR-status turn made no documentation progress; this continuation
revalidated the existing application edits and completed the unfinished regression
module. Source review covers the entire original 2,798-line application, both
bootstrap modules, 94-line launcher, and 1,007-line test module. All changes in
these five files are docstrings; no executable implementation or assertion changed.

Work remains local on `codex/project-docstrings`, based on `edf6bf05`. The separate
maintainability PR remains unchanged. No commit, push, or submodule publication
accompanies this batch.

## Reviewed contracts

- [app.py](../src/LiuXin_alpha/surfaces/web_readwrite/app.py): all three classes
  and 72 functions, including the nested allowed-values helper, are documented.
  Module/class docs distinguish this experimental editor from a hardened public
  administration surface: there is no authentication or CSRF check here, secret
  and policy columns are exposed, and escaped backend errors reach the UI.
  Commands execute synchronously, not as queued jobs. Multi-step writes and
  refreshes have no encompassing rollback in this adapter.
- Routing normalizes before percent decoding, drops blank query values, retains
  HEAD bodies, and resets its ContextVar notice even after failure. Notices are
  query-supplied presentation, not authenticated receipts. Layout inserts escaped
  banner/notices around trusted rendered HTML. Redirect query rebuilding collapses
  repeated values and does not constrain the destination. Administrative column
  visibility excludes only the exact lowercase _scratch suffix. Home counts
  suppress all count/coercion errors, unlike the read-only host's narrower policy.
- Form parsing reads at most one MiB and keeps the first URL-encoded value;
  multipart parsing reads at most 64 MiB, keeps the last duplicate text field,
  and retains the last named file part regardless of its field name. Reads use
  clamped declared length once: missing length means empty, oversized input is
  capped rather than rejected/drained, and short reads are not retried. Unknown
  text codecs can raise. Multipart defects are not explicitly rejected here.
- Schema helpers describe caught versus propagated failures, unvalidated
  reference suggestions, import-time backend choices, and editable-field
  exclusions. Widget selection is a naming/value heuristic, not schema typing.
  Numeric/boolean invalid text survives for downstream validation; invalid
  JSON/date/datetime raises. Date normalization accepts a valid ten-character
  prefix. Numeric timestamps use local time, while ISO inputs lose seconds and
  offsets before local conversion. JSON input retains original whitespace.
  Literal NULL becomes None for every kind: create payloads omit it, updates
  retain it. The existing UI hint is not silently rewritten to hide that mismatch.
- Grouped forms retain remaining editable columns, including credentials, and
  have no CSRF token. Suggestions are not enforcement. Upload basenames remove
  path components but retain dot names; this is not storage-key confinement.
  Writable-store hints are metadata-based and do not probe backend availability.
- Uploads bootstrap storage, resolve the selected store, optionally create a
  WEMI chain, put base64 content, create the legacy file row, then refresh reads.
  A later failure can leave earlier rows or bytes. Receipt shape checks and
  rereads are explicit. Generic octet-stream may reach the storage command while
  file metadata falls back to a filename MIME guess. The store_row compatibility
  argument is unused. Existing-item form defaults do not update that item's
  metadata through the file helper. Successful writes can precede failed refresh.
- Relationship forms distinguish creating a target from linking it. Target
  creation precedes link-metadata coercion, so invalid metadata can leave an
  orphan. The built-in link writer ignores the model receipt and returns a fixed
  successful report. Existing-target handlers treat changed=False as an info
  redirect without refresh; create-target handlers still refresh and report
  success. Selected target-lookup backend failures become 404. Link edits/deletes
  verify primary-row ownership but do not imply authentication or a transaction.
  Missing link-schema/row reads can suppress whole management sections during
  rendering; later row coercion/render errors can still propagate.
- Row mutations document their actual status/error boundaries: create checks
  writability, but edit can submit an empty update without a separate 405 guard;
  delete does not require a prior preview request. Error rendering, selected
  schema reads, and post-write refresh can fail outside mutation catches.
- [__init__.py](../src/LiuXin_alpha/surfaces/web_readwrite/__init__.py) and
  [__main__.py](../src/LiuXin_alpha/surfaces/web_readwrite/__main__.py): package
  imports do not start a listener; the guarded entrypoint forwards main's code
  through SystemExit. Configuration retains inherited settings without validation.
  Main clamps page sizes independently, enables storage, disables maintenance,
  and supplies no cache-selection kwargs. Core/server contexts own cleanup; the
  printed URL precedes binding and retains requested port zero.
- [run_web_readwrite.py](../scripts/run_web_readwrite.py): explicit database or
  endpoint selection, endpoint-only Core timeout, existence-only interpreter
  check, platform paths, diagnostic POSIX quoting versus list execution, copied
  PYTHONPATH, repository-root cwd, inherited streams, no child-process timeout,
  and preserved child exit code. No paging or cache-selection flags are exposed.
- [test_web_readwrite.py](../tests/surfaces/test_web_readwrite.py): all 27 functions
  are documented, including both WSGI callbacks. Harnesses split path/query,
  collapse duplicate response headers, ignore exc_info, supply no write callable,
  and close results even after join failure. URL forms turn None into empty text;
  multipart uses literal None, a fixed boundary, and trusted unescaped header
  interpolation. Fixtures distinguish metadata-only stores, caller-created
  managed directories, and actual upload/download bytes. Sixteen existing cases
  cover row/relation forms, notices, cache visibility, widget coercion, grouped
  rendering, and three real managed-store upload paths. View/reference guard
  cases are GET-only, not POST-rejection proof. Receipt-named cases observe
  persisted rows/notices, not raw receipt objects. EPUB fixture bytes are not
  valid-publication or atomicity evidence.

Batch: **5 modules, 3 classes, 102 functions = 110 declarations**. Cumulative:
**216 modules, 260 classes, 2,438 functions = 2,914 reviewed Python declarations**,
plus the separately reviewed native C and runtime-created vacuum adapter.

## Verification

Overlapping selections must not be added into a distinct-test total.

- Strict structural audit and normalizer --check pass for all **216 reviewed
  Python files**, including the tracked data-submodule generator. All five
  current files also pass individually. Their executable ASTs match HEAD after
  stripping genuine leading docstrings; signatures, imports, UI/parser literals,
  route conditions, fixture values, and test assertions remain unchanged.
- No new Ruff findings relative to HEAD. Existing findings remain: application
  I001 once, F401 once, UP045 21 times, UP032 91 times; entrypoint I001 once;
  launcher F401 once; regression module I001 once and UP032 43 times. The package
  initializer is clean. No incidental warning or formatter issue was fixed.
- Source/launcher/test doctests plus writable-web, surface read-error, CLI-help,
  and Core-boundary regressions: **119 passed, 69 explicitly skipped examples**
  in 291.86 seconds. This run opened no HTTP listener.
- With explicitly permitted local sockets, shared direct/RPC surface acceptance:
  **1 passed** in 31.99 seconds. This exercises writable web and the other five
  adapters on both direct and HTTP Core clients, not a mocked transport claim.
  No displayed browser, Tk window, or curses interaction is claimed.
- Full `bash scripts/run_type_checks.sh`: **passed**, retaining 159 formatter
  files, 456 annotated modules, 221 protected dependency modules, 188 strict-mypy
  files, and 37 rejected invalid examples per checker. Selected formatting,
  lint, complexity, dependency direction, and both type checkers are green.
- Isolated mocked checks passed for notice reset after exceptions, HEAD bodies,
  untrusted/escaped notices, capped/short/missing-length reads, permissive media
  type handling, unknown-codec propagation, first URL-form versus last multipart
  values, arbitrary file-part names, credential visibility, create-NULL omission
  versus update None, date-prefix and timezone/seconds loss, permissive numeric
  and boolean coercion, and dot-name retention.
- Mocked failure checks passed for storage-before-metadata writes, MIME fallback,
  target-create-before-link-validation ordering, target backend failure to 404,
  and the different changed=False policies. The first scratch check mistakenly
  compared the response's iterable body directly to bytes; correcting the check
  to join the body made the full check pass. No implementation/docstring change
  was needed for that harness error.
- Mocked runner/launcher checks passed for exact Core flags, page clamps, both
  context exits, port-zero output, guarded import/execution, module selection,
  endpoint-only timeout, environment copying, cwd, child failure code, and rejected
  paging/cache wrapper flags. No listener or real child server was opened there.
- Adjacent migration, public-doc/link, dependency, and ownership selection:
  **82 passed, 5 failed** in 204.29 seconds. Failures are the same known physical
  line/statement conflicts in Core services, program facade, browser components,
  windowed components, and windowed root. This selection is not green; no guard
  logic or limits changed.
- Collection confirms **sixteen existing writable-web integration cases**, all
  selected for SQLite here. Other drivers are not silently counted as covered.
- All **209 local link destinations across seventeen working-memory files**,
  including the index, resolve. Root and data-submodule whitespace checks pass;
  the staging area is empty. All verification processes have completed.

No APSW/live PostgreSQL, installed-wheel, or remote-CI verification is claimed.

## Inventory and next work

Whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**, with no
parse failures. Missing/blank docs remain on **1,103 modules, 2,022 classes, and
22,639 functions = 25,764 declarations**. Additional overlapping findings:
4,241 delimiter-layout, 10,515 missing-example, 3,054 parameter-field, 3,995
return-field, 4,131 empty-parameter, 5,563 empty-return, and 28 missing-summary.
Reports: `/tmp/liuxin-docstring-current-2026-09-10.json` and
`/tmp/liuxin-docstring-reviewed-2026-09-10.json`.

Next: CLI composition, parser contracts, completion, and shared helpers, then
all command families. The starting files are cli/__init__.py (14 lines),
__main__.py (9), app.py (111), parser_types.py (14), parsers.py (87),
completion.py (139), and common.py (456). These have not yet been fully reviewed
for this documentation goal. The Core HTTP transport is already reviewed; the
earlier CLI/HTTP reminder does not require repeating that completed work.
Continue through all remaining source/scripts/examples/tests, including native
and data-submodule scope and configuration-exec cautions recorded in the main
ledger. The whole-project goal remains active and unfinished.
