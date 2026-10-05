# Shared surface contracts and profile docstrings — 2026-09-09

Continuation of the active [whole-project descriptive reST goal](project-docstrings-2026-09-08.md),
following the [shared surface/Core adapter](project-docstrings-surface-core-2026-09-09.md).
The preceding goal turn made verified progress. This turn revalidated all five
new source targets as clean before editing. Documentation remains local on
`codex/project-docstrings`, based on `edf6bf05`, without a commit, push, PR update,
submodule publication, or delegation. The full project objective is unfinished.

## Reviewed files and contracts

Five production modules, all read completely before their documentation pass:

- [surfaces/__init__.py](../src/LiuXin_alpha/surfaces/__init__.py): lazy-name
  membership, import/cache ordering, visible failed imports, and import-free
  directory listing. Advertisement is not a ban on directly importing other modules.
- [surfaces/api.py](../src/LiuXin_alpha/surfaces/api.py): all seven host/response/target
  protocols and 35 method/property declarations, preserving ellipsis bodies and
  inheritance. Describe Core borrowing, response cleanup, catalogue projections,
  file/image delivery, and OPDS hooks without inventing default implementations,
  runtime validation, serialized-dict guarantees, or authorization boundaries.
  Read-model, image, catalogue, OPDS, acquisition, and web implementation excerpts
  were inspected as supporting evidence, not counted as completed files.
- [acquisition_types.py](../src/LiuXin_alpha/surfaces/acquisition_types.py): structural
  byte-reader contract, passive frozen delivery fields, repeated uncached reads,
  unchanged returned payload identity, discarded metadata, and visible reader errors.
- [presentation.py](../src/LiuXin_alpha/surfaces/presentation.py): HTML text/quoted-
  attribute escaping rather than generic sanitization; CR normalization without
  whitespace flattening; character-count truncation whose dots can exceed narrow
  widths; initial broad integer-parse fallback versus uncaught fallback/bound errors;
  lower-then-upper clamp order; and KeyError-only row subscription fallback.
- [system_profile.py](../src/LiuXin_alpha/surfaces/system_profile.py): all fifteen
  functions plus its value record and module introduction. Covers explicit/environment/
  persisted precedence, named versus path selectors, relative XDG differences,
  non-recursive profile listing, bounded reads, pointer chains/cycles, final-manifest
  identity versus outer selection source, and selective namespace mutation.
  Main manifest versions use int coercion (including True/fractional floats);
  pointer versions require an actual int. Paths are resolved, not confined or
  checked for operational readiness. Persistence checks target file existence,
  not manifest contents, and preserves pre-replacement versus post-commit failure
  distinctions. Directory sync ignores only open OSError. Redaction masks selected
  key substrings and URL passwords, but not arbitrary nested/query/fragment secrets;
  invalid credential URLs may remain unchanged and IPv6 brackets are not restored.
  These limitations are documented, not silently repaired by unrelated behavior changes.

Existing tests are part of the goal:

- [test_shared_surface_dependencies.py](../tests/surfaces/test_shared_surface_dependencies.py):
  module, two classes, and ten functions, including the nested failing-row double
  and recording acquisition reader. Executable assertions and subprocess checks
  are unchanged. The reader doctest uses hex output to avoid an escaped-literal
  normalization ambiguity; the normalizer was not weakened.
- [test_surface_package_api.py](../tests/surfaces/test_surface_package_api.py):
  module and all three export/re-export identity tests. Docs distinguish export
  availability from a fresh-interpreter proof of laziness.
- New [test_shared_surface_documentation_contracts.py](../tests/surfaces/test_shared_surface_documentation_contracts.py):
  module, one nested class, and all twenty functions. The autouse fixture confines
  XDG paths and working directory to pytest temporary storage and removes only
  test-scoped environment selectors. Tests cover selection, version coercion,
  pointer recursion, byte bounds, stale targets, exact permissions, pre/post-write
  failure visibility, selective copying, limited redaction, presentation fallback,
  payload identity, and lazy import/cache behavior. They never modify the user's
  actual profiles or access the example endpoint. The fixture's example uses
  pytest request.getfixturevalue, not an invalid direct call to a decorated fixture.

Batch: **8 modules, 15 classes, 92 functions = 115 declarations**. Cumulative
reviewed scope: **163 modules, 224 classes, 1,658 functions = 2,045 declarations**,
plus the previously documented native C and runtime-created vacuum adapter.

## Verification

Selections overlap and must not be summed as distinct tests.

- Strict structural audit and normalizer --check pass across all 163 reviewed
  files, including the data-submodule generator. The new batch is strictly clean.
- Initial five-source/shared-dependency doctests and tests: **48 passed, 52
  explicitly skipped examples** in 53.40 seconds. New profile/shared-boundary
  module doctests and tests: **29 passed, seventeen skipped** in 1.45 seconds.
- Integrated five-source/doctest, three current test modules, acquisition/image/
  OPDS contracts, migration/public-boundary, and both ownership selections:
  **187 passed, 74 skipped, five failed** in 126.46 seconds. The five failures are
  exactly the known Core/terminal physical-line guards. There were no additional
  behavioral failures or socket-permission failures in this selection.
- Four selected existing CLI operator regressions: **four passed** in 36.53 seconds.
  They exercise profile show/validation/argument resolution, named profile add/remove,
  a real initialized local Core selected by system root, and persisted connect/
  status/precedence/disconnect. The rest of the large operator test module remains
  outside this documentation batch and is not claimed as fully rerun or documented.
- Full `bash scripts/run_type_checks.sh`: **passed**, unchanged scopes of 159
  formatter files, 456 annotated modules, 221 protected dependencies, 188 strict-
  mypy files, and 37 negative examples rejected by each checker. No production
  typing errors; selected lint/complexity/formatting gates pass. This runner does
  not execute the physical-line ownership pytest guards.
- All five current production files and both edited existing test modules have
  identical executable ASTs to HEAD after genuine leading docstrings are removed.
  No runtime-documentation exception is needed in this batch. Protocol properties,
  signatures, and ellipsis bodies are preserved. Root/submodule diffs are whitespace-clean.
- No new Ruff findings in the seven existing files. Existing out-of-scope findings
  remain: package root I001; API I001/UP035/six UP040/five UP045; profile I001/ten
  UP032/two B010; package-API test I001. Maintained acquisition/presentation/shared-
  dependency files and the new test module are clean. The new test's initial import-
  layout issue was corrected; no existing warning was waived or fixed incidentally.

After the integrated run, the response-protocol example was tightened to show
cleanup in finally and the fixture example was corrected to use pytest's request
API. Final protocol/new-test/package-export doctests and tests: **35 passed,
59 explicitly skipped examples** in 13.56 seconds. Final full reviewed-file audit,
normalizer check, and seven-file executable AST comparison pass. The new test
remains formatter/lint-clean. Public/developer-link handoff suites: **23 passed**
in 18.01 seconds; all **139 local destinations across nine working-memory notes**
resolve. All observed test, quality, AST, audit, and link-check handles completed;
none is pending. The staging area remains empty.

The five ownership failures remain unresolved, with their logic/limits unchanged.
Permission for docstring-aware counting remains unanswered. Do not shrink useful
documentation, omit larger owners, or hide those failures to make a gate green.
This is not a full-project test result, live PostgreSQL run, displayed GUI, or
fresh HTTP/installed-wheel acceptance claim.

## Remaining project scope

The current whole-project audit inventories **2,727 modules, 4,400 classes,
35,077 functions** with no parse failures. Missing/blank docs remain on **1,117
modules, 2,040 classes, 23,351 functions = 26,508 declarations**. Other overlapping
prose/field/layout findings are recorded in the main ledger. The full objective
is not complete.

Generated reports remain `/tmp/liuxin-docstring-current-2026-09-09.json` and
`/tmp/liuxin-docstring-reviewed-2026-09-09.json`. Next shared owners include
categories (339 lines), tags/icons (51), thumbnail cache (314), and the read-model
(793), image (190), catalogue, acquisition, and OPDS implementations. Only relevant
dependency excerpts have been inspected for several of those; they are not completed
file claims. Continue through all other source/scripts/examples/tests afterward.
Preserve the configuration-exec dictionary warning and tracked submodule inventory.
