# Category, thumbnail, and image docstrings — 2026-09-09

Continuation of the active [whole-project descriptive reST goal](project-docstrings-2026-09-08.md),
following [shared surface contracts and profiles](project-docstrings-shared-surfaces-2026-09-09.md).
The intervening PR-status turn did not advance documentation. This continuation
revalidated the pending files and recovered their verification: the earlier tool
output was unavailable and no surviving pytest process was observed. No missing
result was counted as a pass. Documentation remains local on
`codex/project-docstrings`, based on `edf6bf05`; PR #115 is separate and unchanged.

## Reviewed files and contracts

All five production modules were read completely before their initial edits and
read again while verifying this batch:

- [categories.py](../src/LiuXin_alpha/surfaces/categories.py): mutable legacy
  category items rather than modern Core records; reference-shared ID sets,
  halved ratings, tooltip composition, and the actual Python 3 str/repr recursion.
  Describes books-table discovery and branch precedence, partial Tag factories,
  category-key versus label icon lookup, preference normalization collisions and
  swallowed save failures, in-place stable sorting, provider mutation, incomplete
  rating deduplication, empty selection forwarding, and per-call composite metadata
  caching. Grouped category generation requires nonempty user categories; its
  lowercase lookup/original-key storage and shallow-copy ID sets retain observable
  casing and source-mutation quirks. Saved searches use their own name ordering.
- [tags_icons.py](../src/LiuXin_alpha/surfaces/tags_icons.py): required key presence,
  accepted None values, copied subset/shared values, mutable later aliases, and
  partial state after failed explicit reinitialization. Resource names are not
  loaded icon objects.
- [thumbnail_cache.py](../src/LiuXin_alpha/surfaces/thumbnail_cache.py): lazy index
  creation, cross-group LRU/budget accounting, group-specific lookup/invalidation,
  unchanged byte payloads, filename-derived metadata and rounded timestamps,
  orphaned replacement files, duplicate-key accounting, and non-closing shutdown.
  Documents trusted local paths/pickle input, absent process locking/atomic writes,
  deletion-before-accounting failures, and limited empty cleanup. Error logging
  currently evaluates an undefined as_unicode; binary invalidation records fail
  text parsing and separate appends lack a separator. Size invalidation mutates
  its live OrderedDict iterator: it can fail after partial deletion, but removing
  the final entry can exhaust normally. No production repair was folded into docs.
- [images/api.py](../src/LiuXin_alpha/surfaces/images/api.py): borrowed Core lifecycle,
  direct-first relationship traversal, first-ID order with later-row replacement,
  numeric-conversion boundaries, naming/MIME precedence, shallow compatibility
  aliases, visible provider errors, and resolution ordering. Reader availability
  follows readable, redirects follow delivery without independent URL validation,
  and concrete read-model reuse differs from per-call fallback construction.
  SVG documentation distinguishes escaped title text and clamped font size from
  unvalidated dimensions and expanding Unicode uppercase initials.
- [images/__init__.py](../src/LiuXin_alpha/surfaces/images/__init__.py): backend/host
  re-exports without claiming a raster transformation or thumbnail-cache service.

Tests are part of the full-project documentation scope:

- [test_images_api_contracts.py](../tests/surfaces/test_images_api_contracts.py):
  document every double, helper, method, and test, preserving executable code.
  Examples exercise deterministic discovery/acquisition failures and naming/SVG
  contracts without network access or raster rendering.
- [test_images_api.py](../tests/surfaces/test_images_api.py): newly document its
  module, seven catalogue fixture helpers, and two driver-parametrized integration
  tests. Explain the real work/expression/manifestation/item/cover graph, shared
  backend identity, and Core-backed local-byte read. The PNG signature fixture is
  not a complete renderable image, and helper metadata insertion is not ingestion.
- New [test_category_thumbnail_documentation_contracts.py](../tests/surfaces/test_category_thumbnail_documentation_contracts.py):
  fully documented doubles, nested composite callback, and focused contracts for
  the legacy edge cases above. Temporary directories contain all cache writes,
  corrupt-order bytes, and deliberate removals; user cache/configuration is untouched.
  Image cases also cover model reuse, blank redirects, alias precedence, nested
  sharing, expanding initials, and retained invalid dimensions.

Batch: **8 modules, 8 classes, 98 functions = 114 declarations**. Cumulative
reviewed Python scope: **171 modules, 232 classes, 1,756 functions = 2,159 declarations**,
plus the separately documented native C and runtime-created vacuum adapter.

## Verification

Overlapping selections must not be added into a distinct-test total.

- First recovered new-contract run: **27 passed, seven explicitly skipped examples,
  one failed**. The failure disproved a test prediction that removing the final
  OrderedDict entry would also raise. Corrected the new test and clarified the
  helper docstring; no production implementation was changed.
- Final eight-file source/test/doctest selection: **101 passed, 44 explicitly
  skipped examples** in 30.14 seconds. Includes the real-database image integration
  module as well as in-memory edge contracts. Collection confirms the two image
  integration cases used SQLite in this environment; this is not an APSW or
  PostgreSQL execution claim. Skipped fixture-dependent examples are not additional
  integration executions.
- Strict audit and normalizer --check pass across **all 171 reviewed files**,
  including the tracked data-submodule generator. The eight-file batch has no
  structural, missing-description, parameter, return, example, or delimiter findings.
- All seven edited existing Python files have identical executable ASTs to HEAD
  after genuine leading docstrings are stripped. No runtime-documentation exception
  is needed in this batch. The new test module is separately formatter/lint-clean.
- Ruff comparisons against HEAD add no findings. Existing out-of-scope findings
  remain: categories UP010/I001/UP029/UP004/two UP031/E722/E731/E721/B007;
  icons UP031; thumbnail UP010/I001/UP004/twelve UP024/twelve F821/three UP031;
  image API I001/two UP045/UP032; image integration test I001. The package initializer
  and existing image-contract test are clean. No warning was waived or incidentally fixed.
- Full `bash scripts/run_type_checks.sh`: **passed** with unchanged scopes of
  159 formatter files, 456 annotated modules, 221 protected dependency modules,
  188 strict-mypy files, and 37 negative examples rejected by each checker.
  Selected formatting, lint, complexity, dependency, and production typing checks pass.
- An initial broader pytest command named a nonexistent acquisition-contract
  file and collected no tests. It was corrected to the discovered acquisition
  integration module. The replacement migration/public-documentation/link,
  shared-boundary, acquisition, read-failure, and ownership selection completed:
  **279 passed, five failed** in 83.51 seconds. All failures are the existing
  Core service/facade and terminal component/root physical-line guards; no new
  behavioral failures or socket-permission errors occurred.
- Final public-documentation/developer-navigation suites after the handoff edits:
  **23 passed** in 17.42 seconds. All **149 local destinations across ten
  working-memory notes** resolve. Root and data-submodule diffs are whitespace-clean,
  and the staging area is empty. The workflow-owner test is unchanged from HEAD;
  the terminal-owner test's executable AST is unchanged after stripping its docs.
  All observed audit, test, lint, formatting, and quality handles have completed;
  no pending verification result is claimed as a pass. No commit, push, submodule
  publication, or PR mutation was made.

The five previously observed Core/terminal physical-line ownership failures
remain documented separately from functional results. Their limits and executable
assertions are unchanged; this batch does not repair or hide them. No full-project
pytest, live PostgreSQL, displayed GUI, remote-CI, or installed-wheel result is claimed.

## Inventory and next work

The refreshed whole-project inventory contains **2,728 modules, 4,400 classes,
35,096 functions**, with no parse failures. Missing/blank docs remain on **1,116
modules, 2,036 classes, 23,277 functions = 26,429 declarations**. Additional overlapping
findings: 4,292 delimiter-layout, 10,550 missing-example, 3,060 parameter-field,
4,002 return-field, 4,140 empty-parameter, 5,577 empty-return, and 28 missing-summary.

Reports: `/tmp/liuxin-docstring-current-2026-09-09.json` and
`/tmp/liuxin-docstring-reviewed-2026-09-09.json`. The full objective remains active
and unfinished. Next are the complete shared read-model, catalogue, acquisition,
and OPDS implementations, then CLI/HTTP/web and the remaining project source,
scripts, examples, tests, and inherited code. The 793-line read-model API has only
been inspected in dependency excerpts and a declaration inventory, not reviewed
or documented as a complete module. Preserve configuration-exec compatibility
checks and tracked submodule scope from the main ledger.
