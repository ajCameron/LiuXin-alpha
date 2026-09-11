# Shared read-model docstrings — 2026-09-09

Continuation of the active [whole-project descriptive reST goal](project-docstrings-2026-09-08.md),
following [category, thumbnail, and image documentation](project-docstrings-category-images-2026-09-09.md).
The previous goal turn made verified progress. This turn confirmed the read-model
source/test targets were clean, read them completely, and documented their actual
contracts. Work remains local on `codex/project-docstrings`, based on `edf6bf05`.
No commit, push, submodule publication, or PR change is part of this continuation.

## Reviewed source and tests

- [read_model/api.py](../src/LiuXin_alpha/surfaces/read_model/api.py): the full
  previously 793-line module, its dataclass, and all **54 functions**, including
  the nested file-deduplication helper and all image delegates. The implementation
  was not split, omitted, or exempted because of its size. Documents borrowed
  collaborators; refresh receipts without local schema invalidation; confirmed
  absence versus visible read/iteration failures; first-match row identity;
  ordered/fallback relationship grouping; and work-agent role/priority ordering.
  Optimized queries fall back only on explicit incompleteness or missing sort
  fields. Fast pagination clamps converted values, while fallback slices original
  values and can issue another work-order query. These are retained distinctions,
  not normalization repairs.

  Browse documentation covers per-agent-table ordering, tags/labels global
  precedence, materialized counts, placeholder rating sorting, shallow copies,
  and noncanonical selector echoes. Metadata documentation covers direct-first
  file discovery, last-row replacement without position changes, truthy zero-size
  fallback, duplicate format tokens with last-value format metadata, unconditionally
  advertised format download routes, explicit placeholder fields, repeated reads,
  and HTML escaping versus URL quoting. Search describes unknown-table fallback,
  host-side matching, complete/incomplete candidates, and full-result counts before
  slicing. Image wrappers retain argument/result identity; static thumbnail initials
  intentionally bypass an injected image adapter.
- [read_model/__init__.py](../src/LiuXin_alpha/surfaces/read_model/__init__.py):
  backend/host-protocol exports without construction, Core reads, or lifecycle ownership.
- [test_read_model_failure_contracts.py](../tests/surfaces/test_read_model_failure_contracts.py):
  document the strict recording fixture, receipt helper, all failure/absence tests,
  failing iterator, nested row/number doubles, and AST catch-all prohibition.
  The fixture example uses request.getfixturevalue rather than directly invoking
  a decorated pytest fixture. Executable assertions remain unchanged.
- [test_read_model_transport_errors.py](../tests/surfaces/test_read_model_transport_errors.py):
  document direct and real loopback HTTP error parity, narrow cache-capability
  translation, exact query counts, nested failure handlers, and finally cleanup.
- [test_read_model_api.py](../tests/surfaces/test_read_model_api.py): all twelve
  composition/entity/asset helpers and five integration tests. Describe real
  WEMI graph persistence and shared payloads, tags over labels, explicitly disabled
  database fallback in a loaded cache, and local cover-byte delivery. Fixture
  ebook/PNG bytes are not claimed to be valid parsed publications or images.
- [test_read_model_metadata_parity.py](../tests/surfaces/test_read_model_metadata_parity.py):
  all five value/relation/graph helpers and three real-database parity tests.
  The permissive test-only Row/mapping/attribute accessor is distinguished from
  production failure policy. Optional facet-ID keys, no ingestion/rollback, and
  WorkMetadata versus item-rooted WEMI comparisons are documented explicitly.
- New [test_read_model_documentation_contracts.py](../tests/surfaces/test_read_model_documentation_contracts.py):
  one fully documented collaborator helper and ten focused tests, all with executed
  examples. Cover query-free construction and retained schema after refresh,
  first matching row identity, different pagination paths/requerying, credit role/
  priority order and shallow sharing, host grouping identity, global tag selection,
  placeholder category ratings, duplicate file formats, capability-dependent versus
  unconditional routes, repeated subtitles, missing-ID visible rows, last-value
  metadata keys, unknown-table search fallback, full-result group counts, and all
  image delegation identities. No real storage or network is used by these tests.

Batch: **7 modules, 3 classes, 112 functions = 122 declarations**. Cumulative
reviewed Python scope: **178 modules, 235 classes, 1,868 functions = 2,281 declarations**,
plus the separately documented native C and runtime-created vacuum adapter.

## Verification

Overlapping selections must not be summed into a distinct-test total.

- Initial source/failure doctests and all eight database-backed API/parity cases:
  **128 passed, 63 explicitly skipped examples** in 119.28 seconds. This preceded
  adding docstrings to the API/parity fixture modules; their executable code was
  subsequently verified unchanged. A final combined run covers those additions.
- New documentation-contract tests and examples: **21 passed** in 8.68 seconds.
- The first loopback transport run failed all five cases at socket creation with
  sandbox PermissionError, not at a read-model assertion. The permission-approved
  rerun completed: **five passed, two explicitly skipped examples** in 12.16 seconds.
  No failure was waived, and no external endpoint was contacted.
- Strict structural audit and normalizer --check pass for all **178 reviewed files**,
  including the data-submodule generator. The new seven-file batch is also clean.
- All **six edited existing Python files** have identical executable ASTs to HEAD
  after genuine leading docstrings are stripped. No runtime-documentation exception
  is needed here. Doc-only formatting of the maintained failure/transport tests
  preserves their assertions and nested handlers.
- No new Ruff findings against HEAD. Existing out-of-scope read-model findings
  remain I001/UP035/F401/seven UP045/three E731/eight UP032; API and metadata-parity
  tests each retain I001. The initializer, failure/transport tests, and new contract
  module are clean. The new module is independently formatter-clean.
- Full `bash scripts/run_type_checks.sh`: **passed** with unchanged scopes of
  159 formatter files, 456 annotated modules, 221 protected dependencies, 188
  strict-mypy files, and 37 negative examples rejected by each checker. Selected
  lint, complexity, dependency, formatting, and production typing gates pass.
- Final permission-approved combined source/read-model/image doctests, all five
  read-model test modules, acquisition/catalogue integration, migration, and
  public-documentation selection: **261 passed, 91 explicitly skipped examples**
  in 151.11 seconds. All eight read-model API/metadata-parity integration cases
  were collected for SQLite; this is not an APSW or PostgreSQL execution claim.
- Final public-documentation/developer-link tests after the handoff edits:
  **23 passed** in 22.45 seconds. All **158 local destinations across eleven
  working-memory notes** resolve. Root and data-submodule diffs are whitespace-clean;
  the staging area remains empty. All observed audit, test, lint, format, AST,
  and quality handles have completed. No unobserved or still-running result is
  counted as success; no verification job from this turn remains pending.

The five previously verified Core/terminal physical-line ownership failures are
unchanged by this batch, not newly fixed. The quality runner does not execute
those pytest guards. No ownership logic, limit, or default quality scope changed.
This is not a whole-project pytest, live PostgreSQL, interactive UI, installed-wheel,
or remote-CI success claim.

## Inventory and next work

The full audit now covers **2,729 Python modules, 4,400 classes, 35,107 functions**,
with no parse failures. Missing/blank docs remain on **1,114 modules, 2,034 classes,
23,182 functions = 26,330 declarations**. Other overlapping findings: 4,281
delimiter-layout, 10,543 missing-example, 3,055 parameter-field, 3,996 return-field,
4,140 empty-parameter, 5,577 empty-return, and 28 missing-summary findings.

Reports remain `/tmp/liuxin-docstring-current-2026-09-09.json` and
`/tmp/liuxin-docstring-reviewed-2026-09-09.json`. The entire project goal remains
unfinished. Next shared packages are catalogue, acquisition, and OPDS (each has
an api.py and initializer), then CLI/HTTP/web and the rest of the project. Current
catalogue/acquisition tests were selected for regression but have not thereby
become documented/reviewed-file claims. Preserve the configuration-exec dictionary
warning and tracked submodule inventory from the main ledger.
