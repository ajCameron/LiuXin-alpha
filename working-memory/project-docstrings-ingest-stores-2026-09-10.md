# Store ingest, reports, and legacy discovery docstrings — 2026-09-10

Continuation of the [whole-project descriptive reST goal](project-docstrings-2026-09-08.md),
following the [application/operator batch](project-docstrings-ingest-application-operator-2026-09-10.md).
The previous goal turn made verified progress. Rechecked target paths against the
worktree, reread the style guide, and fully reviewed four source modules and two
regression modules before documenting every named declaration.

Branch remains `codex/project-docstrings`, based on `edf6bf05`. These edits change
documentation only, not runtime logic, annotations, imports, tests, guard policy,
or parser literals. No commit, push, or PR mutation is part of this batch.

## Reviewed files and contracts

- [ingest/__init__.py](../src/LiuXin_alpha/ingest/__init__.py): lazy exports cache
  original owner objects; direct __getattr__ calls still resolve the owner again.
  __dir__ sorts globals plus exports without deduplicating cached names. Imports
  resolve symbols but do not themselves start acquisition.
- [models.py](../src/LiuXin_alpha/ingest/models.py): document all seven classes and
  their fields/properties. Remote registration counters are mutable and unchecked;
  epoch-millisecond duration may be negative after clock changes. to_dict recursively
  copies dataclass data but performs neither JSON serialization nor redaction.
- Store result records are frozen containers, not independent validators. Report
  ok tests only the failures tuple; it does not prove complete enumeration, no
  skipped/unseen objects, no next page, or durability. A failed first page may have
  next_cursor=None, so that value alone is not completion evidence. Checkpoints
  extracted from failures are retained references, not file checks.
- Object checkpoint construction checks Store/Location agreement, guarded read
  mode, version presence, size ranges, SHA-256 shape, and a single safe basename.
  It does not verify bytes or source version. Empty version text passes the
  non-None test; bytes.fromhex accepts embedded whitespace; numeric checks do not
  enforce integer types. The error wrapper retains the original cause without
  setting Python exception chaining itself or sanitizing its message.
- [stores.py](../src/LiuXin_alpha/ingest/stores.py): all 25 functions, including
  nested _consume, now describe selection, preparation, publication, pagination,
  checkpointing, metadata, and failure boundaries. Independent Store instances
  are valid copy sources; adoption always resolves an attached source. Explicit
  destinations must be attached. Caller-owned manager/Stores are not closed here.
- Extension selection occurs both before and after preparation/stat. The first
  selection is outside object-error handling; unsupported stat alone falls back
  to listing metadata. Continuation catches object Exceptions, not setup,
  enumeration, initial-filter, or BaseException failures. Error text is retained
  without generic scrubbing. Earlier successful mutations are not rolled back.
- max_files counts listed entries, including skips/failures. A recorded failure
  completes its paged batch then retains the input cursor and stops. Results stay
  in inventory order with threads; shutdown waits for submitted work after errors,
  so other objects may already have completed. Automatic worker selection can
  downgrade copy concurrency for a nonconcurrent destination; explicit requests
  fail instead. No independent manager thread-safety probe is performed.
- Staging directory creation precedes later setup validation; existing permissions
  are not tightened. Resume indexing validates type/source/unique Location, not
  files. Retained-prefix acquisition checks prepared claims and existing identity,
  then appends one-MiB reads. A fully staged known-size object skips reopening
  the source. Known size plus authoritative SHA-256 selects the identified stream
  fast path; other cases use ordinary stream ingest.
- Overflow attempts removal before raising; ordinary acquisition/manager failures
  with a remaining stage fsync/hash it and raise a checkpoint wrapper. Failures
  while generating that checkpoint can mask the original error. Initial creation,
  stat, validation, and BaseException failures are outside that catch. Success
  ignores unlink OSError and may leave a complete stage. No total staging quota
  or all-operation transaction is added.
- Staged-file symlink/regular-file checks and later opens are separate observations,
  not race-free access. Prefix hashing follows normal filesystem lookup without
  locking/change bounds. _checkpoint_for_file append-open can create a missing
  stage; file fsync does not establish directory persistence.
- Inventory rejects nonadvancing/repeated tokens, oversized pages, and global
  page/entry limits before yielding each batch. Empty advancing pages and changing
  snapshot tokens are allowed. Token bounds count characters rather than UTF-8
  bytes; nonempty whitespace/NUL tokens are not rejected. No Location deduplication
  occurs. A bare extension string is treated as an iterable of characters.
- Provenance filtering omits userinfo or known sensitive query names; it is not
  comprehensive secret removal. Unknown names/fragments survive. Derived metadata
  fingerprints sensitive Location keys, but arbitrary hint metadata and overrides
  remain trusted. Placement merging can retain preexisting sensitive/conflicting
  keys even when a fingerprint is added or no new URI is supplied.
- [adding.py](../src/LiuXin_alpha/ingest/adding.py): document the retained legacy
  contracts without repairing behavior or annotations. Readable nondirectory paths
  can include symlinks/special files. listdir preserves relative roots; discovery
  makes its root absolute but does not resolve symlinks. Rule order is first-match,
  regex uses match rather than search, and no match returns None despite bool typing.
- Discovery yields lists of per-format path lists, not the flatter annotated shape.
  All same-format paths are retained; OPF-only groups are omitted. single_fmt warns
  and is reset before its historical branches. Multi-book import accepts but ignores
  compiled_rules. Single-book import ignores callback returns; multi-book import
  uses truthy title callbacks to stop that directory, while recursive import uses
  a separate empty-string callback to stop the outer walk.
- Legacy adapters lazily require LiuXin_alpha.metadata.meta, absent from this
  checkout. Documented algorithms are not claims of working real metadata imports.
  Catalog/news format attachment runs after releasing the metadata write lock and
  cannot roll earlier metadata back here. Catalog reuses a non-rewound stream;
  news-owned streams close only on normal completion. These are preserved limits.
- [test_store_ingest.py](../tests/ingest/test_store_ingest.py): real temporary
  filesystem bytes with in-memory manager metadata; HTTP response doubles, not
  live requests. Document filtering/deduplication/publication refusal, bounded
  HTTP-layer diagnostics, close failure, and version-pinned prefix resume. Parallel
  assertions count results but do not measure maximum active threads. The interrupted
  response is tailored to positive-sized reads; negative reads can exceed its prefix.
- [test_adding_api.py](../tests/databases/adding/test_adding_api.py): real filename
  grouping on arbitrary-text fixtures, not valid ebook parsing. Recursive routing
  patches child importers; their eagerly evaluated positional default is documented.
  No real legacy database write or metadata extraction is claimed.

Batch: **6 modules, 13 classes, 90 functions = 109 AST declarations**. Cumulative:
**279 modules, 293 classes, 3,100 functions = 3,672 reviewed AST declarations**,
plus the existing native-C documentation and three runtime-doc metadata exceptions.

## Verification

- Strict batch audit is clean. The normalizer initially requested one preserved
  :type field be placed after :return; that documentation-only adjustment was made.
  Strict audit and normalizer now pass for all **279 reviewed Python files**, including
  the data-submodule generator.
- All six executable ASTs match HEAD after removing only leading literal docstrings.
  Ruff findings remain identical by code/message: initializer 1, models 1, stores 5,
  adding 22, and both test files zero. No new runtime-doc exception was needed.
- Full `bash scripts/run_type_checks.sh`: **passed**. Scope remains 159 formatter
  files, 456 annotation-covered modules, 221 protected dependency modules, 188
  strict-mypy files, and 37 invalid examples rejected per checker. Do not treat
  selected formatter/lint coverage as a whole-tree clean claim.
- Six-file doctests plus remote-discovery-pathology and storage/operator CLI tests:
  **111 passed, 85 explicitly skipped integration examples**, 74.13 seconds.
- Adjacent advanced-ingest API, S3-double, encrypted-storage, and unified-library
  regressions: **97 passed**, 49.62 seconds. These are local/injected checks, not
  live S3, credentials, or an external endpoint.
- Migration/public-doc/link/ownership selection: **62 passed, 6 failed**,
  49.43 seconds. The same six known docstring-sensitive guard parameters fail:
  Core services and facade, CLI storage owners, browser components, windowed
  components, and windowed_ui.py. This batch adds no owner-guard conflict and
  changes no size/statement-count logic or limits.
- Focused temporary/mock checks passed lazy-export duplication and rejection,
  negative duration and copied containers, checkpoint normalization/validation
  limits, unchained error construction, report health with a pending cursor,
  bare-string extension behavior, Unicode/NUL token acceptance, and known-key-only
  provenance filtering with retained arbitrary metadata/placement hints.
- Further checks passed changed snapshot acceptance, failed-first-page retry with
  cursor None and one successful sibling, existing staging-mode preservation,
  retained complete files after manager failure, retry without reopening a fully
  staged source, ignored success-unlink error, checkpoint creation of a missing
  empty file, and rejected symlink checkpoints.
- Legacy checks verified the absent metadata.meta module and first-match/regex
  behavior. A temporary injected metadata module checked ignored multi-book rules,
  added-ID mutation, and stop-on-title callback semantics; that is explicitly a
  mocked adapter check, not repaired or live legacy metadata integration.
- All **292 local link destinations across twenty-six working-memory files**
  resolve. Root and data-submodule diffs are whitespace-clean, staging is empty,
  and every verification process has reached a terminal state.

No whole-project pytest, installed-wheel, remote CI, external service, interactive
UI, or live network result is claimed. Passing behavioral/quality checks do not
erase the separately recorded ownership failures; no guard was relaxed.

## Inventory and next work

Whole inventory: **2,730 modules, 4,400 classes, 35,119 functions**, no parse errors.
Missing/blank docs: **1,099 modules, 2,011 classes, 22,098 functions = 25,208
declarations**. Additional overlapping counts: 4,112 delimiter-layout,
10,372 missing-example, 3,013 parameter-field, 3,946 return-field, 4,061 empty-param,
5,491 empty-return, and 28 missing-summary. Reports:
`/tmp/liuxin-docstring-ingest-stores-baseline-2026-09-10.json`,
`/tmp/liuxin-docstring-ingest-stores-2026-09-10.json`,
`/tmp/liuxin-docstring-reviewed-2026-09-10.json`, and
`/tmp/liuxin-docstring-current-2026-09-10.json`.

Next: the ten remaining ingest source modules: remote_html.py, both pipelines
modules, and all seven sources modules. Then fully review their regression
helpers, including test_remote_discovery_pathologies.py (only its first 160 lines
were read here). Advanced-ingest API excerpts and additional executed suites are
dependencies/regression evidence, not newly completed docstring-review claims.
Continue through the whole source/tests/scripts/examples/data-submodule scope;
the goal remains active and the branch remains uncommitted/unpushed.
