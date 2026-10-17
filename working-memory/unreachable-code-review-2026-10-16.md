# Unreachable-code review and fixes — 2026-10-16

Scope: fix the concrete unreachable-code review findings, review the broader
import-graph candidates, and checkpoint the result on `catalog-write-consolidation`.
The separate PostgreSQL consolidation signoff remains pending in
[catalog-write-consolidation-2026-10-15.md](catalog-write-consolidation-2026-10-15.md).

## Behavior restored

- `Catalog.apply.tag` resolves a whole string before considering iterables. Row,
  text and generator inputs use the shared relationship owner; invalid inputs and
  later persistence failures roll back all creations and links. Resource validation
  precedes resolution, and duplicate linking remains idempotent.
- `CustomColumns` now resolves the cooperative CRUD mixin before its backend
  adapter. Create/edit refresh both definition maps, including the stored `editable`
  key, names, labels and display settings. Notification suppression works, optional
  preference updates run after success, and deletion accepts a label or positional
  numeric ID. Attachment defaults to the facade's table. Refresh clears its pending
  trigger/removal lists rather than accumulating duplicate bookkeeping.
- The shared hydrator test double now implements portable column extrema, matching
  the relationship-priority contract introduced in the preceding checkpoint.

## Proven dead code removed

- 46 superseded definitions across Database/Driver API mixins, the customization
  Cache API and the duplicated web store-detail renderer. The final active
  definition/property accessor is preserved.
- Five unconditional unreachable blocks in ODF-to-MoinMoin, ODF-to-XHTML,
  RTF token processing and vendored dateutil timezone code.
- Six numbered copy artifacts and unused `utils/plugins/linux/images_rc.py`
  (7,497,537 bytes of generated Qt resources). The copy artifacts had 5,279 lines.
- Eleven isolated Library modules (4,738 lines): unused Library API, backup/restore,
  memory SQLite cache and NumPy cache prototypes. Searches found no inbound
  consumers outside that retired group, including tests, examples and registrations.
- Wheel verification rejects numbered copies, the retired Qt resource file and
  retired Library paths. Eleven dynamic-import baseline entries belonging to
  deleted copies were removed.

## Broader reachability disposition

The initial graph covered 1,816 source modules. Roots were all 228 Core/surface
modules, following runtime/deferred imports and package parents. It reached 1,407
modules, with 39 unresolved dynamic imports. This is an application-root inventory,
not proof that every other library module is unused. Of 154 Library/file-format
candidates, 68 had direct test/tool references; supporting dependencies and dynamic
entrypoints also matter. After removal, the architecture gate sees 1,798 modules
and 28 unresolved dynamic imports.

| Retained family | Consumer or contract |
| --- | --- |
| `library.legacy` and its backend/support dependencies | `tests/library/test_legacy_identifier_contract.py`; frozen compatibility contract in the Catalog handoff |
| Base/Calibre cache fields and relation tables | `tests/databases/caches/test_cache_imports.py`, `test_calibre_cache_06_custom_column_semantics.py`, `test_calibre_cache_07_relation_field_semantics.py`; older opt-in cache suites remain disabled under FRBR |
| `library.library_metadata` | `tests/catalog/test_catalog_aggregate_mutations.py` |
| `library.comments` | `examples/utilities/comments_to_html_example.py` |
| Markdown extensions | Dynamic named-extension resolution in `file_formats/markdown/__init__.py`; Markdown and TXT conversion tests |
| ODF, OEB, readability and format helpers | Public constructors and direct format regression suites; not all are reachable from application imports |
| DOCX dump, LRF converters and eReader inspector | Standalone command entrypoints; absence from Core imports is expected |

Deleting those remaining families requires a deliberate compatibility/feature
retirement, not a broader static-graph sweep.

## Validation

The complete quality script passed in an isolated checkout: 169 formatting targets,
463 file-format annotation targets, architecture/import-cycle checks, 21 cold-import
budgets, lint/complexity, basedpyright, mypy (188 files), and both internal contract
checks (37 rejected invalid examples each).

The broad regression selection covers all Catalog/Library/cache tests, custom-column
facade/storage contracts, read-only web, wheel verification, documentation links,
plugin resolution, ODF and RTF regressions. It recorded **1,156 passed, 20 skipped**,
with five teardown errors caused by running the quality/build commands concurrently:
the test root-artifact guard removed newly created `.ruff_cache`, `.mypy_cache` and
`build` directories. This also invalidated the first wheel build. The affected tests
and wheel verification are rerun sequentially; final outcomes are recorded below.
Skips include eight unconfigured live PostgreSQL cases, six opt-in legacy cache
modules and backend-specific cache behavior. This does not certify PostgreSQL.

Concurrent documentation moves and plugin/storage comments, and the existing data
submodule changes, belong to other work and are excluded from this commit. The
isolated checkout was used to keep validation attributable to this change.

Final sequential checks:

- The five affected tests reran successfully: **5 passed** in 21.55 seconds.
- A fresh wheel passed `scripts/verify_wheel_install.py`: **1,790 Python files,
  403 runtime assets**, retired files absent; temporary target installation,
  catalogue creation/reopening and HTML-to-EPUB conversion all passed. Dependency
  origins were verified inside the installed target.
- AST comparison confirms the final active method signatures, decorators and
  implementation bodies are unchanged in all eight duplicate-cleanup files.
  Excess blank lines left by removal were collapsed without changing their AST.
- Source/tests in the main checkout match the isolated verification snapshot.
  Logs are retained locally in `working-memory/test-results/code-review-2026-10-16/`
  and remain gitignored. The tracked results above travel with the branch.

The concrete unreachable-code findings are resolved. PR #118 remains a draft for
its separate, previously recorded PostgreSQL and entrypoint-audit work. Resume that
work from the Catalog consolidation note on a stable machine.
