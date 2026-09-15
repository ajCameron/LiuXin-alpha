# Remaining example documentation — 2026-09-12

## Scope and checkpoint

Continues the unfinished whole-project goal after the
[storage/catalog examples](project-docstrings-storage-catalog-examples-2026-09-11.md).
Work remains on `codex/project-docstrings` at `9790d020`; no commit or push by this
continuation. Existing working-memory and data-submodule changes are preserved.

Read and documented the five conversion modules, Library facade example, three
metadata examples, and comments-to-HTML example. All ten files matched HEAD before
editing. Their **10 modules, 2 classes, and 50 functions = 62 declarations** include
all three nested shim callbacks, private projections/queue/options helpers, and
logger/metadata-stub methods. Source payload strings and interpreter headers remain.

Cumulative: **616 modules, 807 classes, 6,866 functions = 8,289 declarations**.
The [reviewed manifest](project-docstrings-reviewed-files.txt) now includes every
tracked Python file under examples: **28 modules, 3 classes, 90 functions**. This
completes the examples tree's source review/documentation, not the whole-project goal.

## Contracts clarified

- The conversion scratch context updates one process-global setting and restores
  it before directory cleanup. Cached _base_dir values and imported aliases can
  still direct legacy temporaries elsewhere. The customize.ui shim replaces a
  sys.modules entry without restoration; its input sentinels and filename-derived
  metadata are not a full registry or source metadata parser.
- ExampleLog ignores keyword arguments, prints to stdout only when verbose, and
  adds no traceback in exception(). MetadataStub explains empty-author fallback,
  mutable defaults, explicit emptiness rules, copied identifiers, and series-index
  display. Option factories share one mutable profile within each returned namespace.
- Batch output names can collide across source directories. Children run
  sequentially with captured output and no timeout; nonzero exits are reported
  while launch failures propagate. --clean-output can remove earlier output when
  names collide. A successful earlier report does not prove its files remain.
- OEB conversion describes extension dispatch, recommendation/default precedence,
  duck-typed result acceptance, OPF path handling, optional postprocessing, and
  output cleanup/retention. Fixed XML sample strings are not documentation literals.
  EPUB/MOBI reports print a planned work_dir_cleaned flag before best-effort removal;
  they do not prove cleanup succeeded or fail merely because reported output size is zero.
- Library's helper updates only allowed fields on the first exact-root row, while
  creation supplies all fields. Refresh and name-based Store selection occur later.
  Main reports retrieval length/preview without an equality assertion or bootstrap.ok
  exit branch; completed database/filesystem effects remain after later failures.
- Metadata display limits apply after discovery: Google drains at least one queued
  item when its limit is nonpositive, while the aggregate pipeline can display no
  entries and still report a positive total/success. Only identifier conversion is
  guarded in projection helpers. OpenLibrary forwards invalid ISBN text when
  normalization fails, records queue receipt as cover_found, and uses the selected
  image-save helper or raw-byte fallback without expanding the destination.

## Verification

- All ten executable ASTs match `9790d020` after stripping only leading literal
  docstrings. Ruff remains **30 to 30**, with no introduced finding. Original
  interpreter headers are byte-identical; no runtime-doc exception is added.
- Final strict audit/normalizer passed for the batch and the full **616-file
  reviewed set**, including the scratch-setting qualification. Final AST/header/
  Ruff checks pass after that clarification as well.
- All nine before/after --help stdout/stderr/status results are identical.
- Example doctests plus existing inventory/category-help contracts: **31 passed,
  25 skipped**, 147.65s. Skipped examples describe side effects or external objects.
- Adjacent Library/Google/identify/OpenLibrary/conversion regression assertions:
  **67 passed**, plus one teardown error caused by the validation interaction below.
  Migration/public-documentation/link assertions: **38 passed**, plus one teardown
  error from the same interaction. The two affected cases both passed on rerun:
  **2 passed**, 34.87s. The original exit-1 logs remain; those runs are not relabelled
  as clean runs or added twice to unique-test totals.
- Full quality runner passed: 159 formatted files, 456 annotated modules, 221
  protected dependency modules, lint/complexity, both production type checkers,
  188 strict-mypy files, and both sets of 37 invalid-call examples.
- Local CLI execution initially completed TXT-to-OEB, then reported one failed TXT
  child and one successful HTML child in the batch. The corrected harness completed
  **six local workflows and two no-query validation paths**. TXT and both batch
  inputs produced OEB output; EPUB produced a valid ZIP with the required mimetype;
  EPUB/MOBI files were nonempty and their automatic work directories were absent
  after process exit. Library retrieved the expected 20 UTF-8 bytes and text;
  comments rendered both paragraphs. Google/aggregate identify returned the expected
  status two before query execution. No live metadata endpoints were used.
- Independent inventory/recount confirms all 28 tracked examples in the 616-file
  manifest. Root and data-submodule diffs are whitespace-clean, local handoff links
  resolve, and the repository scratch directory from the failed harness is absent.

The original standalone CLI harness ran concurrently with pytest and inherited the
checkout's default LiuXin_scratch path. Pytest's root-artifact guard detected and
removed that newly created directory, producing two teardown errors and removing
the TXT child's intermediate HTML. This is a validation orchestration error; the
guards and runtime implementation are unchanged. The corrected CLI harness sets
LIUXIN_BASE_DIR, LIUXIN_PREFS_DIR, and LIUXIN_CONFIG_DIR to owned temporary paths and
uses a temporary cwd before starting subprocesses. Original logs remain intact.

Durable results use `working-memory/test-results/` prefix
`docstrings-remaining-examples-2026-09-12-` with doctests, regression, quality,
contracts, cli, cli-isolated, and guard-recovery log/done pairs. CLI child logs use
separate original/isolated names. Static reports:
`/tmp/liuxin-remaining-examples-ast-lint-2026-09-12.json`,
`/tmp/liuxin-docstring-reviewed-2026-09-12.json`, and
`/tmp/liuxin-docstring-current-2026-09-12.json`.

Whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**, no parse
failures. Missing/blank docs remain **1,066 modules, 1,895 classes, 20,136 functions
= 23,097 declarations**, down 52. Overlapping findings: 3,550 delimiter layout,
9,716 examples, 2,777 parameter fields, 3,656 return fields, 3,151 empty parameter
descriptions, 4,119 empty return descriptions, and 28 missing summaries.

## Next

The [Catalog foundation continuation](project-docstrings-catalog-foundation-2026-09-12.md)
has now completed the five initial source modules and two adjacent test modules,
bringing the reviewed set to 623 files. The inventory below describes this example
batch's original handoff; consult that newer note for current verification and work.

Continue into catalog source, beginning with package/API exports, common candidate/
result types, Catalog APIs, and the concrete facade, then its other owners and tests.
Inventory only: **113 catalog source modules, 144 classes, 860 functions**, none yet
in the reviewed manifest. Related catalog examples do not count as review of their
implementation dependencies. Preserve the full remaining source/test/script/
inherited/data-submodule scope, the three narrow runtime-doc exceptions, all seven
unresolved ownership-guard failures, and the legacy FRBR fingerprint follow-up in
the [main ledger](project-docstrings-2026-09-08.md). The goal stays active.
