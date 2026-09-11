# Local Store plugins and regressions — 2026-09-10

## Scope and checkpoint

Continues the unfinished whole-project documentation goal after the
[SquashFS checkpoint](project-docstrings-squashfs-2026-09-10.md).
Branch remains `codex/project-docstrings`, based on `edf6bf05`, without a commit
or push. Existing unrelated changes and data-submodule work are retained.

Source-reviewed and documented in full:

- The backend-plugin package initializer and all modules in `on_disk_flat`,
  `on_disk_calibre_like`, `on_disk_existing_managed_drive`, and
  `on_disk_existing_unmanaged_drive`, including compatibility modules and aliases.
- All six adjacent test modules in `tests/storage/store_backend_plugins` under
  `on_disk_flat`, `on_disk_calibre_like`, `on_disk_existing_managed`, and
  `on_disk_unmanaged_drive`, including dictionary/database/hint doubles and the
  Location test class.

Newly completed: **26 modules, 10 classes, 84 functions = 120 declarations**.
Cumulative: **451 modules, 565 classes, 5,403 functions = 6,419 declarations**.
The [reviewed manifest](project-docstrings-reviewed-files.txt) contains 451 unique
paths. All 65 modules in `storage/store_backend_plugins` are now reviewed,
alongside the already completed seventeen raw-driver modules.

One previously reviewed file also has a prose correction: the SquashFS builder's
two collision-mode descriptions now say that supplying both mode and write_mode
rejects even if they agree. The inherited helper rejects their joint presence,
not only disagreement. The 27-file verification scope includes this correction;
it does not count that file again as newly completed.

## Contracts clarified

- Constructors configure filesystem roots and policy without creating/probing
  them. Managed/flat startup can create missing roots. Unmanaged construction can
  retain a missing root; startup reports unavailable without creating it.
- Managed automatic allocation uses `.liuxin-managed/objects`, while explicit
  keys can address other paths under the Store root. Reserved classification tests
  the `.liuxin-managed/` prefix and excludes the bare directory key; it is not an
  independent write restriction or existence check.
- Flat byte/file writes use a supplied digest or compute SHA-256, then choose
  `<value>.file` for false destination values. Direct inherited allocation retains
  the `objects` prefix. Stream fallback uses None only, so empty explicit stream
  keys reject. Names omit the digest algorithm, and existing keys follow collision
  policy without automatic deduplication. The misleadingly named historical
  `...and_dedupes` test explicitly asserts duplicate rejection in its new prose.
- The unmanaged single-file facade resolves the filename, binds its parent as a
  read-only Store root, and derives UUID5 from the resolved file URI. Binding does
  not require that particular file to exist. Its attributes remain mutable; streams
  returned to callers have their own lifetime. Managed single-file compatibility
  inherits this exact read-only implementation without overrides. The Calibre-like
  single-file compatibility name is a FileInfo alias rather than a reader wrapper.
- Location compatibility names all refer to the common Location value. Re-export
  modules preserve class identity rather than introducing adapters or extra policy.
- Calibre-like placement prioritizes a preferred key, then title/author/ID/stem/
  extension hints. No-title digest fallback uses `.liuxin/managed_drive`, distinct
  from the managed superclass prefix. Selected paths are neither reserved nor
  collision-checked during allocation.
- Calibre-like authors distinguish a present unusable primary_agents value from
  an absent one. ID selection skips booleans but accepts zero/negative integers;
  extra IDs omit manifestation_id. Extension selection considers only the first
  sequence element before trying later fields. Component cleaning preserves Unicode,
  imposes no length/reserved-name policy, and returns fallback text without cleaning
  it again. Downstream Location validation remains separate.
- File publication precedes the Calibre-like Store's second hint conversion and
  optional database update. A provider method Exception is suppressed by the
  shared derive helper, while attribute-lookup and other uncaught conversion errors
  can escape after publication. The second conversion occurs even without a database.
- Database updates skip missing database/file ID/getter/row cases. Top-level file_id
  takes priority, with extra considered only for None. Mapping assignments fall back
  to existing attributes only for the three caught exception types; other errors
  propagate. Row sync follows assignments and can fail after partial row changes and
  committed bytes. Setters retain database and Store-ID values without validation or
  retroactive synchronization.
- Test documentation distinguishes actual temporary filesystem bytes, in-memory
  manager registration, and fake row synchronization from live database persistence.

## Verification

- Strict audit and normalizer pass on all 27 touched files. Both checks also pass
  across the complete 451-file reviewed set after the final provider-boundary
  prose refinement.
- All 27 executable ASTs equal HEAD after removing only leading literal docstrings,
  including a final recheck after the prose refinement. Signatures, annotations,
  assertions, runtime strings, and policy limits are unchanged. No runtime-doc
  exception was added.
- All 27 Ruff result multisets match their baselines, without new findings,
  suppressions, formatter exceptions, or guard changes.
- Selected batch/filesystem doctests and local/archive/Store/API/ingest regressions:
  **298 passed, 251 explicitly skipped integration examples**, 38.96 seconds.
- Final Calibre-like source/test doctests and placement-hint regressions after the
  provider-boundary clarification: **28 passed, 19 explicitly skipped examples**,
  9.90 seconds. Skips are not executed integration proof.
- Full quality runner passed: 159 formatted files, annotation coverage across 456
  modules, 221 protected dependency modules, selected lint/complexity, both production
  type checkers, 188 strict-mypy files, and 37 invalid examples rejected by each checker.
- Migration, public-documentation, and developer-link contracts: **38 passed**,
  18.09 seconds.
- Isolated observations passed for missing-root startup, compatibility type identity,
  flat false/None destinations and duplicate rejection, managed-prefix classification,
  unreserved preferred-key allocation, partial database updates after file publication,
  suppressed provider-method errors versus propagated provider-lookup errors, duplicate
  collision-mode arguments, and hint-selection priorities. The initial observation
  incorrectly expected a provider-method RuntimeError to escape; source inspection
  corrected that assumption and refined the docs before the successful final run.
  Temporary files, Stores, and streams were closed within a temporary directory.
- The six known docstring-sensitive ownership-test failures remain outstanding.
  This batch did not rerun or weaken those unrelated guards.
- Root and data-submodule whitespace checks passed. All verification process
  handles completed with observed exit-zero results; the failed initial observation
  was corrected and rerun rather than counted as a pass.
- Final manifest recount confirms 451 unique modules, 565 classes, and 5,403
  functions. Local links resolve in the index (113), main ledger (3), this note
  (2), and the corrected SquashFS note (4). Placement-hint read-ahead remains
  unchanged from HEAD and outside the reviewed set.

Durable logs/exit metadata:
`working-memory/test-results/docstrings-local-plugins-2026-09-10-{regression,quality,contracts}.{log,done}`.
The follow-up log/marker records the observed result rather than inventing a process
start time. Isolated observations are saved in
`working-memory/test-results/docstrings-local-plugins-2026-09-10-observations.json`.
Static reports:
`/tmp/liuxin-docstring-local-plugins-batch-2026-09-10.json`,
`/tmp/liuxin-local-plugins-ast-lint-2026-09-10.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-10.json`.

Whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**, with no
parse failures. Missing/blank docs: **1,079 modules, 1,924 classes, 20,772 functions
= 23,775 declarations**. Overlapping findings: 3,812 delimiter layout, 10,226
missing examples, 2,910 parameter fields, 3,834 return fields, 3,613 empty parameter
descriptions, 4,758 empty return descriptions, and 28 missing summaries. Structural
counts alone do not establish descriptive completeness.

## Next: storage API values and placement hints

The raw driver, concrete Store, Store/raw-driver API, storage utilities, ingest,
reconcile, and backend-plugin source groups are complete. Storage still has 49
unreviewed API modules, including package exports, common models, characteristics,
error/location values, placement hints, persistence, manager contracts/models, and
workflow contracts. Manager implementations and the wider project backlog remain
in scope too.

Start with common API values and hints: `storage/api/models.py` (528 lines),
`characteristics_api.py` (214), `errors.py` (175), `location_api.py` (17),
`placement_hints_api.py` (995), API exports (488), and storage package exports
(168) / legacy errors (58), plus their actual adjacent tests.

The complete placement-hints module was read during dependency review, but remains
unedited and outside the completed manifest. Other common models only received
selected dependency reads and must not be counted fully reviewed. Carry these
placement-hint findings forward:

- Four frozen dataclass hint families add no field validation; extra mappings
  remain mutable. to_mapping uses dataclasses.asdict, with recursive copying rather
  than a shallow view. Two runtime protocols describe providers/relation sources.
- Existing known hint values and Mapping inputs pass through by identity. Provider
  lookup precedes the try block; only Exceptions from invoking the provider are
  suppressed to None. Invalid provider return types can fall through to structural
  WEMI projection. Projection dispatch order is item, work, manifestation, expression.
- Attribute/row mapping conversion is not generally guarded. Relation access catches
  KeyError/ValueError/TypeError around invocation and iterable consumption, but not
  property lookup or arbitrary runtime errors. Repeated relation retrieval can
  observe changing data; there is no per-projection snapshot or size bound.
- Work title falls back to the first expression display; agents are all distinct
  displays. Formats inspect manifestation/file/image links. Item targets prefer
  primary relation links even when the primary target is None; title precedence is
  expression, work, then item source basename stem. Item manifestation-ID fallback
  uses truthiness before integer conversion, so zero can select the item fallback.
- Item agents first filter primary links, then fall back to all when no displays
  remain. Formats combine manifestation/file/image/asset/replica sources. Preferred
  storage keys scan replicas, then files, then images. Counts can retrieve relations
  again independently of earlier selections.
- Expression language and manifestation title use the first relation entry; their
  agent helper includes primary links or the sole link, without general deduplication.
  Manifestation file_formats uses link display values rather than format-token parsing.
- Row mappings prefer row_dict, then callable to_mapping returning a Mapping, then
  Mapping itself. Display selection uses prioritized fields followed by insertion-order
  fallback excluding *_id and *_timestamp_ep_k, then str(value). Whitespace-only values
  are not consistently filtered. Format tokens are stripped/lowercased for uniqueness,
  uppercased for output, and can include an empty string after stripping whitespace.
- Optional integer conversion accepts booleans and truncatable floats. Only TypeError
  and ValueError are caught; infinite floats can raise OverflowError. Preferred-key
  helpers stringify but do not validate, trim, or reserve their result.

Do not reduce the goal to storage alone: all remaining metadata, catalogue/cache,
formats, source/test/script/example/inherited code, and tracked data-submodule Python
remain included. Preserve runtime-doc consumer cautions and the three narrow metadata
exceptions in the main ledger.
