# ISO reader, adapter, and regressions — 2026-09-10

## Scope and checkpoint

Continues the unfinished whole-project documentation goal after the
[7z checkpoint](project-docstrings-sevenzip-2026-09-10.md).
Branch remains `codex/project-docstrings`, based on `edf6bf05`, with no commit
or push. Existing unrelated edits and tracked data-submodule work are retained.

Source-reviewed and documented in full:

- [Raw ISO reader](../src/LiuXin_alpha/storage/drivers/iso.py): all records,
  extent readers, direct parser, UDF helpers, limits, naming, times, and errors.
- [Configured ISO Store](../src/LiuXin_alpha/storage/store_backend_plugins/iso_readonly/iso_readonly_storage_backend.py),
  [Location alias](../src/LiuXin_alpha/storage/store_backend_plugins/iso_readonly/iso_readonly_location.py),
  and [plugin initializer](../src/LiuXin_alpha/storage/store_backend_plugins/iso_readonly/__init__.py).
- [Complete ISO reader regression module](../tests/storage/store_backend_plugins/iso_readonly/test_iso_readonly_storage_backend.py),
  including three image helpers, 21 tests, two nested callbacks, and the fake UDF
  image class and its three methods.

Batch: **5 modules, 14 classes, 99 functions = 118 declarations**.
Cumulative: **407 modules, 537 classes, 5,075 functions = 6,019 declarations**.
The [exact reviewed manifest](project-docstrings-reviewed-files.txt) contains
407 unique paths. Fifteen of seventeen raw-driver modules are complete.

## Contracts clarified

- Direct ISO/Rock Ridge/Joliet reads use image extents; optional pycdlib UDF reads
  stage complete members before exposing ranges. Passive records validate neither
  extents nor size/path consistency. The extent reader trusts constructor inputs,
  requires exact byte reads, and retains partial progress when a later segment fails.
- Direct selection prefers a detected root SP/Rock Ridge marker, then highest
  Joliet, then primary ISO. Enabled UDF can replace a non-Rock-Ridge bridge
  projection. Only an ImportError-caused unsupported failure permits hybrid
  fallback; UDF-only integrity failures are reclassified to the explicit unsupported
  boundary. Recognition markers alone do not establish a valid UDF filesystem.
- Index construction returns its initial descriptor signature without a final
  restat. UDF opens the path separately. Direct reads check their opened descriptor;
  UDF reads close it, extract through pycdlib, then compare path metadata. Matching
  signatures are neither content hashes nor protection against later in-place writes.
- The UDF sink bounds current position plus offered bytes, allows overwrites and
  unbounded seeks, and borrows its destination. Materialization compares final
  position with indexed size, not independently measured file length or a digest.
  Post-extraction stat failures explicitly close the spool; parser construction
  after spool allocation occurs before the extraction cleanup guard.
- Parser state is retained across calls. Revisited directories are identified by
  LBA/data length without extended-attribute blocks. Directory payloads and tuples
  precede entry counting; self/parent records are excluded and relocation records
  count before omission. Multi-extent parts accumulate by key, taking the first
  timestamp and enforcing size limits when a final record completes the file.
- Direct unsafe members can be omitted under policy; UDF always rejects them.
  Directory handling precedes direct unsafe-kind checks. Physical overlap and
  multi-extent adjacency are not independently checked. Topology and bounds still
  reject the explicitly handled invalid shapes.
- SUSP continuation bytes consume a shared per-record budget; initial system-use
  bytes do not. Depth greater than four rejects. Malformed entry headers stop
  iteration silently; ST terminates it and CE recurses rather than being yielded.
- Joliet preserves surrogate units and maps odd trailing bytes. ISO whole-key
  limits use surrogatepass, unlike archive-common surrogateescape. Numeric version
  suffixes and one trailing dot are stripped only from non-alternate file names;
  directory and Rock Ridge alternate names retain them. No Unicode normalization.
- ISO and UDF timestamp helpers have distinct field, offset, fraction, and failure
  contracts. UDF initial attribute-presence checks occur outside its conversion
  guard. Signature rendering validates no tuple shape or freshness.
- Supplied Store configuration wins for UUID and is retained unchanged, while
  runtime path/options come from constructor arguments. Explicit UUID/name and
  durable options are not reconciled. Compatibility db_path/root_path identify the
  image file. Registry URI conversion and constructor pathname handling are separate.
- Test descriptions distinguish real direct/bridge fixtures, malformed placeholders,
  injected imports/extraction, advertised policy, and exercised limits. The shared
  name helper and image-builder entry points were inspected as dependencies; neither
  source file is newly counted in the completed manifest.

## Verification

- Strict batch audit and normalizer pass. Both checks passed across all **407
  reviewed files** before the manifest expanded.
- All five executable ASTs equal HEAD after removing only leading literal
  docstrings. Signatures, annotations, assertions, runtime strings, and limits are
  unchanged. No new runtime-documentation exception was introduced.
- Ruff findings equal baseline: ISO reader 11, each of the other four modules 1.
  No new findings, suppressions, or gate changes.
- Selected batch/shared-archive doctests and local archive, ISO writer, SquashFS,
  filesystem, Store/API/configuration/error/ingest regressions:
  **349 passed, 271 explicitly skipped integration examples**, 56.90 seconds.
  Skipped examples are not executed integration proof.
- Full quality runner passed: 159 formatted files, complete annotation coverage
  across 456 modules, 221 protected dependency modules, selected lint/complexity,
  both production type checkers, 188 strict-mypy files, and 37 deliberately invalid
  examples rejected by each checker.
- Migration, public-documentation, and developer-link contracts: **38 passed**.
- Isolated observations confirmed sink overwrite/seek behavior; a seek-only fake
  UDF extractor can satisfy position=4 while returning a zero-length file; a parser
  that appends to the image returns the initial 64-byte signature after the path
  grows to 71 bytes; supplied configuration can retain depth=1 while the driver uses
  depth=5 and ignores an invalid explicit UUID; and odd Joliet bytes retain their
  surrogatepass accounting. These are targeted mocked failure-boundary observations,
  not claims of real-parser conformance.
- Named observation images were scoped to a temporary directory and all returned
  spools were explicitly closed. A final path-parameter wording correction was
  followed by batch audit/normalizer and five-file AST/Ruff rechecks; executable
  examples were unchanged.
- The six recorded docstring-sensitive ownership-test failures remain outstanding.
  This batch did not rerun or weaken those unrelated guards.
- All verification handles completed with observed results. Root and data-submodule
  diffs are whitespace-clean. Local links resolve in the index (113), main ledger
  (3), and this note (7). The two remaining raw drivers are unchanged from HEAD.

Durable logs/exit metadata:
`working-memory/test-results/docstrings-iso-reader-2026-09-10-{regression,quality,contracts}.{log,done}`.
Focused observations:
`working-memory/test-results/docstrings-iso-reader-2026-09-10-observations.json`.
Static reports:
`/tmp/liuxin-docstring-iso-reader-batch-2026-09-10.json`,
`/tmp/liuxin-iso-reader-ast-lint-2026-09-10.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-10.json`.

Fresh whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**, with
no parse failures. Missing/blank docs: **1,082 modules, 1,928 classes, 20,980
functions = 23,990 declarations**. Overlapping findings: 3,881 delimiter layout,
10,257 missing examples, 2,922 parameter fields, 3,853 return fields, 3,676 empty
parameter descriptions, 4,854 empty return descriptions, and 28 missing summaries.
Structural counts do not establish descriptive completeness.

## Next work

Continue with `storage/drivers/iso_writer.py`, its remaining adapter modules and
complete regression module, then SquashFS. Those two raw-driver files are still
unmodified and outside the manifest, totaling 3,836 lines. Only the first 310 lines
of the ISO writer were read here: source/tree/continuation records and the beginning
of staging-session logic. That read-ahead is not completed-file review.

Writer follow-through should distinguish accepted write counts and incrementally
hashed bytes from later staged-file contents, as with earlier archive writers.
Inspect publication, cleanup, deterministic layout, name/continuation planning,
metadata-loss policy, and inherited ISO-reader assumptions before writing their
contracts. The source and tests may require additional specific observations.

The wider source/test/script/example/inherited/data-submodule backlog remains in
scope. Preserve runtime-doc consumer cautions, the three narrowly checked metadata
exceptions, and the unresolved ownership-test accounting issue.
