# File Formats: MOBI Hardening

Date: 2026-05-23
Branch: `file-formats-mobi-hardening`

## Scope

Started after PR #84 merged and `main` was fast-forwarded to `c95f1d1`.

The goal is to bring MOBI/KF8/AZW-style handling up to the same level of
format-specific documentation, reusable fixtures, unicode coverage, and
hostile-input hardening as the recent ODT, EPUB, DOCX, HTMLZ, comic, LIT, FB2,
and FBZ passes.

## Stage Status

1. Create branch: complete.
2. Inventory entry points and durable docs: complete.
3. Add reusable PalmDB/MOBI fixtures: complete.
4. Add malformed/hostile binary parser tests and source hardening: complete.
5. KF8/decompression/resource hardening and final docs: complete.

## Entry Points

- Input conversion:
  - `src/LiuXin_alpha/file_formats/conversion/plugins/mobi_input.py`
  - `MOBIInput` supports `mobi`, `prc`, `azw`, `azw3`, and `pobi`.
  - It uses `MobiReader` for MOBI6/PalmDB parsing and dispatches KF8/MOBI8 to
    `Mobi8Reader`.
- Metadata:
  - `src/LiuXin_alpha/metadata/file_sources/mobi.py`
  - Supports `MOBI`, `PRC`, `AZW`, `AZW3`, `AZW4`, and `POBI`.
  - Reads `MetadataHeader`/EXTH metadata, dispatches `TPZ` payloads to Topaz
    metadata, and patches EXTH metadata with `MetadataUpdater`.
- Output:
  - `MOBIOutput` and `AZW3Output` in
    `src/LiuXin_alpha/file_formats/conversion/plugins/mobi_output.py`.
  - Old MOBI output uses `writer2.main.MobiWriter`.
  - KF8/AZW3 output uses `writer8.main.KF8Writer` and `writer8.mobi.KF8Book`.

## Existing Coverage

Current focused file-format tests:

- `tests/file_formats/mobi/test_mobi_modernized.py`
- `tests/file_formats/mobi/test_mobi_headers_regressions.py`
- `tests/file_formats/mobi/test_mobi_exth_unicode_torture.py`
- `tests/file_formats/mobi/test_mobi_end_to_end_and_unicode_torture.py`
- `tests/file_formats/mobi/test_mobi_output_end_to_end_and_unicode_torture.py`

Current metadata tests:

- `tests/metadata/file_sources/test_mobi_metadata_source.py`
- `tests/metadata/file_sources/test_mobi_edge_cases.py`
- malformed reader fuzzing includes several MOBI payloads in
  `tests/metadata/file_sources/test_malformed_input_fuzzing.py`

Coverage is already meaningful for unicode EXTH metadata, metadata writer
sanitization, optional real fixtures, old-MOBI output round-trips, and low-level
utility helpers. The main gap is the absence of generated PalmDB/MOBI binary
fixtures for hostile reader/parser cases.

## Durable Docs

Added:

- `docs/development/file-formats/mobi/README.md`

Updated:

- `docs/development/file-formats/README.md`
- `docs/development/file-format-unicode-conversion.md`

The MOBI dossier records entry points, current tests, expected binary parser
contract, KF8/index boundaries, unicode coverage, hostile gaps, salvage/report
direction, and the next fixture slice.

## Stage 3 Fixtures

Added `tests/support/file_format_mobi.py` before touching parser behavior. The
helper creates small in-memory PalmDB/MOBI payloads rather than trying to emit
full realistic books immediately.

Useful helpers now available:

- PalmDB header and record-table builder
- minimal MOBI record 0 builder
- EXTH record/block builder
- section-offset corruption helpers
- truncation/mutation helpers
- shared log/options stubs

Added `tests/file_formats/mobi/test_mobi_binary_framework.py` to prove the
generated fixtures work against:

- `MetadataHeader`
- `MobiReader.extract_text`
- `metadata.file_sources.mobi.read_metadata_from_stream`
- EXTH unicode parsing
- PalmDB offset rewrite/truncation helpers

## Stage 3 Verification

- `python3 -m py_compile tests/support/file_format_mobi.py tests/file_formats/mobi/test_mobi_binary_framework.py`
- `python3 -m pytest tests/file_formats/mobi/test_mobi_binary_framework.py -q` -> `6 passed`
- `python3 -m pytest tests/file_formats/mobi -q` -> `37 passed`
- `python3 -m pytest tests/metadata/file_sources/test_mobi_metadata_source.py
  tests/metadata/file_sources/test_mobi_edge_cases.py -q` -> `20 passed`
- `git diff --check`

## Stage 4 Hostile Parser Hardening

Added `tests/file_formats/mobi/test_mobi_malformed_hostile.py`.

Covered cases:

- truncated PalmDB headers
- record table shorter than declared record count
- out-of-file, duplicate, or non-monotonic record offsets
- record 0 too short for MOBI fields
- impossible MOBI header lengths
- title and EXTH offsets outside record 0
- EXTH items smaller than their own header or beyond the EXTH block

Source hardening:

- `src/LiuXin_alpha/file_formats/mobi/reader/headers.py`
  - Added shared PalmDB header/record-table constants and validation helpers.
  - `EXTHHeader` now validates signature, declared length, item count, item
    sizes, and fixed-width integer payload sizes before unpacking.
  - `BookHeader` now raises named `MobiError` for truncated record 0 data,
    invalid MOBI signature, impossible header length, out-of-record title
    offsets, and malformed EXTH.
  - `MetadataHeader` now validates stream reads, record counts, record-table
    offsets, section bounds, and out-of-range section access.
- `src/LiuXin_alpha/file_formats/mobi/reader/mobi6.py`
  - `MobiReader` now uses the shared PalmDB record-table preflight before
    slicing sections.

Stage 4 verification:

- `python3 -m pytest tests/file_formats/mobi/test_mobi_malformed_hostile.py -q` -> `17 passed`
- `python3 -m py_compile src/LiuXin_alpha/file_formats/mobi/reader/headers.py
  src/LiuXin_alpha/file_formats/mobi/reader/mobi6.py
  tests/file_formats/mobi/test_mobi_malformed_hostile.py
  tests/support/file_format_mobi.py`
- `python3 -m pytest tests/file_formats/mobi -q` -> `54 passed`
- `python3 -m pytest tests/metadata/file_sources/test_mobi_metadata_source.py
  tests/metadata/file_sources/test_mobi_edge_cases.py -q` -> `20 passed`
- `python3 -m pytest tests/metadata/file_sources/test_malformed_input_fuzzing.py -q` -> `133 passed`
- `python3 -m pytest tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py
  tests/file_formats/conversion/test_conversion_top_level_smoke.py -q` -> `6 passed`
- `git diff --check`

## Stage 5 Deep Hostile Hardening

Added `tests/file_formats/mobi/test_mobi_deep_hostile.py`.

Covered cases:

- missing HUFF records
- truncated HUFF table headers and table offsets
- malformed CDIC table headers, bit widths, offset tables, and phrase slices
- DH-compressed MOBI records whose HUFF/CDIC range points outside the section
  table
- truncated or wrong-signature INDX and TAGX records
- out-of-range `read_index` references
- out-of-range KF8 FDST index references
- malformed FDST records, including wrong signatures, truncated tables,
  reversed ranges, and ranges beyond raw markup
- invalid skeleton and NCX index data wrapped as `MobiError`
- resource ranges outside the section table
- CRES image resources without an active CONT container

Source hardening:

- `src/LiuXin_alpha/file_formats/mobi/huffcdic.py`
  - Added HUFF/CDIC length, offset, and dictionary bounds checks.
  - Direct CDIC loading now initializes dictionary state and raises
    `MobiError` instead of raw `struct.error`/`AttributeError`.
  - HUFF unpacking now rejects unloaded tables and out-of-range dictionary
    references as MOBI parser failures.
- `src/LiuXin_alpha/file_formats/mobi/reader/mobi6.py`
  - DH/HUFF record ranges are checked before section slicing.
- `src/LiuXin_alpha/file_formats/mobi/reader/index.py`
  - INDX/TAGX parsers now validate section sizes, offsets, IDXT tables, and
    variable-width integer consumption before unpacking.
  - `read_index` now rejects out-of-range INDX/CNCX section references.
- `src/LiuXin_alpha/file_formats/mobi/reader/mobi8.py`
  - Added KF8 section lookup, index wrapping, and FDST validation helpers.
  - SKEL/DIV/OTH/NCX failures now surface as named `MobiError` failures.
  - Resource extraction now rejects impossible ranges and CRES records without
    an active CONT container.

Stage 5 verification:

- `python3 -m py_compile src/LiuXin_alpha/file_formats/mobi/huffcdic.py
  src/LiuXin_alpha/file_formats/mobi/reader/index.py
  src/LiuXin_alpha/file_formats/mobi/reader/mobi8.py
  src/LiuXin_alpha/file_formats/mobi/reader/mobi6.py
  tests/file_formats/mobi/test_mobi_deep_hostile.py`
- `python3 -m pytest tests/file_formats/mobi/test_mobi_deep_hostile.py -q` -> `25 passed`
- `python3 -m pytest tests/file_formats/mobi -q` -> `79 passed`
- `python3 -m pytest tests/metadata/file_sources/test_mobi_metadata_source.py
  tests/metadata/file_sources/test_mobi_edge_cases.py
  tests/metadata/file_sources/test_malformed_input_fuzzing.py -q` -> `153 passed`
- `python3 -m pytest tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py
  tests/file_formats/conversion/test_conversion_top_level_smoke.py -q` -> `6 passed`
- `python3 -m pytest tests/file_formats/mobi
  tests/metadata/file_sources/test_mobi_metadata_source.py
  tests/metadata/file_sources/test_mobi_edge_cases.py
  tests/metadata/file_sources/test_malformed_input_fuzzing.py -q` -> `232 passed`
- `git diff --check`

## Remaining Follow-Ups

- bounded decompression expansion checks for PalmDOC and HUFF/CDIC streams
- realistic KF8 fixture products for skeleton/div insertion, NCX href
  creation, and resource mapping through `MOBIInput`
- image/font/container resource fixtures with explicit skip, warn, and fail
  behavior
- guarded trusted-input overrides only for bounded limits, not invalid offsets
  or unreadable headers
