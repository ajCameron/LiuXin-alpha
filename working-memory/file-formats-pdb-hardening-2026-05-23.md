# File Formats: PDB Hardening

Date: 2026-05-23
Branch: `file-formats-pdb-hardening`

## Scope

Started after PR #85 merged and `main` was fast-forwarded to `a3f604d`.

The goal is to bring PDB and its PalmDB subformats up to the same fixture,
unicode, and hostile-input standard as the recent legacy/container passes. PDB
is the natural follow-up to MOBI because the outer container is also PalmDB,
but it dispatches into several older formats with separate record-local
assumptions.

## Stage Status

1. Create branch: complete.
2. Add durable PDB docs and working-memory setup: complete.
3. Add reusable PDB/PalmDB fixtures: complete.
4. Add malformed wrapper/header tests and source hardening: complete.
5. Add PalmDOC/zTXT subreader-specific hostile coverage and hardening:
   complete.
6. Add eReader hostile subreader coverage and hardening: complete.
7. Add Plucker hostile subreader coverage and hardening: complete.
8. Continue Haodoo hostile subreader coverage: complete.

## Entry Points

- Input conversion:
  - `src/LiuXin_alpha/file_formats/conversion/plugins/pdb_input.py`
  - `PDBInput` supports `pdb` and `updb`.
  - It reads the wrapper with `PdbHeaderReader`, dispatches through
    `file_formats.pdb.get_reader`, then writes conversion output through the
    chosen subreader.
- Core wrapper:
  - `src/LiuXin_alpha/file_formats/pdb/header.py`
  - `PdbHeaderReader` reads identity, record count, wrapper title, record table
    entries, and section data.
  - `PdbHeaderBuilder` creates compact PalmDB/PDB fixtures and writer headers.
- Registered conversion readers:
  - `TEXtREAd`: PalmDOC
  - `zTXTGPlm`: zTXT
  - `PNPdPPrs` and `PNRdPPrs`: eReader
  - `.pdfADBE`: Adobe Reader/PDF-in-PDB
  - `DataPlkr`: Plucker
  - `BOOKMTIT` and `BOOKMTIU`: Haodoo.net
- Metadata:
  - `src/LiuXin_alpha/metadata/file_sources/pdb/__init__.py`
  - eReader, Plucker, and Haodoo specialized metadata readers.
  - eReader metadata writer plus wrapper-title updates for all parseable PDBs.
- Output:
  - `src/LiuXin_alpha/file_formats/conversion/plugins/pdb_output.py`
  - PalmDOC, zTXT, and eReader writers.

## Existing Coverage

Focused file-format tests:

- `tests/file_formats/pdb/test_pdb_modernized.py`
- `tests/file_formats/pdb/test_pdb_binary_framework.py`
- `tests/file_formats/pdb/test_pdb_malformed_hostile.py`
- `tests/file_formats/pdb/test_pdb_subreader_hostile.py`
- `tests/file_formats/pdb/test_pdb_ereader_hostile.py`
- `tests/file_formats/pdb/test_pdb_plucker_hostile.py`

Metadata tests:

- `tests/metadata/file_sources/test_pdb_metadata_source.py`
- `tests/metadata/file_sources/test_pdb_metadata_fixtures.py`
- `tests/metadata/file_sources/test_pdb_subreader_edge_cases.py`
- malformed reader fuzzing includes PDB payloads in
  `tests/metadata/file_sources/test_malformed_input_fuzzing.py`

Coverage is already useful for module imports, header builder/reader
round-trips, registry dispatch, writer helper bytes, legacy hashed metadata
fixtures, eReader metadata writes, Plucker metadata edges, and strict corrupt
wrapper behavior in metadata paths.

The main gap is conversion-facing hostile/container coverage. The conversion
readers still trust record counts, offsets, short fixed-width headers, section
ranges, decompression payloads, and image/resource names in many places.

## Durable Docs

Added:

- `dev-docs/file-formats/pdb/README.md`

Updated:

- `dev-docs/file-formats/README.md`
- `dev-docs/file-format-unicode-conversion.md`

The PDB dossier records entry points, registered subformats, binary input
contract, subformat boundaries, unicode coverage, hostile gaps, salvage/report
direction, and the next fixture-first slice.

## Setup Verification

- `python3 -m pytest tests/file_formats/pdb -q` -> `6 passed`
- `python3 -m pytest tests/metadata/file_sources/test_pdb_metadata_source.py
  tests/metadata/file_sources/test_pdb_metadata_fixtures.py
  tests/metadata/file_sources/test_pdb_subreader_edge_cases.py -q` -> `25 passed`
- `git diff --check`

## Stage 3 Fixtures

Added `tests/support/file_format_pdb.py`.

Useful helpers now available:

- PalmDB/PDB header and record-table builder
- explicit identity/title helpers
- section-offset corruption helpers
- truncation helpers
- `PdbLog`, `pdb_input_options`, and `pdb_stream`
- minimal PalmDOC and zTXT fixture builders
- eReader 132-byte and 116/202-byte header builders plus metadata and image
  record builders
- Plucker record 0, generic section, text section, metadata section, and
  composite-image section builders

Added `tests/file_formats/pdb/test_pdb_binary_framework.py` to prove the
generated fixtures work against:

- `PdbHeaderReader`
- PalmDOC reader header/text extraction
- zTXT reader header/decompression path
- eReader metadata source reads
- Plucker metadata source reads
- offset rewrite and truncation helpers for future hostile payloads

Stage 3 verification:

- `python3 -m py_compile tests/support/file_format_pdb.py
  tests/file_formats/pdb/test_pdb_binary_framework.py`
- `python3 -m pytest tests/file_formats/pdb/test_pdb_binary_framework.py -q` -> `8 passed`
- `python3 -m pytest tests/file_formats/pdb -q` -> `14 passed`
- `python3 -m pytest tests/metadata/file_sources/test_pdb_metadata_source.py
  tests/metadata/file_sources/test_pdb_metadata_fixtures.py
  tests/metadata/file_sources/test_pdb_subreader_edge_cases.py -q` -> `25 passed`
- `python3 -m pytest tests/metadata/file_sources/test_malformed_input_fuzzing.py -q` -> `133 passed`

## Stage 4 Hostile Wrapper Hardening

Added `tests/file_formats/pdb/test_pdb_malformed_hostile.py`.

Covered cases:

- truncated PalmDB wrapper headers
- short record tables
- record offsets inside the header/table
- duplicate, reversed, and out-of-file record offsets
- out-of-range section access for `section_data`, `section_offset`, and
  `full_section_info`
- metadata strict-by-default behavior for malformed wrappers

Source hardening:

- `src/LiuXin_alpha/file_formats/pdb/header.py`
  - Added PalmDB header/table constants.
  - `PdbHeaderReader` now computes stream length up front.
  - Fixed-width header reads now require exact bytes and raise `PDBError`.
  - Record counts, record table size, and record offsets are preflighted before
    section slicing.
  - Section access now raises `PDBError` for out-of-range records.
  - Section table entries are cached after validation.

Stage 4 verification:

- `python3 -m py_compile src/LiuXin_alpha/file_formats/pdb/header.py
  tests/file_formats/pdb/test_pdb_malformed_hostile.py
  tests/support/file_format_pdb.py`
- `python3 -m pytest tests/file_formats/pdb/test_pdb_malformed_hostile.py -q` -> `12 passed`
- `python3 -m pytest tests/file_formats/pdb -q` -> `26 passed`
- `python3 -m pytest tests/metadata/file_sources/test_pdb_metadata_source.py
  tests/metadata/file_sources/test_pdb_metadata_fixtures.py
  tests/metadata/file_sources/test_pdb_subreader_edge_cases.py -q` -> `25 passed`
- `python3 -m pytest tests/metadata/file_sources/test_malformed_input_fuzzing.py -q` -> `133 passed`
- `python3 -m pytest tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py
  tests/file_formats/conversion/test_conversion_top_level_smoke.py -q` -> `6 passed`

## Stage 5 PalmDOC/zTXT Hostile Subreaders

Added `tests/file_formats/pdb/test_pdb_subreader_hostile.py`.

Covered cases:

- PalmDOC short record 0 payloads raise `PDBError`
- PalmDOC declared text record counts beyond available sections raise
  `PDBError`
- unsupported PalmDOC compression raises `PDBError`
- direct PalmDOC out-of-range section access raises `PDBError`
- PalmDOC decompressor failures are wrapped as `PDBError`
- zTXT short record 0 payloads raise `zTXTError`
- zTXT declared text record counts beyond available sections raise `zTXTError`
- unsupported zTXT version and flags stay named `zTXTError` failures
- direct zTXT out-of-range section access raises `zTXTError`
- malformed zlib text sections are wrapped as `zTXTError`

Source hardening:

- `src/LiuXin_alpha/file_formats/pdb/palmdoc/reader.py`
  - Added fixed-width record 0 size validation.
  - Added supported compression validation.
  - Added text-record count bounds.
  - Added section-index checks.
  - Wrapped PalmDOC decompressor failures in `PDBError`.
- `src/LiuXin_alpha/file_formats/pdb/ztxt/reader.py`
  - Added fixed-width record 0 size validation.
  - Added text-record count bounds.
  - Added section-index checks.
  - Wrapped zlib decompression failures in `zTXTError`.

Stage 5 verification:

- `python3 -m py_compile src/LiuXin_alpha/file_formats/pdb/palmdoc/reader.py
  src/LiuXin_alpha/file_formats/pdb/ztxt/reader.py
  tests/file_formats/pdb/test_pdb_subreader_hostile.py`
- `python3 -m pytest tests/file_formats/pdb/test_pdb_subreader_hostile.py -q` -> `12 passed`
- `python3 -m pytest tests/file_formats/pdb -q` -> `38 passed`
- `python3 -m pytest tests/metadata/file_sources/test_pdb_metadata_source.py
  tests/metadata/file_sources/test_pdb_metadata_fixtures.py
  tests/metadata/file_sources/test_pdb_subreader_edge_cases.py -q` -> `25 passed`
- `python3 -m pytest tests/metadata/file_sources/test_malformed_input_fuzzing.py -q` -> `133 passed`
- `python3 -m pytest tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py
  tests/file_formats/conversion/test_conversion_top_level_smoke.py -q` -> `6 passed`

## Stage 6 eReader Hostile Subreader Hardening

Added `tests/file_formats/pdb/test_pdb_ereader_hostile.py`.

Covered cases:

- eReader dispatcher rejects unsupported short record 0 payloads with
  `EreaderError`
- 132-byte Dropbook text ranges beyond available sections raise `EreaderError`
- 132-byte Dropbook image ranges beyond available sections raise `EreaderError`
- malformed Dropbook zlib text records are wrapped as `EreaderError`
- Dropbook image names are sanitized before `dump_images()` writes files
- 116/202-byte Makebook text ranges beyond available sections raise
  `EreaderError`
- Makebook PalmDOC decompressor failures are wrapped as `EreaderError`
- Makebook image names are sanitized when parsed from image records

Source hardening:

- `src/LiuXin_alpha/file_formats/pdb/ereader/__init__.py`
  - `image_name()` now handles bytes safely, strips NULs, normalizes
    backslashes, keeps only the basename, supplies an empty-name fallback, and
    still returns the 32-byte-padded eReader name shape expected by writers.
- `src/LiuXin_alpha/file_formats/pdb/ereader/reader132.py`
  - Added fixed-width record 0 validation.
  - Added text/image/link/metadata/footnote/sidebar range validation.
  - Added section-index checks.
  - Switched PalmDOC decompression to the maintained compression module.
  - Wrapped PalmDOC/zlib/decode failures in `EreaderError`.
  - Parsed image names byte-safely and rejects truncated image records.
- `src/LiuXin_alpha/file_formats/pdb/ereader/reader202.py`
  - Added 116/202-byte record 0 validation.
  - Added text-range and section-index checks.
  - Fixed Python 3 byte-wise Makebook XOR handling.
  - Wrapped PalmDOC/decode failures in `EreaderError`.
  - Parsed image names byte-safely, rejects truncated image records, and made
    `dump_images()` use the Makebook non-text scan path instead of undefined
    Dropbook-only header fields.

Stage 6 verification:

- `python3 -m py_compile src/LiuXin_alpha/file_formats/pdb/ereader/__init__.py
  src/LiuXin_alpha/file_formats/pdb/ereader/reader132.py
  src/LiuXin_alpha/file_formats/pdb/ereader/reader202.py
  tests/support/file_format_pdb.py
  tests/file_formats/pdb/test_pdb_ereader_hostile.py`
- `python3 -m pytest tests/file_formats/pdb/test_pdb_ereader_hostile.py -q` -> `8 passed`
- `python3 -m pytest tests/file_formats/pdb -q` -> `46 passed`
- `python3 -m pytest tests/metadata/file_sources/test_pdb_metadata_source.py
  tests/metadata/file_sources/test_pdb_metadata_fixtures.py
  tests/metadata/file_sources/test_pdb_subreader_edge_cases.py -q` -> `25 passed`
- `python3 -m pytest tests/metadata/file_sources/test_malformed_input_fuzzing.py -q` -> `133 passed`
- `python3 -m pytest tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py
  tests/file_formats/conversion/test_conversion_top_level_smoke.py -q` -> `6 passed`

## Stage 7 Plucker Hostile Subreader Hardening

Added `tests/file_formats/pdb/test_pdb_plucker_hostile.py`.

Covered cases:

- short Plucker record 0 payloads raise `PluckerError`
- record 0 reserved-table overruns raise `PluckerError`
- short per-section headers raise `PluckerError`
- text paragraph tables that overrun section data raise `PluckerError`
- declared section sizes beyond available data raise `PluckerError`
- malformed metadata record lengths raise `PluckerError`
- composite image layout overruns raise `PluckerError`
- composite image references to missing image records raise `PluckerError`
- truncated PHTML control operands raise `PluckerError`
- PHTML embedded-image references to missing image records raise `PluckerError`
- malformed compressed PHTML payloads are wrapped as `PluckerError`

Source hardening:

- `src/LiuXin_alpha/file_formats/pdb/plucker/__init__.py`
  - Added named `PluckerError`.
- `src/LiuXin_alpha/file_formats/pdb/plucker/reader.py`
  - Added fixed-width and bounded-slice helpers.
  - Added strict record 0, section header, paragraph table, metadata record,
    and composite layout validation.
  - Added supported compression validation.
  - Fixed metadata-section index handling when metadata is the first parsed
    section.
  - Keeps empty-but-recognized sections instead of dropping falsey payloads.
  - Validates composite-image references after all sections are known.
  - Uses the maintained metadata/customize import paths.
  - Wraps PHTML decompression failures in `PluckerError`.
  - Validates PHTML operand lengths and image references before reading them.

Stage 7 verification:

- `python3 -m py_compile src/LiuXin_alpha/file_formats/pdb/plucker/__init__.py
  src/LiuXin_alpha/file_formats/pdb/plucker/reader.py
  tests/support/file_format_pdb.py
  tests/file_formats/pdb/test_pdb_plucker_hostile.py`
- `python3 -m pytest tests/file_formats/pdb/test_pdb_plucker_hostile.py -q` -> `17 passed`
- `python3 -m pytest tests/file_formats/pdb -q` -> `63 passed`
- `python3 -m pytest tests/metadata/file_sources/test_pdb_metadata_source.py
  tests/metadata/file_sources/test_pdb_metadata_fixtures.py
  tests/metadata/file_sources/test_pdb_subreader_edge_cases.py
  tests/metadata/file_sources/test_plucker_metadata_source.py -q` -> `33 passed`
- `python3 -m pytest tests/metadata/file_sources/test_malformed_input_fuzzing.py -q` -> `133 passed`
- `python3 -m pytest tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py
  tests/file_formats/conversion/test_conversion_top_level_smoke.py -q` -> `6 passed`

## Stage 8 Haodoo Hostile Subreader Hardening

Added `tests/file_formats/pdb/test_pdb_haodoo_hostile.py`.

Covered cases:

- valid generated Haodoo CP950 and UTF-16LE fixtures produce conversion output
  with multilingual title, chapter, and body text
- `PDBInput` dispatches a generated Haodoo fixture through the plugin path
- malformed legacy and unicode header separators raise `PDBError`
- non-integer record counts raise `PDBError`
- chapter-title count mismatches raise `PDBError`
- declared chapter records beyond available PalmDB sections raise `PDBError`
- direct out-of-range Haodoo section access raises `PDBError`

Source hardening:

- `src/LiuXin_alpha/file_formats/pdb/haodoo/reader.py`
  - Normalizes string/byte Haodoo identities before choosing legacy CP950 or
    unicode UTF-16LE parsing.
  - Converts chapter-title iterators to lists and fixes Python 3 string
    stripping around decoded text.
  - Validates header field shape, integer record counts, chapter-title counts,
    declared chapter ranges, and direct section access.

Stage 8 verification:

- `python3 -m py_compile src/LiuXin_alpha/file_formats/pdb/haodoo/reader.py
  tests/support/file_format_pdb.py
  tests/file_formats/pdb/test_pdb_binary_framework.py
  tests/file_formats/pdb/test_pdb_haodoo_hostile.py`
- `python3 -m pytest -q tests/file_formats/pdb/test_pdb_binary_framework.py
  tests/file_formats/pdb/test_pdb_haodoo_hostile.py` -> `20 passed`
- `python3 -m pytest tests/file_formats/pdb -q` -> `75 passed`
- `python3 -m pytest tests/metadata/file_sources/test_pdb_metadata_source.py
  tests/metadata/file_sources/test_pdb_metadata_fixtures.py
  tests/metadata/file_sources/test_pdb_subreader_edge_cases.py -q` -> `25 passed`
- `python3 -m pytest tests/metadata/file_sources/test_malformed_input_fuzzing.py -q` -> `133 passed`
- `python3 scripts/run_file_formats_lane.py --lane fast` -> `787 passed, 1 skipped`

## Next Slice

The PDB hostile subreader pass is complete through PalmDOC, zTXT, eReader,
Plucker, and Haodoo. Next PDB work should be driven by conversion-product
sign-off or real-corpus defects rather than the known hostile subreader gaps.

High-value focused commands:

- `python3 -m pytest tests/file_formats/pdb -q`
- `python3 -m pytest tests/metadata/file_sources/test_pdb_metadata_source.py
  tests/metadata/file_sources/test_pdb_metadata_fixtures.py
  tests/metadata/file_sources/test_pdb_subreader_edge_cases.py -q`
- `python3 -m pytest tests/metadata/file_sources/test_malformed_input_fuzzing.py -q`
