# PDB File Format Notes

## Status

PDB-family input conversion enters through `PDBInput` in
`src/LiuXin_alpha/file_formats/conversion/plugins/pdb_input.py`. The plugin
supports `pdb` and `updb`, reads the PalmDB wrapper through `PdbHeaderReader`,
then dispatches by the 8-byte PalmDB identity through
`src/LiuXin_alpha/file_formats/pdb/__init__.py`.

The currently registered input readers are:

- `TEXtREAd`: PalmDOC text
- `zTXTGPlm`: zTXT compressed text
- `PNPdPPrs` and `PNRdPPrs`: eReader
- `.pdfADBE`: Adobe Reader/PDF-in-PDB
- `DataPlkr`: Plucker
- `BOOKMTIT` and `BOOKMTIU`: Haodoo.net legacy and unicode variants

Metadata reading and writing enters through
`src/LiuXin_alpha/metadata/file_sources/pdb/__init__.py`. Metadata readers
exist for eReader, Plucker, and Haodoo. Metadata writes update the PalmDB
wrapper title for all parseable PDB files, and can update body metadata for
supported eReader variants.

Output conversion enters through `PDBOutput` in
`src/LiuXin_alpha/file_formats/conversion/plugins/pdb_output.py`. Output
writers exist for PalmDOC, zTXT, and eReader.

The current checked-in tests live in:

- `tests/file_formats/pdb/test_pdb_binary_framework.py`
- `tests/file_formats/pdb/test_pdb_ereader_hostile.py`
- `tests/file_formats/pdb/test_pdb_malformed_hostile.py`
- `tests/file_formats/pdb/test_pdb_plucker_hostile.py`
- `tests/file_formats/pdb/test_pdb_subreader_hostile.py`
- `tests/file_formats/pdb/test_pdb_modernized.py`
- `tests/metadata/file_sources/test_pdb_metadata_source.py`
- `tests/metadata/file_sources/test_pdb_metadata_fixtures.py`
- `tests/metadata/file_sources/test_pdb_subreader_edge_cases.py`
- `tests/metadata/file_sources/test_malformed_input_fuzzing.py`

The reusable binary fixture module is `tests/support/file_format_pdb.py`. It
builds small parser-facing PalmDB/PDB payloads rather than full production
books, with helpers for wrapper headers, record tables, PalmDOC/zTXT/eReader
record 0 payloads, Plucker record 0/section/text/metadata/composite-image
sections, offset rewrites, truncation, stream wrappers, and shared log/options
stubs.

## Binary Input Contract

PDB is a PalmDB container. It shares the same first-layer risk profile as MOBI:
fixed-size wrapper fields, a record count, an offset table, and section data
sliced by adjacent offsets. Default parsing should be strict at that binary
structure boundary. Wrong-format, truncated, or structurally incoherent files
should raise `PDBError`, `PdbFormatError`, a subformat error such as
`EreaderError` or `zTXTError`, or another named domain exception, not raw
`struct.error`, `IndexError`, `KeyError`, `OverflowError`, or an unbounded loop.

The current wrapper behavior is mixed:

- metadata reads reject unreadable wrapper headers by default
- metadata can opt into fallback metadata with `fallback_on_parse_error=True`
- parseable but unsupported PDB identities return header-only metadata
- `PDBInput` dispatches unsupported identities as `PDBError`
- several conversion subreaders still assume section offsets and record-local
  lengths are valid once the wrapper header has been read

The current wrapper preflight covers:

- minimum readable PalmDB header and record table
- bounded record count
- record table entries contained inside the file
- monotonically increasing record offsets
- section access checked before reads
- identity decoding that is stable for malformed bytes

PalmDOC, zTXT, eReader, and Plucker subreader preflight now covers:

- record 0 payloads checked before fixed-width unpacking
- section headers checked before fixed-width unpacking
- declared text record counts contained inside the section table
- unsupported PalmDOC compression reported as `PDBError`
- unsupported zTXT versions and flags reported as `zTXTError`
- unsupported eReader compression/version reported as `EreaderError` or
  `DRMError`
- direct out-of-range text-section access reported as named parser errors
- eReader text, image, metadata, link, footnote, and sidebar ranges bounded by
  the PalmDB section table
- PalmDOC, zTXT, zlib, and eReader text decompression failures reported as
  named parser errors
- eReader image names sanitized before extraction so resource output remains
  inside `images/`
- Plucker section declared sizes, text paragraph tables, metadata records,
  composite-image layouts, PHTML operands, PHTML image references, and PHTML
  decompression failures reported as `PluckerError`

Still-open preflight targets are now concentrated inside the remaining richer
subreaders:

- declared text, image, link, metadata, and chapter ranges contained inside
  available sections
- generated output paths for image/resource extraction kept inside the
  conversion work directory

## Subformat Boundaries

PalmDOC and zTXT mostly convert text records through the TXT input plugin.
Their current hardening checks record count and decompression bounds: record 0
must contain the expected fixed-width fields, declared text record counts must
fit the PalmDB section table, and malformed compressed sections fail with named
parser errors.

eReader has two reader families: 132-byte Dropbook records and 116/202-byte
Makebook records. These readers derive text, image, metadata, footnote,
sidebar, and link ranges from record 0. Current hardening checks unsupported
compression/version paths, out-of-range text/image/metadata/link/footnote/
sidebar offsets, malformed compressed text pages, and hostile image names.

Plucker has the richest internal structure. Record 0 maps local IDs to record
types, and later sections contain per-section headers, PHTML payloads, metadata
records, image records, and composite image layouts. Current hardening checks
short record 0/table data, short section headers, declared section-size
overruns, malformed metadata record lengths, text paragraph tables that overrun
the section, PHTML control codes that lack enough following bytes, composite
image layout overruns, and composite/PHTML image references to missing image
records.

Haodoo has legacy CP950 and UTF-16LE variants. Its first hardening target is
header record parsing: malformed separators, non-integer record counts,
chapter-title mismatches, and declared chapter counts beyond the PalmDB section
table should fail or fall back deterministically.

The `.pdfADBE` reader should be treated as an embedded-PDF handoff. The PDB
wrapper must still be valid before any PDF-specific handling runs.

## Unicode And Locale Coverage

Existing PDB coverage already includes:

- wrapper title sanitization and round-trips
- hashed legacy metadata fixtures
- eReader body metadata writes with hostile text cleanup
- Plucker metadata with UTF-8 title/author fields and timestamp edges
- Haodoo metadata fallback for malformed synthetic sections
- path-like and stream metadata reads
- writer title updates that preserve unsupported-body fallback behavior

The next conversion-side unicode tests should add small generated fixtures for:

- PalmDOC/zTXT text containing multilingual payloads
- eReader PML text with non-ASCII content and hostile control characters
- Plucker PHTML with UTF-8 metadata and escaped markup-sensitive content
- Haodoo CP950 and UTF-16LE chapter titles/body text

## Hostile Corpus

The checked-in hostile metadata coverage currently includes:

- unreadable wrapper headers raising by default
- truncated wrapper headers
- short record tables
- duplicate, non-monotonic, inside-header, and out-of-file record offsets
- out-of-range wrapper section access
- explicit fallback metadata for corrupt paths/streams
- unsupported but parseable PDB identities returning header-only metadata
- PalmDOC short record 0, impossible text record counts, direct section bounds,
  unsupported compression, and decompressor failures
- zTXT short record 0, impossible text record counts, unsupported
  version/flags, direct section bounds, and malformed zlib records
- eReader unsupported short record 0 dispatch, impossible Dropbook/Makebook
  text ranges, impossible Dropbook image ranges, direct section bounds,
  malformed compressed text records, and hostile image names
- eReader metadata field cleanup
- safe handling of missing eReader cover/metadata sections
- Plucker short record 0/table data, short section headers, impossible section
  declared sizes, malformed metadata record lengths, paragraph-table overruns,
  truncated PHTML operands, missing PHTML/composite image references, and bad
  PHTML compression payloads
- Plucker metadata record iteration on short and overlong records

Missing hostile coverage is now concentrated in conversion readers:

- Haodoo malformed header fields and chapter count mismatches

## Salvage And Reporting Direction

PDB already distinguishes wrong-format fallback from valid-container fallback:
unreadable wrapper headers raise by default, while parseable but unsupported
PDB identities can return header-only metadata. Keep that split.

Future recovery or trusted-input modes should report:

- whether header-only metadata fallback was used
- unsupported identity and human-readable identity name
- subreader selected
- skipped sections, images, links, chapters, or metadata records
- decompression failures and affected record numbers
- replaced characters and lossy encodings
- any relaxed size/count/decompression budget

Default conversion should continue to fail when the PalmDB wrapper is
undefined. A future trusted-input profile may raise bounded size/count limits,
but it should not permit invalid record offsets, path escape behavior, unreadable
headers, or silent content loss.

## Next Hardening Slice

The next useful PDB slice should continue subreader record 0 and range checks in
the remaining richer format:

- add Haodoo tests for malformed header separators, non-integer record counts,
  and chapter count mismatches

High-value focused commands:

- `python3 -m pytest tests/file_formats/pdb -q`
- `python3 -m pytest tests/metadata/file_sources/test_pdb_metadata_source.py
  tests/metadata/file_sources/test_pdb_metadata_fixtures.py
  tests/metadata/file_sources/test_pdb_subreader_edge_cases.py -q`
- `python3 -m pytest tests/metadata/file_sources/test_malformed_input_fuzzing.py -q`
