# MOBI File Format Notes

## Status

MOBI-family input conversion enters through `MOBIInput` in
`src/LiuXin_alpha/file_formats/conversion/plugins/mobi_input.py`. The input
plugin supports `mobi`, `prc`, `azw`, `azw3`, and `pobi`, and delegates binary
parsing to `MobiReader` in `src/LiuXin_alpha/file_formats/mobi/reader/mobi6.py`.
Standalone and joint KF8/MOBI8 files are then handled by `Mobi8Reader` in
`src/LiuXin_alpha/file_formats/mobi/reader/mobi8.py`.

Metadata reading and writing enters through
`src/LiuXin_alpha/metadata/file_sources/mobi.py`, which covers `MOBI`, `PRC`,
`AZW`, `AZW3`, `AZW4`, and `POBI`. It parses PalmDB/MOBI headers through
`MetadataHeader`, reads EXTH metadata, can dispatch Topaz payloads that start
with `TPZ`, and can patch EXTH metadata in place with `MetadataUpdater`.

Output conversion enters through `MOBIOutput` and `AZW3Output` in
`src/LiuXin_alpha/file_formats/conversion/plugins/mobi_output.py`. Old MOBI6
output is serialized by `MobiWriter` in
`src/LiuXin_alpha/file_formats/mobi/writer2/main.py`; KF8/AZW3 output is built
by `KF8Writer` and `KF8Book` in the `writer8` package.

The current checked-in tests live in:

- `tests/file_formats/mobi/test_mobi_modernized.py`
- `tests/file_formats/mobi/test_mobi_headers_regressions.py`
- `tests/file_formats/mobi/test_mobi_binary_framework.py`
- `tests/file_formats/mobi/test_mobi_malformed_hostile.py`
- `tests/file_formats/mobi/test_mobi_deep_hostile.py`
- `tests/file_formats/mobi/test_mobi_exth_unicode_torture.py`
- `tests/file_formats/mobi/test_mobi_end_to_end_and_unicode_torture.py`
- `tests/file_formats/mobi/test_mobi_output_end_to_end_and_unicode_torture.py`
- `tests/metadata/file_sources/test_mobi_metadata_source.py`
- `tests/metadata/file_sources/test_mobi_edge_cases.py`

The reusable binary fixture module is `tests/support/file_format_mobi.py`. It
builds small parser-facing PalmDB/MOBI payloads rather than full production
books, with helpers for PalmDB headers, record tables, minimal MOBI record 0
payloads, EXTH blocks, offset rewrites, truncation, stream wrappers, and shared
log/options stubs.

## Binary Input Contract

MOBI is a PalmDB-backed binary container with several generations layered on
top of the same record table. Default parsing should be strict at the binary
structure boundary. Wrong-format, truncated, or structurally incoherent files
should raise `MobiError`, `TopazError`, `DRMError`, or another named domain
exception, not raw `struct.error`, `IndexError`, `KeyError`, `OverflowError`,
or an unbounded loop.

The current preflight contract covers:

- a minimum readable PalmDB header and record table
- `BOOKMOBI` or `TEXTREAD` identity at the PalmDB type field
- a bounded record count
- record table entries that fit inside the file
- monotonically increasing record offsets
- non-empty record 0 before MOBI header parsing
- bounded MOBI header length
- title offset and title length contained inside record 0
- valid EXTH header length, item count, and item record sizes when EXTH is
  present
- HUFF/CDIC record ranges contained inside the available record list
- valid HUFF and CDIC table sizes before decompression
- valid INDX and TAGX signatures, offsets, and variable-width value reads
- KF8 FDST, SKEL, DIV, OTH, and NCX references contained inside the available
  KF8 section list
- FDST flow ranges ordered and contained inside the raw markup
- resource extraction ranges contained inside the available section list
- CRES resources only accepted while a CONT container is active

Still-open preflight targets are now mostly policy and budget decisions:

- bounded decompression expansion behavior for hostile but syntactically valid
  PalmDOC and HUFF/CDIC streams
- stricter image, thumbnail, font, and container-resource validation once real
  resource fixtures exist

`MOBIInput` currently retries `MobiReader` with `try_extra_data_fix=True` after
any first-pass reader failure. That fallback is useful for real corpus drift,
but the hardening pass should make it visible and keep binary preflight failures
that leave the record table undefined as hard failures.

## KF8 And Index Contract

KF8/MOBI8 handling depends on EXTH record `121`, optional joint-file
`BOUNDARY` records, and several index records: FDST, SKEL, DIV, OTH, NCX, TAGX,
CNCX, and related flow/resource records.

Current code now pins the first layer of those expectations:

- KF8 index section numbers must be inside the available section list
- FDST flow pairs must fit inside the raw markup and be ordered
- INDX and TAGX records must have valid signatures and internal offsets
- TAGX variable-length strings must consume bytes monotonically
- malformed index data should fail or warn deterministically instead of
  surfacing raw parser exceptions

Full skeleton/div insertion products are still a next-layer target for
realistic KF8 conversion fixtures.

## Unicode And Locale Coverage

Existing tests already cover useful unicode boundaries:

- EXTH title, authors, publisher, comments, tags, language, and page
  progression with multilingual payloads
- malformed UTF-8 EXTH fields decoded with replacement
- CP1252 metadata payloads
- variable-width integer and trailing-byte helpers
- old MOBI output round-tripped back through `MOBIInput` with multilingual OEB
  content
- output handling for lone surrogate metadata
- deterministic old-MOBI bytes under frozen runtime values
- optional real-corpus MOBI/AZW3 input and metadata reads when
  `LiuXin_alpha_data` fixtures are available

The generated fixture layer now adds small PalmDB/MOBI payloads with non-ASCII
titles, authors, EXTH fields, and text records, so unicode assertions do not
depend only on optional external fixtures or full end-to-end writer output.

## Hostile Corpus

The checked-in hostile coverage currently includes:

- truncated PalmDB headers
- short PalmDB record tables
- duplicate, non-monotonic, and out-of-file record offsets
- short record 0 payloads
- impossible MOBI header lengths
- out-of-range title offsets
- malformed EXTH block signatures, declared lengths, and item sizes
- out-of-range metadata section access
- metadata malformed-input fuzzing for empty, tiny, wrong-format, zip, PDF, and
  marker-only MOBI payloads
- invalid metadata payloads that raise by default and can opt into fallback
  metadata
- Topaz dispatch failure paths
- malformed EXTH UTF-8 replacement
- hostile metadata text sanitization before EXTH writes
- low-level `StreamSlicer`, record patching, and EXTH update helper edges
- output-side lone surrogate replacement
- missing, truncated, and malformed HUFF/CDIC tables
- DH-compressed MOBI records whose HUFF/CDIC range points outside the section
  table
- truncated, wrong-signature, and out-of-range INDX/TAGX records
- out-of-range KF8 index references
- malformed FDST records, including wrong signatures, truncated tables,
  reversed ranges, and ranges beyond raw markup
- invalid skeleton and NCX indices wrapped as MOBI parser errors
- resource ranges outside the section table
- CRES resources without an active CONT container

Missing hostile coverage is now concentrated in syntactically valid but
pathological conversion products: decompression expansion budgets, real
image/font/container resource decoding edges, and full KF8 skeleton/div
position products built from realistic fixtures.

## Salvage And Reporting Direction

MOBI has several existing recovery paths: retrying the input reader with
`try_extra_data_fix=True`, stripping random bytes from malformed HTML markup,
using BeautifulSoup for bad paragraph nesting, falling back to filename
metadata when requested, and tolerating missing optional image metadata.

Those recovery paths should remain intentional and visible. Future recovery or
trusted-input modes should report:

- whether the extra-data retry path was used
- malformed record-table or header checks that were relaxed
- skipped records or resources
- index records ignored or rebuilt
- decompression failures and affected record numbers
- markup bytes stripped or characters replaced
- fallback metadata source used

Default conversion should continue to fail fast when PalmDB/MOBI structure is
undefined. Any future trusted-input profile may raise bounded size/count limits,
but it should not permit invalid record offsets, path escape behavior in debug
extraction, unreadable header structure, or silent content loss.

## Reusable Fixture Helpers

`tests/support/file_format_mobi.py` provides:

- PalmDB header and record-table construction
- minimal MOBI record 0 construction
- EXTH block construction
- section-offset corruption
- truncation and mutation helpers
- log/options stubs shared by input and metadata tests

## Next Hardening Slice

The next useful MOBI slice should move from parser preflight into conversion
policy:

- add bounded decompression expansion checks for PalmDOC and HUFF/CDIC payloads
- add realistic KF8 fixture products that exercise skeleton/div insertion,
  NCX href creation, and resource mapping through `MOBIInput`
- add image/font/resource fixtures that make skip, warn, and fail behavior
  explicit
- add optional trusted-input overrides only for bounded limits, not for invalid
  offsets or unreadable headers

High-value focused commands:

- `python3 -m pytest tests/file_formats/mobi -q`
- `python3 -m pytest tests/metadata/file_sources/test_mobi_metadata_source.py
  tests/metadata/file_sources/test_mobi_edge_cases.py
  tests/metadata/file_sources/test_malformed_input_fuzzing.py -q`
- `python3 -m pytest tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py
  tests/file_formats/conversion/test_conversion_top_level_smoke.py -q`
- `python3 scripts/run_file_formats_lane.py --lane fast`
