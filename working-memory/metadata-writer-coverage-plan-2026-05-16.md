# Metadata Writer Coverage Plan - 2026-05-16

Branch: `renderer-coverage-tests`

## Context

The metadata file-source extractor pass is validated at `347 passed`,
`114 skipped`, `19 warnings`, with focused `metadata.file_sources` coverage at
90%. That pass also found a real writer-side PDB/eReader header corruption bug,
so the next high-value lane should be metadata writers.

Durable docs:

- `dev-docs/metadata-writer-coverage-contract.md`

## Writer Contract

Treat writer tests as corruption-prevention tests, not just coverage.

Core invariants:

- write metadata, then read it back with the local reader
- validate archive/container structure where applicable
- preserve unrelated payload bytes or archive members
- reject, sanitize, or escape unsafe unicode deliberately
- avoid unpaired surrogate, control-character, XML, RTF, and PDF escape damage
- fail cleanly without partially trashing a file

Local tests cannot prove external app compatibility, such as Acrobat accepting a
rewritten PDF. They can prove local structural validity with project readers,
stdlib parsers, archive checks, and optional PDF tooling when present.

## Initial Work Order

1. OPF and XML-backed writers. - In progress/completed for OPF and FB2
   hostile XML text on 2026-05-16.
   - Round-trip through XML parsing and the local OPF reader.
   - Include hostile unicode and XML-invalid character handling.

2. RTF, PDB/eReader, and Topaz. - In progress/completed for hostile text
   writer safety on 2026-05-16.
   - Use `BytesIO` and synthetic builders where possible.
   - Assert read-after-write and byte preservation around metadata regions.

3. EPUB, EXTZ, DOCX, and ZIP-like containers. - In progress/completed for
   archive integrity and hostile XML text on 2026-05-16.
   - Validate `zipfile` integrity after writes.
   - Assert non-metadata members survive unchanged.
   - Exercise embedded OPF replacement and cover behavior.

4. MOBI and PDF. - In progress/completed for hostile binary metadata text on
   2026-05-16.
   - Treat as the hard binary lane.
   - Prefer compact synthetic builders; use copied fixtures only when local
     builders become more complex than the writer behavior under test.
   - Use optional dependencies for sanity checks when installed, but do not make
     the core tests depend on external desktop applications.

## Unicode Torture Matrix

Use a shared hostile metadata set across writer tests:

- combining and precomposed forms
- CJK, Greek, Cyrillic, Arabic/right-to-left text
- emoji and astral-plane characters
- smart punctuation and full-width separators
- embedded NULs and C0 controls
- unpaired surrogates
- XML-invalid characters
- RTF braces, backslashes, and `\uNNNN?` escape edges
- PDF parentheses, backslashes, hex strings, and name escapes
- very long titles, authors, tags, comments, and identifiers

## Notes

- Do not mutate checked-in binary fixtures in place; copy to temp files first.
- Make unsupported writer behavior explicit in tests.
- Keep generated coverage artifacts out of commits; working-memory notes and
  source/test/doc changes are the durable record.
- Completed on 2026-05-16 through item 4. The next metadata-writer work should
  come from fresh coverage gaps or production bugs rather than this initial
  hardening plan.

## 2026-05-16 Item 1 Pass

- Fresh OPF creation now sanitizes the same XML-invalid metadata values as OPF
  updates before handing metadata to the OPF serializer.
- FB2 writing now strips XML-invalid characters before assigning text or
  annotation paragraphs into lxml nodes, preventing partial write failures from
  NUL/control characters and unpaired surrogates.
- Added OPF helper tests for fresh OPF serialization and `update_opf_file`
  output-path writes, including XML reparsing, local reader round-trip,
  source-file preservation, and input metadata immutability.
- Added FB2 hostile XML metadata coverage for copied fixtures, including XML
  reparsing, local reader round-trip, and input metadata immutability.
- Focused validation:
  `python3 -m pytest tests/metadata/test_opf_tools.py tests/file_formats/opf/test_opf_facade_write_unicode_torture.py tests/metadata/file_sources/test_fb2_metadata_source.py tests/metadata/file_sources/test_fb2_edge_cases.py -q`
  passed with `34 passed, 5 warnings`.

## 2026-05-16 Item 2 Pass

- RTF metadata writing now removes invalid Unicode scalar/control characters,
  escapes literal RTF braces and backslashes, and emits astral-plane characters
  as UTF-16 `\u` escapes. RTF metadata reading now unescapes literal RTF text
  escapes and repairs surrogate pairs before returning text.
- PDB wrapper title writing now strips invalid scalar/control characters before
  ASCII header normalization. eReader payload writing now cleans the joined
  author field as well as title, publisher, and ISBN so embedded NUL/control
  characters cannot shift metadata record fields.
- Topaz metadata reads/writes now clean invalid scalar/control characters at
  the text boundary while preserving valid UTF-8, extra metadata fields, and
  trailing payload bytes.
- Added focused writer tests for RTF hostile markup escaping and body
  preservation, PDB/eReader hostile text field-boundary safety, and Topaz
  hostile text with extra-field/trailing-payload preservation.
- Focused validation:
  `python3 -m pytest tests/metadata/file_sources/test_rtf_metadata_source.py tests/metadata/file_sources/test_pdb_metadata_source.py tests/metadata/file_sources/test_pdb_subreader_edge_cases.py tests/metadata/file_sources/test_topaz_metadata_source.py -q`
  passed with `30 passed`.

## 2026-05-16 Item 3 Pass

- EXTZ metadata writing now reuses the OPF XML sanitizer before `OPF.smart_update`,
  so embedded NUL/control characters and unpaired surrogates cannot reach OPF
  XML serialization.
- DOCX metadata writing now sanitizes a clone of the metadata object before
  updating `docProps/core.xml` and `docProps/app.xml`, preserving caller data
  while keeping XML-invalid characters out of document property XML.
- Added container-contract tests for EPUB, EXTZ, and DOCX writers that verify
  `zipfile` integrity, unchanged non-metadata members, hostile XML text
  sanitization, read-after-write behavior, caller metadata immutability, and
  cover replacement for EPUB/EXTZ.
- Focused validation:
  `python3 -m pytest tests/metadata/file_sources/test_epub_metadata_source.py tests/metadata/file_sources/test_epub_edge_cases.py tests/metadata/file_sources/test_extz_metadata_source.py tests/metadata/file_sources/test_txtz_metadata_source.py tests/metadata/file_sources/test_docx_metadata_source.py tests/metadata/file_sources/test_archive_container_edge_cases.py tests/metadata/file_sources/test_archive_metadata_source.py tests/metadata/file_sources/test_zip_metadata_source.py -q`
  passed with `87 passed`.

## 2026-05-16 Item 4 Pass

- MOBI metadata writing now sanitizes EXTH/title text fields before binary
  encoding, avoids mutating the caller metadata object, and guards optional
  cover payload access.
- PDF metadata normalization now strips invalid scalar/control characters at
  the shared text boundary before backend metadata generation and reader-side
  parsing.
- Added compact fake-updater and fake-pypdf tests to exercise hard binary
  corruption-prevention behavior without requiring external desktop tools.
- Focused validation:
  `python3 -m pytest tests/metadata/file_sources/test_mobi_metadata_source.py tests/metadata/file_sources/test_mobi_edge_cases.py tests/metadata/file_sources/test_pdf_metadata_source.py tests/metadata/file_sources/test_pdf_edge_cases.py tests/file_formats/pdf/test_pdf_headless_fallback.py -q`
  passed with `55 passed`.

## 2026-05-16 PR Prep

- CI exposed one stale RTF helper expectation from the earlier writer hardening
  work: decoded `\u-1?` output is now sanitized instead of preserving `\uffff`.
  Updated the edge-case test to match the current invalid-Unicode stripping
  contract before pushing the PR branch.
