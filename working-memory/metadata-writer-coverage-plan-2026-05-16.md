# Metadata Writer Coverage Plan - 2026-05-16

Branch: `renderer-coverage-tests`

## Context

The metadata file-source extractor pass is validated at `347 passed`,
`114 skipped`, `19 warnings`, with focused `metadata.file_sources` coverage at
90%. That pass also found a real writer-side PDB/eReader header corruption bug,
so the next high-value lane should be metadata writers.

Durable docs:

- `docs/development/metadata-writer-coverage-contract.md`

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

1. OPF and XML-backed writers.
   - Round-trip through XML parsing and the local OPF reader.
   - Include hostile unicode and XML-invalid character handling.

2. RTF, PDB/eReader, and Topaz.
   - Use `BytesIO` and synthetic builders where possible.
   - Assert read-after-write and byte preservation around metadata regions.

3. EPUB, EXTZ, DOCX, and ZIP-like containers.
   - Validate `zipfile` integrity after writes.
   - Assert non-metadata members survive unchanged.
   - Exercise embedded OPF replacement and cover behavior.

4. MOBI and PDF.
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
