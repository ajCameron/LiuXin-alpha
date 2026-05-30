# Conversion Pipeline Architecture TODO - 2026-05-24

## Context

The recent file-format lane has built a strong fixture and hostile-input test
spine across text/markup, PML, ODT, EPUB, DOCX, HTMLZ, comic, FB2/FBZ, LIT,
MOBI, and PDB. The gap is no longer only parser safety. The broader conversion
pipeline is still mostly the legacy `input -> OEB -> output` shape, and that
does not give us a clean way to sign pipeline behavior off as done.

## Durable Doc

Added:

- `dev-docs/conversion_pipeline_todo.md`

Updated:

- `dev-docs/global_todo.md`
- `docs/development/file-format-unicode-conversion.md`

## Current Decision

Treat conversion as a graph of explicit capability edges:

- OEB-normalized edges remain first-class.
- Direct `A -> B` edges are allowed where they preserve more structure.
- External-tool adapters need version, timeout, fallback, and diagnostics
  reporting.
- Lossy conversion must be visible and testable, starting with PML
  unsupported-character replacement.

## Sign-Off Direction

A format or conversion edge should not be signed off just because it does not
crash. Sign-off should require:

- multilingual valid fixture coverage
- malformed and hostile input coverage
- conversion-product assertions
- metadata reader/writer coverage where applicable
- explicit lossy-boundary tests
- reportable diagnostics for recoverable loss
- focused validation commands recorded in durable docs or working memory

## Suggested Next Slice

Progress through 2026-05-26:

1. Add a tiny report object or structured warning surface for conversion.
2. Pin PML unsupported-character replacement as recoverable output plus a
   visible loss event.
3. Add tests that assert the output is produced and the loss is reported.
4. Add explicit edge descriptions for the current legacy OEB-backed path and
   future direct/external adapters.
5. Share ZIP archive member preflight policy across FBZ, HTMLZ, EPUB, DOCX,
   ODT, and comic containers.

Next likely slice: create the sign-off table that can track each format and
conversion edge against valid fixtures, hostile input, product validation,
metadata coverage, loss reporting, and remaining blockers.

Useful current baseline command:

```text
python3 scripts/run_file_formats_lane.py --lane fast
```

Then run the heavy lane separately and record whether failures are real defects
or environment/tooling boundaries:

```text
python3 scripts/run_file_formats_lane.py --lane heavy
```
