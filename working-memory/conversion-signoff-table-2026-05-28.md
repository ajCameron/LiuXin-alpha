# Conversion Sign-Off Table - 2026-05-28

## Context

Stage 6 of `dev-docs/conversion_pipeline_todo.md` is to create the durable
format/pipeline sign-off table after the first conversion report, edge model,
shared archive preflight, and Stage 5 format-tail work landed.

Durable table:

- `dev-docs/conversion_pipeline_signoff.md`

## What Changed

Added the initial sign-off matrix with these status buckets:

- `Candidate`: ready for sign-off review, subject to release/heavy-lane
  validation.
- `Provisional`: core coverage exists, but a named gap remains.
- `Blocked`: a dependency or required capability is missing.
- `Open`: design or implementation work remains before sign-off review.

The matrix tracks:

- reader/input coverage
- writer/output coverage
- metadata coverage
- hostile boundary coverage
- conversion-product assertions
- loss/diagnostics coverage
- remaining blockers
- current sign-off state

## Signed-Off Rows

Rows already reviewed and signed off:

- FB2/FBZ input/output/metadata conversion, signed off 2026-05-31 for the
  current format scope.

## Remaining Candidate Rows

Rows that are ready for a deliberate sign-off review:

- ODT input/container conversion
- EPUB input/container conversion
- DOCX input/container conversion
- PDB input/metadata hardening scope
- PML output lossy-boundary behavior

## Rows Not Ready Yet

Rows that still have named blockers:

- LIT full output remains blocked by the unavailable LZX compressor backend.
- MOBI/KF8 remains provisional pending realistic skeleton/div/NCX products,
  non-image resource fixtures, and trusted budget policy.
- HTMLZ/comic/TXT remain provisional mostly around structured diagnostics and
  backend variance.
- Markdown/Textile direct or external edges remain open design work.
- Pipeline-wide report/fallback semantics remain provisional beyond the first
  PML report and edge-model slices.

## Validation Reference

The table records the latest Stage 5 fast-lane baseline:

```text
python3 scripts/run_file_formats_lane.py --lane fast
792 passed, 1 skipped, 15 warnings in 66.75s
```

Stage 6 doc-only validation:

```text
git diff --check
clean
```

## FB2/FBZ Review

FB2/FBZ was reviewed and signed off on 2026-05-31. Focused validation passed:

```text
python3 -m pytest tests/file_formats/fb2 tests/metadata/file_sources/test_fb2_metadata_source.py tests/metadata/file_sources/test_fb2_edge_cases.py -q
76 passed in 14.17s

python3 -m pytest tests/file_formats/test_archive_preflight.py tests/file_formats/fb2/test_fb2_malformed_hostile.py tests/file_formats/fb2/test_fb2_zip_framework.py -q
48 passed in 12.08s
```

## Next Useful Step

Pick the next candidate row and perform the same explicit sign-off review. ODT,
EPUB, or DOCX are the most direct next candidates because each is a bounded
archive/container input scope with similar product assertions.
