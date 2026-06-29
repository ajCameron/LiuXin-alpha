# Conversion FB2/FBZ Sign-Off - 2026-05-31

## Decision

FB2/FBZ is signed off for the current format scope in
`dev-docs/conversion_pipeline_signoff.md`.

The signed-off scope includes:

- XML-backed `.fb2` input conversion for UTF-8 and UTF-16 fixtures
- strict single-FB2-member `.fbz` archive selection and preflight
- metadata read/write for plain and zipped FB2 payloads
- `FB2MLizer`/`FB2Output` unicode serialization
- hostile XML, archive, embedded-binary ID, and corrupt-base64 boundaries
- generated OPF, XHTML, CSS, extracted-binary, and output-FB2 product
  assertions

## Evidence

Durable docs reviewed:

- `docs/development/file-formats/fb2/README.md`
- `working-memory/file-formats-fb2-unicode-hostile-2026-05-21.md`
- `working-memory/file-formats-fbz-hardening-2026-05-22.md`

Focused validation:

```text
python3 -m pytest tests/file_formats/fb2 tests/metadata/file_sources/test_fb2_metadata_source.py tests/metadata/file_sources/test_fb2_edge_cases.py -q
76 passed in 14.17s

python3 -m pytest tests/file_formats/test_archive_preflight.py tests/file_formats/fb2/test_fb2_malformed_hostile.py tests/file_formats/fb2/test_fb2_zip_framework.py -q
48 passed in 12.08s
```

## Boundary

This is a format-scope sign-off, not a claim that pipeline-wide diagnostics are
complete. The broader goal of structured `ConversionReport` events for every
recoverable warning remains a pipeline-wide reporting workstream.

Future real-corpus FB2/FBZ defects should be added as regressions or new
follow-up rows rather than reopening the already-covered baseline.
