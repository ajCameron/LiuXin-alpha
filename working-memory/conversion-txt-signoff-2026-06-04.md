# Conversion TXT Sign-Off - 2026-06-04

## Decision

TXT input/output encoding-loss report behavior is signed off for the current row
scope.

The signed-off scope includes:

- encoded TXT input fixtures and newline/output serializer matrix coverage
- malformed TXT input byte replacement reported as
  `input-decoding-byte-replacement`
- final TXT output encoding replacement reported as
  `output-encoding-character-replacement`
- conversion-edge context on TXT output loss events
- no loss events for UTF-8 TXT output that preserves the shared corpus
- TXT/TXTZ metadata file-source coverage as supporting evidence
- stable current Markdown/Textile extension-path behavior

## Validation

```text
python3 -m pytest tests/file_formats/txt/test_txt_unicode_torture.py tests/file_formats/txt/test_txt_output_serializers_unicode_framework.py -q
13 passed in 7.88s

python3 -m pytest tests/file_formats/txt -q
39 passed, 1 warning in 8.54s

python3 -m pytest tests/file_formats/conversion/test_conversion_report.py tests/file_formats/conversion/test_conversion_edges.py tests/file_formats/conversion/test_conversion_top_level_smoke.py tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py -q
13 passed in 11.08s

python3 -m pytest tests/metadata/file_sources/test_txt_metadata_source.py tests/metadata/file_sources/test_txtz_metadata_source.py -q
22 passed in 15.43s

python3 -m pytest tests/file_formats/txt tests/file_formats/markdown tests/file_formats/textile -q
90 passed, 3 warnings in 10.01s
```

## Boundaries

Malformed Markdown/Textile parser failures remain hard failures rather than
recoverable loss events in the current TXT row. Direct/external markup edge
selection and broader markup loss diagnostics remain separate pipeline work.

## Next Useful Step

There are no remaining candidate rows ready for sign-off. Pick a provisional row,
close its named blocker, and promote it to candidate. Good next choices are HTMLZ
optional-enrichment diagnostics, comic RAR/backend variance, shared archive
preflight metadata-reader policy, or broader conversion-report plumbing.
