# Conversion HTMLZ Diagnostics Slice - 2026-06-04

## Decision

HTMLZ optional-enrichment diagnostics are promoted to candidate for focused
sign-off review after this implementation slice merges.

The implemented scope preserves existing warning-and-continue behavior and adds
structured recoverable loss events:

- `optional-opf-enrichment-failed` when optional top-level OPF cannot be read
- `optional-cover-unsafe-path` when OPF cover references are unsafe
- `optional-cover-missing` when OPF cover references point at missing files

## Validation

```text
python3 -m py_compile src/LiuXin_alpha/file_formats/conversion/plugins/htmlz_input.py tests/file_formats/htmlz/test_htmlz_malformed_hostile.py
clean

python3 -m pytest tests/file_formats/htmlz/test_htmlz_malformed_hostile.py -q
18 passed in 14.53s

python3 -m pytest tests/file_formats/htmlz -q
31 passed in 5.36s

python3 -m pytest tests/file_formats/test_archive_preflight.py tests/file_formats/htmlz/test_htmlz_malformed_hostile.py -q
33 passed in 4.85s

python3 -m pytest tests/file_formats/conversion/test_conversion_report.py tests/file_formats/conversion/test_conversion_edges.py tests/file_formats/conversion/test_conversion_top_level_smoke.py tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py -q
13 passed in 10.59s
```

## Boundaries

Required top-level HTML failures and hostile archive preflight remain strict
conversion failures rather than recoverable loss events. Broader HTMLZ salvage
or trusted-input behavior remains separate pipeline/container policy work.

## Next Useful Step

After this branch merges, perform a focused HTMLZ sign-off review for optional
OPF/cover enrichment diagnostics.
