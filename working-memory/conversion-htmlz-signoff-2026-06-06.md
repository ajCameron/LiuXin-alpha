# Conversion HTMLZ Sign-Off - 2026-06-06

## Decision

HTMLZ optional-enrichment diagnostics are signed off for the current row scope.

The signed-off scope includes:

- required top-level HTML/XHTML discovery
- optional OPF and cover enrichment
- warning-and-continue behavior for optional enrichment losses
- recoverable `ConversionReport` events for malformed optional OPF, unsafe cover
  references, and missing cover files
- strict failures for missing required HTML and hostile archive boundaries
- multilingual plugin-path HTML product assertions

## Validation

```text
python3 -m py_compile src/LiuXin_alpha/file_formats/conversion/plugins/htmlz_input.py tests/file_formats/htmlz/test_htmlz_malformed_hostile.py
clean

python3 -m pytest tests/file_formats/htmlz/test_htmlz_malformed_hostile.py -q
18 passed in 13.22s

python3 -m pytest tests/file_formats/htmlz -q
31 passed in 15.69s

python3 -m pytest tests/file_formats/test_archive_preflight.py tests/file_formats/htmlz/test_htmlz_malformed_hostile.py -q
33 passed in 7.61s

python3 -m pytest tests/file_formats/conversion/test_conversion_report.py tests/file_formats/conversion/test_conversion_edges.py tests/file_formats/conversion/test_conversion_top_level_smoke.py tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py -q
13 passed in 7.91s
```

## Boundaries

Broader HTMLZ salvage/reporting policy and trusted archive-budget behavior remain
separate pipeline/container policy work. Future real-corpus HTMLZ defects should
be added as regressions or new follow-up rows.

## Next Useful Step

There are no remaining candidate rows after HTMLZ sign-off. Pick a
provisional/open row and remove a named blocker before the next sign-off review:
comic structured diagnostics/backend variance, MOBI/KF8 richer product coverage,
or pipeline-wide report/fallback semantics.
