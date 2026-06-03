# Conversion TXT Loss-Report Slice - 2026-06-03

## Decision

TXT input/output encoding-loss behavior is promoted to candidate for focused
sign-off review after this implementation slice merges.

The implemented scope keeps existing recoverable bytes and adds structured loss
events:

- malformed TXT input bytes decoded with replacement emit
  `input-decoding-byte-replacement` in the `txt-input` phase
- final TXT output characters that cannot be represented by the selected output
  encoding emit `output-encoding-character-replacement` in the `txt-output`
  phase
- output events use the current conversion edge context when available
- UTF-8 output with no replacement still attaches a report with no loss events

## Validation

```text
python3 -m py_compile src/LiuXin_alpha/file_formats/conversion/plugins/txt_input.py src/LiuXin_alpha/file_formats/conversion/plugins/txt_output.py tests/file_formats/txt/test_txt_unicode_torture.py tests/file_formats/txt/test_txt_output_serializers_unicode_framework.py
clean

python3 -m pytest tests/file_formats/txt/test_txt_unicode_torture.py tests/file_formats/txt/test_txt_output_serializers_unicode_framework.py -q
13 passed in 6.26s

python3 -m pytest tests/file_formats/txt -q
39 passed, 1 warning in 5.31s

python3 -m pytest tests/file_formats/conversion/test_conversion_report.py tests/file_formats/conversion/test_conversion_edges.py tests/file_formats/conversion/test_conversion_top_level_smoke.py tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py -q
13 passed in 6.88s

python3 -m pytest tests/file_formats/test_conversion_framework.py tests/file_formats/test_unicode_framework.py -q
10 passed in 0.53s
```

## Boundaries

Malformed Markdown/Textile parser failures remain hard failures rather than
recoverable loss events in the current TXT row. Direct/external markup edge
selection and broader markup loss diagnostics remain separate pipeline work.

## Next Useful Step

After this branch merges, perform a focused TXT sign-off review for the current
input/output encoding-loss report scope.
