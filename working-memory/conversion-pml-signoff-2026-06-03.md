# Conversion PML Sign-Off - 2026-06-03

## Decision

PML output lossy-boundary behavior is signed off for the current row scope.

The signed-off scope includes:

- deterministic `PMLMLizer` output over the shared OEB fixture
- deterministic `.pmlz` bytes from `PMLOutput`
- supported unicode emitted as PML escapes
- recoverable `?` replacement for unsupported characters
- aggregate `unsupported-character-replacement` `ConversionReport` loss event
- loss-event count, samples, replacement details, recoverability, and edge
  context
- explicit legacy OEB-backed conversion edge naming for reports

## Validation

```text
python3 -m pytest tests/file_formats/pml tests/file_formats/conversion/test_conversion_report.py tests/file_formats/conversion/test_conversion_edges.py tests/file_formats/conversion/test_conversion_top_level_smoke.py -q
57 passed in 8.97s

python3 -m pytest tests/metadata/file_sources/test_pml_metadata_source.py -q
12 passed in 9.69s

python3 -m pytest tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py -q
4 passed in 7.37s
```

## Boundaries

This signs off the PML output boundary only. It does not sign off
pipeline-wide loss reporting, fallback execution, external-tool planning, or
loss diagnostics for other formats.

## Next Useful Step

There are no remaining candidate rows ready for sign-off. Pick a provisional
row, close its named blocker, and promote it to candidate. Good next choices are
HTMLZ optional-enrichment diagnostics, comic RAR/backend variance, TXT
loss-report semantics, or broader conversion-report plumbing.
