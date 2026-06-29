# Metadata Coverage Fallback Alignment - 2026-05-17

Branch: `metadata-coverage-fallback-test-alignment`

## Context

After the text-reader malformed-input PR merged, the external full coverage run
reported four failures in stale tests. The implementation contract is now:

- individual metadata readers are strict by default for non-credible or corrupt
  format inputs
- conservative shell metadata is only expected when an explicit fallback knob is
  passed
- a later central best-effort metadata facade can own routing and fallback
  policy across readers

## Implemented

Aligned the stale tests with that contract:

- PDB truncated/corrupt payloads now assert `PdbFormatError` by default, then
  assert shell metadata with `fallback_on_parse_error=True`
- LIT broken-container behavior now asserts `LitFormatError` by default, then
  asserts fallback metadata with `fallback_on_parse_error=True`
- PMLZ invalid archives now assert `PmlFormatError` by default, then assert the
  explicit fallback return
- PML wrong stream object types now assert `TypeError` by default, then assert
  explicit fallback metadata

## Validation

Focused validation:

```bash
python3 -m pytest \
  tests/metadata/file_sources/test_pdb_metadata_fixtures.py \
  tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py \
  -q
```

Result: `21 passed`.

Broader reader validation:

```bash
python3 -m pytest \
  tests/metadata/file_sources/test_pdb_metadata_fixtures.py \
  tests/metadata/file_sources/test_pdb_metadata_source.py \
  tests/metadata/file_sources/test_pdb_subreader_edge_cases.py \
  tests/metadata/file_sources/test_lit_metadata_source.py \
  tests/metadata/file_sources/test_pml_metadata_source.py \
  tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py \
  tests/metadata/file_sources/test_malformed_input_fuzzing.py \
  -q
```

Result: `184 passed`.

External full coverage rerun:

- Pytest JSON:
  `working-memory/test-results/full-suite-2026-05-17-161924.json`
- Coverage XML:
  `working-memory/test-results/coverage-2026-05-17-161711.xml`
- Outcome: `3766 passed`, `43 skipped`, `22 xfailed`, `5 xpassed`, exit code
  `0`
- Overall coverage: `51.68%` line, `33.81%` branch
- `metadata.file_sources`: `91.4%` line, `81.5%` branch
- `metadata.file_sources.pdb`: `93.7%` line, `87.5%` branch

## Next

Metadata readers are now above 90% line coverage. The next high-value coverage
work is probably outside this reader lane: either the broader metadata container
/ standardization surfaces, or the larger legacy/vendor-heavy project-wide
coverage holes if the goal shifts from metadata confidence to headline
coverage.
