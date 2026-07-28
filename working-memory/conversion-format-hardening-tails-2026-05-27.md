# Conversion Format Hardening Tails - 2026-05-27

## Context

Stage 5 of `dev-docs/conversion_pipeline/conversion_pipeline_todo.md` is the cleanup pass for
current format-hardening tails before moving to the sign-off table.

## Stage 5A: PDB Haodoo

Implemented and validated the PDB Haodoo hostile subreader tail.

Added:

- `tests/file_formats/pdb/test_pdb_haodoo_hostile.py`
- Haodoo fixture helpers in `tests/support/file_format_pdb.py`

Updated:

- `src/LiuXin_alpha/file_formats/pdb/haodoo/reader.py`
- `tests/file_formats/pdb/test_pdb_binary_framework.py`
- `dev-docs/file-formats/pdb/README.md`
- `dev-docs/file-format-unicode-conversion.md`
- `dev-docs/conversion_pipeline/conversion_pipeline_todo.md`

Behavior:

- Generated CP950 and UTF-16LE Haodoo fixtures now exercise reader output and
  the `PDBInput` plugin path.
- Malformed header separators, non-integer record counts, chapter-title count
  mismatches, declared chapter records beyond available sections, and direct
  out-of-range section access now raise `PDBError` instead of raw Python
  parser/indexing errors.
- Metadata fallback remains unchanged: metadata reads can still fall back to
  the PalmDB wrapper title when Haodoo body parsing fails.

Validation so far:

```text
python3 -m py_compile src/LiuXin_alpha/file_formats/pdb/haodoo/reader.py \
  tests/support/file_format_pdb.py \
  tests/file_formats/pdb/test_pdb_binary_framework.py \
  tests/file_formats/pdb/test_pdb_haodoo_hostile.py

python3 -m pytest -q \
  tests/file_formats/pdb/test_pdb_binary_framework.py \
  tests/file_formats/pdb/test_pdb_haodoo_hostile.py
20 passed in 13.68s

python3 -m pytest tests/file_formats/pdb -q
75 passed in 7.82s

python3 -m pytest \
  tests/metadata/file_sources/test_pdb_metadata_source.py \
  tests/metadata/file_sources/test_pdb_metadata_fixtures.py \
  tests/metadata/file_sources/test_pdb_subreader_edge_cases.py -q
25 passed in 16.09s

python3 -m pytest tests/metadata/file_sources/test_malformed_input_fuzzing.py -q
133 passed in 27.77s

python3 scripts/run_file_formats_lane.py --lane fast
Discovered 119 file_formats test files
Lane 'fast' has 100 test files
787 passed, 1 skipped, 15 warnings in 66.78s
```

## Stage 5B: MOBI Decompression And KF8 Resources

Implemented and validated the MOBI bounded decompression/resource-product tail.

Updated:

- `src/LiuXin_alpha/file_formats/mobi/reader/mobi6.py`
- `src/LiuXin_alpha/file_formats/mobi/huffcdic.py`
- `tests/file_formats/mobi/test_mobi_deep_hostile.py`
- `dev-docs/file-formats/mobi/README.md`
- `dev-docs/file-format-unicode-conversion.md`
- `dev-docs/conversion_pipeline/conversion_pipeline_todo.md`

Behavior:

- PalmDOC text decompression now enforces per-record and total uncompressed-size
  budgets derived from MOBI header record size and hard caps.
- HUFF/CDIC text decompression enforces the same per-record budget, including
  recursive phrase expansion.
- Decompressor failures surface as `MobiError` with record context.
- KF8 direct image sections and contained CRES image sections now have concrete
  extraction-product assertions.

Validation:

```text
python3 -m py_compile src/LiuXin_alpha/file_formats/mobi/reader/mobi6.py \
  src/LiuXin_alpha/file_formats/mobi/huffcdic.py \
  tests/file_formats/mobi/test_mobi_deep_hostile.py

python3 -m pytest -q tests/file_formats/mobi/test_mobi_deep_hostile.py
29 passed in 10.76s

python3 -m pytest tests/file_formats/mobi -q
83 passed in 25.29s

python3 -m pytest \
  tests/metadata/file_sources/test_mobi_metadata_source.py \
  tests/metadata/file_sources/test_mobi_edge_cases.py \
  tests/metadata/file_sources/test_malformed_input_fuzzing.py -q
153 passed in 28.48s
```

## Stage 5C: LIT LZX Output Boundary

Implemented and validated the LIT writer boundary around the unavailable LZX
compressor backend.

Updated:

- `src/LiuXin_alpha/file_formats/lit/writer.py`
- `tests/file_formats/lit/test_lit_conversion_unicode_framework.py`
- `dev-docs/file-formats/lit/README.md`
- `dev-docs/file-format-unicode-conversion.md`
- `working-memory/file-formats-lit-hostile-hardening-2026-05-22.md`
- `dev-docs/conversion_pipeline/conversion_pipeline_todo.md`

Behavior:

- `LitWriter` now exposes `LitWriterError` for output-side writer failures.
- Missing LZX compression fails before opening a filesystem output path, so the
  unavailable backend does not create a partial `.lit` file.
- `_build_storage()` uses the same LZX guard, replacing the old raw
  `RuntimeError` path.

Validation:

```text
python3 -m py_compile src/LiuXin_alpha/file_formats/lit/writer.py \
  tests/file_formats/lit/test_lit_conversion_unicode_framework.py

python3 -m pytest -q \
  tests/file_formats/lit/test_lit_conversion_unicode_framework.py \
  tests/file_formats/lit/test_lit_modernized.py
11 passed in 2.99s

python3 -m pytest tests/file_formats/lit -q
59 passed in 6.36s
```

## Stage 5 Status

All named Stage 5 format-hardening tails are implemented.

Final validation:

```text
python3 scripts/run_file_formats_lane.py --lane fast
792 passed, 1 skipped, 15 warnings in 66.75s

git diff --check
clean
```
