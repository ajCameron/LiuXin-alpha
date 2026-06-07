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
- ODT input/container conversion, signed off 2026-06-01 for the current format
  scope.
- EPUB input/container conversion, signed off 2026-06-02 for the current format
  scope.
- DOCX input/container conversion, signed off 2026-06-02 for the current format
  scope.
- PDB input/metadata hardening, signed off 2026-06-03 for the current format
  scope.
- PML output lossy-boundary behavior, signed off 2026-06-03 for the current
  row scope.
- TXT input/output encoding-loss report behavior, signed off 2026-06-04 for
  the current row scope.
- HTMLZ optional-enrichment diagnostics, signed off 2026-06-06 for the current
  row scope.

## Remaining Candidate Rows

Rows that are ready for a deliberate sign-off review:

- Comic CBZ/CBC/CBR structured diagnostics

## Rows Not Ready Yet

Rows that still have named blockers:

- LIT full output remains blocked by the unavailable LZX compressor backend.
- MOBI/KF8 remains provisional pending realistic skeleton/div/NCX products,
  non-image resource fixtures, and trusted budget policy.
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

## ODT Review

ODT was reviewed and signed off on 2026-06-01. Focused validation passed:

```text
python3 -m pytest tests/file_formats/odt tests/file_formats/odf tests/metadata/file_sources/test_odt_metadata_source.py tests/metadata/file_sources/test_odt_beta_metadata_source.py tests/metadata/file_sources/test_text_odt_edge_cases.py -q
48 passed, 12 warnings in 13.79s

python3 -m pytest tests/file_formats/test_archive_preflight.py tests/file_formats/odt/test_odt_container_framework.py tests/file_formats/odt/test_odt_malformed_hostile.py -q
28 passed in 8.79s
```

## EPUB Review

EPUB was reviewed and signed off on 2026-06-02. Focused validation passed:

```text
python3 -m pytest tests/file_formats/epub tests/metadata/file_sources/test_epub_metadata_source.py tests/metadata/file_sources/test_epub_edge_cases.py tests/metadata/file_sources/test_opf_metadata_source.py tests/metadata/file_sources/test_opf_edge_cases.py -q
79 passed, 7 warnings in 29.75s

python3 -m pytest tests/file_formats/test_archive_preflight.py tests/file_formats/epub/test_epub_container_framework.py tests/file_formats/epub/test_epub_malformed_hostile.py -q
42 passed in 15.61s
```

## DOCX Review

DOCX was reviewed and signed off on 2026-06-02. Focused validation passed:

```text
python3 -m pytest tests/file_formats/docx tests/metadata/file_sources/test_docx_metadata_source.py -q
39 passed in 25.49s

python3 -m pytest tests/file_formats/test_archive_preflight.py tests/file_formats/docx/test_docx_container_framework.py tests/file_formats/docx/test_docx_malformed_hostile.py -q
39 passed in 16.98s
```

## PDB Review

PDB input/metadata hardening was reviewed and signed off on 2026-06-03. Focused
validation passed:

```text
python3 -m pytest tests/file_formats/pdb tests/metadata/file_sources/test_pdb_metadata_source.py tests/metadata/file_sources/test_pdb_metadata_fixtures.py tests/metadata/file_sources/test_pdb_subreader_edge_cases.py -q
100 passed in 24.10s

python3 -m pytest tests/metadata/file_sources/test_malformed_input_fuzzing.py -q
133 passed in 26.09s

python3 -m pytest tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py tests/file_formats/conversion/test_conversion_top_level_smoke.py -q
6 passed in 17.27s
```

## PML Review

PML output lossy-boundary behavior was reviewed and signed off on 2026-06-03.
Focused validation passed:

```text
python3 -m pytest tests/file_formats/pml tests/file_formats/conversion/test_conversion_report.py tests/file_formats/conversion/test_conversion_edges.py tests/file_formats/conversion/test_conversion_top_level_smoke.py -q
57 passed in 8.97s

python3 -m pytest tests/metadata/file_sources/test_pml_metadata_source.py -q
12 passed in 9.69s

python3 -m pytest tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py -q
4 passed in 7.37s
```

## TXT Candidate Promotion

TXT input/output encoding-loss report behavior was promoted to candidate on
2026-06-03. Focused validation passed:

```text
python3 -m pytest tests/file_formats/txt/test_txt_unicode_torture.py tests/file_formats/txt/test_txt_output_serializers_unicode_framework.py -q
13 passed in 6.26s

python3 -m pytest tests/file_formats/txt -q
39 passed, 1 warning in 5.31s

python3 -m pytest tests/file_formats/conversion/test_conversion_report.py tests/file_formats/conversion/test_conversion_edges.py tests/file_formats/conversion/test_conversion_top_level_smoke.py tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py -q
13 passed in 6.88s

python3 -m pytest tests/file_formats/test_conversion_framework.py tests/file_formats/test_unicode_framework.py -q
10 passed in 0.53s
```

## TXT Review

TXT input/output encoding-loss report behavior was reviewed and signed off on
2026-06-04. Focused validation passed:

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

## Comic Candidate Promotion

Comic CBZ/CBC/CBR diagnostics were promoted to candidate on 2026-06-06.
Focused validation passed:

```text
python3 -m py_compile src/LiuXin_alpha/file_formats/conversion/plugins/comic_input.py tests/file_formats/comic/test_comic_malformed_hostile.py tests/file_formats/comic/test_comic_container_framework.py
clean

python3 -m pytest tests/file_formats/comic/test_comic_malformed_hostile.py tests/file_formats/comic/test_comic_container_framework.py -q
50 passed in 14.76s

python3 -m pytest tests/file_formats/comic -q
67 passed in 10.47s

python3 -m pytest tests/file_formats/test_archive_preflight.py tests/file_formats/comic/test_comic_malformed_hostile.py -q
54 passed in 10.28s

python3 -m pytest tests/file_formats/conversion/test_conversion_report.py tests/file_formats/conversion/test_conversion_edges.py tests/file_formats/conversion/test_conversion_top_level_smoke.py tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py -q
13 passed in 10.03s
```

The candidate scope is structured diagnostics for recoverable comic behavior:
missing CBC listed comics emit `cbc-listed-comic-missing`, names-only CBR/RAR
fallback preflight emits `rar-names-only-preflight-limited`, and strict archive
or required-product failures remain strict. A small redistributable real CBR
corpus remains future regression coverage rather than a blocker for this row.

## HTMLZ Review

HTMLZ optional-enrichment diagnostics were reviewed and signed off on
2026-06-06. Focused validation passed:

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

## HTMLZ Candidate Promotion

HTMLZ optional-enrichment diagnostics were promoted to candidate on 2026-06-04.
Focused validation passed:

```text
python3 -m pytest tests/file_formats/htmlz/test_htmlz_malformed_hostile.py -q
18 passed in 14.53s

python3 -m pytest tests/file_formats/htmlz -q
31 passed in 5.36s

python3 -m pytest tests/file_formats/test_archive_preflight.py tests/file_formats/htmlz/test_htmlz_malformed_hostile.py -q
33 passed in 4.85s

python3 -m pytest tests/file_formats/conversion/test_conversion_report.py tests/file_formats/conversion/test_conversion_edges.py tests/file_formats/conversion/test_conversion_top_level_smoke.py tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py -q
13 passed in 10.59s
```

## Next Useful Step

After the comic diagnostics slice merges, perform a focused Comic sign-off
review. The candidate scope is structured diagnostics for recoverable CBC
missing-member salvage and CBR/RAR names-only preflight variance while strict
archive failures remain strict.
