# Metadata Comic Reader Fuzzing - 2026-05-17

Branch: `metadata-comic-reader-fuzzing`

## Context

After PR #48 merged, the next metadata reader pass is CBR/CBZ. The old
`ComicMetadataReader` kept archive extraction logic inline in
`customize.builtins.metadata_readers`; it failed for many bad archives but could
still return shell metadata for empty/non-comic CBZ inputs because comment
metadata parsing was tolerant.

## Implemented

- Added `metadata.file_sources.comic` as the canonical CBR/CBZ reader wrapper.
- Added `ComicFormatError` for strict comic archive failures.
- The built-in `ComicMetadataReader` now delegates to the wrapper.
- Strict default behavior:
  - unreadable/wrong-format CBR/CBZ raises
  - readable archives with no image members raise
  - empty first image member raises
- Preserved valid fallback behavior:
  - valid image comic archives without ComicBookInfo comments return shell
    metadata with `cover_data`
  - malformed ComicBookInfo comments are ignored, not fatal
  - `fallback_on_parse_error=True` remains available for future best-effort
    routing
- Extended the shared malformed-input corpus and registry assertions for
  `cbr`/`cbz`.

## Validation

Focused validation:

```bash
python3 -m pytest \
  tests/metadata/file_sources/test_comic_metadata_source.py \
  tests/metadata/file_sources/test_malformed_input_fuzzing.py \
  -q
```

Result: `125 passed`.

Archive/comic focus set:

```bash
python3 -m pytest \
  tests/metadata/file_sources/test_comic_metadata_source.py \
  tests/metadata/file_sources/test_archive_metadata_source.py \
  tests/metadata/file_sources/test_archive_container_edge_cases.py \
  tests/metadata/file_sources/test_zip_metadata_source.py \
  tests/metadata/file_sources/test_rar_metadata_source.py \
  tests/metadata/file_sources/test_malformed_input_fuzzing.py \
  -q
```

Result: `151 passed`, `1 skipped`.

Broader metadata-reader validation:

```bash
python3 -m pytest \
  tests/metadata/file_sources/test_malformed_input_fuzzing.py \
  tests/metadata/file_sources/test_metadata_reader_registry.py \
  tests/metadata/file_sources/test_dispatcher_modernized.py \
  tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py::test_dispatcher_plugin_adapter_and_failure_edges \
  tests/metadata/file_sources/test_opf_metadata_source.py \
  tests/metadata/file_sources/test_opf_edge_cases.py \
  tests/metadata/file_sources/test_fb2_metadata_source.py \
  tests/metadata/file_sources/test_fb2_edge_cases.py \
  tests/metadata/file_sources/test_extz_metadata_source.py \
  tests/metadata/file_sources/test_txtz_metadata_source.py \
  tests/metadata/file_sources/test_text_odt_edge_cases.py \
  tests/metadata/file_sources/test_archive_metadata_source.py \
  tests/metadata/file_sources/test_archive_container_edge_cases.py \
  tests/metadata/file_sources/test_zip_metadata_source.py \
  tests/metadata/file_sources/test_rar_metadata_source.py \
  tests/metadata/file_sources/test_comic_metadata_source.py \
  tests/metadata/file_sources/test_pdf_metadata_source.py \
  tests/metadata/file_sources/test_pdf_edge_cases.py \
  tests/metadata/file_sources/test_mobi_metadata_source.py \
  tests/metadata/file_sources/test_mobi_edge_cases.py \
  tests/metadata/file_sources/test_pdb_metadata_source.py \
  tests/metadata/file_sources/test_pdb_subreader_edge_cases.py \
  tests/metadata/file_sources/test_lrf_metadata_source.py \
  tests/metadata/file_sources/test_odt_metadata_source.py \
  tests/metadata/file_sources/test_odt_beta_metadata_source.py \
  tests/metadata/file_sources/test_rtf_metadata_source.py \
  tests/metadata/file_sources/test_snb_metadata_source.py \
  tests/metadata/file_sources/test_lrx_metadata_source.py \
  tests/metadata/file_sources/test_rb_metadata_source.py \
  tests/metadata/file_sources/test_imp_metadata_source.py \
  tests/metadata/file_sources/test_lit_metadata_source.py \
  tests/metadata/file_sources/test_pml_metadata_source.py \
  tests/metadata/file_sources/test_topaz_metadata_source.py \
  tests/metadata/file_sources/test_legacy_format_adapter_edge_cases.py \
  -q
```

Result: `404 passed`, `2 skipped`, `19 warnings`.

The skips are expected when no `unrar` runtime is available locally and when no
optional `.lrx` fixture is present. The warnings are existing date deprecations
from `utils/date.py`.

## Next

Run the broader metadata-reader focus set. If green, commit this branch. The
next malformed-reader lane after CBR/CBZ is text-like safety for TXT, HTML and
plain PML: Unicode torture, no hangs, sanitization, and controlled parser
errors rather than strict rejection of arbitrary text.
