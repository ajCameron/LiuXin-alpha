# Metadata Text Reader Fuzzing - 2026-05-17

Branch: `metadata-text-reader-fuzzing`

## Context

After PR #49 merged, the next malformed-input pass is the permissive
text-reader lane: TXT, HTML and plain PML. Unlike container readers, these
formats can validly be sparse or malformed, so this pass focuses on safe
fallbacks and sanitization rather than strict rejection.

## Implemented

- TXT:
  - added binary-signature detection before title/byline parsing
  - preserved UTF-16 BOM handling and permissive control-character sanitization
  - added tests for binary signatures, UTF-16, and multiscript/control torture
- HTML:
  - direct byte payloads now parse as payloads instead of being treated as
    filesystem paths
  - obvious binary signatures return safe default metadata
  - parser-internal exceptions are contained as safe default metadata
  - added tests for direct bytes, binary signatures, and parser failure
- Plain PML:
  - added safety tests for binary-ish payloads, malformed comment blocks, and
    control-character sanitization
  - PMLZ remains strict archive handling and is not part of the permissive
    text bucket
- Shared malformed-input corpus now has text-like safety cases for TXT, HTML
  and plain PML, plus registry assertions for the text-like reader set.
- Durable policy doc updated in `dev-docs/malformed-input-fuzzing.md`.

## Validation

Focused validation:

```bash
python3 -m pytest \
  tests/metadata/file_sources/test_txt_metadata_source.py \
  tests/metadata/file_sources/test_html_metadata_source.py \
  tests/metadata/file_sources/test_pml_metadata_source.py \
  tests/metadata/file_sources/test_malformed_input_fuzzing.py \
  -q
```

Result: `184 passed`.

Broader metadata-reader validation:

```bash
python3 -m pytest \
  tests/metadata/file_sources/test_malformed_input_fuzzing.py \
  tests/metadata/file_sources/test_metadata_reader_registry.py \
  tests/metadata/file_sources/test_dispatcher_modernized.py \
  tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py::test_dispatcher_plugin_adapter_and_failure_edges \
  tests/metadata/file_sources/test_txt_metadata_source.py \
  tests/metadata/file_sources/test_html_metadata_source.py \
  tests/metadata/file_sources/test_pml_metadata_source.py \
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
  tests/metadata/file_sources/test_topaz_metadata_source.py \
  tests/metadata/file_sources/test_legacy_format_adapter_edge_cases.py \
  -q
```

Result: `462 passed`, `2 skipped`, `19 warnings`.

The skips are expected when no `unrar` runtime is available locally and no
optional `.lrx` fixture exists. The warnings are existing date deprecations from
`utils/date.py`.

## Next

This branch is a coherent PR candidate after commit.
