# Conversion EPUB Sign-Off - 2026-06-02

## Decision

EPUB is signed off for the current input/container conversion scope in
`dev-docs/conversion_pipeline_signoff.md`.

The signed-off scope includes:

- readable ZIP, `mimetype`, and OCF `META-INF/container.xml` validation
- OPF package discovery through `container.xml`
- OPF package, manifest, and spine validation before extraction
- shared archive preflight budgets for member count, member expansion, total
  expansion, invalid compressed sizes, and suspicious compression ratios
- hostile archive member path rejection before extraction
- valid nested and non-ASCII OPF rootfile, XHTML, CSS, image, and extra asset
  paths
- multilingual EPUB conversion products and normalized `content.opf`
- EPUB/OPF metadata file-source checks
- visible `EPUB preflight rejected ...` diagnostics for strict failures

## Evidence

Durable docs reviewed:

- `docs/development/file-formats/epub/README.md`
- `working-memory/file-formats-epub-container-2026-05-21.md`

Focused validation:

```text
python3 -m pytest tests/file_formats/epub tests/metadata/file_sources/test_epub_metadata_source.py tests/metadata/file_sources/test_epub_edge_cases.py tests/metadata/file_sources/test_opf_metadata_source.py tests/metadata/file_sources/test_opf_edge_cases.py -q
79 passed, 7 warnings in 29.75s

python3 -m pytest tests/file_formats/test_archive_preflight.py tests/file_formats/epub/test_epub_container_framework.py tests/file_formats/epub/test_epub_malformed_hostile.py -q
42 passed in 15.61s
```

## Boundary

This is a format-scope sign-off, not a claim that broader OPF metadata field
parity is complete or that an EPUB salvage/reporting mode exists. Those remain
separate metadata and pipeline/container policy workstreams.

Future real-corpus EPUB defects should be added as regressions or new follow-up
rows rather than reopening the already-covered baseline.
