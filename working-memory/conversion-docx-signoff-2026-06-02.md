# Conversion DOCX Sign-Off - 2026-06-02

## Decision

DOCX is signed off for the current input/container conversion scope in
`dev-docs/conversion_pipeline/conversion_pipeline_signoff.md`.

The signed-off scope includes:

- readable ZIP, `[Content_Types].xml`, and `_rels/.rels` validation
- main document discovery and package relationship checks
- malformed content-types, relationships, main document, properties, and styles
  failures as named `InvalidDOCX` errors
- shared archive preflight budgets for member count, member expansion, total
  expansion, invalid compressed sizes, and suspicious compression ratios
- hostile archive member path rejection before extraction
- valid nested and non-ASCII media paths
- multilingual DOCX conversion products: `metadata.opf`, `index.html`,
  `docx.css`, and copied `images/...` assets
- core/app metadata and DOCX metadata file-source checks

## Evidence

Durable docs reviewed:

- `dev-docs/file-formats/docx/README.md`
- `working-memory/file-formats-docx-container-2026-05-21.md`

Focused validation:

```text
python3 -m pytest tests/file_formats/docx tests/metadata/file_sources/test_docx_metadata_source.py -q
39 passed in 25.49s

python3 -m pytest tests/file_formats/test_archive_preflight.py tests/file_formats/docx/test_docx_container_framework.py tests/file_formats/docx/test_docx_malformed_hostile.py -q
39 passed in 16.98s
```

## Boundary

This is a format-scope sign-off, not a claim that DOCX salvage/reporting
behavior or trusted archive-budget overrides exist. Those remain separate
pipeline/container policy workstreams.

Future real-corpus DOCX defects should be added as regressions or new follow-up
rows rather than reopening the already-covered baseline.
