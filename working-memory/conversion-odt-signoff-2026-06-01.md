# Conversion ODT Sign-Off - 2026-06-01

## Decision

ODT is signed off for the current input/container conversion scope in
`dev-docs/conversion_pipeline_signoff.md`.

The signed-off scope includes:

- required archive-member validation for `META-INF/manifest.xml`, `meta.xml`,
  and `content.xml`
- shared archive preflight budgets for member count, member expansion, total
  expansion, invalid compressed sizes, and suspicious compression ratios
- multilingual metadata and body extraction
- generated `metadata.opf`, `index.xhtml`, `odfpy.css`, and copied
  `Pictures/...` assets
- valid nested and non-ASCII picture paths
- unsafe picture path rejection without extraction outside the intended tree
- malformed XML and non-ZIP rejection before partial conversion output
- ODT/ODF compatibility and ODT metadata file-source checks

## Evidence

Durable docs reviewed:

- `docs/development/file-formats/odt/README.md`
- `working-memory/file-formats-odt-container-2026-05-21.md`

Focused validation:

```text
python3 -m pytest tests/file_formats/odt tests/file_formats/odf tests/metadata/file_sources/test_odt_metadata_source.py tests/metadata/file_sources/test_odt_beta_metadata_source.py tests/metadata/file_sources/test_text_odt_edge_cases.py -q
48 passed, 12 warnings in 13.79s

python3 -m pytest tests/file_formats/test_archive_preflight.py tests/file_formats/odt/test_odt_container_framework.py tests/file_formats/odt/test_odt_malformed_hostile.py -q
28 passed in 8.79s
```

## Boundary

This is a format-scope sign-off, not a claim that trusted-input archive-budget
overrides exist. That remains a future pipeline/container policy feature.

Future real-corpus ODT defects should be added as regressions or new follow-up
rows rather than reopening the already-covered baseline.
