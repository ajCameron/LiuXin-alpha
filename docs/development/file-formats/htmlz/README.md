# HTMLZ File Format Notes

## Status

HTMLZ conversion enters through `HTMLZInput` in
`src/LiuXin_alpha/file_formats/conversion/plugins/htmlz_input.py`; output uses
`HTMLZOutput` in the sibling plugin module. HTMLZ is a ZIP-backed container, but
the input contract is intentionally smaller than EPUB: a usable top-level HTML
or XHTML file is the required conversion product, while top-level OPF metadata
and cover references are optional enrichments.

The focused test fixtures live in:

- `tests/support/file_format_htmlz.py`
- `tests/file_formats/htmlz/test_htmlz_container_framework.py`
- `tests/file_formats/htmlz/test_htmlz_malformed_hostile.py`

The reusable fixture builds multilingual `.htmlz` archives with top-level
`index.html`, optional `metadata.opf`, CSS, cover image assets, non-ASCII nested
asset paths, and rewrite helpers for malformed archive cases.

## Container Contract

Default HTMLZ conversion is strict before extraction for archive safety and the
required top-level HTML member:

- require a readable ZIP archive
- require at least one top-level `.html`, `.xhtml`, or `.htm` member
- prefer `index.html`, `index.xhtml`, or `index.htm` when present
- warn when multiple top-level HTML files exist and only one will be used
- reject archive member names that can escape or confuse the conversion work
  directory
- no more than `4096` archive members
- no member expanding beyond `256 MiB`
- no archive expanding beyond `512 MiB` total
- no member at or above `1 MiB` with a compression ratio above `1000`
- no non-empty member reporting a zero compressed size

Required HTML failures abort before the downstream HTML plugin is called. The
current required failures include no top-level HTML and an empty selected
top-level HTML file.

## Optional OPF And Cover Policy

HTMLZ accepts optional top-level OPF metadata, but malformed or missing optional
metadata should not prevent conversion of an otherwise usable HTML payload.

Current optional metadata behavior:

- unreadable or malformed top-level OPF logs a warning, emits
  `optional-opf-enrichment-failed`, and conversion continues
- missing cover files referenced by OPF log a warning, emit
  `optional-cover-missing`, and conversion continues
- unsafe OPF cover paths such as parent traversal, absolute paths, or drive-like
  paths are ignored with a warning and `optional-cover-unsafe-path`
- valid nested non-ASCII cover paths are preserved and attached to the OEB
  manifest/guide without loss events

This is deliberately different from EPUB and DOCX, where the package metadata
is part of the core container contract. For HTMLZ, the top-level HTML file is
the conversion product; OPF/cover data is enrichment.

## Unicode And Locale Coverage

The current fixtures exercise multilingual title, authors, description,
publisher, subject, body text, image alt text, CSS paths, cover paths, and
extra asset names. Tests assert the bytes handed from `HTMLZInput` to the
downstream HTML plugin so replacement characters, dropped combining marks, and
broken non-ASCII paths are visible.

## Hostile Corpus

The checked-in hostile corpus currently covers:

- non-ZIP `.htmlz` payloads
- missing top-level HTML
- empty top-level HTML
- malformed optional OPF metadata
- missing optional cover files
- unsafe OPF cover references
- parent traversal, absolute-looking, and Windows drive-looking archive names
- archive member-count budget failures
- oversized expanded members
- excessive total expanded archive size
- suspicious compression-ratio payloads

Future regressions should be added here when real-world HTMLZ files expose new
edge cases, especially around declared encodings, linked resources, nested
assets, and OPF recovery behavior.

## Salvage And Reporting Direction

There is no separate HTMLZ salvage mode today. The default policy already
salvages optional OPF and cover failures by warning and continuing, while still
failing hard for unsafe archive structure and missing or empty top-level HTML.

If future trusted-input overrides are added, they should only raise bounded
archive budgets. They must not bypass path safety, unreadable ZIP structure, or
the required top-level HTML invariant.

Diagnostics for future recovery work should record:

- selected top-level HTML member
- ignored OPF member and parse error, when applicable
- ignored or missing cover path, when applicable
- relaxed archive limit, observed value, and active trusted-input profile
- dropped linked resources or broken non-ASCII paths, when cheaply available

## Diagnostics Status

The HTMLZ optional-enrichment diagnostics slice promoted HTMLZ to candidate on
2026-06-04. The current scope preserves existing warning-and-continue behavior
for optional OPF/cover enrichment failures while adding structured recoverable
loss events:

- `optional-opf-enrichment-failed` for unreadable or malformed optional OPF
- `optional-cover-unsafe-path` for unsafe OPF cover references
- `optional-cover-missing` for missing cover files referenced by OPF

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

Required top-level HTML failures and hostile archive preflight remain strict
conversion failures rather than recoverable loss events. Broader HTMLZ salvage
or trusted-input behavior remains separate pipeline/container policy work.
