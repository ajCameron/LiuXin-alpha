# ODT File Format Notes

## Status

ODT conversion currently runs through `Extract` in
`src/LiuXin_alpha/file_formats/odt/input.py`, with the plugin path entering
through `ODTInput`. The conversion product is a work directory containing
`metadata.opf`, `index.xhtml`, `odfpy.css`, and copied `Pictures/...` assets.

The focused test fixtures live in:

- `tests/support/file_format_odt.py`
- `tests/file_formats/odt/test_odt_container_framework.py`
- `tests/file_formats/odt/test_odt_malformed_hostile.py`

## Container Contract

Default ODT conversion is strict at the archive boundary. It should reject an
input before partial conversion output is produced when the file is not a
credible ODT container.

Current preflight requires:

- a readable ZIP archive
- `META-INF/manifest.xml`
- `meta.xml`
- `content.xml`
- no more than `4096` archive members
- no member expanding beyond `256 MiB`
- no archive expanding beyond `512 MiB` total
- no member at or above `1 MiB` with a compression ratio above `1000`
- no non-empty member reporting a zero compressed size

Embedded picture extraction should accept valid nested and non-ASCII
`Pictures/...` paths, create needed subdirectories, and reject archive member
names that would escape the output `Pictures` directory.

## Guarded Override Direction

Some real-world ODT files may exceed conservative archive budgets without being
malicious. Future override support should make those cases possible for trusted
input, but it should not be a blanket archive-safety bypass.

Do not make these checks overridable:

- path traversal or absolute-path extraction attempts
- destination paths outside the conversion work directory
- unreadable ZIP structure
- structural failures that make the conversion product undefined

These limits may be overridable only through explicit trusted-input profiles:

- archive member count
- maximum expanded size for a single member
- maximum total expanded size
- compression-ratio threshold

The intended shape is:

- `default`: conservative limits for normal ingestion
- `relaxed`: larger bounded limits for unusual but trusted books
- `trusted-custom`: caller-provided bounded limits, recorded in diagnostics

Even trusted modes should stay bounded. Error messages should report the
observed value, the active limit, and which profile or setting would be relevant
for a user trying to convert a known-safe file.

## Unicode And Locale Coverage

The current fixtures exercise multilingual title, author, body text, comments,
and nested picture names. Tests should continue to assert generated XHTML and
OPF bytes directly enough to catch replacement characters, dropped combining
marks, and broken non-ASCII asset paths.

## Hostile Corpus

The checked-in hostile corpus currently covers:

- missing required archive members
- malformed `content.xml`
- invalid UTF-8 in declared UTF-8 XML
- non-ZIP payloads sent through `ODTInput`
- nested valid picture paths
- picture path traversal and absolute-looking archive names
- archive member-count budget failures
- oversized expanded members
- excessive total expanded archive size
- suspicious compression-ratio payloads

Future ODT work should add named regressions here when a real fixture exposes a
new failure mode.

## Sign-Off Status

ODT is signed off for the current input/container conversion scope as of
2026-06-01.

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

Focused sign-off validation:

```text
python3 -m pytest tests/file_formats/odt tests/file_formats/odf tests/metadata/file_sources/test_odt_metadata_source.py tests/metadata/file_sources/test_odt_beta_metadata_source.py tests/metadata/file_sources/test_text_odt_edge_cases.py -q
48 passed, 12 warnings in 13.79s

python3 -m pytest tests/file_formats/test_archive_preflight.py tests/file_formats/odt/test_odt_container_framework.py tests/file_formats/odt/test_odt_malformed_hostile.py -q
28 passed in 8.79s
```

The broader trusted-input archive-budget override direction remains outside
this format sign-off row. Future real-corpus ODT defects should be added as
regressions against this contract.
