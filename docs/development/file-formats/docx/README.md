# DOCX File Format Notes

## Status

DOCX conversion runs through `Convert` in
`src/LiuXin_alpha/file_formats/docx/to_html.py`, with the plugin path entering
through `DOCXInput` in
`src/LiuXin_alpha/file_formats/conversion/plugins/docx_input.py`. The conversion
product is a work directory containing `metadata.opf`, `index.html`, `docx.css`,
and copied `images/...` assets when the source document references embedded
media.

The focused test fixtures live in:

- `tests/support/file_format_docx.py`
- `tests/file_formats/docx/test_docx_container_framework.py`
- `tests/file_formats/docx/test_docx_malformed_hostile.py`

## Container Contract

Default DOCX conversion is strict at the archive boundary. It should reject an
input before partial conversion output is produced when the file is not a
credible DOCX package.

Current preflight requires:

- a readable ZIP archive
- `[Content_Types].xml`
- `_rels/.rels`
- no member name that can escape or confuse the extraction directory
- no more than `4096` archive members
- no member expanding beyond `256 MiB`
- no archive expanding beyond `512 MiB` total
- no member at or above `1 MiB` with a compression ratio above `1000`
- no non-empty member reporting a zero compressed size

Package parsing also raises named `InvalidDOCX` failures for malformed
`[Content_Types].xml`, malformed `_rels/.rels`, missing or malformed main
document XML, malformed document relationships, malformed core/app properties,
and malformed default styles metadata.

Embedded media should accept valid nested and non-ASCII paths, including paths
such as `word/media/深/cover_世界.png`. Hostile package member paths with parent
traversal, absolute-looking paths, or Windows drive-looking prefixes must fail
before extraction.

## Guarded Override Direction

Some real-world DOCX files may exceed conservative archive budgets without
being malicious. Future override support should make those cases possible for
trusted input, but it should not be a blanket archive-safety bypass.

Do not make these checks overridable:

- path traversal or absolute-path extraction attempts
- destination paths outside the conversion work directory
- unreadable ZIP structure
- missing core package members
- malformed package parts that leave the conversion product undefined

These limits may be overridable only through explicit trusted-input profiles:

- archive member count
- maximum expanded size for a single member
- maximum total expanded size
- compression-ratio threshold

Even trusted modes should stay bounded. Error messages should report the
observed value, the active limit, and which profile or setting would be relevant
for a user trying to convert a known-safe file.

## Unicode And Locale Coverage

The current fixtures exercise multilingual title, authors, description,
publisher, subject, keywords, body text, image alt text, and nested image names.
Tests should continue to assert generated HTML and OPF bytes directly enough to
catch replacement characters, dropped combining marks, broken bidirectional
text, and broken non-ASCII asset paths.

## Hostile Corpus

The checked-in hostile corpus currently covers:

- missing `[Content_Types].xml`
- malformed or wrong-root `[Content_Types].xml`
- missing `_rels/.rels`
- malformed or wrong-root `_rels/.rels`
- missing main document
- malformed or wrong-root main document XML
- malformed core properties
- parent traversal, absolute-looking, and Windows drive-looking archive names
- archive member-count budget failures
- oversized expanded members
- excessive total expanded archive size
- suspicious compression-ratio payloads

Future DOCX work should add named regressions here when real fixtures expose
new failure modes, especially around relationships, external links, embedded
fonts, footnotes/endnotes, tables, and unusual image/media encodings.

## Salvage And Reporting Direction

There is currently no DOCX salvage mode. Default conversion should keep failing
fast on invalid package structure rather than silently producing partial output.

If a future recovery mode is added for real-world malformed-but-readable DOCX
files, it should be explicit and reportable:

- recovery must be opt-in or limited to a clearly named trusted-input profile
- path traversal, absolute paths, unreadable ZIP structure, missing core package
  members, and undefined conversion products must remain hard failures
- archive-budget overrides may only raise bounded member-count, expanded-size,
  total-size, or compression-ratio limits
- diagnostics should record which check was relaxed, the observed value, and
  the active limit or profile
- structural recovery should record the package relationships used, main
  document selected, relationships skipped, embedded media dropped, and
  metadata parts ignored
- unicode or markup recovery should record replacement characters, dropped
  fragments, and source locations when cheaply available

Tests for any future salvage path should assert both outcomes: the recovered
conversion product is usable, and the losses or relaxed checks are visible in a
user-facing log or a machine-readable conversion report.
