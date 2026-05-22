# File Format Unicode Conversion Hardening

This note records the current direction for the file-format unicode and
malformed-input hardening work. The tests should keep using reusable fixtures
and helpers so the same corpus can later drive conversion-matrix coverage.

Format-specific durable notes now live under `docs/development/file-formats/`.
Use one folder per format once the format has enough behavior, edge cases, or
security policy to need a local contract. ODT, EPUB, DOCX, and HTMLZ now have
dedicated format dossiers. Comic CBZ/CBC now has a dedicated format dossier as
well. FB2 now has a dedicated XML/input-output dossier. LIT now has a dedicated
legacy binary-container/parser dossier.

## Current Test Contract

- Keep shared unicode and conversion helpers under `tests/support/`.
- Exercise real conversion code where possible, not only parser leaf helpers.
- Cover foreign-language text, combining marks, control-like content, invalid
  bytes, malformed markup, and deterministic output bytes.
- Include stress inputs for delimiter-heavy lightweight markup so regex and
  parser behavior stays deterministic around hostile Markdown/Textile/TXT.
- Make known lossy boundaries explicit in tests instead of hiding them behind
  broad "contains text" assertions.
- Prefer small, format-specific tests that reuse common corpora over large
  one-off payloads.

## Pipeline Direction

The current conversion tests mostly exercise the legacy `input -> OEB -> output`
shape. That remains important because OEB is the common normalization layer, but
it should not be the only pipeline model.

Future conversion planning should treat conversion as a graph of capabilities:

- normalized edges that pass through OEB
- direct `A -> B` edges when a tool can preserve more structure or metadata
- external-tool adapters with explicit discovery, version reporting, timeout
  behavior, and deterministic diagnostics
- fallback ordering that records which path was used and what loss was reported

This is especially relevant for lightweight markup formats. Existing tools can
convert between Markdown, Textile, HTML, and other markup dialects directly, and
some of those paths may be better than forcing every conversion through OEB.
Those adapters should be tested as pipeline edges with the same unicode,
malformed-input, and loss-reporting corpus used by the in-tree converters.

## Container Direction

Archive/XML formats need a stricter contract than plain text and lightweight
markup:

- required archive members should be validated before conversion produces
  partial output
- archive preflight should reject bomb-shaped inputs using bounded checks for
  member count, per-member expanded size, total expanded size, and suspicious
  compression ratios
- future override support should raise bounded archive budgets only for
  explicitly trusted input; it must not disable path safety, archive
  readability, or conversion-product invariants
- malformed XML and invalid declared encodings should fail clearly unless a
  format has an intentional recovery path
- embedded assets should be copied without allowing archive member names to
  escape the conversion work directory
- nested and non-ASCII asset paths should be supported when they are valid
- generated `metadata.opf`, XHTML, CSS, and copied assets should be checked as
  a single conversion product

The ODT pass was the first container/XML slice using this contract. It validates
required `META-INF/manifest.xml`, `meta.xml`, and `content.xml` members,
preflights suspicious archive expansion, preserves multilingual metadata/body
text, copies valid `Pictures/...` assets, and rejects hostile picture paths
that would escape the `Pictures` output tree.

The EPUB pass applies the same shape to OCF/OPF containers: reusable fixtures,
valid multilingual conversion/read coverage, `container.xml` and OPF package
discovery, manifest/spine checks, nested non-ASCII assets, hostile archive
paths, and zip-bomb-shaped limits. EPUB preflight failures remain strict before
extraction and are logged with the rejection reason. Any future EPUB salvage
mode should be opt-in and report every relaxed check or dropped resource.

The DOCX pass applies the container contract to OOXML packages: reusable
fixtures, valid multilingual conversion through `Convert` and `DOCXInput`,
content types and package relationship checks, main-document and metadata
malformed cases, nested non-ASCII media extraction, hostile archive paths, and
zip-bomb-shaped limits. DOCX failures now use named `InvalidDOCX` errors for
malformed package parts. Any future DOCX salvage mode should be opt-in and
report relaxed archive checks, selected relationships, skipped media, and
metadata or markup loss.

The HTMLZ pass applies the archive-safety half of the container contract while
preserving HTMLZ's looser structure: a top-level HTML/XHTML member is required,
but top-level OPF metadata and cover references are optional enrichment. HTMLZ
now has reusable multilingual fixtures, valid plugin-path coverage, optional
OPF/cover warning coverage, hostile archive path checks, and zip-bomb-shaped
limits. Optional OPF/cover failures should remain visible through warnings
without aborting an otherwise usable HTML conversion.

The comic pass applies archive-safety to ZIP-backed CBZ/CBC input and CBR/RAR
preflight: reusable CBZ/CBC fixtures, valid multilingual plugin-path coverage,
CBC `comics.txt` decoding and missing-member warning behavior, nested CBZ
preflight, CBR listing preflight, hostile archive path checks, password-entry
rejection, and zip-bomb-shaped limits where the archive backend exposes sizes.
RAR variants that only expose member names still enforce readability, path
safety, member count, and the required comic-page output invariant.

The FB2 pass applies the same fixture-driven hardening shape to both the
XML-backed `.fb2` path and the zipped `.fbz` container path: reusable
multilingual FB2 fixtures, reusable zip fixture helpers, valid plugin-path
coverage for UTF-8 and UTF-16 input, recoverable malformed XML coverage,
unsafe embedded-binary ID sanitization, corrupted base64 warning behavior,
strict single-FB2-member `.fbz` selection, hostile archive path checks,
zip-bomb-shaped `.fbz` limits, metadata reader/writer `.fbz` registration, and
output-side `FB2MLizer`/`FB2Output` unicode serialization coverage. `.fbz`
preflight failures remain strict before conversion output is created; non-FB2
extra zip members are ignored rather than extracted.

The LIT pass applies the same safety direction to a legacy binary container:
small parser-facing fixtures for manifest, namelist, sized UTF-8 strings, and
binary XHTML markup; malformed whole-file/header/manifest/namelist/control
coverage; and parser hardening so truncated or hostile inputs raise `LitError`
instead of raw unpack/index failures or infinite loops. Conversion-facing
unicode coverage now checks optional real LIT input products,
`LITInput.postprocess_book`, output-side `ReBinary` XHTML serialization, and
`LitWriter` manifest serialization. Complete `.lit` archive output remains
blocked in this environment by the unavailable LZX compressor backend, so the
writer coverage intentionally pins the conversion surfaces below compression.

## PML Status

The PML pass currently captures two boundaries:

- `PMLMLizer` and `PMLOutput` can serialize supported characters with PML
  escapes and deterministic `.pmlz` output.
- PML cannot represent the full unicode corpus. Unsupported characters are
  currently replaced with `?`.

The replacement behavior is acceptable as an output fallback, but it should not
be silent. Lossy conversion must become visible and reportable before the
broader conversion work depends on it.

## Loss Reporting Direction

Future conversion reporting should expose:

- format and phase, for example `pml-output` or `pml-input-decode`
- number of replaced or unrepresentable characters
- a small sample of affected code points or fragments
- source location when cheaply available
- log/report integration without aborting otherwise recoverable conversion

Tests for that work should assert both sides of the contract: output remains
recoverable, and the loss is recorded in a user-visible or machine-readable
conversion report.
