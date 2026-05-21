# File Format Unicode Conversion Hardening

This note records the current direction for the file-format unicode and
malformed-input hardening work. The tests should keep using reusable fixtures
and helpers so the same corpus can later drive conversion-matrix coverage.

Format-specific durable notes now live under `docs/development/file-formats/`.
Use one folder per format once the format has enough behavior, edge cases, or
security policy to need a local contract. ODT starts that structure in
`docs/development/file-formats/odt/`.

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

The ODT pass is the first container/XML slice using this contract. It validates
required `META-INF/manifest.xml`, `meta.xml`, and `content.xml` members,
preflights suspicious archive expansion, preserves multilingual metadata/body
text, copies valid `Pictures/...` assets, and rejects hostile picture paths
that would escape the `Pictures` output tree.

EPUB is the next container target. Treat OPF as part of that EPUB slice first:
build the reusable EPUB fixture framework, then cover OCF `container.xml`, OPF
package discovery, manifest/spine correctness, XHTML/XML content, nested assets,
unicode paths, and hostile archive behavior.

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
