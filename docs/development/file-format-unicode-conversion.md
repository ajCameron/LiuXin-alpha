# File Format Unicode Conversion Hardening

This note records the current direction for the file-format unicode and
malformed-input hardening work. The tests should keep using reusable fixtures
and helpers so the same corpus can later drive conversion-matrix coverage.

Format-specific durable notes now live under `docs/development/file-formats/`.
Use one folder per format once the format has enough behavior, edge cases, or
security policy to need a local contract. ODT, EPUB, DOCX, and HTMLZ now have
dedicated format dossiers. Comic CBZ/CBC now has a dedicated format dossier as
well. FB2 now has a dedicated XML/input-output dossier. LIT now has a dedicated
legacy binary-container/parser dossier. MOBI and PDB now have dedicated
PalmDB-backed binary-container dossiers.

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

The durable project TODO and sign-off checklist for this work lives in
`dev-docs/conversion_pipeline_todo.md`, with the current status matrix in
`dev-docs/conversion_pipeline_signoff.md`. Keep this development note focused
on file-format hardening policy, and use the `dev-docs` checklist and matrix to
decide when a format or pipeline edge is done enough to sign off.

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

Shared ZIP archive member checks now live in
`LiuXin_alpha.file_formats.archive_preflight`. The helper owns member-count,
per-member expansion, total expansion, invalid compressed-size,
compression-ratio, and default unsafe-path policy for FBZ, HTMLZ, EPUB, DOCX,
ODT, and comic ZIP preflight. Format modules still own their structural checks
and recovery decisions; ODT keeps its existing skip-unsafe-picture-entry
extraction behavior while sharing the same archive budget checks.

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
`LITInput.postprocess_book`, output-side `ReBinary` XHTML serialization,
`LitWriter` manifest serialization, and the explicit unavailable-LZX writer
boundary. Complete successful `.lit` archive output remains blocked in this
environment by the unavailable LZX compressor backend, but missing compression
now fails with a named writer error before a filesystem output path is opened.

The MOBI pass established the current PalmDB-backed hardening pattern. MOBI is
PalmDB-backed rather than ZIP-backed, but it has the same need for explicit
structure checks before conversion. Current coverage has reusable
PalmDB/MOBI/EXTH fixture builders, unicode EXTH metadata, optional real-corpus
input, old-MOBI output round-trips, metadata writer sanitization, and hostile
parser tests for truncated PalmDB headers, short record tables, invalid record
offsets, short record 0 payloads, impossible MOBI header lengths, out-of-range
title offsets, malformed EXTH blocks, invalid section access, malformed
HUFF/CDIC tables, DH HUFF/CDIC range checks, malformed INDX/TAGX records,
invalid KF8 FDST/SKEL/DIV/OTH/NCX references, resource-range/CRES failures,
bounded PalmDOC/HUFF decompression expansion, and concrete direct/CRES KF8 image
resource extraction products. Remaining hardening should move next into richer
KF8 skeleton/div/NCX conversion products, non-image resource fixtures, and
trusted-input budget policy.

The PDB pass is the next PalmDB-backed target. PDB reuses the wrapper shape
that MOBI just hardened, but dispatches into PalmDOC, zTXT, eReader, Plucker,
Haodoo, and embedded-PDF style subreaders. Current coverage has reusable
PDB/PalmDB fixtures, header builder/reader round-trips, strict wrapper
record-table validation, legacy metadata fixtures, eReader metadata writes,
Plucker and Haodoo metadata edges, PalmDOC/zTXT short-record and decompression
failures, eReader range/decompression/image-name failures, and strict
corrupt-wrapper behavior for metadata paths. Plucker now also has conversion
reader hardening for short headers, record-local length overruns, malformed
metadata records, truncated PHTML operands, missing image references, and PHTML
decompression failures. Haodoo now has conversion-reader coverage for
CP950/UTF-16LE chapter output, malformed header fields, record count parsing,
chapter-title mismatches, declared chapter ranges, and direct section bounds.
Remaining PDB hardening should now be driven by real corpus defects or
conversion-product sign-off, not known subreader-hostile gaps.

## PML Status

The PML output lossy-boundary row was signed off on 2026-06-03 for the current
row scope. The signed-off scope includes:

- `PMLMLizer` and `PMLOutput` serialization of supported characters with PML
  escapes and deterministic `.pmlz` output.
- Recoverable replacement of unsupported characters with `?`.
- An aggregate `unsupported-character-replacement` `ConversionReport` loss event
  with count, samples, replacement details, recoverability, and edge context.
- The current legacy OEB-backed path exposed as `ConversionEdge` for report
  naming, without changing execution behavior.

Focused validation for sign-off passed:

```text
python3 -m pytest tests/file_formats/pml tests/file_formats/conversion/test_conversion_report.py tests/file_formats/conversion/test_conversion_edges.py tests/file_formats/conversion/test_conversion_top_level_smoke.py -q
57 passed in 8.97s

python3 -m pytest tests/metadata/file_sources/test_pml_metadata_source.py -q
12 passed in 9.69s

python3 -m pytest tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py -q
4 passed in 7.37s
```

This signs off the PML output boundary only. Broader loss-report plumbing across
other lossy formats, fallback execution, and pipeline-wide planner semantics
remain provisional conversion-pipeline work.

## TXT Loss-Report Status

The TXT input/output encoding-loss report row was signed off on 2026-06-04 for
the current row scope. The signed-off scope keeps existing recoverable behavior
but reports deterministic encoding loss:

- malformed input bytes decoded with replacement emit an
  `input-decoding-byte-replacement` event in the `txt-input` phase.
- final TXT output characters that cannot be represented by the selected output
  encoding emit an `output-encoding-character-replacement` event in the
  `txt-output` phase.
- output reports use the current conversion edge context when available,
  matching the PML report pattern.
- UTF-8 TXT output that preserves the shared corpus still attaches a report with
  no loss events.

Focused sign-off validation passed:

```text
python3 -m pytest tests/file_formats/txt/test_txt_unicode_torture.py tests/file_formats/txt/test_txt_output_serializers_unicode_framework.py -q
13 passed in 7.88s

python3 -m pytest tests/file_formats/txt -q
39 passed, 1 warning in 8.54s

python3 -m pytest tests/file_formats/conversion/test_conversion_report.py tests/file_formats/conversion/test_conversion_edges.py tests/file_formats/conversion/test_conversion_top_level_smoke.py tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py -q
13 passed in 11.08s

python3 -m pytest tests/metadata/file_sources/test_txt_metadata_source.py tests/metadata/file_sources/test_txtz_metadata_source.py -q
22 passed in 15.43s

python3 -m pytest tests/file_formats/txt tests/file_formats/markdown tests/file_formats/textile -q
90 passed, 3 warnings in 10.01s
```

Malformed Markdown/Textile parser failures remain hard failures rather than
recoverable loss events in the current TXT row. Direct/external markup edge
selection and broader markup loss diagnostics remain separate pipeline work.

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
