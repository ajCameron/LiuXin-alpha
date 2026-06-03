# Conversion Pipeline Sign-Off Matrix

Date: 2026-05-28

## Purpose

This matrix is the durable Stage 6 sign-off table for conversion work. It turns
format hardening notes into a reviewable status view: what is ready to sign off,
what is provisional, and what still has a named blocker.

A row should move to `Signed off` only when the remaining blocker is empty or
explicitly accepted as outside the row scope. External dependency blockers can
sign off a narrower sub-scope, such as LIT input/parser behavior, but not full
format output.

## Status Legend

- `Signed off`: the row has been explicitly reviewed, has no blocker inside
  its stated scope, and future real-corpus defects should be tracked as
  regressions or new follow-up rows.
- `Candidate`: the current format-specific contract is strong enough for a
  sign-off review. Future real-corpus defects may still add regressions.
- `Provisional`: core behavior is covered, but a known fixture, product, writer,
  metadata, or diagnostics gap remains.
- `Blocked`: a required dependency or capability is unavailable.
- `Open`: implementation or design work is still needed before sign-off review.
- `N/A`: the column does not apply to the row scope.

## Current Baseline

Latest fast lane recorded during Stage 5 completion:

```text
python3 scripts/run_file_formats_lane.py --lane fast
792 passed, 1 skipped, 15 warnings in 66.75s
```

The heavy lane is not yet recorded in this sign-off matrix. A row can be treated
as a focused sign-off candidate from the fast lane plus its format-specific
validation, but release-level sign-off should either run the heavy lane or
record why the heavy lane is intentionally skipped.

## Container And Archive Formats

| Area | Reader/Input | Writer/Output | Metadata | Hostile Boundary | Product Assertions | Loss/Diagnostics | Remaining Blocker | State |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Shared ZIP preflight | N/A | N/A | N/A | Done: member count, per-member size, total size, ratio, compressed-size, and path checks | N/A | Provisional: callers wrap reasons in format errors or warnings | Decide whether metadata readers share the helper and whether trusted budgets are exposed | Provisional |
| ODT | Done: required members, manifest/meta/content, archive budgets, safe picture extraction | N/A in current scope | Done: multilingual metadata/body extraction and ODT metadata file-source checks | Done: required-member, malformed XML, hostile archive, picture path, bomb-shaped inputs | Done: OPF, XHTML, CSS, and copied assets | Signed off for row scope: strict failures and warnings cover required-member, malformed XML, unsafe pictures, and archive preflight behavior | None for current ODT input/container scope; future real-corpus defects become regressions | Signed off |
| EPUB | Done: OCF container, OPF package discovery, manifest/spine, nested and non-ASCII assets | N/A in current scope | Done through EPUB/OPF metadata file-source checks | Done: malformed container/package and hostile archive boundaries | Done: multilingual read/conversion product and assets | Signed off for row scope: strict failures and visible preflight logging cover missing structure, unsafe paths, archive budgets, manifest/spine failures, and extraction boundaries | None for current EPUB input/container scope; broader OPF field parity and future salvage/reporting remain separate rows | Signed off |
| DOCX | Done: OOXML package, content types, relationships, main document, styles, and nested media | N/A in current scope | Done: core/app properties and conversion metadata | Done: malformed package parts, hostile archive, nested media paths, and package XML failures | Done: multilingual document conversion, metadata OPF, HTML/CSS, and extracted media | Signed off for row scope: named `InvalidDOCX` failures and strict preflight cover missing structure, unsafe paths, archive budgets, malformed XML, and extraction boundaries | None for current DOCX input/container scope; future salvage/reporting and trusted budget overrides remain separate rows | Signed off |
| HTMLZ | Done: top-level HTML/XHTML requirement, optional OPF/cover enrichment | N/A in current scope | Provisional: optional OPF/cover warnings covered | Done: missing HTML, hostile archive, bomb-shaped inputs | Done: multilingual plugin-path HTML product | Done through warnings for optional OPF/cover problems | Decide if optional enrichment loss should emit `ConversionReport` events | Provisional |
| Comic CBZ/CBC/CBR | Done: CBZ/CBC ZIP and CBR/RAR listing/extraction boundaries | N/A in current scope | N/A | Done: path safety, password entries, member budgets where backend exposes sizes | Done: multilingual CBC and comic-page output invariant | Done through warnings/strict failures; RAR name-only backends have limited size data | Real RAR backend variance and optional structured diagnostics | Provisional |
| FB2/FBZ | Done: FB2 XML and single-FB2-member FBZ selection | Done: `FB2MLizer` and `FB2Output` unicode serialization | Done: FBZ metadata registration and reader/writer paths | Done: malformed XML, hostile archive, embedded-binary ID safety, corrupt base64 warnings | Done: UTF-8/UTF-16 input and zipped/unzipped products | Signed off for row scope: warnings and strict failures cover skipped binaries, unsafe IDs, parser recovery, metadata archive rejection, and archive preflight | None for current FB2/FBZ format scope; future real-corpus defects become regressions | Signed off |

## Legacy Binary And PalmDB Formats

| Area | Reader/Input | Writer/Output | Metadata | Hostile Boundary | Product Assertions | Loss/Diagnostics | Remaining Blocker | State |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| LIT | Done: parser fixtures, manifests, namelists, `UnBinary`, DRM boundary | Blocked for successful full archive output; unavailable LZX fails with `LitWriterError` before output open | Limited to OPF/manifest surfaces in current scope | Done: malformed headers, secondary blocks, manifest/namelist, binary markup controls | Partial: input products, `postprocess_book`, `ReBinary`, writer manifest, unavailable-LZX boundary | Done for named parser/writer failures; no structured report yet | Testable LZX compressor backend for successful `.lit` output | Blocked for full output; input/parser is candidate |
| MOBI/AZW/KF8 | Done: PalmDB/MOBI/EXTH, HUFF/CDIC, INDX/TAGX, KF8 FDST/resources | Partial: old MOBI output round-trip and writer behavior covered; richer KF8 output remains | Done: EXTH read/write, Topaz dispatch, fallback metadata policy | Done: record table, headers, EXTH, decompression budgets, KF8 indices/resources | Partial: old-MOBI output, direct/CRES image resources; skeleton/div/NCX products still open | Provisional: recovery paths visible but not structured across all paths | Realistic KF8 skeleton/div/NCX products, non-image resources, trusted budget policy | Provisional |
| PDB family | Done: PalmDB wrapper plus PalmDOC, zTXT, eReader, Plucker, Haodoo subreaders | Limited in current scope; PalmDOC/zTXT/eReader output writers exist and metadata writes are covered where supported | Done: legacy metadata fixtures, eReader writes, fallback behavior, wrapper-title updates | Done: wrapper validation, subreader range/decompression/image/name/header failures, Haodoo CP950/UTF-16LE bounds | Done: plugin-path products for supported subreaders, including Haodoo CP950/UTF-16LE; metadata fallback/product boundaries covered | Signed off for row scope: named parser errors and strict/fallback metadata behavior cover wrapper and subreader boundaries | None for current PDB input/metadata hardening scope; broader output-product expansion and real-corpus defects remain separate rows | Signed off |

## Text, Markup, And Lossy Output Formats

| Area | Reader/Input | Writer/Output | Metadata | Hostile Boundary | Product Assertions | Loss/Diagnostics | Remaining Blocker | State |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| TXT | Done: encoded payloads, newline/output serializer matrix, shared unicode corpus | Done: output serializers and encoding/newline behavior | Limited/N/A in current scope | Done: markup-extension hostile inputs and malformed text-like payloads | Done: multilingual TXT input/output products | Provisional: encoding and markup losses are not yet consistently structured | Decide which TXT recovery/loss cases should emit `ConversionReport` events | Provisional |
| Markdown | Done: malformed delimiter/reference/footnote stress through current input path | N/A in current scope | N/A | Done: hostile markup preserves multilingual text | Done: current OEB-backed conversion products | Open: direct/external edge diagnostics not modeled beyond generic edge support | Decide direct/external Markdown edge candidates and loss reporting | Open |
| Textile | Done: malformed delimiter stress through current input path | N/A in current scope | N/A | Done: hostile markup preserves multilingual text | Done: current OEB-backed conversion products | Open: direct/external edge diagnostics not modeled beyond generic edge support | Decide direct/external Textile edge candidates and loss reporting | Open |
| PML | Reader/input not the current focus | Done: `PMLMLizer` and `PMLOutput` deterministic output | N/A in current scope | Done: unsupported-character boundary pinned | Done: recoverable `.pmlz` bytes and supported unicode escapes | Done: aggregate `unsupported-character-replacement` `ConversionReport` event | Extend report plumbing to more lossy formats before pipeline-wide sign-off | Candidate for output boundary; pipeline integration provisional |

## Pipeline Infrastructure

| Area | Reader/Input | Writer/Output | Metadata | Hostile Boundary | Product Assertions | Loss/Diagnostics | Remaining Blocker | State |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Conversion report | N/A | N/A | N/A | N/A | Done for PML output loss tests | Done: source, target, edge, warnings, loss events, samples, recoverability | Attach report consistently across more plugins and recovery paths | Provisional |
| Conversion edge model | N/A | N/A | N/A | N/A | Done for deterministic edge registry and legacy OEB-backed edge tests | Provisional: edge name reaches PML report | Add planner/fallback execution semantics and external-tool discovery | Provisional |
| Legacy OEB-backed path | Done as current default path | Done as current default path | Format-dependent | Format-dependent | Done where each format row has product assertions | Provisional: fallback and loss reporting are not globally consistent | Promote from implicit default to inspectable planner behavior | Provisional |

## Signed-Off Reviews

### PDB Family - 2026-06-03

Decision: signed off for the current PDB input/metadata hardening scope. The
signed-off scope includes the PalmDB wrapper, PalmDOC, zTXT, eReader, Plucker,
and Haodoo subreader boundaries, metadata reader fallback behavior, wrapper-title
updates, supported eReader body metadata writes, generated plugin-path products,
Haodoo CP950 and UTF-16LE fixtures, and named parser failures before partial or
unsafe output.

Validation:

```text
python3 -m pytest tests/file_formats/pdb tests/metadata/file_sources/test_pdb_metadata_source.py tests/metadata/file_sources/test_pdb_metadata_fixtures.py tests/metadata/file_sources/test_pdb_subreader_edge_cases.py -q
100 passed in 24.10s

python3 -m pytest tests/metadata/file_sources/test_malformed_input_fuzzing.py -q
133 passed in 26.09s

python3 -m pytest tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py tests/file_formats/conversion/test_conversion_top_level_smoke.py -q
6 passed in 17.27s
```

Broader PDB output-product expansion, structured conversion reports, and future
real-corpus defects remain separate follow-up rows, not blockers for this
input/metadata hardening row.

### DOCX - 2026-06-02

Decision: signed off for the current DOCX input/container conversion scope. The
signed-off scope includes OOXML package validation, content-types and
relationship checks, main document discovery, core/app properties, default
styles, nested and non-ASCII media extraction, multilingual conversion
products, DOCX metadata file-source checks, hostile archive member rejection,
shared archive preflight budgets, malformed package XML failures, and named
`InvalidDOCX` errors before partial output.

Validation:

```text
python3 -m pytest tests/file_formats/docx tests/metadata/file_sources/test_docx_metadata_source.py -q
39 passed in 25.49s

python3 -m pytest tests/file_formats/test_archive_preflight.py tests/file_formats/docx/test_docx_container_framework.py tests/file_formats/docx/test_docx_malformed_hostile.py -q
39 passed in 16.98s
```

Future DOCX salvage/reporting behavior and trusted archive-budget overrides
remain separate pipeline/container policy work, not blockers for this
input/container row. Future real-corpus DOCX defects should be added as
regressions or new follow-up rows.

### EPUB - 2026-06-02

Decision: signed off for the current EPUB input/container conversion scope. The
signed-off scope includes OCF container validation, OPF package discovery, OPF
manifest/spine checks, nested and non-ASCII resource extraction, multilingual
EPUB conversion products, EPUB/OPF metadata file-source checks, hostile archive
member rejection, shared archive preflight budgets, and visible preflight
rejection diagnostics before extraction.

Validation:

```text
python3 -m pytest tests/file_formats/epub tests/metadata/file_sources/test_epub_metadata_source.py tests/metadata/file_sources/test_epub_edge_cases.py tests/metadata/file_sources/test_opf_metadata_source.py tests/metadata/file_sources/test_opf_edge_cases.py -q
79 passed, 7 warnings in 29.75s

python3 -m pytest tests/file_formats/test_archive_preflight.py tests/file_formats/epub/test_epub_container_framework.py tests/file_formats/epub/test_epub_malformed_hostile.py -q
42 passed in 15.61s
```

Broader OPF metadata field parity and any future EPUB salvage/reporting mode
remain separate follow-up work, not blockers for this input/container row.
Future real-corpus EPUB defects should be added as regressions or new follow-up
rows.

### ODT - 2026-06-01

Decision: signed off for the current ODT input/container conversion scope. The
signed-off scope includes required archive members, shared archive preflight
budgets, valid nested and non-ASCII `Pictures/...` extraction, unsafe picture
path rejection, multilingual metadata/body extraction, generated OPF/XHTML/CSS
products, copied picture assets, ODT/ODF compatibility smoke coverage, and ODT
metadata file-source checks.

Validation:

```text
python3 -m pytest tests/file_formats/odt tests/file_formats/odf tests/metadata/file_sources/test_odt_metadata_source.py tests/metadata/file_sources/test_odt_beta_metadata_source.py tests/metadata/file_sources/test_text_odt_edge_cases.py -q
48 passed, 12 warnings in 13.79s

python3 -m pytest tests/file_formats/test_archive_preflight.py tests/file_formats/odt/test_odt_container_framework.py tests/file_formats/odt/test_odt_malformed_hostile.py -q
28 passed in 8.79s
```

The broader trusted-input archive-budget override idea remains a future
pipeline/container policy feature, not an ODT sign-off blocker. Future
real-corpus ODT defects should be added as regressions or new follow-up rows.

### FB2/FBZ - 2026-05-31

Decision: signed off for the current FB2/FBZ format scope. The signed-off scope
includes XML-backed FB2 input, strict single-member FBZ archive input,
metadata read/write for plain and zipped FB2, `FB2MLizer`/`FB2Output` unicode
serialization, hostile XML/archive/binary-ID boundaries, and generated
conversion-product assertions for UTF-8, UTF-16, zipped, and unzipped products.

Validation:

```text
python3 -m pytest tests/file_formats/fb2 tests/metadata/file_sources/test_fb2_metadata_source.py tests/metadata/file_sources/test_fb2_edge_cases.py -q
76 passed in 14.17s

python3 -m pytest tests/file_formats/test_archive_preflight.py tests/file_formats/fb2/test_fb2_malformed_hostile.py tests/file_formats/fb2/test_fb2_zip_framework.py -q
48 passed in 12.08s
```

The broader goal of structured `ConversionReport` events for every recoverable
warning remains a pipeline-wide reporting workstream, not an FB2/FBZ blocker.
Future real-corpus defects should be added as regressions or new follow-up rows.

## First Review Queue

The next row worth reviewing for actual sign-off is the remaining candidate
row with no known product blocker beyond broader release validation:

- PML output lossy-boundary behavior

Rows that should not be promoted yet:

- LIT full output, blocked by unavailable LZX compression
- MOBI/KF8 full product behavior, still missing skeleton/div/NCX and non-image
  resource fixtures
- Markdown/Textile direct or external edges, still design-open
- Pipeline-wide report/fallback semantics, still provisional

## Update Rules

When a row changes status, update this file with:

- the focused validation command and result
- the exact product or diagnostic assertion that changed
- any remaining blocker that was removed or accepted
- a link to the format dossier or working-memory note that explains the detail

Do not mark a row `Signed off` only because the fast lane is green. The fast
lane is necessary evidence, not sufficient evidence.
