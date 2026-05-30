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
| ODT | Done: required members, manifest/meta/content, archive budgets, safe picture extraction | N/A in current scope | Done: multilingual metadata/body extraction | Done: required-member, malformed XML, hostile archive, picture path, bomb-shaped inputs | Done: OPF, XHTML, CSS, and copied assets | Done for current warnings/strict failures; no structured report yet | Heavy-lane/release validation before final promotion | Candidate |
| EPUB | Done: OCF container, OPF package discovery, manifest/spine, nested assets | N/A in current scope | Done through OPF package coverage | Done: malformed container/package and hostile archive boundaries | Done: multilingual read/conversion product and assets | Done for strict rejection logging; no structured report yet | Heavy-lane/release validation before final promotion | Candidate |
| DOCX | Done: OOXML package, content types, relationships, main document | N/A in current scope | Done: core properties and conversion metadata | Done: malformed package parts, hostile archive, nested media paths | Done: multilingual document conversion and media extraction | Done for named `InvalidDOCX` failures; no structured report yet | Heavy-lane/release validation before final promotion | Candidate |
| HTMLZ | Done: top-level HTML/XHTML requirement, optional OPF/cover enrichment | N/A in current scope | Provisional: optional OPF/cover warnings covered | Done: missing HTML, hostile archive, bomb-shaped inputs | Done: multilingual plugin-path HTML product | Done through warnings for optional OPF/cover problems | Decide if optional enrichment loss should emit `ConversionReport` events | Provisional |
| Comic CBZ/CBC/CBR | Done: CBZ/CBC ZIP and CBR/RAR listing/extraction boundaries | N/A in current scope | N/A | Done: path safety, password entries, member budgets where backend exposes sizes | Done: multilingual CBC and comic-page output invariant | Done through warnings/strict failures; RAR name-only backends have limited size data | Real RAR backend variance and optional structured diagnostics | Provisional |
| FB2/FBZ | Done: FB2 XML and single-FB2-member FBZ selection | Done: `FB2MLizer` and `FB2Output` unicode serialization | Done: FBZ metadata registration and reader/writer paths | Done: malformed XML, hostile archive, embedded-binary ID safety, corrupt base64 warnings | Done: UTF-8/UTF-16 input and zipped/unzipped products | Done for current warnings/strict failures; no structured report yet | Heavy-lane/release validation before final promotion | Candidate |

## Legacy Binary And PalmDB Formats

| Area | Reader/Input | Writer/Output | Metadata | Hostile Boundary | Product Assertions | Loss/Diagnostics | Remaining Blocker | State |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| LIT | Done: parser fixtures, manifests, namelists, `UnBinary`, DRM boundary | Blocked for successful full archive output; unavailable LZX fails with `LitWriterError` before output open | Limited to OPF/manifest surfaces in current scope | Done: malformed headers, secondary blocks, manifest/namelist, binary markup controls | Partial: input products, `postprocess_book`, `ReBinary`, writer manifest, unavailable-LZX boundary | Done for named parser/writer failures; no structured report yet | Testable LZX compressor backend for successful `.lit` output | Blocked for full output; input/parser is candidate |
| MOBI/AZW/KF8 | Done: PalmDB/MOBI/EXTH, HUFF/CDIC, INDX/TAGX, KF8 FDST/resources | Partial: old MOBI output round-trip and writer behavior covered; richer KF8 output remains | Done: EXTH read/write, Topaz dispatch, fallback metadata policy | Done: record table, headers, EXTH, decompression budgets, KF8 indices/resources | Partial: old-MOBI output, direct/CRES image resources; skeleton/div/NCX products still open | Provisional: recovery paths visible but not structured across all paths | Realistic KF8 skeleton/div/NCX products, non-image resources, trusted budget policy | Provisional |
| PDB family | Done: PalmDB wrapper plus PalmDOC, zTXT, eReader, Plucker, Haodoo subreaders | Limited in current scope; metadata writes covered where supported | Done: legacy metadata fixtures, eReader writes, fallback behavior | Done: wrapper validation, subreader range/decompression/image/name/header failures | Done: plugin-path products for major subreaders, including Haodoo CP950/UTF-16LE | Done for named `PDBError`/strict fallback behavior; no structured report yet | Heavy-lane/release validation or real-corpus defects before final promotion | Candidate |

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

## First Review Queue

The first rows worth reviewing for actual sign-off are the candidate rows with
no known product blocker beyond broader release validation:

- ODT input/container conversion
- EPUB input/container conversion
- DOCX input/container conversion
- FB2/FBZ input/output/metadata conversion
- PDB input/metadata hardening scope
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
