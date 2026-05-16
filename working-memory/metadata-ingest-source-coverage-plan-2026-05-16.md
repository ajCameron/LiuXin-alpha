# Metadata Ingest Source Coverage Plan - 2026-05-16

Branch: `renderer-coverage-tests`

## Context

Metadata file-source readers are a production-heavy path. Treat them as a
high-value coverage lane, with emphasis on unicode, malformed payloads, soft
failure behavior, and realistic metadata normalization.

Latest inspected full coverage:

- Pytest JSON: `working-memory/test-results/full-suite-2026-05-16-042829.json`
- Coverage XML: `working-memory/test-results/coverage-2026-05-16-042639.xml`
- Outcome: `3403 passed`, `162 skipped`, `22 xfailed`, `5 xpassed`

Current high-miss file-source modules:

- `metadata/file_sources/fb2.py`: `391 / 651` missing
- `metadata/file_sources/pdf.py`: `315 / 728` missing
- `metadata/file_sources/mobi.py`: `314 / 457` missing
- `metadata/file_sources/epub.py`: `186 / 361` missing
- `metadata/file_sources/from_string.py`: `108 / 506` missing
- `metadata/file_sources/opf.py`: `96 / 445` missing
- `metadata/file_sources/odt.py`: `80 / 305` missing
- `metadata/file_sources/odt_beta.py`: `80 / 326` missing
- `metadata/file_sources/extz.py`: `76 / 214` missing
- `metadata/file_sources/docx.py`: `71 / 96` missing

## Testing Strategy

1. Start with parsers that can be exercised without large external fixture
   corpora:
   - `from_string`
   - `fb2`
   - `opf`
   - focused helper/error paths in `epub`, `mobi`, and `pdf`

2. Add unicode torture cases deliberately:
   - combining marks and precomposed forms
   - CJK, Greek, Cyrillic, Arabic, Devanagari
   - emoji and astral-plane characters
   - smart punctuation, em/en dashes, full-width separators
   - right-to-left text in authors/titles/comments
   - invalid bytes and declared-encoding mismatches

3. Cover malformed and hostile inputs:
   - truncated XML
   - invalid encodings
   - missing title/author sections
   - empty containers
   - duplicate fields
   - duplicate/invalid identifiers
   - no-network parser behavior
   - archive members with unexpected names

4. Prefer small inline fixtures and fake collaborators over broad golden files:
   - makes tests deterministic in CI
   - avoids requiring external metadata-test corpora
   - lets the suite exercise failure branches directly

5. Keep full-book binary fixture tests as secondary. Use them where format
   libraries make small synthetic binaries impractical, but do not block parser
   coverage on external fixture availability.

## Initial Work Order

1. `from_string`: expand heuristic/parser torture around path-like input,
   explicit regex groups, invalid regexes, ISBN/date stripping, nested bracket
   tokens, series/tags/comments, bytes, empty values, and multilingual names.
2. `fb2`: expand inline XML/zip tests for namespaces, fallback sections,
   annotations, covers, publishers, ISBNs, dates, series, bad zip payloads, and
   malformed XML.
3. `opf`: add inline OPF package tests for dc/meta variants, duplicate fields,
   unicode normalization, identifiers, series metadata, and malformed XML.
4. `epub`/`mobi`/`pdf`: add helper-level and collaborator-fake tests first,
   then only use binary fixture tests where needed.

## Progress - 2026-05-16

Added inline, deterministic edge-case coverage for the first ingest-source pass:

- `tests/metadata/file_sources/test_from_string_edge_cases.py`
  - regex group aliases, path/full-path parsing, bytes and invalid UTF-8,
    bracket/token parser edges, ISBN/date helpers, unicode tags/authors/series.
- `tests/metadata/file_sources/test_fb2_edge_cases.py`
  - inline FB2 XML and zipped payloads, unicode title/author/tags/comments,
    document-info fallback, cover extraction/coercion, null writes, bad streams.
- `tests/metadata/file_sources/test_opf_edge_cases.py`
  - OPF helper and stream handling, malformed XML, metadata node selection,
    unicode identifiers, calibre merge edges, LiuXin conversion fallbacks.
- `tests/metadata/file_sources/test_epub_edge_cases.py`
  - inline EPUB zip/container fixtures, OCF/encryption parsing, cover extraction
    and render fallback, OPF writer collaboration, path/stream failure behavior.
- `tests/metadata/file_sources/test_mobi_edge_cases.py`
  - stream slicing, cover/header helpers, Topaz failure handling, synthetic EXTH
    update records, MOBI updater binary helper methods.
- `tests/metadata/file_sources/test_pdf_edge_cases.py`
  - PDF literal/hex/name/array/dict parser torture, compressed streams, XMP
    fallback extraction, path/stream reads, Info/XMP normalization, writer shims.

Focused module coverage after this pass:

- `from_string.py`: 95%
- `fb2.py`: 86%
- `opf.py`: 98%
- `epub.py`: 96%
- `mobi.py`: 80%
- `pdf.py`: 91%

Aggregate validation:

```bash
.venv/bin/python -m pytest tests/metadata/file_sources \
  --cov=LiuXin_alpha.metadata.file_sources \
  --cov-report=term-missing -q
```

Result: `320 passed`, `114 skipped`, `19 warnings`; focused
`metadata.file_sources` coverage is now 83%.

Notable remaining black spots after this pass:

- `docx.py`: still corpus-dependent and low in this environment.
- `extz.py`, `odt.py`, `odt_beta.py`, archive/container helpers: still worth a
  second pass.
- `mobi.py`: remaining uncovered code is mostly full binary patch construction
  and `MetadataUpdater.__init__`; further gains need either a compact synthetic
  MOBI builder or a stable fixture.
- `pdf.py`: remaining writer branch coverage depends on optional `pypdf`.

## Validation Loop

Use focused coverage while developing:

```bash
.venv/bin/python -m pytest tests/metadata/file_sources \
  --cov=LiuXin_alpha.metadata.file_sources \
  --cov-report=term-missing -q
```

For tighter iteration, run individual module tests with matching `--cov` target
before the broader file-source pass.

## Progress - Later 2026-05-16

Extended the same inline/malformed-input treatment across the remaining
metadata extractor modules, without depending on the external metadata-test
corpus:

- `tests/metadata/file_sources/test_archive_container_edge_cases.py`
  - archive JSON/comment helpers, CBZ/CBR dispatch paths, ZIP OPF/cover
    resolution, EXTZ cover helper fallbacks, DOCX fake-container cover/write
    edges.
- `tests/metadata/file_sources/test_text_odt_edge_cases.py`
  - TXT parser/source edge cases, TXTZ fallback readers/helpers, ODT and
    ODT-beta cover/error/fallback helper paths.
- `tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py`
  - LIT href/cover/log fallbacks, PML/PMLZ parser and cover selection edges,
    Haodoo author normalization, dispatcher plugin adapter failure paths, and
    worker merge/import/job helper behavior.
- `tests/metadata/file_sources/test_pdb_subreader_edge_cases.py`
  - PDB wrapper type/header fallbacks, eReader cover and write paths, Plucker
    record iteration/decode/timestamp handling.
- `tests/metadata/file_sources/test_legacy_format_adapter_edge_cases.py`
  - RAR, IMP, LRX, RB, RTF, SNB, and Topaz helper/error paths, including stream
    restoration, unicode decode behavior, metadata replacement, and synthetic
    malformed containers.

Production fix found while adding tests:

- `src/LiuXin_alpha/metadata/file_sources/pdb/ereader.py`
  - `set_metadata()` no longer rewrites every 16-bit eReader header field when
    creating a missing metadata record. It now updates only section-offset
    fields, preserving the compression value.

Focused module movement after this pass:

- Dispatcher package `metadata/file_sources/__init__.py`: 99%
- `lit.py`: 92%
- `pml.py`: 93%
- `worker.py`: 95%
- PDB readers: wrapper 90%, eReader 94%, Haodoo 100%, Plucker 91%
- Older adapters: RAR 93%, RB 91%, LRX 91%, SNB 90%, Topaz 91%, IMP 88%,
  RTF 89%

Aggregate validation:

```bash
.venv/bin/python -m pytest tests/metadata/file_sources \
  --cov=LiuXin_alpha.metadata.file_sources \
  --cov-report=term-missing -q
```

Result: `347 passed`, `114 skipped`, `19 warnings`; focused
`metadata.file_sources` coverage is now 90%.

Remaining meaningful gaps:

- `docx.py` remains low in this environment because the corpus-backed tests are
  skipped without the external metadata-test fixtures.
- `mobi.py` remains at 80%; further improvement needs either a compact
  synthetic MOBI builder or stable binary fixtures for full update paths.
- `extz.py`, `odt.py`, and `odt_beta.py` are now covered by helper/fallback
  tests, but still have writer/container branches that need deeper synthetic
  fixtures for 90%+.
