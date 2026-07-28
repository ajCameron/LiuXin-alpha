# Metadata Legacy Format Fuzzing - 2026-05-17

Branch: `metadata-legacy-format-fuzzing`

## Context

PR #47 merged the metadata reader registry plus the first malformed-input lanes
for strict containers, structured XML, archive-text readers, and binary
PDF/MOBI/PDB readers.

This branch continues the same policy for legacy/specialty readers: individual
format readers should reject non-credible inputs by default, while valid sparse
or unsupported containers may still return conservative metadata.

Durable docs:

- `dev-docs/malformed-input-fuzzing.md`

## RTF/SNB/LRX Lane

Tightened default behavior:

- RTF now rejects payloads that do not start with an RTF header; valid RTF
  files without an `\info` block still return shell metadata.
- SNB now rejects invalid SNB archives; valid SNB archives without `book.snbf`
  still return shell metadata.
- LRX now rejects arbitrary short or wrong headers; unsupported but identifiable
  Librie LRX remains a valid-container fallback.
- LRX Librie detection was corrected from an unreachable four-byte comparison
  against the three-byte `LRX` marker.

All three readers keep explicit fallback opt-ins for a later best-effort
metadata facade.

Added dispatcher-level malformed cases for RTF, SNB, and LRX using:

- empty bytes
- tiny binary bytes
- PNG header bytes
- HTML document bytes
- empty ZIP central-directory bytes

Focused validation:

```bash
python3 -m pytest \
  tests/metadata/file_sources/test_malformed_input_fuzzing.py \
  tests/metadata/file_sources/test_rtf_metadata_source.py \
  tests/metadata/file_sources/test_snb_metadata_source.py \
  tests/metadata/file_sources/test_lrx_metadata_source.py \
  tests/metadata/file_sources/test_legacy_format_adapter_edge_cases.py \
  -q
```

Result: `102 passed, 1 skipped`.

## RB/IMP/LIT Lane

Extended the same malformed-input policy to the next legacy group:

- RB now rejects payloads without the RB magic header by default.
- RB truncated payloads with a recognized header now raise by default, with an
  explicit fallback opt-in.
- IMP now rejects payloads without the IMP magic header by default.
- LIT now raises on container/reader failures by default, with an explicit
  fallback opt-in.
- Valid sparse RB/IMP wrappers can still return shell metadata.

Added dispatcher-level malformed cases for RB, IMP, and LIT using:

- empty bytes
- tiny binary bytes
- PNG header bytes
- HTML document bytes
- empty ZIP central-directory bytes

Focused validation:

```bash
python3 -m pytest \
  tests/metadata/file_sources/test_malformed_input_fuzzing.py \
  tests/metadata/file_sources/test_rtf_metadata_source.py \
  tests/metadata/file_sources/test_snb_metadata_source.py \
  tests/metadata/file_sources/test_lrx_metadata_source.py \
  tests/metadata/file_sources/test_rb_metadata_source.py \
  tests/metadata/file_sources/test_imp_metadata_source.py \
  tests/metadata/file_sources/test_lit_metadata_source.py \
  tests/metadata/file_sources/test_legacy_format_adapter_edge_cases.py \
  -q
```

Result: `138 passed, 1 skipped`.

## PMLZ/Topaz Lane

Finished the obvious legacy/specialty readers in this pass:

- PML remains text-like and can be sparse, but PMLZ now rejects invalid ZIP
  payloads and archives without `.pml` members by default.
- Topaz now raises on invalid reader targets and unreadable Topaz containers by
  default.
- Both keep explicit fallback opt-ins for future best-effort routing.

Added dispatcher-level malformed cases for PMLZ and Topaz using:

- empty bytes
- tiny binary bytes
- PNG header bytes
- HTML document bytes
- empty ZIP central-directory bytes

Focused validation:

```bash
python3 -m pytest \
  tests/metadata/file_sources/test_malformed_input_fuzzing.py \
  tests/metadata/file_sources/test_rtf_metadata_source.py \
  tests/metadata/file_sources/test_snb_metadata_source.py \
  tests/metadata/file_sources/test_lrx_metadata_source.py \
  tests/metadata/file_sources/test_rb_metadata_source.py \
  tests/metadata/file_sources/test_imp_metadata_source.py \
  tests/metadata/file_sources/test_lit_metadata_source.py \
  tests/metadata/file_sources/test_pml_metadata_source.py \
  tests/metadata/file_sources/test_topaz_metadata_source.py \
  tests/metadata/file_sources/test_legacy_format_adapter_edge_cases.py \
  -q
```

Result: `166 passed, 1 skipped`.
