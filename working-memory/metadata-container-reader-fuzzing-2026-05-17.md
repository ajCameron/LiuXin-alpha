# Metadata Container Reader Fuzzing - 2026-05-17

Branch: `metadata-legacy-format-fuzzing`
PR: #48 (`Harden legacy metadata malformed reads`)

## Context

After the legacy/specialty malformed-input pass, the next reader lane is
container-adjacent metadata readers that were still outside the shared fuzz
corpus. Keep this on the same branch/PR until the user asks to split it.

## Implemented

- Opened PR #48 for the existing legacy-reader hardening commit.
- Added named ODT failures:
  - `metadata.file_sources.odt.OdtFormatError`
  - `metadata.file_sources.odt_beta.OdtFormatError`
- ODT and ODT beta now reject unreadable/missing-`meta.xml` containers by
  default, while keeping `fallback_on_parse_error=True` as an explicit
  best-effort opt-in.
- Added `metadata.file_sources.lrf` as a file-source wrapper around
  `file_formats.lrf.meta` so the registered LRF metadata reader raises
  `LrfFormatError` instead of leaking lower-level `struct.error`/parser
  exceptions.
- Updated the built-in `LRFMetadataReader` to use the new wrapper.
- Extended deterministic malformed-input coverage:
  - ODT and RAR in the strict container corpus.
  - LRF in the strict binary corpus.
  - Registry assertions include `ODTMetadataReader`, `RARMetadataReader`, and
    `LRFMetadataReader`.

## Validation

Focused validation:

```bash
python3 -m pytest \
  tests/metadata/file_sources/test_odt_metadata_source.py \
  tests/metadata/file_sources/test_odt_beta_metadata_source.py \
  tests/metadata/file_sources/test_lrf_metadata_source.py \
  tests/metadata/file_sources/test_malformed_input_fuzzing.py \
  -q
```

Result: `135 passed, 12 warnings`.

Broader metadata-reader validation after the final edit: `388 passed`,
`2 skipped`, `19 warnings`. The skips are expected when no `unrar` runtime is
available and no optional `.lrx` fixture exists locally. The warnings are
existing ODT/OPF date deprecation warnings from `utils/date.py`.

## Next

Run the broader metadata-reader focus set before committing/pushing. The next
unaddressed malformed-reader lane is the comic reader (`cbr`/`cbz`), which
still intentionally returns shell metadata for readable comic archives with no
comment but should be reviewed for unreadable/wrong-format archives.
