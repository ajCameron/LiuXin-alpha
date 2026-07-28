# File Formats PML Unicode Hardening Slice

Branch: `file-formats-pml-unicode-hardening`

Started after PR #69 merged. This follows the TXT/Markdown/Textile/RTF
framework work and uses the shared file-format fixtures from
`tests/support/file_format_unicode.py`, `file_format_conversion.py`, and
`file_format_oeb.py`.

Durable doc: `dev-docs/file-format-unicode-conversion.md`.

## Scope

First PML pass focuses on output conversion behavior:

- real `PMLMLizer` over the shared minimal OEB fixture
- real `PMLOutput.convert` into a `.pmlz`
- PML output round-tripped back through `PML_HTMLizer`

## Changes

- Added `tests/file_formats/pml/test_pml_unicode_framework.py`.
- Covered deterministic PML output from `PMLMLizer`.
- Covered deterministic `.pmlz` bytes from `PMLOutput.convert`.
- Captured the PML character-set boundary explicitly:
  - supported characters are emitted as ASCII-safe PML escapes such as
    `\a233`, Greek `\U....`, and Hebrew `\U....`
  - unsupported characters are replaced deterministically with `?`
  - supported foreign-language fragments round-trip back through
    `PML_HTMLizer`
- Added `tests/file_formats/pml/test_pml_malformed_hostile.py`.
- Covered malformed/hostile PML input with multilingual text around:
  - unclosed headings, footnotes, sidebars, links, and huge indent controls
  - bad `\U....` and odd `\a...` escapes
  - escaped braces/backslashes and path-like image references
  - invalid raw bytes in otherwise UTF-8 PML streams
  - `.pmlz` archives with multiple multilingual pages and copied image assets
- Hardened `PMLInput.process_pml` to decode input bytes with replacement so
  bad byte sequences do not abort conversion before the parser can recover.

## Decisions

- Unsupported-character replacement in PML output should be visible and
  reportable. Keeping `?` as the actual fallback output is acceptable, but
  silent data loss is not.
- Future reporting should include the conversion format/phase, count, and a
  small sample of replaced or unrepresentable characters where possible.

## Validation

- `python3 -m py_compile tests/file_formats/pml/test_pml_unicode_framework.py`
  - clean
- `python3 -m pytest tests/file_formats/pml/test_pml_unicode_framework.py -q`
  - `3 passed`
- `python3 -m pytest tests/file_formats/pml -q`
  - `40 passed`
- `python3 -m py_compile src/LiuXin_alpha/file_formats/conversion/plugins/pml_input.py tests/file_formats/pml/test_pml_malformed_hostile.py`
  - clean
- `python3 -m pytest tests/file_formats/pml/test_pml_malformed_hostile.py -q`
  - `5 passed`
- `python3 -m pytest tests/file_formats/pml -q`
  - `45 passed`
- `git diff --check`
  - clean
- `python3 -m pytest tests/file_formats -q`
  - `592 passed, 1 skipped, 124 warnings`

## Next

- Implement visible/reportable diagnostics for lossy PML output replacement
  before broader conversion-matrix work depends on the PML fallback behavior.
- Add more PML image-output tests if conversion fidelity becomes image-heavy.
- Move to the next complexity step, likely ODT/OPF/container conversion, once
  this PML slice is merged.
