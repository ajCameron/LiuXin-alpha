# File Formats PML Unicode Hardening Slice

Branch: `file-formats-pml-unicode-hardening`

Started after PR #69 merged. This follows the TXT/Markdown/Textile/RTF
framework work and uses the shared file-format fixtures from
`tests/support/file_format_unicode.py`, `file_format_conversion.py`, and
`file_format_oeb.py`.

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

## Validation

- `python3 -m py_compile tests/file_formats/pml/test_pml_unicode_framework.py`
  - clean
- `python3 -m pytest tests/file_formats/pml/test_pml_unicode_framework.py -q`
  - `3 passed`
- `python3 -m pytest tests/file_formats/pml -q`
  - `40 passed`
- `git diff --check`
  - clean
- `python3 -m pytest tests/file_formats -q`
  - `587 passed, 1 skipped, 124 warnings`

## Next

- Add malformed/hostile PML input tests that combine foreign-language content
  with broken/unclosed control sequences.
- Exercise `PMLInput.convert` for encoded byte streams and `.pmlz` archives.
- Decide whether unsupported-character replacement should remain `?` or gain a
  visible/reportable conversion warning before broader conversion work.
