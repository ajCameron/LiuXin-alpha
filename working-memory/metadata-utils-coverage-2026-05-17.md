# Metadata Utils Coverage - 2026-05-17

Branch: `metadata-utils-coverage`

## Context

After the standardization coverage pass, the next metadata-core target was the
shared helper layer:

- `metadata/utils.py`
- `metadata/ebook_metadata_tools.py`

These modules are reused by OPF handling, metadata readers/writers, database
write paths, terminal commands, renderers, and compatibility facades.

## Implemented

Added focused coverage in `tests/metadata/test_utils_coverage.py` for:

- author string splitting/rendering/sort-key helpers
- title article sorting across English, French, German, and Spanish
- ISBN-10/ISBN-13/ISSN/DOI validation and formatting
- series-index Roman numeral formatting
- `Resource` / `ResourceCollection` URL and filesystem helpers
- OPF parsing, manifest item creation, uniqueness, and language normalization
- timestamp coercion in `ebook_metadata_tools.to_epoch_ms`
- unicode/foreign-language torture using CJK, Japanese, Arabic, Hebrew,
  Mandarin/Simplified Chinese-style tags, RTL marks, BOMs, combining accents,
  emoji, and unicode filenames/fragments

Narrow fixes surfaced by the tests:

- `metadata.utils.author_to_author_sort()` now honors `copy` mode correctly;
  `comma` mode no longer returns the original author unchanged.
- Roman series-index formatting now uses a reusable tuple instead of an
  exhausted `zip` iterator.
- `metadata.utils.Resource` now uses Python 3 URL quoting/unquoting,
  handles byte paths/basedirs, and has a working `from_path()` constructor.
- `ResourceCollection.from_directory_contents()` now returns file resources
  instead of attempting to join directory lists into paths.
- `parse_opf()` accepts `os.PathLike` inputs.
- `normalize_languages()` preserves OPF country codes across ISO-2/ISO-3
  language-code differences and handles script-region tags such as
  `zh-Hant-TW`.
- `create_manifest_item()` now calls `makeelement()` with the Python 3/stdlib
  compatible argument shape.
- `ebook_metadata_tools.check_isbn13()` no longer subscripts a Python 3
  `map` object.
- `ebook_metadata_tools.check_issn()` now accepts the `check == 11` / `0`
  valid edge case already handled by `metadata.utils`.
- `to_epoch_ms()` strips boundary BOM / zero-width / directional markers
  before parsing otherwise-valid timestamp strings.

## Validation

Focused coverage:

```bash
.venv/bin/python -m pytest \
  tests/metadata/test_utils_coverage.py \
  --cov=LiuXin_alpha.metadata.utils \
  --cov=LiuXin_alpha.metadata.ebook_metadata_tools \
  --cov-report=term-missing:skip-covered \
  -q
```

Result:

- `7 passed`
- combined focused coverage: `93%`
- `metadata.utils`: `91%`
- `metadata.ebook_metadata_tools`: `96%`

Adjacent validation:

```bash
.venv/bin/python -m pytest \
  tests/metadata/test_utils_coverage.py \
  tests/metadata/test_opf_tools.py \
  tests/metadata/test_metadata_top_level_facade.py \
  tests/metadata/book/test_book_metadata_base.py \
  tests/surfaces/test_renderers_calibre_metadata.py \
  -q
```

Result: `41 passed`.

```bash
.venv/bin/python -m pytest \
  tests/databases/database_calibre_emultation/test_calibre_library_builder.py \
  tests/databases/database_driver_plugins/SQLite_database_driver/test_sqlite_database_driver_comprehensive.py \
  -q
```

Result: `34 passed`, `1 skipped`, `1 xfailed`, `2 xpassed`.

```bash
.venv/bin/python -m pytest \
  tests/file_formats/opf \
  tests/metadata/file_sources/test_opf_metadata_source.py \
  tests/metadata/file_sources/test_opf_edge_cases.py \
  tests/metadata/file_sources/test_epub_edge_cases.py \
  -q
```

Result: `48 passed`, with existing date deprecation warnings.

Also clean:

- OPF unicode adjacency: `16 passed`
- `py_compile` for touched source/tests
- `git diff --check`

## Next

Remaining focused misses are mostly preference/import fallback paths and
defensive exception branches. The next metadata coverage target is likely
either the WEMI writer/container low spots or the web-source layer, depending
on whether we want core API coverage or mocked-provider coverage next.
