# Metadata Standardize Coverage - 2026-05-17

Branch: `metadata-standardize-coverage`

## Context

Metadata remains the current coverage priority because it is expected to see
heavy production use and may later be split into its own project. After the
`metadata.book` pass, the latest coverage run showed `metadata.standardize.py`
as the next useful target at roughly `35%` line coverage, with
`metadata.standardization.py` already higher but still carrying overlapping
legacy behavior.

## Implemented

Narrow fixes surfaced by the new tests:

- `standardize_title` in both modules now escapes separators correctly instead
  of inserting literal backslashes, and now applies the final `.strip()`.
- `cleanup_tags` in both modules now handles Python 3 `str`, `bytes`,
  `bytearray`, `None`, and non-string values safely before comma replacement,
  whitespace compaction, and case-insensitive de-duplication.
- `standardize_creator_name` in both modules now capitalizes the first ASCII
  letter without truncating hyphenated or apostrophe names such as `Jean-Luc`
  and `O'Neill`.

Added/expanded focused tests for:

- unicode torture and malformed-ish strings across the standardization surface
- title/search/hash helpers, ISBN/identifier normalization, language and genre
  paths, publisher/series fallbacks, and tag cleanup
- invalid author-splitting preference fallback on module reload
- pinned creator-name edge cases including initials, Mc/Mac joining, hyphens,
  apostrophes, and non-Latin names
- parity between the overlapping shared APIs in `metadata.standardize` and
  `metadata.standardization`
- `standardization.classify_fiction_genre` delegation

## Validation

Focused coverage:

```bash
.venv/bin/python -m pytest \
  tests/metadata/test_standardization_torture.py \
  tests/metadata/test_standardize_coverage.py \
  tests/metadata/standarize \
  --cov=LiuXin_alpha.metadata.standardize \
  --cov=LiuXin_alpha.metadata.standardization \
  --cov-report=term-missing:skip-covered \
  -q
```

Result:

- `59 passed`
- `standardize.py`: `98%`
- `standardization.py`: `97%`
- combined focused coverage: `97%`

Adjacent validation:

```bash
.venv/bin/python -m pytest \
  tests/metadata/containers/calibre_like_book_metadata/test_metadata_identifiers.py \
  tests/metadata/containers/calibre_like_book_metadata/test_metadata_creators.py \
  tests/metadata/test_genre_tree_wiring.py \
  -q
```

Result: `19 passed`.

Also clean:

- `py_compile` for touched source/tests
- `git diff --check`

## Next

The remaining uncovered lines are mostly defensive branches that normal string
splitting cannot reach, plus an empty-token creator-name branch. A larger future
cleanup should probably consolidate `metadata.standardize` and
`metadata.standardization` rather than continuing to patch both forever.
