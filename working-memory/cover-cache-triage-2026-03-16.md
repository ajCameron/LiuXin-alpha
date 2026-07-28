# Cover Cache Triage

Date: 2026-03-16

## Question

- Should the legacy cover-cache utility tests be ported now?

Short answer:
- not yet

## What I Checked

- The legacy test in question is:
  - `upstream/src/LiuXin_tests/folder_stores/cover_cache/on_disk/utils_test.py`
- Old runtime references still exist in alpha:
  - `src/LiuXin_alpha/databases/backend.py`
  - [folder_store_driver.py](../src/LiuXin_alpha/customize/folder_store_driver.py)
- But the corresponding implementation module does not exist in this checkout:
  - `LiuXin_alpha.folder_stores.cover_caches.on_disk`
  - `LiuXin_alpha.folder_stores.folderstoremanager`
  - `LiuXin_alpha.folder_stores.location`
- Import probe confirms those modules currently raise `ModuleNotFoundError`.

## Conclusion

- There is no live alpha-side `CoverCache` implementation to test directly right now.
- That means the old utility test should not be ported as-is.
- First there has to be one of:
  - a restored current implementation, or
  - an explicit replacement seam that owns cover-path generation and retrieval semantics

## Current Related Coverage

Higher-level cover/image behavior is already exercised in:
- [test_images_api.py](../tests/surfaces/test_images_api.py)
- [test_web_calibre_readonly.py](../tests/surfaces/test_web_calibre_readonly.py)
- [test_metadata_files_and_covers.py](../tests/metadata/containers/calibre_like_book_metadata/test_metadata_files_and_covers.py)

But those tests do not lock down the old cache-path utilities such as:
- theoretical cover name generation
- theoretical cover path generation
- missing-cover cache error semantics

## Recommendation

- Do not port the legacy cover-cache utility tests yet.
- Treat them as a separate triage item.
- If cover-cache semantics become product-significant again:
  - add small focused tests next to the real implementation
  - do not revive the old `folder_stores` utility test shape
