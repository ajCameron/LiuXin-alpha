# Torrent Discovery

Date: 2026-03-15

Context:
- No existing torrent-specific code or tests are present in the repo yet.
- Implemented now:
  - `scripts/torrent_ebook_inventory.py`
  - `tests/utils/test_torrent_ebook_inventory_script.py`
- Short-term goal addressed:
  - analyze a `.torrent` file
  - list ebook-shaped files inside it
  - output structured JSON suitable for later store ingestion
- Longer-term TODO:
  - support a torrent-backed store
  - allow on-demand download of files from that store via a torrent client integration

Assessment:
- `.torrent` file analysis was the right low-risk first slice.
- It does not require a running torrent client.
- It is now implemented with Python stdlib only by parsing bencoded data and walking the embedded file list.

Current script shape:
- input: path to a `.torrent` file
- output:
  - torrent metadata summary
  - all embedded files
  - ebook-shaped subset
  - grouped logical-book candidates by directory + normalized stem
  - optional human-readable terminal report via `--report text`
- no resume state, because local `.torrent` analysis is a one-shot parse rather than a crawl

Output sections:
- `torrent`
  - `name`
  - `info_hash`
  - `announce`
  - `announce_list`
  - `piece_length`
  - `file_count`
  - `ebook_file_count`
  - `group_count`
  - `directory_group_count`
  - `multi_variant_group_count`
  - `multi_stem_directory_group_count`
  - `total_size`
- `files`
- `ebook_files`
- `groups`
- `directory_groups`

Grouping modes:
- `groups`
  - keyed by `directory + normalized stem`
  - best for multi-format logical books
- `directory_groups`
  - keyed by parent directory only
  - best for messy dump torrents where one folder may contain several unrelated books

Terminal report mode:
- `--report text`
- intended for quick local inspection without opening JSON
- currently includes:
  - torrent summary
  - likely-book groups
  - messy directory groups
  - ebook file listing

Validation:
- `pytest -q tests/utils/test_torrent_ebook_inventory_script.py`
  - `5 passed`
- `python -m py_compile scripts/torrent_ebook_inventory.py tests/utils/test_torrent_ebook_inventory_script.py`

Likely next step:
- decide whether to add optional magnet support via an external metadata fetch path later
- keep store integration separate for now
