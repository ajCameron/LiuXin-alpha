# LiuXin Alpha Examples

All examples assume you run from repository root and have dependencies installed.

General pattern:

```bash
python examples/<script>.py --help
```

Run a local non-network smoke tour:

```bash
bash examples/quickstart.sh
```

## Library + Storage

- `library_facade_example.py`
  - End-to-end `Library` usage: ensure store row, refresh storage, add/retrieve a file.
- `library_register_unmanaged_disk_example.py`
  - Register all ebook files under a disk root as an unmanaged store.
- `storage_bootstrap_report_example.py`
  - Load stores from DB `stores` table and print bootstrap report/issues.
- `storage_manager_manual_roundtrip_example.py`
  - Use `StorageManager` directly (without DB wiring) for a simple add/retrieve round-trip.
- `reconcile_with_database_path_example.py`
  - Call `register_existing_disk_with_database_path(...)` directly.
- `quickstart.sh`
  - Runs a non-network sequence of the local Library/Storage examples end-to-end.

## Metadata / Web Sources

- `openlibrary_plugin_example.py`
  - Query OpenLibrary for cover bytes by ISBN and optionally save the cover.
- `google_books_plugin_example.py`
  - Query Google Books plugin directly and optionally save cover bytes.
- `metadata_identify_example.py`
  - Run identify pipeline across enabled metadata plugins and print normalized results.

## Utilities

- `comments_to_html_example.py`
  - Convert plain text comments to minimal HTML using library helper.

## Conversion

- `conversion_oeb_to_epub_example.py`
  - Convert an OPF/OEB source directory into `.epub` using the EPUB output plugin.
  - If `--input-opf` is omitted, the script generates a sample OEB input automatically.
- `conversion_oeb_to_mobi_example.py`
  - Convert an OPF/OEB source directory into `.mobi` using the MOBI output plugin.
  - If `--input-opf` is omitted, the script generates a sample OEB input automatically.
- `conversion_to_oeb_example.py`
  - Convert many input formats into an OEB directory (OPF/NCX/XHTML/CSS assets).
  - Supported formats include: `txt`, `md`, `markdown`, `textile`, `html`, `xhtml`, `htmlz`, `epub`, `mobi`, `azw`, `azw3`, `azw4`, `pdf`, `fb2`, `rtf`, `odt`, `docx`, `pdb`, `rb`, `pml`, `tcr`, `lit`, `lrf`, `snb`, `chm`, `djvu`, `cbz`, `cbr`, `cbc`.
- `conversion_batch_to_oeb_example.py`
  - Batch wrapper around `conversion_to_oeb_example.py` for multiple input files.

Common usage:

```bash
python examples/conversion_to_oeb_example.py --input /path/to/book.epub --output-dir /tmp/book_epub_oeb --clean-output
python examples/conversion_to_oeb_example.py --input /path/to/book.mobi --output-dir /tmp/book_mobi_oeb --clean-output
python examples/conversion_to_oeb_example.py --input /path/to/book.docx --output-dir /tmp/book_docx_oeb --clean-output
python examples/conversion_to_oeb_example.py --list-formats
```

Batch usage:

```bash
python examples/conversion_batch_to_oeb_example.py \
  --output-root /tmp/oeb_batch \
  --inputs /path/to/a.epub /path/to/b.mobi /path/to/c.docx \
  --clean-output
```
