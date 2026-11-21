# Language codes

Reviewed **2026-11-20**. How LiuXin turns what a book or a source says about its
language into a code, and where each form is used.

## The table

calibre's language functions read a table it generates when it is built
(`iso639.pickle`, now `iso639.calibre_msgpack`). LiuXin never shipped one, so
until #207 every function in `utils/localization` fell back to something broken:

- `canonicalize_lang` returned its input lowercased (`French` gave `french`);
- `get_language` raised `FileNotFoundError` for most codes;
- `lang_as_iso639_1` returned three-letter codes;
- `lang_map` and `langnames_to_langcodes` used `dict.iteritems`.

They now read `utils/libraries/iso639/iso_codes.json.gz`. That is calibre's table,
built from the iso-codes project with calibre's rules by
`scripts/build_upstream_calibre_iso_codes.py`, and it covers ISO 639-3 (about 7,900
languages). The ported calibre readers read the same file (it used to sit beside
them in `metadata/upstream_calibre`).

`utils/libraries/iso639/calibre_table.py` adds what library data uses and calibre's
table lacks, from the ISO 639-2 list that ships beside it:

- **Bibliographic codes** (`fre`, `ger`, `chi`) map to calibre's terminologic ones
  (`fra`, `deu`, `zho`). calibre itself does not know them.
- **ISO 639-2's 66 collective codes** (`bnt` Bantu, `afa` Afro-Asiatic) are known.
  Five of them are also the name of one language in calibre's table: `bih`, `day`,
  `nai`, `sio` and `tai`. calibre reads those as the names; LiuXin reads them as the
  codes.

Every other code, two-letter code and one-word English name in calibre's table gives
calibre's answer. `tests/utils/test_language_codes.py` checks all of them.

## The forms

- **Readers** give calibre's three-letter code: `eng`, `fra`, `cmn`. A two-letter
  code, a bibliographic code or an English name is converted. FB2 and MOBI's
  language record pass the file's own code through unchanged, as calibre's readers
  do. On every book in `LiuXin_alpha_data`, LiuXin's readers report the languages
  calibre's ported readers do (`tests/metadata/file_sources/test_reader_languages.py`).
- **Writers** convert back where a format wants two letters:
  - DOCX `dc:language`;
  - the MOBI language record;
  - OPF output, through `normalize_languages`, which keeps any country (`en-GB`).
- **The catalogue** stores languages by name. `metadata/languages.py` canonicalises
  through `utils/libraries/iso639/iso639_tools`, which gives ISO 639-2's English
  names, and the `languages` table is seeded from the same list, so any code form
  finds the same row. Nothing in this changed.

`canonicalize_lang`, like calibre's, looks only at the first word: `en-GB` is
`eng`, but `Modern Greek` is `modern`, which is not a language.

## Also fixed

The ISO 639-2 list starts with two byte-order marks, which stayed in Afar's code
(`aar`), so it could not be found, and new databases seeded it with them.
