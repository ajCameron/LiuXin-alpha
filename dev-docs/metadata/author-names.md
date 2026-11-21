# Author names

Reviewed **2026-11-20**. How an author's name gets from a book into LiuXin's metadata
record unchanged, and the one place it still does not.

## Strings and lists

Many formats hold every author in one field, joined as calibre joins them: `` & ``
between names, an ampersand within a name doubled (`Ann Writer & Smith && Sons`).
That is an **author string**. LiuXin's metadata record
(`CalibreLikeLiuXinBookMetaData`) takes both forms:

- **A string** is parsed as an author string: split on `&` (and on `` and `` and
  `` with ``, as calibre's default `authors_split_regex` does), `&&` kept as one
  ampersand, and each name title-cased.
- **A list or tuple** holds names. Each is kept as given, apart from surrounding
  space, so "Smith & Sons", "bell hooks" and "Ann with Bob" stay as they are.

Until #208 the constructor took a list one name at a time, as strings, so every name a
reader passed it was parsed again. "Smith & Sons" came back as two authors,
"bell hooks" as "Bell Hooks", and "Ann with Bob" as two; the PDF reader did the same
by assigning names one by one. Readers now pass the names they find as a list.

## By format

What LiuXin's writers put in each field, and what both readers make of it:

- **Joined author strings** (LRF, DOCX, PDF Info): `` & `` between names, `&&`
  within one. Both readers split them back into the names written.
- **One element per name** (FB2, MOBI's EXTH records, PDF XMP): each name is
  separate. calibre's XMP reader still splits each element on `&`, and LiuXin's
  follows it, so a name holding `&` survives in a PDF through its Info dictionary.
- **RTF** `\author`: names are joined with commas, which is how calibre writes and
  reads them; a name without a comma is one name, whatever else it holds. LiuXin's
  writer once joined them with `& ` (no space before it), which the reader still
  splits on.
- **OPF 2** (EPUB 2, HTMLZ, TXTZ): one `dc:creator` per name, but calibre reads each
  as an author string, so "Smith & Sons" comes back as "Smith" and "Sons" in calibre
  too. calibre does not escape the ampersand when it writes, and LiuXin follows it
  rather than read the same EPUB differently. OPF 3 does not split.

`tests/metadata/file_sources/test_reader_author_names.py` checks each of these in both
readers.
