# Subjects and tags

Reviewed **2026-11-21**. How LiuXin keeps subject headings apart from tags,
from the file readers through to the writers.

## Two tables, two meanings

LiuXin has a table for each (see the [metadata table notes](../databases/liuXin_metadata_tables_notes.md)):

- **tags** are flexible labels, such as collections, workflows and placement
  hints;
- **subjects** are library-style subject headings, which can be hierarchical.

calibre has only tags. Its record, its readers and the formats it writes have
one field for both, so until this change LiuXin merged the two wherever calibre's
shape was involved:

- the reading pool filed every tag under the Work's subjects;
- `to_calibre()` added a LiuXin record's subjects to its tags.

## The readers' record

LiuXin's copy of calibre's record (`metadata/book/base.py`) has a `subjects`
field: an ordered list, empty by default. It is LiuXin's own:

- `LIUXIN_METADATA_FIELDS` in `metadata/book/__init__.py` names it;
- the calibre compatibility constants stay calibre's;
- like the tags, `smart_update` merges the subjects (case ignored, case kept)
  rather than copying them;
- `metadata_as_dict` and the JSON codec carry it.

`to_calibre()` puts a LiuXin record's subjects there and its tags in `tags`.
`from_calibre()` reads them back.

## Reading

The reading pool's mapping (`file_formats/providers/wemi_mapping.py`) puts
calibre's `tags` at the Work's `tags` and the record's `subjects` at the Work's
`subjects`.

A format's field becomes a subject only where the format keeps subject headings
apart from keywords. Elsewhere the reader follows calibre:

- OPF and EPUB `dc:subject`, MOBI's subject records, DOCX and PDF keywords and
  HTML `<meta name="subject">` are read as tags;
- RTF's `\subject` is the comments;
- PDF's Info Subject depends on the `pdf_metadata_merge` policy (#204).

## Writing

A writer plugin whose format keeps subjects apart sets `writes_subjects = True`
and writes the record's `subjects` itself.

For every other writer, `set_file_type_metadata` hands the plugin a copy of the
record:

- its tags are followed by the subjects not already among them (case ignored,
  `tags_and_subjects`);
- its `subjects` field is emptied.

So formats with one field for both still store both there. Read back, they are
all tags: the format cannot tell them apart.

The OPF builders for calibre's record work the same way, so that conversion and
other OPF output keep the subjects:

- `OPFCreator`;
- `calibre_metadata_to_opf`, and its copy in the legacy OPF module;
- OPF 3's `apply_metadata`;
- conversion's `meta_info_to_oeb_metadata`.

LiuXin's own OPF for its own record is different: it already writes the tags as
`dc:subject` and the subjects as `<meta name="custom_subject">`, and reads them
back apart.
