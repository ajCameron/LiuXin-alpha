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

Formats whose tags and subjects share one field (OPF and EPUB `dc:subject`, MOBI's
subject records, DOCX and PDF keywords) are read as tags, as calibre reads them.

## A document's Subject field

Four formats have a field of their own named Subject. calibre reads each
differently. The `read_subject_fields_as` preference chooses
(`metadata/file_sources/subject_fields.py`):

| Field | `subjects` (default) | `calibre` |
| --- | --- | --- |
| HTML `<meta name="subject">` | Subjects | Tags |
| RTF `\subject` | Subjects; the comments are `\doccomm` | The comments |
| PDF Info `/Subject` | Subjects, under every merge policy; the comments are the packet's `dc:description` | The first tag, under every merge policy, as calibre reads it |
| ODT `dc:subject` | Subjects | Not read |

Under `subjects`, several headings share the one field, joined by semicolons, so a
heading may hold commas but not a semicolon. The writers follow the same setting:

- **RTF** writes the subjects to `\subject` and the comments to `\doccomm`. calibre
  reads `\subject` as the comments and ignores `\doccomm`, so in calibre such a book
  shows its subjects as its comments.
- **PDF** writes the subjects to `/Subject` and the comments only to the XMP packet.
  A file without a packet gets one when there are comments to write. A file whose
  appended update would not read back without that packet is still rewritten whole,
  as before. calibre reads the comments from the packet. Under `calibre`, the writer
  leaves `/Subject` as it is, as calibre's does, and the comments still go only to
  the packet. A Subject left there is read back as a tag only when the Info
  dictionary wins the tags: after a write the packet is as new, so its tags win under
  `newest` and `calibre`; `union` keeps both.
- **ODT** writes the subjects to its Subject.

Under `calibre`, each writer does as calibre's does, and the subjects go among the
tags (see below). HTML has no metadata writer.

## Writing

A writer plugin whose format keeps subjects apart sets `writes_subjects` and writes
the record's `subjects` itself. For RTF, PDF and ODT it is a property that follows
the `read_subject_fields_as` preference.

For every other writer, `set_file_type_metadata` hands the plugin a copy of the
record (`subjects_among_tags` in `metadata/book/base.py`):

- its tags are followed by the subjects not already among them (case ignored,
  `tags_and_subjects`);
- its `subjects` field is emptied.

LiuXin's built-in writer plugins do the same themselves, so a plugin called
directly writes what the runtime would (#218). Doing it twice changes nothing.

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
