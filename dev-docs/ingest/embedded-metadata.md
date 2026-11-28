# Reading embedded metadata at ingest

Started **2026-11-27**. The step has two halves: a dry run that plans the books,
and an apply step (added 2026-11-28) that writes a reviewed plan. This is the
first caller for [ING01](next_tasks.md#ing01-define-candidate-to-write-receipts).

## What it does

`sync store` registers a Store's files in `files`, but those rows have no
`file_item_id`, so no Work, Expression, Manifestation or Item exists for them
yet. The dry run reads each of those files' embedded metadata and plans the books
it would add, without creating anything:

```text
ingest metadata <store_id|store_name> --dry-run [--extensions epub,mobi]
    [--limit <n>] [--max-file-mb <n>] [--report <path>] [--background] [--json]
```

The command submits the Core command `ingest.metadata.plan`, which runs the
`run_ingest_metadata_plan_job` worker. The worker writes the full plan as JSON. By
default it goes to `ingest-plans/metadata-plan-store-<id>-<UTC time>.json` beside
the catalogue. The job result holds the summary, the plan's path and a sample of
ten books and ten failures.

The apply step writes a plan, after you've reviewed it:

```text
ingest metadata <store_id|store_name> --apply <plan.json> [--limit <n>]
    [--report <path>] [--json]
```

It submits `ingest.metadata.apply`, which runs in the Core process (see
[Applying a plan](#applying-a-plan)). The command needs exactly one of
`--dry-run` and `--apply`.

To try it in the terminal UI, sync a Store, then plan and apply a sample before
doing everything:

```text
sync store 1 to-db
ingest metadata 1 --dry-run --limit 50 --report plan.json
ingest metadata 1 --apply plan.json
```

On a remote read-only Store, such as a wget or rclone HTTP Store, each file read
is a download through the Store's own rate limit. Start with `--limit`, and
narrow with `--extensions` if only some formats matter.

The code lives in [`ingest.embedded_metadata`](../../src/LiuXin_alpha/ingest/embedded_metadata):

| Module | Job |
| --- | --- |
| `models` | Frozen plan records, flags and decisions; JSON projection |
| `reading` | File selection, reading through the Store, normalisation |
| `planning` | Name resolution, the Work check, grouping formats, duplicates within the run |
| `runner` | One Store end to end, plan file, job result |
| `applying` | Loading a plan, re-checking it, writing each book, the apply report |

## Steps

1. **Select.** Take the Store's `files` rows without an Item, in key order. Count
   the rest by reason: `already_attached`, `extension_filtered`,
   `no_metadata_reader` or `over_limit`.
2. **Read.** Open each file through its Store and call the metadata readers.
   - A remote read-only Store downloads each file through its own rate limit.
   - A stream that can't seek is copied to a temporary file first.
   - Files over the size limit (512 MiB by default) are not read.
   - A failure is recorded against its file and never stops the run.
   - The readers' own error logs are muted for the run, since the plan records every failure.
   - Tags already in the catalogue are passed to the readers, for keyword splitting.
3. **Normalise.** Apply the agreed boundary rules (see below).
4. **Group.** Files in the same folder with the same title, authors and languages
   are formats of one book. Languages compare by the catalogue row they resolve
   to, so `en` and `eng` agree. The richest format comes first: EPUB, then AZW3,
   MOBI and so on. It supplies the book-level values.
5. **Match.** Use Catalog's read-only matchers: the Work matcher for the book,
   Agent matching for authors and the publisher, and exact lookups for tags,
   series and languages. Each distinct name is looked up once per run.
6. **Plan.** Make one planned book per group, with a decision for every name and
   flags for review.

The dry run writes nothing to the catalogue: its tests check that the change
log does not move. Opening a catalogue used to rewrite the id-0 sentinel rows in
`series` and `agents` every time. That added four change-log entries per open, so
each job worker's open looked like a change. Now they are written only when
missing or wrong.

## Rules (agreed 2026-11-25)

- **A book is always new.** If the Work matcher finds a match, an ambiguity or a
  conflict, the book is flagged `possible_duplicate_in_catalogue` but still
  planned as new. A reviewable duplicate is safer than a wrong merge.
- **Names reuse only a unique match.** Authors, the publisher, tags and series
  resolve to `reuse` (with the row id) on a unique match, or `new` when nothing
  matches. An ambiguous match gives `ambiguous` (with the alternatives), and the
  book is flagged `ambiguous_name`.
- **Languages are never created.** A code that isn't seeded is `unknown`, and
  the book is flagged `unknown_language`.
- **Dates at the boundary.** Calibre's undefined date (year 101) is dropped and
  flagged `undefined_date_dropped`. Dates are converted back to calendar dates
  through LiuXin's safe local zone, the same zone the readers used to parse them.
- **Missing values.** A placeholder or missing title becomes the file name, with
  underscores read as spaces; this is flagged `title_from_filename`. Placeholder
  authors are dropped; a book left with none is flagged `no_authors`.
- **Identifiers.** Record identifiers (`uri`, `uuid`, `calibre`) say which file
  or record this was, not which Work. They are not offered to the Work matcher.

Other flags:
- `possible_duplicate_in_run`: the same title, authors and languages appear in
  another folder of this run.
- `formats_disagree`: a book's formats carry different dates, publishers,
  identifiers or series.
- `match_error`: a lookup raised an exception.

## Known limits

- **Partial dates can't be recovered.** The readers fill a missing month and day
  from the day of reading: a `dc:date` of `1818` read on 27 November becomes
  1818-11-27. Such dates are flagged `date_may_be_partial`, but the precision is
  lost. The real fix is for the readers to keep the original date text and its
  precision. That is metadata work, which is on hold.
- **Work matching scales with the catalogue.** For every file, the Work matcher
  reads and scores every Work. With this change it skips the Agent read for
  titles that can't qualify. That cut a 200-file run against 400 Works from
  134 s to 18 s, but the cost still grows with the number of Works. A title
  index for candidate selection is the next step for archive-scale runs.
- **Only the primary format's tags and credits are used.** Other formats are
  read and kept in the plan, but their tags aren't merged into the book's.

## Applying a plan

The apply step writes what the plan describes, so what you reviewed is what gets
written. Before applying, you can edit the plan:
- change a book's `"action"` from `"create"` to `"skip"` to leave it out;
- set a name's `"decision"` to `"reuse"` with an `"entity_id"` to pick the row
  for an ambiguous match.

It runs in the Core process, which owns the cache, not in a job worker. Each book
is one `metadata.batch`. Any failure rolls back that whole book and the run
continues with the next. On success the cache reconciles once, from the change
log.

### Checks before each book

- **Files.** Each file must still be registered in the plan's Store, at the same
  storage key, with no Item. Otherwise the book is skipped, so applying a plan
  twice skips what the first run attached.
- **Reused names.** A name planned as `reuse` must still exist, or the book is
  skipped.
- **New or ambiguous names.** These are looked up again. A unique match is
  reused; this is usually a row an earlier book in the same run created, so a new
  tag is created once. Otherwise a new row is created, following the rule that a
  reviewable duplicate is safer than a wrong merge.
- **Languages.** A language that isn't seeded is reported, never created.

`--limit <n>` stops after `n` books are created. The remaining books are reported
as `not_attempted`, so a large plan can be applied in runs.

### What it writes

| From the plan | Written as |
| --- | --- |
| The book | One Work (title, sort title, creator sort) and one Expression (first language) |
| Each format | Its own Manifestation (ebook, format, publication year and date) and Item (digital, with source path and Store name), and its file attached through `file_item_id` |
| Authors | `aut` credits on the Work, in file order |
| Publisher | `pbl` credit on every Manifestation of the book |
| Identifiers | On each format's own Manifestation |
| Tags, series | Linked to the Work |
| Description | A comment on the Work, as the file holds it (often HTML) |

A date flagged `date_may_be_partial` stores only its year.

Links carry the origin `ingest.embedded_metadata`.

### Not stored yet

These are reported per book under `not_stored`:
- **Series positions.** The native series model links Works to series without
  volume numbers.
- **Second and later languages.** The Expression takes the first.
- **Covers.** There is no write path yet for storing a cover's bytes as an image
  in a managed Store. The cover stays inside its original file, which is never
  deleted.

### The report

The full report goes beside the plan, as `<plan>.applied-<UTC time>.json`, unless
`--report` names a file. For each book it records:
- the outcome and reason;
- the new Work, Manifestation and Item ids;
- the files attached;
- the names created;
- what wasn't stored.

The terminal shows a summary and the skipped or failed books.

Measured on 300 books against 300 existing Works: about 0.2 s per book, flat from
the first books to the last. Profiling shows about 60% of that is opening database
connections: every write read its normalized-identity declarations through a new
connection, about 13 per book. The cache catch-up is about 15%. #236 reads the
declarations on the open transaction instead, which brings the apply step to about
30 ms per book.

Return to [ingest](README.md).
