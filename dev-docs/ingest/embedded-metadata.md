# Reading embedded metadata at ingest

Started **2026-11-27**. This covers the read half, a dry run. The write phase is
not built yet. This is the first caller for
[ING01](next_tasks.md#ing01-define-candidate-to-write-receipts).

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
ten books and ten failures. Without `--dry-run`, the command refuses.

To try it in the terminal UI, sync a Store, then plan a sample before planning everything:

```text
sync store 1 to-db
ingest metadata 1 --dry-run --limit 50
ingest metadata 1 --dry-run --background
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

## The write phase (next)

The plan records what the write phase needs:
- every format's facts;
- the decision for each name;
- the Work check;
- the cover's format and size.

The write phase will run in the process that owns the cache, not in a worker:
1. Re-check each decision against the catalogue as it is then, because it can
   change between the dry run and the write.
2. Call `create_wemi_stack` and `write_container`.
3. Attach the files with `file_item_id`.
4. Store the cover's original bytes.
5. Report what was created, skipped and failed.

Return to [ingest](README.md).
