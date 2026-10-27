# Register and download files from a website

Updated **2026-10-23**. `liuxin ingest web` can build a list of remote files
without retaining their contents, or download selected files into LiuXin's
managed storage. Registration is the default. Web ingest needs no `sudo`.

## Prepare a library

Use an existing system root, or initialize a directory your account can write:

```bash
liuxin init "$HOME/LiuXin"
```

The examples below use that system root explicitly. If you already use a saved
connection or profile, use your usual selector instead. Downloads require a
configured writable Store that reports available capacity; `init` creates a
managed primary Store by default.

## Record a site's files without mirroring them

Replace the example URL with your source. Use a trailing slash for a directory
root; crawling stays within that URL's scheme, authority and path subtree.

```bash
liuxin ingest web https://example.org/books/ \
  --register-only --system-root "$HOME/LiuXin"

liuxin ingest web https://example.org/books/ \
  --list --system-root "$HOME/LiuXin"
```

The first command fetches navigation pages and records matching links. It does
not acquire book payloads. A recorded link is an observation, not a guarantee
that the target is reachable or contains a valid ebook. A direct book URL such
as `https://example.org/books/novel.epub` can also be registered without fetching
it.

The second command reads the catalogue without contacting the site. Its JSON
contains `files` with source URLs, observation times and any `last_capture`, plus
`runs` with saved run IDs and statuses. A last capture describes a past download;
it does not recheck current remote or local availability.

To preview selection without recording observations, use `--dry-run`. It still
visits navigation pages unless you also specify `--registered-only`.

## Acquire selected copies later

```bash
liuxin ingest web https://example.org/books/ --download --registered-only \
  --extension epub --include '*/selected-series/*' \
  --max-files 100 --max-total-mib 512 --system-root "$HOME/LiuXin"
```

`--registered-only` selects from the same root URL's recorded candidates, so it
does not need to crawl the site's index again. Robots checks and selected file
requests still occur. Omit `--registered-only` to discover and download in one
run.

Known book suffixes, excluding HTML navigation, are selected by default. Repeat
`--extension` to replace that set, for example `--extension epub --extension pdf`.
Repeat `--include` for alternative URL globs; quote them so the shell leaves them
intact. The extension filter and glob filter must both match. Use `--extension
html` explicitly if HTML files themselves are wanted.

Downloads go to the configured default Store, or to the Store UUID selected by
`--destination STORE_UUID`. Complete files are hashed, published and verified
before being reported as acquired. Identical bytes deduplicate even across
different URLs. Changed bytes at a previously captured URL create a new Asset
while preserving the old version.

Receipts record the source and final URLs, referring page when available,
retrieval time, HTTP validators, SHA-256, size and resulting storage identities.
This workflow acquires bytes; bibliographic matching and creation of Works,
Expressions or Manifestations remain separate tasks.

## Native discovery or wget

The default native crawler needs no extra executable. To use an installed
`wget` for discovery:

```bash
liuxin ingest web https://example.org/books/ --crawler wget \
  --register-only --system-root "$HOME/LiuXin"
```

Wget performs a spider crawl, including HEAD probes and navigation reads;
LiuXin still handles any requested book downloads and storage publication.
Native discovery has a per-page HTML byte ceiling. Wget has traversal, output
and time limits, but no equivalent hard quota on navigation scratch. Prefer
native when that bound matters.

Both routes honor robots rules through their respective transports. JavaScript
navigation, authenticated sessions and query-only download endpoints need
additional source support. This command does not create a browsable website
mirror.

## Budgets and free space

| Option | Default |
| --- | --- |
| `--max-depth` | 5 navigation levels |
| `--max-pages` | 1,000 pages, native crawler only |
| `--max-files` | 1,000 selected files |
| `--max-file-mib` | 256 MiB per file |
| `--max-total-mib` | 2,048 MiB of received file payload across the run and its resumes |
| `--min-free-mib` | 1,024 MiB headroom on staging and destination volumes |
| `--request-timeout` | 30 seconds per network request |
| `--max-seconds` | 3,600 seconds per attempt |
| `--requests-per-hour` | Shared crawler preference, falling back to 1,200 |

MiB means 1,048,576 bytes. Failed transfers consume the total byte budget too;
navigation traffic and protocol overhead are outside that counter. An
unknown-length body can require one extra byte to detect overflow; that byte is
counted but not published. The report calls this cumulative counter
`downloaded_bytes`, even when some transfers failed.

Space checks allow for staging and a possible second publication copy. They
cannot reserve capacity against unrelated writers. SQLite/APSW jobs stage under
`web-ingest/` beside the catalogue; `--state-directory PATH` chooses another
parent. Other database backends need an explicit staging path for downloads.
Successful publication removes the staged file. The elapsed-time limit is
cooperative, so a blocking read or publication can finish after the deadline.

Reaching `--max-files` still allows the selected subset to download within the
other budgets, but the run reports `limited` rather than a complete inventory.

## Inspect a run and resume after interruption

Waited-for jobs return their web run UUID in `execution.result.run_id`. This is
different from the managed job ID. If the worker died before returning a report,
find its run UUID through the source listing:

```bash
liuxin ingest web https://example.org/books/ --list --system-root "$HOME/LiuXin"
liuxin ingest web --status RUN_UUID --system-root "$HOME/LiuXin"
liuxin ingest web --resume RUN_UUID --system-root "$HOME/LiuXin"
```

Replace `RUN_UUID` with the saved identifier. `--status` is read-only. Resume
reuses the saved URL, filters, budgets and destination; omit the URL and all
crawl/download controls. Connection, output and managed-job controls remain
available. Completed captures are skipped, complete staged files are rehashed,
and partial transfers restart from byte zero. Previously received bytes remain
charged to the run.

An active lease prevents overlapping resumes. After a crashed worker, wait for
the reported lease to expire. If a run has exhausted its byte budget, start a
new run to deliberately change that budget. Missing or corrupt complete staging
is reported rather than trusted; a fresh run is appropriate if its staging
cannot be recovered.

Commands wait by default. `--detach` returns the job receipt immediately; use
`liuxin jobs watch JOB_ID` and the other job commands for worker state. A
`--wait-timeout` ends only the CLI's wait and does not cancel the job.

## Output and exit status

Results are JSON. `--compact` removes indentation; `--output PATH` writes on the
CLI host and requires `--replace-output` to replace an existing file.

- **0:** successful inspection, completed workflow, or accepted detached job.
- **1:** a failed job, a CLI wait timeout, or a web report marked `partial`,
  `limited`, `failed` or `interrupted`.
- **2:** invalid arguments or another dispatch/runtime error handled by the CLI.

A `complete` report describes the selected bounded operation, not the whole
website. For remote Core connections, source requests, staging and Store access
occur on the Core host. Web execution needs a trusted deployment role;
read-only catalogue access does not authorize ingestion.

See [liuxin-ingest-web(1)](man/man1/liuxin-ingest-web.1) for the complete option
reference, the [developer runbook](../dev-docs/ingest/web_ingest_operations.md) for
ownership and recovery details, or return to the [user documentation](README.md).
