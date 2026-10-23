# Web registration and acquisition

Reviewed **2026-10-22**. `liuxin ingest web` walks a bounded HTTP(S) source
and either records remote candidates or acquires selected files into a managed
Store. Registration is the default. No root privileges are required.

Operator documentation added **2026-10-23**: start with the
[worked user guide](../../docs/web-ingest.md) or
[liuxin-ingest-web(1)](../../docs/man/man1/liuxin-ingest-web.1).
This runbook retains the implementation, ownership and validation details.

## Record first, download selectively

```bash
liuxin init /srv/liuxin
liuxin ingest web https://example.org/books/ --system-root /srv/liuxin
liuxin ingest web https://example.org/books/ --list --system-root /srv/liuxin

# Choose from the recorded candidates without re-crawling navigation pages.
liuxin ingest web https://example.org/books/ --download --registered-only \
  --extension epub --include '*/selected-series/*' --system-root /srv/liuxin

# Or discover and download together, using wget for discovery.
liuxin ingest web https://example.org/books/ --download --crawler wget \
  --max-files 100 --max-total-mib 512 --system-root /srv/liuxin
```

Registration fetches navigation pages and robots rules; wget can also probe
links with HEAD. It does not acquire book payloads into storage. A record means
“this link was observed”, not “this file is currently reachable” or “this is a
valid ebook”. A direct URL with an accepted suffix can be registered without
contacting it. Neither mode extracts bibliographic metadata or creates WEMI
entities automatically.

The native crawler is the default and needs no external executable. `--crawler
wget` uses the existing wget spider adapter for discovery; LiuXin's HTTP driver
and storage manager still own acquisition. HTML navigation is fetched as needed,
and wget scratch is temporary. This is not a site mirror: pages, scripts, images
and converted links are not retained as a browsable website.

Selection uses the URL path's filename suffix. Known book extensions, excluding
HTML navigation suffixes, are the default; repeated `--extension` values replace
that set (including HTML if deliberately selected). Repeated, shell-quoted
`--include` globs narrow accepted URLs. Query-only download endpoints, JavaScript
navigation, authentication flows and arbitrary site-specific metadata need
additional source adapters. Use a trailing slash for a directory-like site root.

## Storage and provenance

Download mode uses the configured default writable Store or `--destination
STORE_UUID`. It requires a Store that reports free capacity. SQLite/APSW jobs
default staging to `web-ingest/` beside the catalogue; `--state-directory PATH`
selects another parent. Other database backends require an explicit staging
directory for downloads. URLs and staging/destination paths are interpreted on
the Core host when using `--core-endpoint`.

Every object is downloaded serially into a private run directory, hashed, then
published through the storage manager with verification. URLs never become
staging filenames. Successful publication removes the staged copy. SHA-256
identity deduplicates identical bytes even when different URLs name them. A new
run that sees changed bytes at an old URL retains a new Asset without replacing
the old one.

Remote observations retain root URL, source URL, first/last seen time, and the
referring page when the crawler supplies it. Capture receipts add final URL,
retrieval time, HTTP validators when supplied, size, SHA-256, operation UUID,
Asset ID and Replica ID. Times are Unix epoch milliseconds. A stored
`last_capture` is historical evidence; it is not a fresh check that the remote
file or retained Replica still exists. Per-run receipts preserve earlier
captures when a later run updates a candidate's latest capture.

## Bounds and failures

| Control | Default | Meaning |
| --- | --- | --- |
| `--max-depth` | 5 | Navigation depth, within the selected root. |
| `--max-pages` | 1,000 | Native crawler page ceiling; wget instead has depth, observation, output and time bounds. |
| `--max-files` | 1,000 | Selected candidates per run. |
| `--max-file-mib` | 256 | Maximum retained bytes per file. |
| `--max-total-mib` | 2,048 | Received payload budget across a run and its resumes. |
| `--min-free-mib` | 1,024 | Headroom on staging and destination volumes. |
| `--request-timeout` | 30 seconds | Network request timeout. |
| `--max-seconds` | 3,600 seconds | Cooperative elapsed-time limit per attempt. |
| `--requests-per-hour` | Shared crawler preference; fallback 1,200 | Request pacing; zero disables spacing. |

Native HTML is capped at 2,000,000 bytes per page and both adapters cap observed
URLs at 100,000. Wget output is capped at 8 MiB of decoded characters. The byte
budget covers returned file-body bytes, including failed attempts, rather than
navigation traffic, HTTP overhead or bytes buffered by the operating system.
Unknown-length bodies may require one extra byte to detect overflow; it is
counted but never published. Reports expose this cumulative count as
`downloaded_bytes`, even when some transfers failed.

Wget does not provide the native crawler's per-page HTML ceiling. Prefer native
discovery when navigation-body bounds are needed; wget's time/output limits and
temporary scratch cleanup do not constitute a hard disk quota.

Capacity checks allow for staging plus a possible second publication copy. They
are observations, not reservations against other writers. Time limits are
cooperative: a blocking read, hash check or storage publication may finish after
the attempt deadline. Network request timeouts remain separate.

The native transport and all payload downloads permit only GET/HEAD and check
each redirect against the selected scheme, authority and root path. The root
host's robots document is the one path exception. Robots denials are respected;
unknown robots fetch failures stop those requests. Wget follows its own spider,
redirect and robots behavior; discovered candidates are still filtered by the
shared scope policy. This operator workflow is not an isolation boundary for
hostile URLs or DNS changes.

`--dry-run` discovers candidates without creating web-ingest tables, observations,
run records or staged files. It still makes navigation requests unless combined
with `--registered-only`.

## Inspect and resume

```bash
liuxin ingest web --status RUN_UUID --system-root /srv/liuxin
liuxin ingest web --resume RUN_UUID --system-root /srv/liuxin
```

The result of `ingest.web.start` is a managed job submission. Its completed
`execution.result` contains the web `run_id`; this is distinct from the job ID.
Use `jobs` commands for process state and `ingest web --status` for durable
selection/capture state. `--detach` returns the job receipt immediately. A CLI
wait timeout does not cancel the worker.

If a worker dies before returning its report, `ingest web SOURCE_URL --list`
also lists saved run UUIDs, status and leases. This makes the run discoverable
for inspection/resume even when no final job result exists.

Resume uses the saved controls and destination UUID; supplying replacements is
an error. Completed captures are skipped. Complete staged files are rehashed
before publication and reuse storage's operation identity, including after a
crash between publication and receipt. Interrupted partial transfers are removed
and restarted from byte zero, so different remote versions cannot be spliced.
An incomplete crawl is repeated while keeping previously selected candidates;
completed discovery needs no further navigation request on resume.

Active run leases reject overlapping resumes. After a worker crash, its claim
must expire before another worker takes over. Expired owners cannot renew or
write receipts. Missing/corrupt complete staging fails visibly rather than
publishing unverified bytes; inspect that run and start a fresh run if recovery
of its staging is impossible. Likewise, a consumed total byte budget remains
consumed on resume: use a new run to deliberately select different controls.

Partial failures retain completed work and observations. `partial`, `limited`,
`failed` and `interrupted` reports cause a waiting CLI invocation to exit nonzero.
Reaching the candidate ceiling still permits acquisition of the selected subset
within the remaining time, byte and space budgets; the report remains `limited`.
A `complete` report means the selected bounded workflow completed, not that an
entire website was inventoried.

## Ownership and validation

[`ingest.web`](../../src/LiuXin_alpha/ingest/web) owns the additive `remote_files`,
`web_ingest_runs` and `web_ingest_items` tables. Candidate UUIDs identify
root-plus-URL observations, not content. No placeholder Assets, Replicas or
zero-byte files are invented for undownloaded candidates. The existing storage
manager remains the only acquisition owner of Assets and Replicas. Queries do
not create these workflow tables.

The older `ingest remote-html` surface remains compatible and registers legacy
`files` rows/Store declarations. It does not acquire bytes and is not migrated by
this change. New remote-candidate/acquisition callers should use `ingest web`.
Unifying historical remote registrations, automatic metadata extraction and
retention/pruning of run histories remain follow-up work.

The [loopback integration tests](../../tests/ingest/test_web_acquisition.py) cover
native and real wget discovery, later selective downloads, deduplication, changed
versions, partial transfers, restart after publication, robots/redirect refusal,
capacity and byte ceilings, and an actual CLI/Core worker round trip. The
[repository tests](../../tests/ingest/test_web_repository.py) exercise SQLite and
APSW, expired claims and staging ownership. No external site is crawled in CI.

Return to [ingest](README.md) or the [operational CLI](../surfaces/operational-cli.md).
