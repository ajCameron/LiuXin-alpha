# Catalogue maintenance

Updated **2026-11-03**. Maintenance has two consistency modes. Prompt work must
succeed with the source write or that write fails. Lazy work is recorded durably
and processed later; a failed or stopped worker does not lose its pending work.

The current optional Catalog plugins maintain Work revision receipts and cached
counts of linked Expressions. They do not alter your book files. Installation
starts tracking subsequent changes; existing Works are not automatically backfilled.

## Install and inspect

Use a configured default library or pass `--database /path/catalog.sqlite` on each
command. Installation changes the catalogue schema and records required plugins:

```bash
liuxin maintenance install --database /path/catalog.sqlite --yes
liuxin maintenance status --database /path/catalog.sqlite
```

Without `--yes`, mutation commands print a preview. Status only inspects: it does
not install tables, start workers or open the jobs ledger. Pending entries include
age, attempts, next retry time, errors, implementation availability and job IDs.

Once installed, required prompt code must be present when writing that catalogue.
Open it through LiuXin's Library/runtime. An older application or external SQL
connection without that implementation will be refused by the capture guards.

## Run bounded work

```bash
liuxin maintenance drain --database /path/catalog.sqlite --max-batches 128 --yes
liuxin maintenance status --database /path/catalog.sqlite
```

A drain runs at most the selected number of maintenance jobs and returns. It
leaves queued storage audits for the normal jobs worker. Repeat it, or
schedule the command, to keep lazy work current. Each pass publishes at most
`--max-events` obligations (default 128, maximum 500). Retry delays can leave work
pending even when a drain has no currently runnable jobs.

The jobs ledger remains separate from the catalogue. SQLite normally uses the
existing adjacent `.jobs.sqlite` path. PostgreSQL requires a configured durable
jobs path in the Core host (`--job-ledger PATH` for CLI sessions). Long-lived hosts can start continued maintenance
polling through Core's `maintenance.run` action `start`.

## Backfill or rebuild Work projections

After installation, explicitly schedule projections for existing Works:

```bash
liuxin maintenance rebuild --database /path/catalog.sqlite --max-events 128 --max-batches 16 --yes
liuxin maintenance drain --database /path/catalog.sqlite --max-batches 128 --yes
liuxin maintenance status --database /path/catalog.sqlite
```

Rebuild scans at most `--max-events` identities per page (1–500) and
`--max-batches` pages per command (1–128, default 1). Repeat until the receipt
says `scheduling_complete: true`. It resumes the saved cursor after interruption,
including from another process. Use `--restart` only to begin a fresh scan.
Each page commits its checkpoint and prompt receipts together. Existing derived
values for that page are invalidated, including corrupt revisions; bibliographic
rows and files are unchanged. Keys belonging to deleted Works become tombstones.

Scheduling leaves lazy work for the ordinary worker or `maintenance drain`.
Status reports rebuild phases `source`, `projections`, `waiting`, and `complete`.
Completion means the scan and obligations through its checkpoint have finished;
later writes can still create new work. Paused or failed lazy work keeps the
rebuild waiting. Concurrent source changes use normal capture, including rows
inserted behind the scan cursor or beyond its initial upper bound.

## Pause, retry and reclaim journal space

```bash
liuxin maintenance pause catalog.work-summary --database /path/catalog.sqlite --yes
liuxin maintenance resume catalog.work-summary --database /path/catalog.sqlite --yes
liuxin maintenance retry catalog.work-summary --database /path/catalog.sqlite --yes
liuxin maintenance prune --database /path/catalog.sqlite --max-events 128 --yes
```

Pause retains new obligations. Resume allows future delivery; retry explicitly
resets failed/exhausted/cancelled obligations. Then run another drain. Pausing a
prompt plugin blocks writes that require it, because its effects cannot be skipped.
An already queued lazy delivery encountered while paused retires with a deferred
receipt. Its obligation and remaining attempts survive; resume publishes a fresh
delivery. Pause does not interrupt a handler already inside its transaction.
Use `maintenance retry` for these jobs. Generic Core `jobs retry` refuses them
before creating a successor, so catalogue and jobs state cannot diverge.

For a failing plugin, inspect the reported error before retrying. Malformed
selection/results and asynchronous hooks fail the invocation; they cannot silently
mark the batch complete. Correct the implementation first, use the explicit upgrade
flow if its declaration/version changed, then retry and drain lazy work. Failed
prompt writes must be resubmitted after their implementation is corrected.

A handler that never returns can still hold its process and database transaction.
Plugin execution is in-process: use external process supervision for a stuck
worker. Terminating it rolls back uncommitted database effects; jobs lease recovery
and the retained catalogue obligation allow a replacement worker to resume.
These guarantees do not roll back arbitrary plugin filesystem/network effects.

Prune removes a bounded number of fully acknowledged change records. It retains
pending work and current projections. SQLite can reuse the freed pages; this does
not promise that the database file immediately shrinks or compact the jobs ledger.

Retire completed maintenance job history separately:

```bash
liuxin maintenance prune-jobs --database /path/catalog.sqlite --older-than-days 7 --max-events 128
liuxin maintenance prune-jobs --database /path/catalog.sqlite --older-than-days 7 --max-events 128 --yes
```

The first command previews the requested policy without opening a ledger; the
second applies it. Each call inspects at most `--max-events` terminal jobs (1–500),
newest first, retaining at least seven days by default. `--older-than-days 0`
explicitly permits immediate retirement. If `has_more` is true, repeat with
`--before-run-id` set to `next_before_run_id`. Start without that cursor on the next
scheduled cleanup pass.

Only this library's current journal generation qualifies. Pending obligations,
active jobs, retained retry successors, scheduled definitions and unrelated job
kinds remain protected. Cleanup works before or after journal pruning. It removes
approved runs, their events and request keys, and unused definitions; old retired
job IDs are no longer inspectable or retryable. Source data and projections remain
intact. SQLite reuses freed ledger pages, but this does not run `VACUUM` or promise
immediate file shrinkage. Cleanup is explicit; schedule it for continuous imports.

For plugin authoring, transaction boundaries and recovery details, see
[maintenance modes](../dev-docs/databases/maintenance-modes.md).

## Upgrade an installed plugin

Deploy the new implementation, then inspect its supported upgrade:

```bash
liuxin maintenance upgrade catalog.work-summary --from-version 1 --database /path/catalog.sqlite
liuxin maintenance upgrade catalog.work-summary --from-version 1 --database /path/catalog.sqlite --yes
liuxin maintenance rebuild --restart --database /path/catalog.sqlite --yes
```

The implementation must declare a migration from the installed version; this
command does not invent one or install new code. Without `--yes` it reads the
actual declaration and reports blockers, pending counts and affected projections.
Optionally pass the preview's `expected_definition` as `--expected-definition` to
require that exact declaration when applying. Every apply also checks
`--from-version` again under the writer lock.

By default, drain pending work with the old implementation before applying an
upgrade. A plugin may explicitly support replaying its old captured events; those
obligations get the new version and a fresh jobs generation. Pending dependents
must still drain first, and changing prompt/lazy mode always requires draining.
Do not replace the old code needed for draining until this prerequisite is met;
use the preview during deployment planning or retain an old-code worker.

Upgrade invalidates the plugin's derived entries and those of its dependents.
Run the owner's rebuild afterward, repeating bounded pages and draining as above.
An older rebuild checkpoint reports `obsolete` and `needs_restart` after its
declaration changes. Declaration, subscriptions, invalidation, migration callback
and pending-work migration commit together; failure rolls everything back.
Old jobs cannot apply effects or consume the new version's retry budget. Old
processes missing required prompt code cannot write affected rows. Status retains
the latest upgrade receipt for each plugin.

## Inspect duplicate candidates

```bash
liuxin maintenance duplicates works work_title --database /path/catalog.sqlite
liuxin maintenance duplicates genres genre --database /path/catalog.sqlite --max-rows 25000
```

This is read-only and starts no maintenance worker. Results contain only repeated
values, grouped as `value`, `scope` and `row_ids`. Declared identity scopes keep,
for example, same-named Genres under different parents separate. Matching titles
are candidates for review; they do not prove that two Works should merge.

Default `--comparison identity` follows the catalogue's normalization declarations;
undeclared text uses Unicode NFKC, collapsed whitespace and casefold. `nocase`
uses NFC/casefold without trimming, and `exact` compares stored text. All modes
respect declared scopes and ignore NULL or blank values.

The default scan budget is 10,000 rows, configurable up to 100,000 with
`--max-rows`. An oversized selection fails with `scan_limit_exceeded`; it does
not return partial groups. Use `--where-file scope.json` to select rows with
physical-column equality filters, for example `{"genre_parent_id": null}`.
The budget limits rows, not byte size. Inspection supports physical tables and
text values. Automatic duplicate merging has been retired; removed capabilities
and future requirements are recorded in the
[retirement notes](../dev-docs/databases/legacy-maintenance-retirement-2026-11-02.md).

## Agent names and sort repair

Agent name maintenance uses Catalog directly and needs no maintenance installation
or worker. Commands preview by default; `--yes` applies the operation:

```bash
liuxin catalog agents rename 42 "Mary Shelley" --sort-mode derive --database /path/catalog.sqlite
liuxin catalog agents rename 42 "Mary Shelley" --sort-mode derive --database /path/catalog.sqlite --yes
liuxin catalog agents repair-sort 42 43 44 --database /path/catalog.sqlite
liuxin catalog agents repair-sort 42 43 44 --database /path/catalog.sqlite --yes
```

The default `--sort-mode preserve` keeps every nonblank existing sort value and
fills missing/blank ones. Historical rows have no flag proving whether a value
was manually chosen, so even text `none` is preserved. Choose `derive` explicitly
to regenerate existing values. Rename also accepts `--sort-name "Shelley, Mary W."`
as an explicit override; it cannot be combined with `derive`.

For people, `--person-sort-method comma` produces `Shelley, Mary`; `nocomma`
produces `Shelley Mary`; `copy` retains display order for names where inversion
is inappropriate. The existing author prefix/suffix rules apply. Organisations,
groups and pseudonyms always retain display order when deriving a sort value.

Renaming trims outer whitespace and changes canonical name and sort together.
It preserves the Agent ID, aliases, subtype name parts, and all credits. Repair
changes only sorts for 1–500 explicit IDs; repeated IDs are processed once.
Missing rows or blank canonical names refuse the complete batch. Write or required
maintenance failures roll back all changes. Applying rereads current values under
database locks, so a preview is a proposal rather than a stored execution plan.

## SQL writers and older repair commands

SQLite CTE writes (`WITH ... INSERT/UPDATE/DELETE`) participate in the same
transaction as prompt work. Explicit autocommit connections require a write
transaction. Custom cursor classes retain these completion hooks.
Changing SQLite's implicit-commit settings also finishes prompt work before
publishing changes. If a caller catches a failed explicit flush or dependent
read, that attempt's effects are rolled back and the source transaction can retry.

When maintenance is installed, SQLite `REPLACE`, `INSERT OR REPLACE` and
`UPDATE OR REPLACE` are refused: their implicit deletions can lose old relationship
endpoints. Use `ON CONFLICT ... DO UPDATE`, or explicit deletion and insertion in
one transaction. Plugins have the same restriction.
PostgreSQL `TRUNCATE` is refused for subscribed tables, including cascading and
external SQL calls. Use `DELETE` to retain the row identities needed by maintenance.

`maintenance merge` remains a physical interlink repair, not a complete metadata
merge. It transfers registered interlinks and deletes the absorbed row in one
transaction. Only identical duplicate link payloads are removed; conflicting
roles, priorities or other values stop the operation without partial changes.
Scalar metadata, intralinks and tree relationships are not combined by this command.

Selective `maintenance clean` is unavailable in the built-in service. It reports
`capability_unavailable` and leaves the selected rows unchanged.

## Catalog repair and inherited metadata

Use `liuxin catalog inherited-tags item 42 --database catalog.sqlite` to inspect
direct and inherited tags, with the source Work/Expression/Item and every WEMI
relationship path retained. A shared Manifestation preserves separate novel and
introduction contributions. This read does not start a worker or copy metadata.

`liuxin catalog repair merge request.json` and `catalog repair cleanup request.json`
preview explicit repair requests. Inspect the returned rows, conflicts and `state`;
apply with `--yes --expected-state STATE`. Both commands accept ordinary connection
options such as `--database`. Changed references or unresolved conflicts block the
whole operation. Successful changes and their durable audit receipt commit together.
Read a receipt with `liuxin catalog repair receipt RECEIPT_ID`.

Merge identifies the retained/source records explicitly and requires decisions for
conflicting populated fields or relationship payloads. Cleanup needs selected IDs,
discard/preserve decisions and reasons; an unlinked record is not automatically
garbage. Neither operation changes physical files. See the
[request examples and supported policies](../dev-docs/catalog/maintenance-repair-policies.md).

The older `catalog metadata merge LEVEL SOURCE TARGET` command now previews by
default and uses the same owner. Its apply requires `--yes --expected-state`; use
`--resolutions-file` and `--relationship-resolutions-file` for decisions. Callers
that previously relied on silent target precedence must now choose it explicitly.

The separate `maintenance merge` command retains its older physical interlink
contract: it does not consolidate scalar metadata, intralinks or tree ownership.
Use `catalog repair merge` for the semantic checks and audit described here.
