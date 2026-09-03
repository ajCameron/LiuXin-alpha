# Metadata CLI

The installed `liuxin metadata` command is the operational metadata surface.
Every leaf command connects to exactly one LiuXin Core, either by opening a
local catalogue with `--database` or by calling a daemon with
`--core-endpoint`. It uses named Core operations in both modes; there is no
second, CLI-only metadata implementation.

Run `liuxin metadata --help` and the help for the relevant leaf command before
an unattended run.

## Catalogue reads and JSON dumps

Read one fully hydrated WEMI record:

```bash
liuxin metadata show --database /srv/liuxin/catalogue.sqlite 42
```

`get` is an alias for `show`. Output is deterministic, sorted-key JSON. It
uses ASCII escapes for non-ASCII and surrogateescaped path text, so even a
record containing a damaged POSIX filename remains valid interoperable JSON.
Use `--no-related` or `--no-legacy` to omit the corresponding projections.

Dump explicitly selected Items:

```bash
liuxin metadata dump-json \
  --database /srv/liuxin/catalogue.sqlite \
  --item-id 42 --item-id 81 \
  --output /srv/liuxin/exports/selected-metadata.json
```

`--item-ids-file` accepts either a UTF-8 JSON array or newline-separated ids;
blank lines and `#` comments are accepted in the latter form. `--all` pages
through the Items table in stable item-id order. It cannot be combined with an
explicit selection. Control JSON and id files have a 16 MiB input ceiling.

The default dump is a versioned document:

```json
{
  "format": "liuxin.metadata.dump",
  "item_count": 1,
  "items": [],
  "version": 1
}
```

`--json-lines` emits one raw hydrated record per line for streaming consumers.
Both forms are disk-spooled rather than accumulated in memory. A file output
is staged beside its destination, flushed, and atomically published. Existing
outputs are refused unless `--replace-output` is explicit. Stdout is published
only after the complete dump succeeds, so a failed record does not leave a
partial JSON stream.

The all-Item enumeration and individual hydration requests are separate Core
queries. A concurrently mutating catalogue is therefore not a transactional
point-in-time snapshot; stop catalogue writers when snapshot semantics matter.

## Catalogue writes and OPF

The simple write surface covers the fields currently supported by the WEMI
metadata writer: tags, labels, genre, subjects, series, and identifiers.

```bash
liuxin metadata set --database /srv/liuxin/catalogue.sqlite 42 \
  --tag reviewed \
  --subject preservation \
  --identifier isbn=9780000000000
```

Values append by default. `--replace` makes every selected field
authoritative. Clearing is deliberately explicit:

```bash
liuxin metadata set --database /srv/liuxin/catalogue.sqlite 42 \
  --replace --clear tags
```

`--values-file` and `--values-json` accept a writable-field object, the output
of `metadata show`, a one-Item versioned dump, or a `metadata file inspect`
report. Only fields supported by the WEMI writer are imported from the latter.
Convenience flags override the same field from JSON. Use repeatable `--field`
to select a subset.
`--target-level` chooses work, expression, manifestation, or item; writes
normally mark that WEMI record dirty for downstream work, with
`--no-mark-dirty` reserved for controlled maintenance.

Export OPF bytes without a JSON/base64 wrapper:

```bash
liuxin metadata export-opf \
  --database /srv/liuxin/catalogue.sqlite 42 \
  --output /srv/liuxin/exports/42.opf
```

OPF and JSON outputs use the same atomic no-clobber policy.

## Embedded file metadata

List the formats provided by enabled reader/writer plugins:

```bash
liuxin metadata file formats --database /srv/liuxin/catalogue.sqlite
```

Inspect a file on the CLI host:

```bash
liuxin metadata file inspect \
  --core-endpoint http://127.0.0.1:8765 \
  /media/incoming/book.epub
```

The path is never sent to Core. The CLI reads bounded bytes and sends base64,
so the behavior is identical for a local Core and a remote daemon and cannot
accidentally open a daemon-local same-named path. The default transfer ceiling
is 512 MiB and can be changed with `--max-transfer-mib`.

Writing creates a new artifact by default. Core rewrites staged bytes, the CLI
re-reads metadata from the result as verification, and only then publishes the
complete output atomically:

```bash
liuxin metadata file write \
  --database /srv/liuxin/catalogue.sqlite \
  /media/incoming/book.epub \
  --output /srv/liuxin/derived/book-with-metadata.epub \
  --item-id 42 \
  --report-output /srv/liuxin/reports/book-with-metadata.json
```

Use `--metadata-file` or `--metadata-json` instead of `--item-id` to embed an
explicit Calibre-like metadata object. A saved `metadata file inspect` report
is accepted directly and its `metadata` member is unwrapped automatically. An
existing artifact or report is never replaced without its corresponding
`--replace-*` option.

`--in-place` exists only for explicitly unmanaged files. It rejects symlinks,
stages and verifies the replacement first, preserves file mode, and creates
`INPUT.bak` before the atomic replacement. Use `--backup`, `--backup-suffix`,
or `--replace-backup` to control that recovery artifact. `--no-backup` is an
explicit opt-out. If the source identity, size, or modification time changes
while Core is staging the result, publication is refused. Do not use this mode
on a catalogued Replica: a managed Store
should commit a new artifact and record its derivation instead of mutating
bytes behind an existing identity.

## Online sources and jobs

Inspect enabled source capabilities:

```bash
liuxin metadata online sources --database /srv/liuxin/catalogue.sqlite
```

Identification and cover discovery are managed Core jobs. The CLI waits by
default and emits the completed result; `--detach` returns the job id for a
separate job monitor.

```bash
liuxin metadata online identify \
  --database /srv/liuxin/catalogue.sqlite \
  --title "Example Book" --author "Example Author" \
  --identifier isbn=9780000000000

liuxin metadata online cover \
  --database /srv/liuxin/catalogue.sqlite \
  --identifier isbn=9780000000000 \
  --cover-output /srv/liuxin/exports/cover.jpg
```

`--source-timeout` bounds source work, `--job-timeout` bounds the managed job,
and `--wait-timeout` bounds only how long this CLI waits. A CLI wait timeout
does not cancel the Core job. Cover bytes can be separated from the JSON report
with `--cover-output`; both destinations retain atomic no-clobber behavior.

## Remote use

Replace `--database PATH` with `--core-endpoint URL` on any leaf command. The
result schema and wire behavior are the same. `--core-timeout` bounds each HTTP
request, while job-specific timeouts retain the meanings above. Local file
commands upload the entire bounded artifact in one request and receive the
entire rewritten artifact in one response; they are suitable for ebook-sized
files, not multi-gigabyte bulk transfer.
