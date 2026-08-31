# LiuXin operational CLI

The installed `liuxin` command exposes named, stable Core operations as
task-oriented command families. It deliberately does not expose a generic
operation dispatcher.

## Connection and output contract

Every Core-backed leaf accepts one explicit transport or one system selector:

```text
--database PATH
--core-endpoint http://127.0.0.1:8765
--system-root /srv/liuxin
--profile production
```

`--database` composes and owns Core in the CLI process. `--core-endpoint`
speaks to an already-running Core daemon. `--system-root` reads
`liuxin-system.json`; `--profile` accepts a manifest path/directory or a name
from `$XDG_CONFIG_HOME/liuxin/profiles/`. `LIUXIN_SYSTEM_ROOT` and
`LIUXIN_PROFILE` provide the same selection for unattended services. Global
selectors work before or after the command, for example:

```bash
liuxin --system-root /srv/liuxin storage status
liuxin catalog search --profile production "Ursula Le Guin"
```

An operator can persist a credential-free pointer to one manifest instead of
repeating a selector:

```bash
liuxin connect /srv/liuxin
liuxin connect status             # inspect persisted/effective selection
liuxin doctor
liuxin storage status
liuxin disconnect
```

The pointer lives at `$XDG_CONFIG_HOME/liuxin/active-connection.json` (or
`~/.config/liuxin/`) with mode 0600 and contains only an absolute manifest
path. `connect` validates the manifest and opens Core by default; use
`--no-health-check` only when deliberately selecting an offline system.
`connect status` reports both the persisted pointer and the effective selector
after environment precedence. Bare `connect` remains a compatibility alias.
`disconnect` removes only this pointer and never changes the manifest,
database, or Stores. Selection precedence is explicit command arguments,
then `LIUXIN_SYSTEM_ROOT`/`LIUXIN_PROFILE`, then the persisted connection.
Consequently an environment selector remains authoritative until it is unset.

Explicit `--database`/`--core-endpoint` values never silently combine with a
profile. `config path|show|validate` explains selection, redacts URL secrets,
checks manifest permissions and validates local paths. JSON output is deterministic,
ASCII-safe, valid for surrogateescaped legacy paths, and atomically published
without replacement unless `--replace-output` is explicit.

Named selectors are managed without editing JSON by hand:

```bash
liuxin config profiles add production /srv/liuxin
liuxin config profiles list
liuxin --profile production status
liuxin config profiles remove production --yes
```

Each named selector is a mode-0600, credential-free absolute manifest pointer.
Removing it does not modify the selected system. `liuxin status` is the concise
readiness projection; `doctor` retains the complete check report. Generate
completion with `liuxin completion bash|zsh|fish` (or write it atomically with
`--output`).

There are two intentionally different path contracts:

- `metadata file ...`, `storage files put/get`, `acquire get`, and the concise
  `ingest SOURCE --system-root ROOT` form read or write paths on the CLI host.
- `ingest disk ...`, `convert`, and `backup` submit managed workflows whose
  source, output, and staging paths are resolved on the Core host. A remote CLI
  does not upload those trees implicitly.

## Command families

- `init [SYSTEM_ROOT]` creates or validates a path-backed catalogue and an
  optional managed primary Store. A system-root invocation also creates
  `ingest-materialized/`, `logs/ingest/`, and a mode-0600
  `liuxin-system.json` manifest. Re-running it is non-destructive and
  idempotent. `init` with no location in an interactive terminal (or explicit
  `init --wizard`) selects SQLite, APSW, or PostgreSQL, displays a redacted
  confirmation plan, and runs the appropriate health/readiness checks.
  The PostgreSQL wizard also asks for a system root and writes the reusable
  profile only after its readiness checks pass.
  PostgreSQL server/database/role provisioning remains explicit because those
  administrative concerns do not belong in an application schema initializer.
  `postgres init --system-root ROOT` can write the equivalent secret-free
  PostgreSQL manifest after successful schema creation; passwords remain in
  `.pgpass`, PGSERVICE, the process environment, or an external secret manager.
- `config path|show|validate|profiles` owns deployment selection rather than exposing
  the database preference store. `connect` and `disconnect` manage the
  credential-free persisted selector. `doctor [--full]` aggregates manifest,
  database, Core, Store, job, capability and optional-tool readiness with a
  stable exit status. `diagnostics collect` produces a redacted JSON support
  bundle and bounded failed-job log tails.
- `ingest SOURCE --system-root ROOT` is the concise local-host entry point for
  a bounded recursive mixed tree. It expands to the mature `storage ingest`
  operator, including archive-bomb budgets, logging, locking, reports, and
  graceful cancellation. `ingest disk ...` remains the distinct managed-job
  form for a source path visible to Core.
- `core health|capabilities|api` inspects one Core. `core serve` owns a local
  database and publishes the Core RPC transport.
- `jobs list|show|wait|watch|result|logs|cancel|retry` owns the complete
  managed-job operator lifecycle. `jobs retry JOB_ID` creates a new run linked
  to the unchanged terminal job; successful jobs require
  `--allow-succeeded`. Workflow commands wait by default; `--detach` returns
  the submission receipt. A CLI wait timeout never silently cancels the Core
  job.
- `catalog` provides global search, browse projections, repository entities,
  hydrated bundles/graphs, WEMI links, identifiers, agents, and field
  definitions. `catalog custom-fields list|show|create|update|delete` exposes
  the stable semantic custom-field lifecycle; deletion is a preview unless
  `--yes` is explicit.
- `acquire formats|cover|resolve|get` discovers and retrieves catalogue
  resources without assuming that all resources are local filesystem paths.
- `metadata` owns hydrated metadata, dump/export, safe embedded-file
  inspection/rewrite, and online enrichment. Its detailed contract is in
  `metadata-cli.md`.
- `storage` retains the bounded recursive local `storage ingest` operator and
  adds Store/default/refresh administration, file and Replica transfer,
  locations, ingest-source registration, digital-asset policy inspection, and
  stable storage-graph resources. `storage files put --metadata-file` carries
  rich Store hints and derivation context alongside the bytes. Common setup no
  longer requires JSON. `storage add` with no arguments is the rclone-inspired
  guided front door: it asks Core for its selectable backend providers, shows
  their capabilities and limitations, collects the Store root/role/topology,
  displays a no-write confirmation plan, then saves, reloads, and optionally
  probes the Store. The equivalent unattended form is `storage add NAME KIND
  ROOT [OPTION=VALUE ...]`; `storage backends` (alias `providers`) exposes the
  same machine-readable provider catalogue. Roots and staging paths are always
  interpreted on the Core host. Durable options reject credential-like fields;
  credentials belong in backend-native profiles/environment injection or an
  external secret provider. A Store row is retained when reload or probing
  reports an unavailable endpoint, so transient remote failures do not discard
  operator intent; durable-only rows remain inspectable with `storage store
  show` and addressable by `storage store update` and `storage store delete
  --delete-from-database`. `storage store add
  KIND ROOT --name NAME` remains the compatibility spelling, while `store save`
  is the complete-object escape hatch. Use `storage sources add KIND LOCATION`
  for ingest sources;
  `sources register` retains its complete-object form. `storage replica verify`,
  `storage asset verify`, and bounded `storage audit` persist integrity
  observations. `storage status` gives an operator-oriented overview of every
  persisted Store row (including deliberately offline rows): name, kind, root,
  role, folder support, registration/default state, current availability and
  writability, capacity, catalogue Asset/Replica counts and bytes, Replica
  state/mode counts, and attributable warnings. Its summary reports overall
  Store, capacity, Replica, configuration-drift, and issue counts; the complete
  manager health value remains in `status` for machine consumers.
  `replica_bytes` sums authoritative Asset size once per live Replica; it is
  not filesystem allocated-block usage. Use
  `--refresh` when a live backend probe is worth its cost. `storage reconcile
  plan|apply` exposes the manager's safe recovery boundary. Reconcile apply
  reloads Stores and verifies Replicas only; placement, deletion and ingest
  retry remain separate explicit operations. `storage repair plan|apply`
  performs bounded verification and policy placement without deletion.
  `storage store evacuate STORE` previews by default, then copies and verifies
  replacements before retiring source claims when `--yes` is supplied.
  `storage recovery list|recover-pending|retry-ingest` exposes the durable
  ingest journal without pretending lost stream inputs can be replayed.
  Common configuration changes use typed `storage store update` options;
  endpoint changes are refused while live Replica claims remain.
- `ingest disk|remote-html` submits semantic workflows for paths or sources
  visible to Core. This is distinct from the heavily instrumented, CLI-local
  `storage ingest` mixed-tree run. `ingest runs list|show|issues|resume`
  indexes the latter's durable reports and JSONL events. Resume reconstructs
  the original safety budget and paths from `cli_started`, reuses the run UUID,
  and refuses discovery/preflight attempts or a successful run without
  deliberate confirmation.
- `convert formats|options|run` inspects conversion support and submits a
  managed conversion using Core-host paths.
- `backup plan|workflows|show|save|run|squashfs|verify|restore` plans and runs
  durable backup declarations and provides the natural recovery aliases
  `backup verify` and `backup restore` for database backup files. The existing
  `squashfs` family remains the detailed sealed archive publication surface.
  Pack planning names a configured source Store and destination Store; it does
  not smuggle an unregistered output directory past the StorageManager.
- `database info|summary|telemetry|backup|verify-backup|restore|migrations|vacuum`
  exposes bounded driver-level inspection and upkeep. An explicit backup path
  is a Core-host path; `verify-backup` and `restore` operate on the CLI host.
  SQLite/APSW restore is offline-only, verifies the source, refuses live WAL
  companions, creates a hash-checked safety backup, and atomically publishes
  the replacement. PostgreSQL restore remains the responsibility of
  `pg_restore`/server backup tooling. Migration status/plan are read-only;
  apply previews unless `--yes` is supplied and currently covers additive
  storage metadata plus normalized-identity migration. Vacuum requires
  `--yes`.
- `maintenance status|duplicates|run|clean|merge` exposes maintenance without
  making repair an accidental side effect. Mutating commands emit a preview
  unless `--yes` is supplied.
- `serve web|web-write|api|opds|calibre` runs packaged application surfaces.
- `plugins inspect` consolidates stable program, Store/source, metadata,
  conversion, and ingest capability advertisements. A failed optional probe is
  reported in its section rather than hiding successful probes.

The internal schema, row, cache, and preference operations remain Core
contracts for specialised application surfaces. They are not promoted as a
generic database-editing shell: semantic catalogue and metadata commands are
the safer operational vocabulary.

## HTTP safety

The current Core and packaged HTTP surfaces do not implement authentication or
TLS. They bind to `127.0.0.1` by default and reject non-loopback binds unless
`--allow-unsafe-remote-bind` explicitly acknowledges the risk. Prefer an SSH
tunnel or a protected, authenticated reverse proxy. The Core daemon also
enforces a configurable request-body ceiling (`--max-request-mib`).

Example tunnel:

```bash
ssh -L 8765:127.0.0.1:8765 liuxin-host
liuxin core health --core-endpoint http://127.0.0.1:8765
```

## Remote workflow example

```bash
liuxin ingest disk \
  --core-endpoint http://127.0.0.1:8765 \
  /srv/import/drives/disk-01 \
  --label disk-01 --detach

liuxin jobs watch \
  --core-endpoint http://127.0.0.1:8765 \
  JOB_ID --timeout 3600
```

The `/srv/import/...` path is on the Core host. `jobs logs JOB_ID --follow`
provides bounded incremental worker output; `jobs cancel JOB_ID` requests a
cooperative cancellation.

## First local ingest

```bash
liuxin init /srv/liuxin
liuxin ingest /media/archive-drives/disk-01 \
  --system-root /srv/liuxin
```

The first command emits machine-readable setup results and a suggested next
command. The second reads the system manifest, refuses a missing catalogue,
uses the configured materialization/log locations, and produces the same
durable report and event stream as the detailed `storage ingest` form.

## Guided initialization

```bash
liuxin init
# or, explicitly:
liuxin init --wizard
```

The wizard requires a terminal and makes no change until its displayed plan is
confirmed. SQLite and APSW create the same self-contained system-root layout as
the scripted form. PostgreSQL accepts either a URL or a `PGSERVICE` profile,
redacts credentials from the plan, creates/upgrades the LiuXin schema, and runs
the full core/storage/helper readiness checker. Its system manifest removes an
embedded URL password before it is written. Password handling remains in
`.pgpass`, `PGSERVICE`, `LIUXIN_POSTGRES_PASSWORD`, or the existing guarded
password prompt; the wizard does not ask to store a separate password.
Before collecting PostgreSQL connection details it detects a missing optional
Python driver and prints the appropriate `[postgres]` installation command.
Connection failures distinguish a missing database from an unavailable local
server/socket or unreachable remote host and point to `postgres setup-sql` or
`pg_isready` as appropriate.

The PostgreSQL database and login role must already exist. Use `liuxin postgres
setup-sql` for reviewable server/database provisioning SQL when they do not,
or keep using `liuxin postgres init` and `liuxin postgres check` directly in
automation.
