# Transport permissions checkpoint, 2026-10-20

A06 is implemented on `core-transport-permissions`, based on A05's `47c5c0d3`.
Final checks passed; publication is next. The full A01–A16 programme
remains open; continue to A07's WEMI default-selection contract afterward.
No PR has been merged. Preserve the six unrelated staged storage files and dirty
data submodule; `/tmp/liuxin-docs-pr-preserved-storage.json` records their hashes
and index entries. They are excluded from the implementation and test candidate.

## Contract

The [transport guide](../dev-docs/core/transport_permissions.md) owns the supported
roles and deployment boundary. Registrations declare remote_capabilities;
unannotated/replaced custom handlers fail closed. The runtime selects handler
and authority together before effects/events. Metadata construction is atomic
with registration, so failure cannot pair a new handler with old permissions.
All built-in families explicitly declare authority, including browse, database
semantics and storage graph APIs.

HTTP has a fixed server-owned role: read-only by default, read-write for semantic
catalogue changes, and explicitly trusted for administration, execution and
compatibility invocation. Writable/non-loopback access needs a bearer token.
Requests cannot claim permissions. Host, Origin/cross-site fetch and JSON media
checks precede dispatch. Read-only denies all commands even if misdeclared; query
spelling does not authorize invocation, refresh, dynamic targets or global events.

Existing clients/proxy children and core_client carry optional bearer credentials.
Authenticated clients refuse redirects and other origins; invalid tokens do not
appear in validation messages. CLI --access/--auth-token-file/--allowed-host and
remote --core-token-file use that same role. Readiness reports grants/auth status
without credentials. TLS termination/tunnels remain the deployment boundary;
this is not a multi-user account or row-authorization system.

CoreReadClientAPI is narrow. ReadOnlyCoreClient restricts web/API/OPDS composition
and injected models without changing the caller's model. Local reads use runtime's
check; remote checks still require an independently restricted server role.
Writable web composition explicitly keeps its writable client. The unused raw
storage-refresh helper is removed. Direct in-process Core calls remain trusted.

## Validation

Final selection: **67 passed in 52.36s**, covering raw HTTP denial before effects,
authenticated read/write parity, registration replacement/failure, credentials,
redirect refusal, CLI token-file serving/client use, restricted local/remote
readers, surface composition, preserved error receipts and the A05 last-copy rule
through authenticated HTTP. Earlier final selection passed 114 tests with two
failures: a redirect double lacked POST and explicit target introspection needed
its intended default. Both were corrected and passed the final selection.

Earlier broader selection: 112 passed / three failures; follow-up: 103 passed /
one failure. Failures were explicit exposure declarations in synthetic handlers
and initially omitted database-semantics registration. All are corrected. These
selections overlap; do not add them into an invented unique count.

Complete quality gates passed: 188 formatted files, annotations, import cycles,
layer direction, 25 import budgets, lint/complexity, basedpyright and mypy.
Both type checkers accept valid examples and reject **38** negative contracts,
including the narrow reader's missing command method. Mypy checks 198 files,
including the two base interfaces added to its scope without broad Any waivers.
Final source is checked. Documentation, links, workflow and proxy checks passed
**30 tests in 10.82s**. The surface documentation/factory suite then passed
**24 tests in 1.38s**, including the updated bearer-token forwarding assertion.
These selections overlap with earlier runs and are not a unique aggregate count.
Subsequent edits only reconcile documentation and handoff state.

Logs are copied into ignored `working-memory/test-results/transport-permissions-20261020`.
Original logs: /tmp/liuxin-a06-{clients,followup,final,verified}.log and
/tmp/liuxin-a06-quality-verified.log, /tmp/liuxin-a06-docs.log and
/tmp/liuxin-a06-adapter.log. Telemetry: /tmp/liuxin-a06-telemetry.
All pytest is serial with immediate scratch cleanup; quality caches are outside
checkouts. Use remote CI for the full suite, not another full VM run.

## Parent status and publication

A04 PR #132 is green on e9efedc4: 7,403 passed / 259 skipped / 17 xfailed.
A05 PR #133 is now at 47c5c0d3. Its first full CI had 7,433 passes and five failures:
one Library expectation and four SquashFS cases sharing one serialization defect.
The isolated parent corrections passed all 10 Library facade tests and 18
SquashFS publication/rollback/CLI tests. They are pushed; inspect the new full CI.
A05's PostgreSQL test cluster is stopped. The six protected files remain unchanged.

Publish A06 as a focused PR based on storage-deletion-policy and inspect its
separate CI result. Do not merge PRs. Continue to A07 when this slice is published.
The isolated candidate /tmp/liuxin-a06-review is detached at 47c5c0d3 with the
current overlay; /tmp/liuxin-sync-a06.py copies only this branch's work and verifies
protected hashes. The clean /tmp/liuxin-a05-followup owns the A05 parent branch.
