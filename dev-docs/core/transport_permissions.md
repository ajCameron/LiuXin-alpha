# Core transport permissions

Implemented for PR review, **2026-10-20**, architecture A06.

HTTP exposure is a separate contract from `transport_stable`. Each built-in
registration declares `remote_capabilities`; custom registrations default to
no remote exposure. Replacing a registration also replaces its declaration.
Core checks the selected handler and its declaration under the same registry
lock, before handler execution or command events.

## Deployment roles

[`CoreTransportPolicy`](../../src/LiuXin_alpha/core/authorization.py) belongs to
the server. Request payloads cannot supply or enlarge its permissions.

| Role | Authority | Authentication |
| --- | --- | --- |
| Default / `read-only` | Explicitly declared catalogue, browse and acquisition reads. No commands or generic invocation. | Optional on loopback; required for a non-loopback listener. |
| `read-write` | Declared catalogue reads and semantic writes. | Bearer token required. |
| `trusted` | Administration, external execution, configuration and generic invocation as well as ordinary reads/writes. | Bearer token required. |

The capabilities are `read`, `write`, `admin`, `execute` and `invoke`. A command
cannot opt itself into a read-only deployment by declaring only `read`. Queries
also require explicit declarations: calling `invoke` through `/rpc/query` does
not turn a mutation into a read. Preferences that select executable behaviour,
external conversion/ingest and backup execution require stronger authority than
ordinary catalogue writes.

This is one authenticated deployment role, not an individual-user account or
per-library row authorization system. An operator can construct a narrower
explicit capability set. Treat `trusted` credentials as access to the hosted
process and its data, including compatibility invocation.

## Serving and connecting

The CLI defaults to read-only:

```sh
liuxin core serve --database library.sqlite --access read-only
liuxin core serve --database library.sqlite --access read-write --auth-token-file /private/core.token
```

Use a securely generated token in a private file. `--core-token-file` is the
corresponding option for remote surface/CLI connections. The Python entry points
accept `bearer_token`:

```python
from pathlib import Path
from LiuXin_alpha.core import core_client

client = core_client(
    endpoint="http://127.0.0.1:8765",
    bearer_token=Path("/private/core.token").read_text().strip(),
)
tables = client.query("schema.tables")
description = client.describe_api(include_targets=False)
```

Tokens are excluded from policy representations and readiness output.
Authenticated Python clients refuse redirects rather than forwarding credentials.
The HTTP daemon does not terminate TLS. Protect remote credentials with TLS at
the network boundary or an SSH tunnel. A non-loopback CLI bind requires both
authentication and the existing explicit bind acknowledgement. Reverse proxies
must preserve a recognized Host or configure `--allowed-host` with the exact
authority, including its port.

## Request boundaries

Both GET helpers and POST RPCs enforce authentication and Host checks. Browser
Origin requests and cross-site fetches are rejected; direct browser RPC/CORS is
not a supported deployment mode. Web surfaces call Core from their server-side
Python clients. POST requests require UTF-8 `application/json`.

Missing/wrong credentials produce 401; refused authority produces 403; a wrong
request media type produces 415. These refusals precede dispatch. Namespace
prefixes only route requests; they do not identify or authorize a caller.

Named introspection is available to readers. Dynamic target descriptions require
`invoke`; global event history requires `admin`, because events can contain
privileged operation details. Read-query refresh switches require `admin`.
Explicit `storage.refresh`, `read-source.refresh` and maintenance commands remain
administrative operations.

## Read-only surfaces and direct callers

[`ReadOnlyCoreClient`](../../src/LiuXin_alpha/core/readonly.py) restricts the
read-only web/API/OPDS host and its injected model. It refuses commands, global
events, shutdown, generic invocation and effectful query options. The narrow
`CoreReadClientAPI` exposes named queries only. Writable web composition retains
its explicit administrative client.

Local runtime reads use the same registration check as HTTP. Remote adapters
check the server declaration and the server independently enforces its policy.
A read-only surface must use a read-only deployment role; a client wrapper cannot
reduce the authority of credentials used by a separate direct HTTP caller.

Ordinary in-process `CoreRuntime` calls remain trusted. The optional execution
capabilities parameter is internal server context, never a field extracted from
a request. This boundary does not replace Catalog validation, storage deletion
policy or database transactions. Even a permitted destructive endpoint must
still satisfy its domain owner's rules.

## Validation

The [transport regressions](../../tests/core/test_transport_authorization.py)
send raw HTTP requests, test named and generic bypasses, assert unchanged state
and no command events after refusal, and exercise authenticated existing clients
and restricted local/remote readers. The final focused selection passed **67 tests**, including real CLI token-file
serving and A05 last-copy refusal through HTTP. Complete quality gates passed;
both type checkers also rejected all **38** negative contract examples. Broader
caller evidence and corrected fixtures are recorded in the
[handoff](../../working-memory/transport-permissions-2026-10-20.md). Full PR CI is
a separate check.
