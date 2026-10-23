# LiuXin user documentation

Updated **2026-10-23**. These guides describe installed commands and everyday
workflows. Start with [web ingest](web-ingest.md) to record files on a website
without mirroring it, then acquire selected copies when needed.

For first-time setup, run `liuxin init` in a terminal. The wizard offers to save
the selected system as your default, so later commands can omit `--system-root`.
See [setup and defaults](local-ingest.md#set-up-your-default-system) for scripted
setup, selecting an existing installation, and per-user behaviour.

For local directories and SquashFS collections, see
[local ingest](local-ingest.md), including disk headroom, WSL host protection,
resource telemetry and resume after a resource stop.
The [operational CLI guide](../dev-docs/surfaces/operational-cli.md) covers other
command families and connection settings. Implementation and ownership notes
belong in [developer documentation](../dev-docs/README.md).

## Manual pages

The package installs these pages under its installation prefix's `share/man/man1`:

| Page | Scope |
| --- | --- |
| [liuxin(1)](man/man1/liuxin.1) | CLI orientation, connection selection and help. |
| [liuxin-ingest(1)](man/man1/liuxin-ingest.1) | Choosing web, local-tree or managed ingest. |
| [liuxin-ingest-web(1)](man/man1/liuxin-ingest-web.1) | Registration, downloads, filters, budgets, inspection and resume. |

```bash
man liuxin
man liuxin-ingest
man liuxin-ingest-web
```

If a virtual environment's manual directory is outside your man search path,
use its prefix explicitly. For an activated environment:

```bash
man -M "$VIRTUAL_ENV/share/man" liuxin-ingest-web
```

From a source checkout, no installation is needed to read a page:

```bash
man -l docs/man/man1/liuxin-ingest-web.1
```

The pages are maintained as roff sources alongside these guides. Check option
spellings against `liuxin ingest web --help`, defaults against the implementation,
and render edited pages with `groff -man -Tutf8`. Changes to acquisition guarantees
also need the [developer runbook](../dev-docs/ingest/web_ingest_operations.md).
