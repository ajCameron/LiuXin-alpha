# CLI composition and dependency direction

The operator CLI has one complete command grammar and one application dispatch
path. Compatibility entry points do not maintain reduced copies of either.

## Ownership

| Module under `surfaces/cli` | Responsibility |
| --- | --- |
| `__init__`, `__main__` | Installed/package entry points; package import remains lazy. |
| `app` | Public `build_parser()` compatibility entry point, argument shortcuts/global selectors, command dispatch, exit/error handling. |
| `parsers` | Complete command-family registration in stable help order. |
| `parser_types` | Standard-library-only, strict `CompletionSubparsers` and `CompletionRegistrar` contracts. |
| `completion` | Shell script rendering, standalone completion command, completion argument registration. |
| `squashfs_parsers` | SquashFS publication/provenance argument declarations. |
| `squashfs_commands` | Core job submission/results and provenance rendering. |
| `squashfs` | Explicit command/helper compatibility aliases and the historical `main()` delegate. |

`parsers.create_parser(register_completion=...)` receives completion
registration explicitly. Both the application and standalone completion pass
the same `build_completion_parser` implementation. Completion can therefore
inspect the complete grammar without importing the application; parser
construction never imports completion back. There is no mutable registration
registry or hidden parser state on argument namespaces.

The narrow subparser protocol declares only the `add_parser(name, help=...)`
operation completion needs. It avoids binding strict application contracts to
argparse's private concrete class while checking the actual registration
function and caller against each other.

## Compatibility and behavior

The installed `liuxin` entry point, `surfaces.cli.main`, `app.main`,
`app.build_parser`, standalone completion functions, and `squashfs.main` retain
their call shapes. The historical SquashFS entry point still accepts the whole
installed tree, including PostgreSQL and metadata commands. Its command and
private-helper aliases refer to the implementation owners, not duplicate bodies.

Command names, aliases, help and option ordering, choices/defaults, shell script
output, profile-selector placement, shortcuts, and error/exit handling are
preserved by this extraction. JSON provenance fields retain their values,
including missing metadata; private TypedDicts make the existing receipt
shape checkable without changing wire data. Job requests and receipt reads
still go through Core; polling and publication failure policies are unchanged.

Tests or integrations replacing dependencies must patch the consuming owner:
for example, `squashfs_commands.open_surface_core_from_args`, not a compatibility
module's old imported global. Public entry-point calls remain supported;
arbitrary monkeypatch forwarding between modules is not introduced.

## Changing the CLI

1. Register a command family in `parsers`, and keep execution in its existing
   command owner. Do not import an entry-point facade from command code.
2. Keep the injected completion registrar and its narrow contract aligned.
   Extend static positive/negative examples when introducing new call shapes.
3. Add behavior tests for parsing, output, and failure paths. Completion must
   cover aliases and nested commands, and compatibility entry points must
   reach the complete installed tree.
4. Run `bash scripts/run_type_checks.sh` and the affected CLI tests. The
   `test_cli_dependency_contracts.py` suite covers cold imports, registration,
   completion output, compatibility dispatch, and SquashFS Core receipts.

The dependency gate includes all 47 CLI modules, including deferred and
type-only imports. Implementations cannot import `cli`, `app`, or `squashfs`
entry-point facades; the three explicit entry wrappers are exceptions.
Parser composition cannot import the completion command, and parser contracts
cannot import another LiuXin module. These direction rules also reject
backward edges that do not yet form a cycle.

Eight reviewed CLI modules enter typing, lint, complexity-10, and formatting
checks. This is not a claim that all CLI internals are strictly typed. The
separate terminal UI dependency cycle remains outside this tranche.
