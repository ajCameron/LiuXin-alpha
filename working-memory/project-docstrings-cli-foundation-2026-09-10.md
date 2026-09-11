# CLI foundation docstrings — 2026-09-10

Continuation of the active [whole-project descriptive reST goal](project-docstrings-2026-09-08.md),
following [read/write web](project-docstrings-write-web-2026-09-10.md). The preceding
goal turn made verified progress. This turn checked initially clean CLI targets,
reviewed all seven foundation modules and both selected regression modules, then
documented every named declaration, including protocol and nested helpers.
Executable behavior, assertions, parser literals, and compatibility exports remain.

Work stays local on `codex/project-docstrings`, based on `edf6bf05`; no commit,
push, submodule publication, or modification to the maintainability PR occurred.

## Reviewed contracts

- [CLI package](../src/LiuXin_alpha/surfaces/cli/__init__.py) and
  [entrypoint](../src/LiuXin_alpha/surfaces/cli/__main__.py): lazy application
  loading preserves argument-object identity and return code. Ordinary entrypoint
  import is inert; main-name execution forwards through SystemExit. Uncaught
  exceptions retain the application/parser boundary rather than becoming success.
- [app.py](../src/LiuXin_alpha/surfaces/cli/app.py): fresh grammar composition,
  case-sensitive ingest shorthand, and global selector hoisting. Reserved ingest
  commands/help bypass rewriting. The first exact --source pair wins over a
  positional source; equals-form --source is not specially handled. Unchanged
  shortcut cases return the original list object. Hoisting recognizes separate
  and equals-form global selectors anywhere, without respecting an option
  terminator or other options' argument roles. A trailing valueless selector
  returns the original input before shortcut normalization. Repeated selectors
  retain argparse's usual last-value behavior; the duplicate destination checks
  are not a general duplicate-token detector. Handler failures and integer-result
  conversion errors print ERROR and return two, while parser/construction errors
  and BaseException subclasses propagate. Missing handler prints help and returns two.
- [parser_types.py](../src/LiuXin_alpha/surfaces/cli/parser_types.py): the minimal
  non-runtime-checkable CompletionSubparsers protocol, documented add_parser
  contract retaining its ellipsis, and callback alias separating grammar assembly
  from completion. This module stays standard-library-only.
- [parsers.py](../src/LiuXin_alpha/surfaces/cli/parsers.py): required command-family
  registration in stable order, completion callback exactly once, global namespace
  destinations, and separation from execution. No callback/handler signature or
  argument declaration changed; no docstrings feed parser description literals.
- [completion.py](../src/LiuXin_alpha/surfaces/cli/completion.py): private argparse
  action traversal, sorted unique spellings, separate alias paths, and the nested
  visitor. Only the last subparser action supplies children; there is no cycle
  guard or option-value expansion. Bash renders nested paths and options but does
  not track option arity, so value tokens resembling commands can affect traversal.
  Zsh/fish intentionally expose only root and immediate child commands, excluding
  options and deeper paths. Fish child conditions are presence-based, not an exact
  full-path match. The tree's depth is not claimed as equal support in every shell.
  Renderers assume the trusted installed grammar and do not run/install scripts.
  Standalone command generation uses the injected registrar and common publication
  policy, not application callback recursion.
- [common.py](../src/LiuXin_alpha/surfaces/cli/common.py): every helper now has
  meaningful parameters, return/exception behavior, and examples. Core composition
  redirects stdout only during factory construction, not session entry, caller
  body, or cleanup. The session context still handles body errors.
- File output stages in the destination directory, fsyncs, optionally chmods,
  then replaces or hard-links with no-clobber semantics. Existing dangling links
  count as targets; the hard-link step protects against races after precheck.
  Modes otherwise follow mkstemp rather than an overwritten file. Stdout is
  buffered before emission, with UTF-8 fallback for text-only streams, but cannot
  roll back partial emission. A published file can precede cleanup/directory-close
  failure. Directory open/fsync OSError is suppressed; close errors are not.
- JSON output sorts keys, ASCII-escapes, retains default nonfinite values, and
  appends one newline. Serialization precedes output opening. Control-file reads
  use the CLI host and default sixteen-MiB cap, accept general JSON values, and
  wrap decode/JSON failures without hiding filesystem errors. Object loading adds
  a shallow outer mapping restriction, not recursive schema validation. Byte
  decoding preserves bytes identity and strictly validates base64 envelopes.
- Managed-job options distinguish CLI wait from Core execution policy. Payload
  augmentation mutates in place and leaves existing keys intact for omitted/falsey
  options. Terminal state is checked before the deadline and results are fetched
  separately with timeout_s=0. A local timeout neither cancels work nor bounds a
  blocking remote query; polling has a 0.01-second floor. Detached receipts need
  no job ID, while waiting does. Existing result submission keys are preserved.
  Exit-code projection recognizes only narrow timeout/execution failure shapes,
  not every top-level ok or job-state indicator.
- [test_cli_dependency_contracts.py](../tests/surfaces/test_cli_dependency_contracts.py):
  all fifteen functions, including four nested helpers, are documented. Cold
  imports use separate interpreters with module-specific forbidden sets; parser,
  export, argument-identity, completion-output, no-clobber, and help/error contracts
  are explicit. Alias-tree assertions do not prove renderer feature parity.
  SquashFS job/provenance cases use autospecced collaborators, not real storage.
- [test_cli_operational_families.py](../tests/surfaces/test_cli_operational_families.py):
  all seventeen functions and the fake-Core class are documented. Histories hold
  shallow payload copies, canned responses do not evolve with commands, and unknown
  calls are recorded before failure. Session fixtures borrow one fake without
  cleanup. Parser-family and capability checks are subset assertions. Local byte
  fixtures and output files are real, but storage/job responses are simulated.
  Surrogate filenames, Unicode search, nested upload hints, MiB conversion,
  Core-host workflow paths, preview/confirmation, and server-refusal checks are
  described at their actual evidence level. The historically named before-import
  refusal test checks status/message, not sys.modules.

Batch: **9 modules, 2 classes, 62 functions = 73 declarations**. Cumulative:
**225 modules, 262 classes, 2,500 functions = 2,987 reviewed Python declarations**,
plus the separately reviewed native C and runtime-created vacuum adapter.

## Verification

Overlapping selections must not be added into a distinct-test total.

- All **225 reviewed Python files**, including the data-submodule generator,
  pass the strict structural audit and normalizer --check. All nine batch files
  pass individually. Executable ASTs match HEAD after stripping genuine leading
  docstrings, including retained protocol ellipsis and unchanged test fixtures.
- No new Ruff findings relative to HEAD. Existing findings: __main__ one I001;
  common one I001 and eleven UP032; operational-family tests one I001, two UP032,
  and one E731. The other six files are clean. Selected formatter files remain
  unchanged by formatting; no incidental style warning or executable code was fixed.
- Initial foundation source and dependency-test doctests, plus dependency,
  operational-family, and operator-hardening regressions: **82 passed, 14 explicitly
  skipped examples** in 108.59 seconds. After completing operational-family prose,
  its explicit module/doctest rerun: **18 passed, 10 explicitly skipped examples**
  in 14.99 seconds. These runs overlap; the larger operator-hardening source remains
  outside this completed documentation batch.
- Full `bash scripts/run_type_checks.sh`: **passed**. Scope remains 159 formatter
  files, 456 annotated modules, 221 protected dependency modules, 188 strict-mypy
  files, and 37 invalid examples rejected by each checker. Selected formatting,
  lint, complexity, annotation, dependency direction, and both type checkers pass.
- Before/after fingerprints match for all **298 command paths**, root help, and
  bash/zsh/fish scripts. No Core operation, shell installation, or shell execution
  is claimed by these text comparisons. SHA-256 values:
  tree `721f1fcbd9dd09da931e32ec0896930005b34959e08a5d3bb06b39a8f14ac64b`;
  help `a1a2509de8389c319da4c24dfe1ee0daa3b4a7b431f7ba7044d128301d6d9632`;
  bash `77e8a5ce3b1186cb055f24cd2e4c17bf72285c623197fb06908b83a39a378c8b`;
  zsh `3c245a7f6fdf4d719bc790a100e43ded05eb5fe9d0eff5691278f78cf3d4dfcb`;
  fish `b7a83c87f204a1a71975501c584cf2a01218bf3b51604f0ab560d6caac5e7f91`.
- Isolated checks passed for reserved-ingest and source precedence, missing-value
  identity, selector hoisting across --, stdout buffering/aborted output, real
  no-clobber races, dangling symlinks, staged cleanup, replacement permissions,
  directory-fsync fallbacks, JSON size/encoding/object restrictions, duplicate-key
  and NaN behavior, strict wire decoding, and bytes identity.
- Mocked lifecycle checks passed for construction-only stdout redirection, session
  cleanup on failure, ordinary-handler and int-conversion error status, escaping
  KeyboardInterrupt, and guarded module entry. Job checks cover terminal-before-
  deadline ordering, non-cancelling timeout, polling floor, in-place controls,
  detached missing-ID acceptance, waiting missing-ID failure, retained submission,
  and narrow exit-code projection. No real worker or remote transport was used.
- Adjacent CLI metadata, SquashFS, PostgreSQL-command, storage, init/ingest,
  migration, public-doc/link, and ownership selection: **133 passed, 5 failed**
  in 137.97 seconds. All failures are the same known physical-line/statement
  conflicts in Core services, program facade, browser components, windowed
  components, and windowed root. No guard logic or limit changed. Two existing
  multiprocessing/fork deprecation warnings came from SQLite SquashFS cases.
  This selection is not green and is not a live-PostgreSQL verification claim.
- All **220 local link destinations across eighteen working-memory files**,
  including the index, resolve. Root and data-submodule diffs are whitespace-clean,
  the staging area is empty, and all verification processes have completed.

No fresh whole-project pytest, installed-wheel, remote-CI, or live PostgreSQL
verification is claimed by this documentation batch.

## Inventory and next work

Full audit still parses **2,730 modules, 4,400 classes, 35,119 functions** with no
failures. Missing/blank docstrings remain on **1,103 modules, 2,021 classes, and
22,601 functions = 25,725 declarations**. Additional overlapping findings:
4,217 delimiter-layout, 10,490 missing-example, 3,041 parameter-field, 3,981
return-field, 4,121 empty-parameter, 5,553 empty-return, and 28 missing-summary.
Reports: `/tmp/liuxin-docstring-current-2026-09-10.json` and
`/tmp/liuxin-docstring-reviewed-2026-09-10.json`.

Next source review: cli/squashfs.py (54 lines), squashfs_commands.py (317),
squashfs_parsers.py (158), and tests/surfaces/test_cli_squashfs.py (397), followed
by cli/core_cli.py (212), jobs.py (253), and the other command families. The two
completed CLI test modules should not be re-documented. Operator-hardening tests
were exercised but only partially read; their 1,205-line source remains unfinished.
The Core HTTP transport was already completed in the Core batch. Preserve the
full source/tests/scripts/examples/native/submodule backlog and configuration-exec
caution from the main ledger. The whole-project goal remains active.
