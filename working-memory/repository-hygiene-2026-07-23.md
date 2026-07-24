# Repository hygiene and post-merge baseline

Date: 2026-07-23
Status: complete

## Objective

Restore a deterministic cross-platform checkout after the Catalog and
file-format typing merge, register the existing data gitlink, repair working
memory bookkeeping, and establish a trustworthy current test baseline.

## Completed hygiene

- verified that the five dirty files in `LiuXin_alpha_data` differed only by
  checkout line endings and normalized exactly to their recorded blobs;
- normalized CRLF bytes from root files already declared `eol=lf`;
- changed the default text policy to `text=auto eol=lf`, while preserving the
  explicit CRLF policy for Windows-native scripts;
- registered `LiuXin_alpha_data` in `.gitmodules` with its existing origin,
  `https://github.com/ajCameron/LiuXin_alpha_data.git`; and
- added the completed 2026-07-23 Catalog migration note to the working-memory
  index.

The root and nested repositories were clean after normalization, before the
intentional housekeeping edits above.

## Verification

- `git submodule status` resolves the registered data repository at
  `00d1d2a62e538f59e211cc19d907b7c667eada6d`;
- the root and nested repositories are clean apart from the intentional root
  housekeeping edits;
- no CRLF bytes remain in tracked source/config/fixture extensions declared
  `eol=lf`;
- `git diff --check` passes;
- the metadata and OPF fixture/hash lane passes: `54 passed`;
- the confidence stream passes: `214 passed, 2 skipped`;
- the fresh full-suite artifact reports `5027 passed, 86 skipped, 17 xfailed`,
  with four failures caused solely by the network-isolated sandbox denying
  localhost socket creation with `PermissionError: [Errno 1]`; and
- those four HTTP-daemon tests pass outside the network-isolated sandbox:
  `4 passed`.

Full-suite artifacts:

- `working-memory/test-results/repository-hygiene-full-2026-07-23.log`
- `working-memory/test-results/repository-hygiene-full-2026-07-23.json`
- `working-memory/test-results/repository-hygiene-full-2026-07-23.done`

## Conclusion

The stale 89-failure pre-merge artifact is superseded. No code failure remains
in the current configured suite: the only nonzero full-wrapper result is the
known execution-environment restriction, confirmed green with localhost socket
access.
