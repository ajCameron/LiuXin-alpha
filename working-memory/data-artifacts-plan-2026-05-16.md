# Data Artifacts Plan - 2026-05-16

Private data artifacts now use a split policy:

- Track small durable fixtures in `LiuXin_alpha_data` when they are useful for
  normal development, starting with `benchmarks/databases/benchmark_db_smoke.test_db`.
- Keep multi-GB ISFDB `.test_db` payloads out of Git. Track their README,
  `build_summary.json`, and manifest hashes instead.
- Use `scripts/build_artifacts.py` in the main repo as the single builder and
  verifier entry point.

Determinism requirements:

- Builder subprocesses run with `PYTHONHASHSEED=0`, `TZ=UTC`, `LC_ALL=C.UTF-8`,
  and `LANG=C.UTF-8`.
- Persisted fixture data must not depend on platform random backends, unseeded
  `random`, `secrets`, or `uuid4`.
- Database reads/writes in builders need explicit ordering where row order can
  affect output.
- Build summaries may include absolute paths and elapsed times, but those are
  diagnostics only. `artifacts_manifest.json` stores the reproducibility
  contract through content hashes and logical row-count summaries.

Artifact policy:

- `benchmark-smoke`: buildable and small enough for Git.
- `isfdb-current`: buildable from `backup-MySQL-55-2026-04-18.zip`, but payload
  remains local/manifest-only because it is roughly 7.5 GB.
- `isfdb-full-legacy`: preserved by hash as an older artifact. It is not marked
  bit-for-bit rebuildable with the current ISFDB builder because the current
  builder now emits richer metadata.

Implementation status:

- `LiuXin_alpha_data` commit `a8a37b6` tracks `.gitignore`, root `README.md`,
  `artifacts_manifest.json`, the small benchmark smoke DB, and README/summary
  metadata for both ISFDB bundles.
- The large ISFDB `.test_db` payloads are ignored locally and remain out of Git.
- `benchmark-smoke` was rebuilt twice through `scripts/build_artifacts.py` and
  verified stable at SHA-256
  `8a1db5b45b4f3f50cc88f9b04974a5f1fb5a2a0d3f51a65d6f7623e54c50e3b0`.
- Full manifest verification passed for all local payloads:
  `isfdb-current` SHA-256
  `03dc66aac75188c28cfc88c2d733b7a49a6aa19c762e27541c655ca22dde77e1`;
  `isfdb-full-legacy` SHA-256
  `b17401c4435b627b4a31c9cc470b9290d84dd01b58b4ca6abc569a992c7cc037`.
