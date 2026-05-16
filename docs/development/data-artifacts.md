# Optional Data Artifacts

Large LiuXin fixtures live in the private `LiuXin_alpha_data` checkout, not in
the main source repository. Use the shell wrapper as the normal entry point:

```bash
scripts/build_artifacts.sh list --data-root LiuXin_alpha_data
scripts/build_artifacts.sh verify --data-root LiuXin_alpha_data
scripts/build_artifacts.sh build --artifact benchmark-smoke --data-root LiuXin_alpha_data --regenerate
scripts/build_artifacts.sh build --artifact isfdb-current --data-root LiuXin_alpha_data --dump-zip backup-MySQL-55-2026-04-18.zip --force
scripts/build_artifacts.sh write-manifest --data-root LiuXin_alpha_data --dump-zip backup-MySQL-55-2026-04-18.zip
```

`scripts/build_artifacts.sh` creates or refreshes `.venv` with
`scripts/create_venv.sh`, then runs `scripts/build_artifacts.py` using the venv
Python. Pass `--skip-install` to reuse the environment without reinstalling, or
`--new-venv` to recreate it before building.

The Python entry point can also be called directly when the environment is
already prepared:

```bash
python3 scripts/build_artifacts.py list
python3 scripts/build_artifacts.py verify --data-root LiuXin_alpha_data
python3 scripts/build_artifacts.py build --artifact benchmark-smoke --data-root LiuXin_alpha_data --regenerate
python3 scripts/build_artifacts.py build --artifact isfdb-current --data-root LiuXin_alpha_data --dump-zip backup-MySQL-55-2026-04-18.zip --force
python3 scripts/build_artifacts.py write-manifest --data-root LiuXin_alpha_data --dump-zip backup-MySQL-55-2026-04-18.zip
```

The small benchmark smoke database is safe to track in Git. Multi-GB ISFDB
database payloads are manifest-only: the data repo tracks their READMEs,
`build_summary.json` files, and `artifacts_manifest.json`, while the `.test_db`
payloads stay local.

Reproducibility rules:

- Child builders run with `PYTHONHASHSEED=0`, `TZ=UTC`, `LC_ALL=C.UTF-8`, and `LANG=C.UTF-8`.
- Persisted data must not depend on platform random backends, `secrets`,
  `uuid4`, or unseeded `random` state.
- Database builders must use explicit ordering for reads and writes.
- Absolute paths and elapsed times in build summaries are diagnostic only;
  artifact SHA-256 values and row-count summaries are the reproducibility
  contract.
