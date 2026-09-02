# Python packaging contract

Status: installed-catalogue wheel gate enforced, 2026-09-02.

## Supported wheel boundary

`pyproject.toml` is the source of truth for Python package discovery and owned
package data. The wheel contains the production `LiuXin_alpha` namespace and
the small local `past` compatibility shim. It must not contain the historical
`LiuXin_tests` source package, repository tests, working memory, or embedded
vendored demo/test subpackages merely because they happen to live below
`src`.

The following non-Python files are runtime-owned package data:

- FRBR database-generator table, trigger, aggregate SQL, and TOML specs;
- `catalog/py.typed`;
- legacy startup folder descriptors;
- the ISO-639 lookup corpus and bundled dateutil zoneinfo archive;
- OEB display/polish CoffeeScript sources used by dynamic compilation paths.

Do not replace this list with a blanket recursive data include. A new runtime
file should be owned by the narrowest package that opens it, declared in
`pyproject.toml`, and added to the verifier when it is not already covered by
the schema inventory. Implicit manifest-driven package data is disabled so a
tracked fixture or note below an included namespace cannot enter the wheel by
accident.

## Acceptance gate

Build and exercise the same contract locally with:

```bash
python -m pip wheel --no-deps --wheel-dir dist .
python scripts/verify_wheel_install.py dist/liuxin_alpha-0.0.0-py3-none-any.whl
```

The verifier checks wheel contents, installs the artifact into a temporary
target, confirms imports resolve from that installation, creates a real SQLite
system with `liuxin init`, and reopens it with a second idempotent init. This is
deliberately stronger than importing from `PYTHONPATH=src` or running `--help`.
The `Installed Wheel` CI job owns this gate.

Warnings and diagnostics belong on stderr. Command receipts and requested data
belong on stdout; in particular, `--compact` JSON must remain directly
parseable even when optional resources are unavailable.

## External compatibility resources

The top-level `LiuXin_resources/calibre_resources` tree is not currently part
of the wheel. It is shipped by `scripts/build_deployment_package.py` and may be
selected through `LIUXIN_BASE_DIR`. Conversion, rendering, and localization
paths which request Calibre templates, fonts, pickles, or compiled browser
resources require that tree.

This is a known distribution boundary, not an assertion that those files are
optional to every LiuXin workflow. Before calling the wheel a complete
conversion distribution, move or generate the resources into a package-owned
layout, teach the resolver to use `importlib.resources`, and add at least one
clean installed-wheel conversion smoke. Preserve external override support for
operators who intentionally supply a different resource bundle.

## Dependency follow-up

The base dependency set supports the validated catalogue/init path. The large
compatibility tree contains guarded imports for format, UI, image, archive, and
backend libraries. Future dependency changes should come from clean installed
workflow tests and should distinguish base requirements from named extras; a
static count of every import in inherited code is not sufficient evidence for
making all of them mandatory.
