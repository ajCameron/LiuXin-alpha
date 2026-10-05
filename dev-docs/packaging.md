# Python packaging contract

Status: installed catalogue and conversion wheel gate enforced, 2026-09-04.

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
- the complete 317-file Calibre compatibility bundle under
  `LiuXin_alpha.resources/calibre`, including templates, images, dictionaries,
  fonts, SQL snapshots, browser assets, and quick-start books.

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

The verifier checks wheel contents, installs the artifact plus the
`conversion` extra into a temporary target, and confirms LiuXin and every
direct dependency resolve from that target. It creates a real SQLite system
with `liuxin init`, reopens it with a second idempotent init, resolves resource
path and byte APIs from the installed package, and converts a Unicode HTML
document into a structurally valid EPUB. This is deliberately stronger than
importing from `PYTHONPATH=src` or running `--help`. The `Installed Wheel` CI
job owns this gate.

Warnings and diagnostics belong on stderr. Command receipts and requested data
belong on stdout; in particular, `--compact` JSON must remain directly
parseable even when optional resources are unavailable.

## Resource resolution and overrides

`LiuXin_alpha.resources` owns the immutable fallback bundle and locates it with
`importlib.resources`. The inherited APIs still return filesystem paths, so
LiuXin supports normal unpacked wheel installations rather than direct zip
imports.

Operators can set `LIUXIN_CALIBRE_RESOURCES_DIR` to a complete or partial
overlay. Existing `LiuXin_resources/calibre_resources` and
`LiuXin_data/calibre_resources` layouts below `LIUXIN_BASE_DIR` remain valid.
Lookup checks overlays first and falls back per file to package-owned data, so
an operator does not need to copy all 317 files merely to replace one template.
The historical Calibre developer/user overlay behavior remains available.

## Dependency boundary

The base dependencies are sufficient for catalogue initialization and normal
Core/storage startup. Conversion-specific libraries are declared in the
`conversion` extra: `cssutils` for OEB CSS processing, Pillow for image and
cover paths, and `regex` for format-aware text operations. Development, full
test, artifact, and source-deployment helpers include this extra by default;
minimal installations do not acquire conversion dependencies implicitly.

ImageMagick/Wand, GUI toolkits, external archive programs, and individual
backend clients remain capability-specific rather than requirements of the
validated HTML-to-EPUB route. Add them to a named extra only when an installed
workflow gate establishes the corresponding supported boundary.
