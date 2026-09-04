#!/usr/bin/env python3
"""Exercise an installed LiuXin HTML-to-EPUB conversion and its resources."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import zipfile
from pathlib import Path
from typing import Any

HTML_DOCUMENT = """<!doctype html>
<html>
  <head>
    <meta charset="utf-8">
    <meta name="dc:title" content="Installed conversion smoke">
    <meta name="author" content="LiuXin verifier">
    <title>Installed conversion smoke</title>
  </head>
  <body>
    <h1>Package-owned resources</h1>
    <p>Unicode survives: Καλημέρα — 你好 — café.</p>
  </body>
</html>
"""


class InstalledConversionError(RuntimeError):
    """Report a failed installed-resource or conversion contract."""


def run_smoke(workspace: Path, expected_package_root: Path) -> dict[str, Any]:
    """Create a small EPUB using only the installed package and its extras."""

    workspace.mkdir(parents=True, exist_ok=True)
    source = workspace / "installed-conversion.html"
    output = workspace / "installed-conversion.epub"
    source.write_text(HTML_DOCUMENT, encoding="utf-8")

    from LiuXin_alpha.core.workflow_jobs import run_conversion_job
    from LiuXin_alpha.utils.resources import P, resource_to_resource

    template = Path(P("templates/html.css")).resolve()
    package_root = expected_package_root.resolve()
    if not template.is_relative_to(package_root):
        raise InstalledConversionError(
            f"Calibre template resolved outside installed package: {template}"
        )
    template_data = resource_to_resource("templates/html.css")
    if not template_data or template.read_bytes() != template_data:
        raise InstalledConversionError("Package resource path/data APIs disagree")

    result = run_conversion_job(
        input_path=str(source),
        output_path=str(output),
    )
    if not result.get("exists") or not output.is_file():
        raise InstalledConversionError("HTML-to-EPUB conversion produced no file")

    try:
        with zipfile.ZipFile(output) as archive:
            names = frozenset(archive.namelist())
            media_type = archive.read("mimetype")
            package_document = archive.read("content.opf")
    except (OSError, KeyError, zipfile.BadZipFile) as exc:
        raise InstalledConversionError(
            f"Conversion output is not a valid EPUB: {exc}"
        ) from exc

    required_entries = {"mimetype", "META-INF/container.xml", "content.opf"}
    missing = sorted(required_entries - names)
    if missing:
        raise InstalledConversionError(
            "Converted EPUB is missing entries: " + ", ".join(missing)
        )
    if media_type != b"application/epub+zip":
        raise InstalledConversionError("Converted EPUB has an invalid mimetype")
    if b"Installed conversion smoke" not in package_document:
        raise InstalledConversionError("Converted EPUB lost its title metadata")

    return {
        "input_format": result.get("input_format"),
        "output_format": result.get("output_format"),
        "output_size_bytes": output.stat().st_size,
        "epub_entries": len(names),
        "resource_path": str(template),
        "resource_sha256": hashlib.sha256(template_data).hexdigest(),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run LiuXin's installed HTML-to-EPUB conversion smoke."
    )
    parser.add_argument("workspace", type=Path)
    parser.add_argument("expected_package_root", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the installed conversion smoke and print its JSON receipt."""

    args = _build_parser().parse_args(argv)
    try:
        result = run_smoke(args.workspace, args.expected_package_root)
    except InstalledConversionError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
