#!/usr/bin/env python3
"""
Example: batch-convert multiple input files to OEB directories.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys

from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch convert many files to OEB directories")
    parser.add_argument("--output-root", required=True, help="Root directory for OEB outputs")
    parser.add_argument("--inputs", nargs="+", required=True, help="Input files to convert")
    parser.add_argument("--clean-output", action="store_true", help="Delete each output directory before converting")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logs in child conversions")
    return parser.parse_args()


def _safe_name(path: Path) -> str:
    ext = path.suffix.lower().lstrip(".") or "noext"
    return f"{path.stem}_{ext}_oeb"


def main() -> int:
    args = parse_args()
    output_root = Path(args.output_root).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    script_path = Path(__file__).resolve().parent / "conversion_to_oeb_example.py"
    results = []
    failed = 0

    for raw_input in args.inputs:
        input_path = Path(raw_input).expanduser().resolve()
        output_dir = output_root / _safe_name(input_path)
        cmd = [sys.executable, str(script_path), "--input", str(input_path), "--output-dir", str(output_dir)]
        if args.clean_output:
            cmd.append("--clean-output")
        if args.verbose:
            cmd.append("--verbose")

        proc = subprocess.run(cmd, capture_output=True, text=True)
        entry = {
            "input": str(input_path),
            "output_dir": str(output_dir),
            "return_code": proc.returncode,
            "stdout": proc.stdout.strip(),
            "stderr": proc.stderr.strip(),
        }
        if proc.returncode != 0:
            failed += 1
        results.append(entry)

    payload = {
        "output_root": str(output_root),
        "total": len(results),
        "failed": failed,
        "succeeded": len(results) - failed,
        "results": results,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
