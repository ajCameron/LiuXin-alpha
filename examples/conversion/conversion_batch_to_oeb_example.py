#!/usr/bin/env python3
"""
Run the single-file OEB conversion example sequentially for several inputs.

Derive output names from each input basename and suffix, invoke a fresh Python
subprocess per input, and report captured text plus return codes. Names are not
unique across input directories, and --clean-output is forwarded to each child.
Completed outputs remain even when a later conversion fails.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys

from pathlib import Path


def parse_args() -> argparse.Namespace:
    """
    Parse a required output root and one-or-more input paths, plus optional --clean-output and
    --verbose flags forwarded to each child. Argparse does not validate source existence or detect
    output-name collisions.

    Example:
        >>> args = parse_args()  # doctest: +SKIP


    :return: Parsed argparse namespace; help and invalid arguments raise SystemExit.
    """
    parser = argparse.ArgumentParser(description="Batch convert many files to OEB directories")
    parser.add_argument("--output-root", required=True, help="Root directory for OEB outputs")
    parser.add_argument("--inputs", nargs="+", required=True, help="Input files to convert")
    parser.add_argument("--clean-output", action="store_true", help="Delete each output directory before converting")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logs in child conversions")
    return parser.parse_args()


def _safe_name(path: Path) -> str:
    """
    Combine the input stem and lowercase final suffix into an OEB directory name. Strip leading dots
    from the suffix and use noext when absent, then append _oeb. Preserve the stem's spelling,
    punctuation, and Unicode. Despite the historical helper name, this does not provide general
    filesystem-name sanitization or uniqueness across directories.

    Example:
        >>> _safe_name(Path("dir/Book.EPUB"))
        'Book_epub_oeb'
        >>> _safe_name(Path("README"))
        'README_noext_oeb'


    :param path: Input path whose basename stem and final suffix determine the output name.
    :return: Derived directory basename, which can collide for distinct input paths.
    """
    ext = path.suffix.lower().lstrip(".") or "noext"
    return f"{path.stem}_{ext}_oeb"


def main() -> int:
    """
    Create the output root and run each requested input through a child conversion process.
    Expand/resolve input and output paths, derive a child directory name, and invoke the adjacent
    conversion_to_oeb_example.py with the current Python interpreter. Forward cleanup/verbosity
    flags and inherit the working directory/environment. Capture full stdout/stderr in memory with
    no timeout, strip their outer whitespace, and continue after nonzero child exits. Subprocess
    launch errors instead propagate immediately.

    Print a Unicode JSON summary with every child return code and captured output. A shared derived
    name can reuse or, with cleanup enabled, erase an earlier child's output, while its earlier
    success remains in the report. No rollback of completed work is attempted.

    Example:
        >>> exit_code = main()  # doctest: +SKIP


    :return: Zero when every child exits zero, otherwise one; uncaught path, launch, and rendering errors propagate.
    """
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
