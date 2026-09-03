#!/usr/bin/env python3
"""
Example: convert an OEB/OPF source to MOBI.
"""

from __future__ import annotations

import argparse
import shutil
import tempfile

from pathlib import Path

from _conversion_example_utils import (
    ExampleLog,
    dump_json,
    install_customize_ui_stub,
    isolated_conversion_scratch,
    load_oeb_from_opf,
    make_mobi_output_opts,
    resolve_oeb_input,
)

from LiuXin_alpha.file_formats.conversion.plugins.mobi_output import MOBIOutput


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert OEB/OPF source to MOBI")
    parser.add_argument("--input-opf", default=None, help="Path to metadata OPF. If omitted, a sample OEB is generated.")
    parser.add_argument("--output", required=True, help="Target MOBI file path")
    parser.add_argument(
        "--mobi-type",
        default="old",
        choices=("old", "both", "new"),
        help="Output MOBI variant",
    )
    parser.add_argument(
        "--extract-to",
        default=None,
        help="Optional directory to extract generated MOBI after conversion",
    )
    parser.add_argument(
        "--work-dir",
        default=None,
        help="Optional working directory used when generating sample input",
    )
    parser.add_argument("--keep-work-dir", action="store_true", help="Do not remove auto-created working directory")
    parser.add_argument("--verbose", action="store_true", help="Print conversion logs")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    output_path = Path(args.output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    extract_to = None
    if args.extract_to:
        extract_to = str(Path(args.extract_to).expanduser().resolve())

    cleanup_workdir = False
    if args.work_dir:
        work_dir = Path(args.work_dir).expanduser().resolve()
        work_dir.mkdir(parents=True, exist_ok=True)
    else:
        work_dir = Path(tempfile.mkdtemp(prefix="liuxin-alpha-mobi-example-"))
        cleanup_workdir = not args.keep_work_dir

    generated_sample = False
    try:
        opf_path, generated_sample = resolve_oeb_input(args.input_opf, workspace=work_dir)
        install_customize_ui_stub()
        oeb = load_oeb_from_opf(opf_path)
        opts = make_mobi_output_opts(mobi_file_type=args.mobi_type, extract_to=extract_to)
        with isolated_conversion_scratch():
            MOBIOutput(None).convert(
                oeb,
                str(output_path),
                None,
                opts,
                ExampleLog(verbose=args.verbose),
            )

        payload = {
            "input_opf": str(opf_path),
            "input_generated_sample": generated_sample,
            "output_mobi": str(output_path),
            "output_size_bytes": output_path.stat().st_size if output_path.exists() else 0,
            "mobi_type": args.mobi_type,
            "extract_to": extract_to,
            "work_dir": str(work_dir),
            "work_dir_cleaned": cleanup_workdir,
        }
        print(dump_json(payload))
        return 0
    finally:
        if cleanup_workdir:
            shutil.rmtree(work_dir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
