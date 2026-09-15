#!/usr/bin/env python3
"""
Convert supplied or generated OEB input through the EPUB output plugin.

Create output parents and a work directory, install the example plugin shim,
load the OPF, and invoke the writer inside the scratch-setting context. Report
output size and paths. The work_dir_cleaned field records intended cleanup before
it runs; auto-created work directories use best-effort removal in finally.
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
    make_epub_output_opts,
    resolve_oeb_input,
)

from LiuXin_alpha.file_formats.conversion.plugins.epub_output import EPUBOutput


def parse_args() -> argparse.Namespace:
    """
    Parse a required EPUB output path and optional OPF input, extraction destination, and work
    directory. Omitted OPF input requests a sample. Explicit work directories are retained;
    --keep-work-dir retains an automatically allocated one. --verbose enables example log output.
    Paths are processed by main rather than validated by argparse.

    Example:
        >>> args = parse_args()  # doctest: +SKIP


    :return: Parsed argparse namespace; help and invalid arguments raise SystemExit.
    """
    parser = argparse.ArgumentParser(description="Convert OEB/OPF source to EPUB")
    parser.add_argument("--input-opf", default=None, help="Path to metadata OPF. If omitted, a sample OEB is generated.")
    parser.add_argument("--output", required=True, help="Target EPUB file path")
    parser.add_argument(
        "--extract-to",
        default=None,
        help="Optional directory to extract generated EPUB after conversion",
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
    """
    Convert an OPF or generated sample to EPUB and print its diagnostic report. Expand/resolve
    output and optional extraction paths, create output parents, and use either an explicitly
    retained work directory or an automatically allocated one. Resolve/generate input, install the
    process-global customize.ui shim without restoring it, and load the OEB book. Call EPUBOutput
    with the example options/logger inside isolated_conversion_scratch; only that scratch setting is
    restored by its context.

    Report generated-input status, paths, and output stat size, using zero when the output does not
    exist. A zero reported size does not independently change the return code. Print
    work_dir_cleaned as the planned cleanup flag before finally attempts rmtree with
    ignore_errors=True, so it is not proof of removal. Caller-selected work directories and
    output/extraction files are retained, including after later failures.

    Example:
        >>> exit_code = main()  # doctest: +SKIP


    :return: Zero after printing the report; uncaught parsing, conversion, rendering, and cleanup errors propagate.
    """
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
        work_dir = Path(tempfile.mkdtemp(prefix="liuxin-alpha-epub-example-"))
        cleanup_workdir = not args.keep_work_dir

    generated_sample = False
    try:
        opf_path, generated_sample = resolve_oeb_input(args.input_opf, workspace=work_dir)
        install_customize_ui_stub()
        oeb = load_oeb_from_opf(opf_path)
        opts = make_epub_output_opts(extract_to=extract_to)
        with isolated_conversion_scratch():
            EPUBOutput(None).convert(
                oeb,
                str(output_path),
                None,
                opts,
                ExampleLog(verbose=args.verbose),
            )

        payload = {
            "input_opf": str(opf_path),
            "input_generated_sample": generated_sample,
            "output_epub": str(output_path),
            "output_size_bytes": output_path.stat().st_size if output_path.exists() else 0,
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
