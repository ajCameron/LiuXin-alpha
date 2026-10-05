#!/usr/bin/env python3
"""
Render a comments string through Library's comments-to-HTML helper and print JSON.

Use the helper's text/HTML heuristics without an additional sanitization layer.
The report preserves the input alongside the returned HTML, with Unicode output
and two-space indentation. No input or output file is opened by this example.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

EXAMPLES_ROOT = Path(__file__).resolve().parents[1]
if str(EXAMPLES_ROOT) not in sys.path:
    sys.path.insert(0, str(EXAMPLES_ROOT))

from _example_utils import bootstrap_src_path

bootstrap_src_path()

from LiuXin_alpha.library.comments import comments_to_html


def parse_args() -> argparse.Namespace:
    """
    Parse optional --text, defaulting to the two-paragraph sample. The argument is passed
    through as a string; this parser does not interpret or validate embedded markup.

    Example:
        >>> args = parse_args()  # doctest: +SKIP


    :return: Parsed argparse namespace; help and invalid arguments raise SystemExit.
    """
    parser = argparse.ArgumentParser(description="comments_to_html example")
    parser.add_argument(
        "--text",
        default="Line one.\nLine two.\n\nSecond paragraph.",
        help="Input text to convert",
    )
    return parser.parse_args()


def main() -> int:
    """
    Convert the process argument text and print input plus resulting HTML as JSON. Delegate
    formatting to comments_to_html, then use ensure_ascii=False and two-space indentation. Do not
    write files, postprocess the HTML, or catch conversion/output errors.

    Example:
        >>> exit_code = main()  # doctest: +SKIP


    :return: Zero after printing the report; parsing, formatting, and output errors propagate.
    """
    args = parse_args()
    html = comments_to_html(args.text)
    print(
        json.dumps(
            {
                "input": args.text,
                "output_html": html,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
