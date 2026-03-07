#!/usr/bin/env python3
"""
Example: convert plain text comments to minimal HTML.
"""

from __future__ import annotations

import argparse
import json

from _example_utils import bootstrap_src_path

bootstrap_src_path()

from LiuXin_alpha.library.comments import comments_to_html


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="comments_to_html example")
    parser.add_argument(
        "--text",
        default="Line one.\nLine two.\n\nSecond paragraph.",
        help="Input text to convert",
    )
    return parser.parse_args()


def main() -> int:
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
