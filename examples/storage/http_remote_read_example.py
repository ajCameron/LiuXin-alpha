#!/usr/bin/env python3
"""Inspect and read one remote object through the read-only HTTP driver."""

from __future__ import annotations

import argparse
import hashlib
import sys

from pathlib import Path
from uuid import uuid4


EXAMPLES_ROOT = Path(__file__).resolve().parents[1]
if str(EXAMPLES_ROOT) not in sys.path:
    sys.path.insert(0, str(EXAMPLES_ROOT))

from _example_utils import bootstrap_src_path, dump_json


bootstrap_src_path()

from LiuXin_alpha.storage.drivers import HttpStorageDriver


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read a scoped remote object with HTTP HEAD/GET requests through "
            "the raw read-only storage driver"
        ),
    )
    parser.add_argument(
        "--base-url",
        required=True,
        help="HTTP(S) root URL which owns the object",
    )
    parser.add_argument(
        "--object-key",
        required=True,
        help="Canonical URL-relative object key",
    )
    parser.add_argument(
        "--output",
        help="Optional local path at which to save the downloaded bytes",
    )
    parser.add_argument(
        "--expected-sha256",
        help="Optional lowercase SHA-256 used to verify the response",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="Per-request timeout in seconds",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    driver = HttpStorageDriver(
        args.base_url,
        address_space_uuid=uuid4(),
        timeout_s=args.timeout,
        max_requests_per_hour=0,
    )

    try:
        status = driver.startup()
        if not status.available:
            raise RuntimeError(status.message or "HTTP endpoint is unavailable")

        address = driver.parse_object_address(args.object_key)
        info = driver.stat_file(address)
        payload = driver.read_file(info, if_version=info.version)
        sha256 = hashlib.sha256(payload).hexdigest()
        if args.expected_sha256 is not None:
            expected = args.expected_sha256.strip().lower()
            if sha256 != expected:
                raise ValueError(
                    f"SHA-256 mismatch: expected {expected}, observed {sha256}"
                )

        output_path: Path | None = None
        if args.output:
            output_path = Path(args.output).expanduser().resolve()
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(payload)

        print(
            dump_json(
                {
                    "driver": type(driver).__name__,
                    "read_only": not status.writable,
                    "root_uri": driver.root_uri,
                    "object_key": str(address),
                    "object_uri": driver.object_uri(address),
                    "size": info.size,
                    "version": info.version,
                    "media_type": info.hints.media_type,
                    "sha256": sha256,
                    "preview_hex": payload[:32].hex(),
                    "output": None if output_path is None else str(output_path),
                }
            )
        )
    finally:
        driver.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
