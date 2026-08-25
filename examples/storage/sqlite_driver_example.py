#!/usr/bin/env python3
"""Use the SQLite BLOB storage driver directly with commit-based writes."""

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

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.drivers import SQLiteStorageDriver


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Write, commit, enumerate, and read a BLOB through the raw "
            "SQLite storage driver"
        ),
    )
    parser.add_argument("--database", required=True, help="SQLite BLOB database path")
    parser.add_argument(
        "--object-key",
        default="example-object",
        help="Opaque object key (slashes are not permitted)",
    )
    parser.add_argument(
        "--payload",
        default="SQLite driver example",
        help="UTF-8 text to store",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    database = Path(args.database).expanduser().resolve()
    payload = args.payload.encode("utf-8")
    expected_digest = api.Digest(
        "sha256",
        hashlib.sha256(payload).hexdigest(),
    )
    driver = SQLiteStorageDriver(database, address_space_uuid=uuid4())

    try:
        status = driver.startup()
        if not status.available:
            raise RuntimeError(status.message or "SQLite driver is unavailable")

        address = driver.parse_object_address(args.object_key)
        with driver.begin_write(
            address,
            mode=api.WriteMode.UPSERT,
            expected_size=len(payload),
            expected_digest=expected_digest,
        ) as write_session:
            write_session.write(payload)
            stored = write_session.commit()

        read_back = driver.read_file(stored)
        inventory = tuple(driver.iter_inventory())
        print(
            dump_json(
                {
                    "driver": type(driver).__name__,
                    "database": str(database),
                    "object_key": str(stored.object_address),
                    "size": stored.size,
                    "sha256": stored.digest.value if stored.digest else None,
                    "read_back": read_back.decode("utf-8"),
                    "inventory": [
                        str(entry.object_address) for entry in inventory
                    ],
                    "atomic_publish": driver.capabilities.atomic_publish,
                }
            )
        )
    finally:
        driver.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
