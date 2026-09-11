#!/usr/bin/env python3
"""
Publish and read an object directly through the filesystem storage driver.

Use a fresh address-space UUID and an UPSERT write session with expected UTF-8 byte
length and SHA-256. Commit before reading and enumerating the root. Print stored
address/URI, bytes decoded as text, inventory, and the declared atomic-publish flag.
This raw-driver example does not create an Asset/Replica catalogue.
"""

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
from LiuXin_alpha.storage.drivers import FilesystemStorageDriver


def parse_args() -> argparse.Namespace:
    """
    Parse a required Store root, an object key defaulting to incoming/example.bin, and payload text
    defaulting to filesystem driver example. Leave path/address validation and UTF-8 encoding to
    main and the driver.

    Example:
        >>> args = parse_args()  # doctest: +SKIP


    :return: Parsed argparse namespace; help and invalid arguments raise SystemExit.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Write, commit, inspect, and read an object through the raw "
            "filesystem storage driver"
        ),
    )
    parser.add_argument("--store-root", required=True, help="Driver root directory")
    parser.add_argument(
        "--object-key",
        default="incoming/example.bin",
        help="Object address relative to the driver root",
    )
    parser.add_argument(
        "--payload",
        default="filesystem driver example",
        help="UTF-8 text to store",
    )
    return parser.parse_args()


def main() -> int:
    """
    Start a filesystem driver, commit the requested UTF-8 payload, and report a readback. Resolve
    the root and compute expected byte size/SHA-256 before starting. An unavailable startup status
    raises RuntimeError. Parse the key and use UPSERT, so an existing target may be replaced. Close
    the write session around commit, read the returned object, and materialize the complete file
    inventory. Report the expected digest and capability flag; the example does not race a reader
    against publication or independently assert readback equality. Always close a successfully
    constructed driver; committed bytes remain.

    Example:
        >>> exit_code = main()  # doctest: +SKIP


    :return: Zero after printing the report; parsing, storage, and cleanup errors propagate.
    """
    args = parse_args()
    root = Path(args.store_root).expanduser().resolve()
    payload = args.payload.encode("utf-8")
    expected_digest = api.Digest(
        "sha256",
        hashlib.sha256(payload).hexdigest(),
    )
    driver = FilesystemStorageDriver(root, address_space_uuid=uuid4())

    try:
        status = driver.startup()
        if not status.available:
            raise RuntimeError(status.message or "filesystem driver is unavailable")

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
                    "root_uri": driver.root_uri,
                    "object_key": str(stored.object_address),
                    "object_uri": driver.object_uri(stored.object_address),
                    "size": stored.size,
                    "sha256": expected_digest.value,
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
