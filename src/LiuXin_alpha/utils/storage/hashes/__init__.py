"""
Expose the supported hashes compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py
"""
from __future__ import annotations

import hashlib
from typing import Union

BytesLike = Union[bytes, bytearray, memoryview]


def sane_hash(
    data: str | BytesLike,
    algo: str = "sha256",
    encoding: str = "utf-8",
    errors: str = "strict",
    hexdigest: bool = True,
) -> str | bytes:
    """
    Hash `data` (bytes-like or str) using a standard algorithm (default: SHA-256).

    Example:
        Exercise sane hash through a consuming regression::

            python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


    :param data: Value supplied for data under the utility contract.
    :param algo: Value supplied for algo under the utility contract.
    :param encoding: Value supplied for encoding under the utility contract.
    :param errors: Value supplied for errors under the utility contract.
    :param hexdigest: Value supplied for hexdigest under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(data, str):
        b = data.encode(encoding, errors)
    elif isinstance(data, (bytes, bytearray, memoryview)):
        b = bytes(data)
    else:
        raise TypeError(f"Unsupported type: {type(data)!r} (expected str or bytes-like)")

    try:
        h = hashlib.new(algo)
    except ValueError as e:
        raise ValueError(
            f"Unknown hash algorithm {algo!r}. "
            f"Try one of: {sorted(hashlib.algorithms_available)}"
        ) from e

    h.update(b)
    return h.hexdigest() if hexdigest else h.digest()
