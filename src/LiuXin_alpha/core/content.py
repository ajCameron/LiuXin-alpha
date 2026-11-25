"""Consume versioned acquisition chunks through the same local/remote contract."""

from __future__ import annotations

import base64
from collections.abc import Callable, Iterator, Mapping
from typing import Any


def acquisition_chunks(
    query: Callable[..., Any], kind: str, resource_id: int
) -> Iterator[tuple[dict[str, Any], bytes]]:
    """Yield at most one MiB at a time and refuse changed or nonprogressing streams."""
    offset = 0
    version = None
    total = None
    while True:
        payload = {
            "kind": kind,
            "id": resource_id,
            "offset": offset,
            "length": 1024 * 1024,
        }
        if version is not None:
            payload["if_version"] = version
        result = query("acquisition.read-range", payload)
        content = result["content"]
        if isinstance(content, Mapping) and content.get("$type") == "bytes":
            content = base64.b64decode(content["base64"], validate=True)
        if not isinstance(content, bytes) or len(content) > 1024 * 1024:
            raise ValueError("Invalid acquisition chunk")
        if total is not None and (
            result["version"] != version or result["total_bytes"] != total
        ):
            raise ValueError("Acquisition changed between chunks")
        total, version = result["total_bytes"], result["version"]
        if result["offset"] != offset or result["next_offset"] != offset + len(content):
            raise ValueError("Invalid acquisition progress")
        if not content and not result["complete"]:
            raise ValueError("Acquisition failed to make progress")
        next_offset = offset + len(content)
        if type(total) is not int or total < next_offset:
            raise ValueError("Invalid acquisition size")
        if result["complete"]:
            if next_offset != total:
                raise ValueError("Acquisition ended before declared size")
        elif version is None:
            raise ValueError("Acquisition has no version for continued reading")
        yield dict(result["resource"]), content
        offset = next_offset
        if result["complete"]:
            return
