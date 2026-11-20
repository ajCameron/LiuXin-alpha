"""Fingerprint native metadata values without relying on cache generations."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING
from uuid import UUID

from LiuXin_alpha.catalog.api.metadata_revisions import MetadataRevision
from LiuXin_alpha.catalog.api.metadata_search import MetadataReference

if TYPE_CHECKING:
    from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_target_api import (
        MetadataRecord,
    )


def _encode(value: object) -> object:
    if isinstance(value, (datetime, date, Decimal, UUID)):
        return {"type": type(value).__name__, "value": str(value)}
    if isinstance(value, bytes):
        return {"type": "bytes", "value": value.hex()}
    raise TypeError(f"Unsupported metadata revision value: {type(value).__name__}")


def revision(
    reference: MetadataReference, payload: MetadataRecord, root: Mapping[str, object]
) -> MetadataRevision:
    """Hash the full hydrated payload using a versioned canonical JSON encoding.

    Ordering of relationships is part of the value; mapping insertion order is
    not. Reject unsupported or non-finite values instead of hashing a lossy repr.
    """
    encoded = json.dumps(
        {"metadata": payload, "root": root},
        default=_encode,
        sort_keys=True,
        ensure_ascii=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return MetadataRevision(
        reference, "metadata-v1:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    )
