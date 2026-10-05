"""
Expose the Calibre-readable, mutable Calibre, and richer LiuXin-compatible metadata contracts.

The facade also exports shared field, identifier, payload, path, and
custom-descriptor aliases. These are structural typing interfaces; concrete book
containers live under their implementation modules.

Example:
    Exercise the owning behavior with pytest::

        python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py
"""

from __future__ import annotations

from LiuXin_alpha.metadata.api.containers_api.calibre_metadata_api.calibre_metadata_input_api import (
    CalibreMetadataInputAPI)
from LiuXin_alpha.metadata.api.containers_api.calibre_metadata_api.calibre_extended_metadata_api import (
    CalibreLikeBookMetadataAPI)
from LiuXin_alpha.metadata.api.containers_api.calibre_metadata_api.calibre_metadata_api import CalibreMetadataAPI
from LiuXin_alpha.metadata.api.containers_api.calibre_metadata_api.calibre_metadata_types import (
    CalibreBinaryReadableAPI,
    CalibreCloseableAPI,
    CalibreCoverData,
    CalibreDescriptorValue,
    CalibreFieldDescriptor,
    CalibreFieldMapping,
    CalibreFieldValue,
    CalibreFilePayload,
    CalibreIdentifierMapping,
    CalibreIdentifierSnapshot,
    CalibreIdentifierSnapshotValue,
    CalibreIdentifierValue,
    CalibreMetadataScalar,
    CalibreMetadataSequence,
    CalibreMetadataSet,
    CalibrePath,
    CalibrePayloadToID,
    CalibreUserMetadata,
    CalibreValueToID,
)

__all__ = [
    "CalibreBinaryReadableAPI",
    "CalibreCloseableAPI",
    "CalibreCoverData",
    "CalibreDescriptorValue",
    "CalibreFieldDescriptor",
    "CalibreFieldMapping",
    "CalibreFieldValue",
    "CalibreFilePayload",
    "CalibreIdentifierMapping",
    "CalibreIdentifierSnapshot",
    "CalibreIdentifierSnapshotValue",
    "CalibreIdentifierValue",
    "CalibreLikeBookMetadataAPI",
    "CalibreMetadataAPI",
    "CalibreMetadataInputAPI",
    "CalibreMetadataScalar",
    "CalibreMetadataSequence",
    "CalibreMetadataSet",
    "CalibrePayloadToID",
    "CalibrePath",
    "CalibreUserMetadata",
    "CalibreValueToID",
]
