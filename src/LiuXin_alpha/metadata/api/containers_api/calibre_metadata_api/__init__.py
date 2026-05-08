"""API contracts for Calibre-shaped book metadata objects.

Category: high-level metadata compatibility API.
This module defines the subset of Calibre's mutable book metadata surface used
by LiuXin import/export and plugin-facing workflows.
"""

from __future__ import annotations

from LiuXin_alpha.metadata.api.containers_api.calibre_metadata_api.calibre_metadata_input_api import (
    CalibreMetadataInputAPI)
from LiuXin_alpha.metadata.api.containers_api.calibre_metadata_api.calibre_extended_metadata_api import (
    CalibreLikeBookMetadataAPI)
from LiuXin_alpha.metadata.api.containers_api.calibre_metadata_api.calibre_metadata_api import CalibreMetadataAPI
from LiuXin_alpha.metadata.api.containers_api.calibre_metadata_api.calibre_metadata_input_api import (
    CalibreMetadataInputAPI)
from LiuXin_alpha.metadata.api.containers_api.calibre_metadata_api.calibre_metadata_types import (
    CalibrePath,
    CalibreFilePayload,
    CalibreValueToID,
    CalibreIdentifierMapping,
    CalibreIdentifierSnapshot,
    CalibreFieldDescriptor,
    CalibreUserMetadata,
    CalibreFieldValue,
    CalibreFieldMapping,
    CalibrePath,
    CalibreBinaryReadableAPI,
    CalibreCloseableAPI,
    CalibreFilePayload,
    CalibreCoverData,
    CalibreMetadataScalar,
    CalibreMetadataSequence,
    CalibreMetadataSet,
    CalibreValueToID,
    CalibrePayloadToID,
    CalibreIdentifierValue,
    CalibreIdentifierMapping,
    CalibreIdentifierSnapshotValue,
    CalibreIdentifierSnapshot,
    CalibreDescriptorValue,
    CalibreFieldDescriptor,
    CalibreUserMetadata,
    CalibreFieldValue,
    CalibreFieldMapping)

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
