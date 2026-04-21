"""
Compatibility module for the project-level unified metadata container.

Historically this file held an unfinished ``LiuXinMetadataContainer`` stub.
The concrete work-level metadata bundle now lives in
``metadata.containers.metadata_containers.wemi_containers.work_metadata_container``.
Keep this name as a thin compatibility alias until callers are normalized on
``WorkMetadataContainer``.
"""

from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import (
    WorkMetadataContainer,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_hydrator import (
    WorkMetadataHydrator,
)

LiuXinMetadataContainer = WorkMetadataContainer
LiuXinMetadataHydrator = WorkMetadataHydrator

__all__ = [
    "LiuXinMetadataContainer",
    "LiuXinMetadataHydrator",
]
