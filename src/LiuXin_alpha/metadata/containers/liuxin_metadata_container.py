"""Historical project-level metadata bundle.

At the moment this is the work-centred metadata bundle, which remains the
broadest convenient read/write metadata surface in the codebase.
"""

from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata

LiuXinMetadataContainer = WorkMetadata

__all__ = ["LiuXinMetadataContainer"]
