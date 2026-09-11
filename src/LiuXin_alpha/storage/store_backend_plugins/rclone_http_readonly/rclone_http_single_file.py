"""
Retain the historical rclone single-file name as an exact alias of shared FileInfo.

    The value carries reported Store file information rather than owning a live
    process, opening remote bytes, or introducing backend-specific metadata fields.
"""

from LiuXin_alpha.storage.api import FileInfo


RcloneHttpReadOnlySingleFile = FileInfo


__all__ = ["RcloneHttpReadOnlySingleFile"]
