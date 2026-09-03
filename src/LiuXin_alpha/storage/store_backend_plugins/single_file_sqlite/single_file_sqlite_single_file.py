"""The new Store API returns FileInfo rather than backend-specific files."""

from LiuXin_alpha.storage.api import FileInfo


SingleFileSqliteSingleFile = FileInfo


__all__ = ["SingleFileSqliteSingleFile"]
