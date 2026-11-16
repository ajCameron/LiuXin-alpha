"""Failures to represent native fields through Calibre calling conventions."""

from LiuXin_alpha.databases.custom_columns.native_types import CustomFieldError


class CompatibilityError(CustomFieldError):
    """A native field operation has no lossless Calibre-compatible representation."""
