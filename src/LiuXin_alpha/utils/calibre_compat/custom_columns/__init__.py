"""Calibre custom-column argument and projection compatibility over native storage."""

from .errors import CompatibilityError
from .facade import CustomColumns
from .manager import CustomColumnsManager

__all__ = ["CompatibilityError", "CustomColumns", "CustomColumnsManager"]
