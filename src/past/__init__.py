"""Minimal compatibility shim for legacy ``from past.builtins import ...`` imports."""

from . import builtins

__all__ = ["builtins"]
