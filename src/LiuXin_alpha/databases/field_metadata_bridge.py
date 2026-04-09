"""Local bridge for field-metadata dependencies.

The database layer should not directly depend on whichever top-level surface package
happens to expose ``FieldMetadata`` today. Import this bridge instead.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_field_metadata_from_surfaces():
    root = Path(__file__).resolve().parents[1]
    module_path = root / "surfaces" / "field_metadata.py"
    spec = importlib.util.spec_from_file_location(
        "LiuXin_alpha._field_metadata_bridge_surfaces",
        module_path,
    )
    if spec is None or spec.loader is None:  # pragma: no cover
        raise ImportError(f"Unable to load FieldMetadata bridge from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.FieldMetadata


try:  # pragma: no cover - compatibility with trees that still ship interfaces
    from LiuXin_alpha.interfaces.field_metadata import FieldMetadata  # type: ignore
except Exception:  # surfaces package currently has a heavy __init__, so load the module file directly
    FieldMetadata = _load_field_metadata_from_surfaces()


__all__ = ["FieldMetadata"]
