"""
Verify explicit surface API imports, protocol exports, and shared re-export identities.

These contracts inspect imported modules and public names without constructing
application instances, opening databases, or asserting runtime protocol validation.
"""

from __future__ import annotations

import importlib
import subprocess
import sys
from pathlib import Path


SURFACE_API_EXPORTS = [
    "AcquisitionHostApi",
    "CalibreCatalogHostApi",
    "ImageHostApi",
    "OpdsHostApi",
    "ReadModelHostApi",
    "ResolvedFileTargetAPI",
    "SurfaceCategoryItem",
    "SurfaceEntitySummary",
    "SurfaceFilePayload",
    "SurfaceRelatedPayload",
    "SurfaceResponseAPI",
    "SurfaceSearchEntry",
    "SurfaceWorkMetadataPayload",
]


def test_surfaces_api_uses_ordinary_submodule_imports() -> None:
    """The namespace exposes explicitly imported submodules without lazy hooks."""
    from LiuXin_alpha.surfaces import api

    surfaces = importlib.import_module("LiuXin_alpha.surfaces")
    assert surfaces.api is api is importlib.import_module("LiuXin_alpha.surfaces.api")
    assert "__getattr__" not in vars(surfaces)


def test_surface_namespaces_do_not_load_implementations() -> None:
    """Test a fresh interpreter so earlier imports cannot mask eager loading."""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import LiuXin_alpha.surfaces; "
            "import LiuXin_alpha.surfaces.renderers; "
            "assert {n for n in sys.modules if n.startswith('LiuXin_alpha.surfaces.')} "
            "== {'LiuXin_alpha.surfaces.renderers'}",
        ],
        capture_output=True,
        text=True,
        cwd=Path(__file__).resolve().parents[2],
    )
    assert result.returncode == 0, result.stderr


def test_surface_api_root_exports_current_contract_names() -> None:
    """
    Preserve the exact ordered API export list and require each advertised name to exist.

    Example:
        >>> test_surface_api_root_exports_current_contract_names()


    :return: None after ordered-list equality and all exported-attribute presence checks.
    """
    surface_api = importlib.import_module("LiuXin_alpha.surfaces.api")

    assert surface_api.__all__ == SURFACE_API_EXPORTS
    for exported_name in SURFACE_API_EXPORTS:
        assert hasattr(surface_api, exported_name), (
            f"surfaces.api is missing {exported_name}"
        )


def test_existing_surface_modules_reexport_shared_host_protocols() -> None:
    """
    Require acquisition, catalogue, image, OPDS, and read-model package/API names to share protocol identities.

    Example:
        >>> test_existing_surface_modules_reexport_shared_host_protocols()


    :return: None after all ten compatibility import locations resolve to the central protocol objects.
    """
    surface_api = importlib.import_module("LiuXin_alpha.surfaces.api")
    module_protocols = [
        ("LiuXin_alpha.surfaces.acquisition.api", "AcquisitionHostApi"),
        ("LiuXin_alpha.surfaces.catalog.api", "CalibreCatalogHostApi"),
        ("LiuXin_alpha.surfaces.images.api", "ImageHostApi"),
        ("LiuXin_alpha.surfaces.opds.api", "OpdsHostApi"),
        ("LiuXin_alpha.surfaces.read_model.api", "ReadModelHostApi"),
        ("LiuXin_alpha.surfaces.acquisition", "AcquisitionHostApi"),
        ("LiuXin_alpha.surfaces.catalog", "CalibreCatalogHostApi"),
        ("LiuXin_alpha.surfaces.images", "ImageHostApi"),
        ("LiuXin_alpha.surfaces.opds", "OpdsHostApi"),
        ("LiuXin_alpha.surfaces.read_model", "ReadModelHostApi"),
    ]

    for module_name, protocol_name in module_protocols:
        module = importlib.import_module(module_name)
        assert getattr(module, protocol_name) is getattr(surface_api, protocol_name)
