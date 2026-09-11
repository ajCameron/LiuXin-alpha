"""
Verify surface-package lazy API exposure, the explicit protocol export list, and shared re-export identities.

These contracts inspect imported modules and public names without constructing
application instances, opening databases, or asserting runtime protocol validation.
"""

from __future__ import annotations

import importlib


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


def test_surfaces_root_exposes_lazy_api_module() -> None:
    """
    Keep api advertised at the surface root and bound to the same importable API submodule.

    This checks public availability and identity, not whether another test already
    loaded the module before this access.

    Example:
        >>> test_surfaces_root_exposes_lazy_api_module()


    :return: None after export membership and package/submodule identity assertions.
    """
    surfaces = importlib.import_module("LiuXin_alpha.surfaces")

    assert "api" in surfaces.__all__
    assert surfaces.api is importlib.import_module("LiuXin_alpha.surfaces.api")


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
        assert hasattr(surface_api, exported_name), f"surfaces.api is missing {exported_name}"


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
