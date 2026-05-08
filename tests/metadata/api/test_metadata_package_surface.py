from __future__ import annotations

import ast
import importlib
import re
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
METADATA_API_ROOT = REPO_ROOT / "src" / "LiuXin_alpha" / "metadata" / "api"


@pytest.mark.parametrize(
    ("module_name", "expected_names"),
    [
        (
            "LiuXin_alpha.metadata.api",
            [
                "WorkIdentityAPI",
                "WorkMetadataAPI",
                "ExpressionIdentityAPI",
                "ExpressionMetadataAPI",
                "ManifestationIdentityAPI",
                "ManifestationMetadataAPI",
                "ItemIdentityAPI",
                "ItemMetadataAPI",
                "AgentIdentityAPI",
                "AgentProfileAPI",
                "ManyManyRelationEdgeAPI",
                "ManyOneRelationEdgeAPI",
                "MetadataRecord",
                "MutableMetadataRecord",
                "OneManyRelationEdgeAPI",
                "OneOneRelationEdgeAPI",
                "RelationCardinality",
                "RelationCardinalityValue",
                "RelationEdge",
                "RelationEdgeAPI",
                "RelationEdgeID",
                "RelationEdgeSource",
                "RelationEdgeType",
                "RelationTarget",
                "WorkRelationEdge",
                "WorkRelationTarget",
                "ExpressionRelationEdge",
                "ExpressionRelationTarget",
                "ManifestationRelationEdge",
                "ManifestationRelationTarget",
                "ItemRelationEdge",
                "ItemRelationTarget",
                "CalibreMetadataInputAPI",
                "CalibreMetadataAPI",
                "CalibreLikeBookMetadataAPI",
                "LiuXinMetadataAPI",
                "LiuXinMetaInformationAPI",
                "LiuXinWEMIMetadataAPI",
                "LiuXinWEMIAPI",
            ],
        ),
        (
            "LiuXin_alpha.metadata.api.containers_api",
            [
                "WorkIdentityAPI",
                "WorkMetadataAPI",
                "ExpressionIdentityAPI",
                "ExpressionMetadataAPI",
                "ManifestationIdentityAPI",
                "ManifestationMetadataAPI",
                "ItemIdentityAPI",
                "ItemMetadataAPI",
                "AgentIdentityAPI",
                "AgentProfileAPI",
                "ManyManyRelationEdgeAPI",
                "ManyOneRelationEdgeAPI",
                "MetadataRecord",
                "MutableMetadataRecord",
                "OneManyRelationEdgeAPI",
                "OneOneRelationEdgeAPI",
                "RelationCardinality",
                "RelationCardinalityValue",
                "RelationEdge",
                "RelationEdgeAPI",
                "RelationEdgeID",
                "RelationEdgeSource",
                "RelationEdgeType",
                "RelationTarget",
                "WorkRelationEdge",
                "WorkRelationTarget",
                "ExpressionRelationEdge",
                "ExpressionRelationTarget",
                "ManifestationRelationEdge",
                "ManifestationRelationTarget",
                "ItemRelationEdge",
                "ItemRelationTarget",
            ],
        ),
        (
            "LiuXin_alpha.metadata.containers",
            [
                "WorkIdentity",
                "WorkMetadata",
                "LiuXinWEMIMetadata",
                "LiuXinWEMI",
                "LiuXinWEMIMetadataHydrator",
                "LiuXinWEMIMetadataWriteReport",
                "LiuXinWEMIMetadataWriter",
                "LazyLiuXinWEMIMetadata",
                "LazyLiuXinWEMI",
                "LazyLiuXinWEMIMetadataHydrator",
                "ExpressionIdentity",
                "ExpressionMetadata",
                "ManifestationIdentity",
                "ManifestationMetadata",
                "ItemIdentity",
                "ItemMetadata",
                "AgentIdentity",
                "AgentProfile",
                "WorkTitlesContainer",
                "WorkNotesContainer",
                "WorkLabelsContainer",
                "WorkGenresContainer",
                "WorkSubjectsContainer",
                "WorkIdentifiersContainer",
                "WorkResourcesContainer",
                "WorkSeriesEntriesContainer",
                "WorkRatingsContainer",
                "WorkDatesContainer",
                "WorkLanguagesContainer",
            ],
        ),
        (
            "LiuXin_alpha.metadata.containers.metadata_containers",
            [
                "WorkIdentity",
                "WorkMetadata",
                "LiuXinWEMIMetadata",
                "LiuXinWEMI",
                "LiuXinWEMIMetadataHydrator",
                "LiuXinWEMIMetadataWriteReport",
                "LiuXinWEMIMetadataWriter",
                "LazyLiuXinWEMIMetadata",
                "LazyLiuXinWEMI",
                "LazyLiuXinWEMIMetadataHydrator",
                "ExpressionIdentity",
                "ExpressionMetadata",
                "ManifestationIdentity",
                "ManifestationMetadata",
                "ItemIdentity",
                "ItemMetadata",
                "AgentIdentity",
                "AgentProfile",
                "WorkTitlesContainer",
                "WorkNotesContainer",
                "WorkLabelsContainer",
                "WorkGenresContainer",
                "WorkSubjectsContainer",
                "WorkIdentifiersContainer",
                "WorkResourcesContainer",
                "WorkSeriesEntriesContainer",
                "WorkRatingsContainer",
                "WorkDatesContainer",
                "WorkLanguagesContainer",
            ],
        ),
        (
            "LiuXin_alpha.metadata.api.from_database_api",
            [
                "CalibreMetadataGetterAPI",
                "DBMetadataSourceAPI",
                "HydratableMetadataKind",
                "HydratedMetadataAPI",
                "LiuXinMetadataGetterAPI",
                "LiuXinWEMIMetadataGetterAPI",
                "MetadataHydratorAPI",
                "MetadataObjectGetterAPI",
            ],
        ),
    ],
)
def test_metadata_package_import_surface(module_name: str, expected_names: list[str]) -> None:
    module = importlib.import_module(module_name)
    for expected_name in expected_names:
        assert hasattr(module, expected_name), f"{module_name} is missing {expected_name}"


def test_metadata_api_root_combines_current_public_api_roots() -> None:
    api_root = importlib.import_module("LiuXin_alpha.metadata.api")
    container_api_root = importlib.import_module(
        "LiuXin_alpha.metadata.api.containers_api"
    )
    calibre_api_root = importlib.import_module(
        "LiuXin_alpha.metadata.api.containers_api.calibre_metadata_api"
    )
    liuxin_api_root = importlib.import_module(
        "LiuXin_alpha.metadata.api.containers_api.liuxin_metadata_api"
    )
    liuxin_wemi_api_root = importlib.import_module(
        "LiuXin_alpha.metadata.api.containers_api.liuxin_wemi_metadata_api"
    )
    assert api_root.__all__ == [
        *container_api_root.__all__,
        *calibre_api_root.__all__,
        *liuxin_api_root.__all__,
        *liuxin_wemi_api_root.__all__,
    ]


def test_metadata_container_root_matches_metadata_containers_root() -> None:
    container_root = importlib.import_module("LiuXin_alpha.metadata.containers")
    metadata_containers_root = importlib.import_module(
        "LiuXin_alpha.metadata.containers.metadata_containers"
    )
    assert container_root.__all__ == metadata_containers_root.__all__


def test_metadata_api_source_scan_observes_repo_files() -> None:
    assert (METADATA_API_ROOT / "__init__.py").is_file()
    assert any(METADATA_API_ROOT.rglob("*.py"))


def test_metadata_api_source_avoids_unbounded_typing() -> None:
    pattern = re.compile(r"\b" + ("A" "ny") + r"\b")
    offenders = [
        str(path)
        for path in METADATA_API_ROOT.rglob("*.py")
        if pattern.search(path.read_text(encoding="utf-8"))
    ]
    assert offenders == []


def test_metadata_api_source_avoids_abstract_implementation_placeholders() -> None:
    offenders = [
        str(path)
        for path in METADATA_API_ROOT.rglob("*.py")
        if "raise NotImplementedError" in path.read_text(encoding="utf-8")
    ]
    assert offenders == []


def test_metadata_api_annotations_avoid_unbounded_objects() -> None:
    unbounded_names = {("A" "ny"), "object"}
    offenders: list[str] = []

    for path in METADATA_API_ROOT.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            annotations: list[ast.AST] = []
            if isinstance(node, ast.AnnAssign):
                annotations.append(node.annotation)
                if _name_from_ast(node.annotation) == "TypeAlias" and node.value is not None:
                    annotations.append(node.value)
            elif isinstance(node, ast.arg) and node.annotation is not None:
                annotations.append(node.annotation)
            elif isinstance(node, ast.FunctionDef) and node.returns is not None:
                annotations.append(node.returns)

            for annotation in annotations:
                used_names = {
                    child.id for child in ast.walk(annotation) if isinstance(child, ast.Name)
                }
                if used_names & unbounded_names:
                    offenders.append(f"{path}:{node.lineno}")

    assert offenders == []


def _name_from_ast(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    return None


@pytest.mark.parametrize(
    ("module_name", "concrete_names"),
    [
        (
            "LiuXin_alpha.metadata.api",
            [
                "WorkMetadata",
                "WorkTitle",
                "WorkLanguagesContainer",
                "WorkAgentCredit",
            ],
        ),
        (
            "LiuXin_alpha.metadata.api.containers_api",
            [
                "WorkMetadata",
                "WorkTitle",
                "WorkLanguagesContainer",
                "WorkAgentCredit",
            ],
        ),
        (
            "LiuXin_alpha.metadata.api.containers_api.wemi_containers_api",
            [
                "WorkMetadata",
                "WorkTitle",
                "WorkLanguagesContainer",
                "WorkAgentCredit",
            ],
        ),
    ],
)
def test_metadata_api_surfaces_do_not_export_concrete_containers(
    module_name: str,
    concrete_names: list[str],
) -> None:
    module = importlib.import_module(module_name)
    for concrete_name in concrete_names:
        assert not hasattr(module, concrete_name), f"{module_name} still exports {concrete_name}"


@pytest.mark.parametrize(
    "module_name",
    [
        "LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.expression_containers.expression_metadata_api",
        "LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.manifestation_containers.manifestation_metadata_api",
    ],
)
def test_metadata_leaf_all_names_exist(module_name: str) -> None:
    module = importlib.import_module(module_name)
    for exported_name in module.__all__:
        assert hasattr(module, exported_name), f"{module_name} exports missing {exported_name}"
