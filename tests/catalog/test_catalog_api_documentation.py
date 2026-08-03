from __future__ import annotations

import ast
import inspect
import re
import textwrap

from pathlib import Path

from LiuXin_alpha.catalog.api import (
    CatalogAPI,
    CatalogMetadataToolsAPI,
    FieldMetadataAPI,
    MatchResult,
)
from LiuXin_alpha.catalog.api.matching_api import CatalogMatchingAPI
from LiuXin_alpha.catalog.api import matching_api
from LiuXin_alpha.catalog.api import metadata_tools_api
from LiuXin_alpha.catalog.api import mutations_api
from LiuXin_alpha.catalog.api import repositories
from LiuXin_alpha.catalog.api import retrieval
from LiuXin_alpha.catalog import api as catalog_api
from LiuXin_alpha.catalog.api.mutations_api import MetadataWriterAPI
from LiuXin_alpha.catalog.api.repositories import (
    BaseRepositoryAPI,
    ItemRepositoryAPI,
    ManifestationRepositoryAPI,
    TitleRepositoryAPI,
)
from LiuXin_alpha.catalog.api.retrieval import BundleRetrieverAPI


REPO_ROOT = Path(__file__).resolve().parents[2]
API_ROOT = REPO_ROOT / "src" / "LiuXin_alpha" / "catalog" / "api"
USAGE_GUIDE = REPO_ROOT / "dev-docs" / "catalog-api-usage.md"
EMPTY_DOC_FIELD = re.compile(
    r"(?m)^\s*:(?:param\s+[^:]+|return):\s*$"
)
PYTHON_FENCE = re.compile(r"```python\n(.*?)```", re.DOTALL)


def test_catalog_api_docstrings_have_no_placeholder_fields() -> None:
    offenders: list[str] = []
    for path in sorted(API_ROOT.rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        if EMPTY_DOC_FIELD.search(text):
            offenders.append(str(path.relative_to(REPO_ROOT)))

    assert offenders == []


def test_public_catalog_api_methods_have_substantive_docstrings() -> None:
    offenders: list[str] = []

    for path in sorted(API_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if node.name.startswith("_"):
                continue
            if any(
                isinstance(decorator, ast.Name) and decorator.id == "overload"
                for decorator in node.decorator_list
            ):
                continue
            doc = ast.get_docstring(node) or ""
            if len(doc.split()) < 5:
                relative_path = path.relative_to(REPO_ROOT)
                offenders.append(f"{relative_path}:{node.lineno}:{node.name}")

    assert offenders == []


def test_primary_catalog_contracts_explain_usage_with_examples() -> None:
    contracts = (
        CatalogAPI,
        CatalogMetadataToolsAPI,
        FieldMetadataAPI,
        BaseRepositoryAPI,
        ItemRepositoryAPI,
        ManifestationRepositoryAPI,
        TitleRepositoryAPI,
        CatalogMatchingAPI,
        BundleRetrieverAPI,
        MetadataWriterAPI,
        MatchResult,
    )

    for contract in contracts:
        doc = inspect.getdoc(contract) or ""
        assert len(doc.split()) >= 20, contract.__qualname__
        assert "example" in doc.casefold(), contract.__qualname__


def test_every_exported_catalog_api_class_has_a_substantive_docstring() -> None:
    modules = (
        catalog_api,
        repositories,
        matching_api,
        retrieval,
        mutations_api,
        metadata_tools_api,
    )
    offenders: list[str] = []

    for module in modules:
        for name in module.__all__:
            value = getattr(module, name)
            if not inspect.isclass(value):
                continue
            doc = inspect.getdoc(value) or ""
            if len(doc.split()) < 12:
                offenders.append(f"{module.__name__}.{name}")

    assert offenders == []


def test_catalog_usage_guide_python_examples_are_syntax_valid() -> None:
    text = USAGE_GUIDE.read_text(encoding="utf-8")
    examples = PYTHON_FENCE.findall(text)

    assert len(examples) >= 12
    for index, example in enumerate(examples, start=1):
        source = textwrap.dedent(example)
        ast.parse(source, filename=f"{USAGE_GUIDE.name}:example-{index}")
