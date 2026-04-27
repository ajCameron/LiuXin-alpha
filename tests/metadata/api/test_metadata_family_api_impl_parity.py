from __future__ import annotations

import importlib
from pathlib import Path

from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers import (
    WorkAgentCredit,
    WorkLanguagesContainer,
    WorkTitle,
)

API_WEMI_DIR = Path("src/LiuXin_alpha/metadata/api/metadata_container_api/wemi_containers_api")

REMOVED_API_FAMILY_MODULES = [
    API_WEMI_DIR / "agent_containers" / "agent_credit_containers.py",
    API_WEMI_DIR / "agent_containers" / "agent_participation.py",
    API_WEMI_DIR / "metadata_additional_containers_api" / "__init__.py",
    API_WEMI_DIR / "metadata_additional_containers_api" / "dates_containers.py",
    API_WEMI_DIR / "metadata_additional_containers_api" / "genres_containers.py",
    API_WEMI_DIR / "metadata_additional_containers_api" / "identifier_containers.py",
    API_WEMI_DIR / "metadata_additional_containers_api" / "labels_containers.py",
    API_WEMI_DIR / "metadata_additional_containers_api" / "languages_containers.py",
    API_WEMI_DIR / "metadata_additional_containers_api" / "notes_containers.py",
    API_WEMI_DIR / "metadata_additional_containers_api" / "ratings_containers.py",
    API_WEMI_DIR / "metadata_additional_containers_api" / "resources_containers.py",
    API_WEMI_DIR / "metadata_additional_containers_api" / "series_containers.py",
    API_WEMI_DIR / "metadata_additional_containers_api" / "subjects_containers.py",
    API_WEMI_DIR / "metadata_additional_containers_api" / "titles_containers.py",
]


def test_api_family_modules_are_not_duplicated_as_leaf_files() -> None:
    for path in REMOVED_API_FAMILY_MODULES:
        assert not path.exists(), f"API-side concrete family module still exists: {path}"


def test_api_root_reexports_canonical_container_implementations() -> None:
    api_module = importlib.import_module(
        "LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api"
    )

    assert api_module.WorkAgentCredit is WorkAgentCredit
    assert api_module.WorkLanguagesContainer is WorkLanguagesContainer
    assert api_module.WorkTitle is WorkTitle
