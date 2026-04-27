from __future__ import annotations

import importlib

import pytest


FAMILY_MODULES = [
    "agent_credit_containers",
    "agent_participation",
    "titles_containers",
    "notes_containers",
    "labels_containers",
    "genres_containers",
    "subjects_containers",
    "identifier_containers",
    "languages_containers",
]


@pytest.mark.parametrize("family_module", FAMILY_MODULES)
def test_api_and_impl_family_modules_export_same_public_names(family_module: str) -> None:
    api_module = importlib.import_module(
        f"LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.{family_module}"
    )
    impl_module = importlib.import_module(
        f"LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.{family_module}"
    )
    assert api_module.__all__ == impl_module.__all__
