from __future__ import annotations

import importlib

import pytest


FAMILY_MODULES = [
    ("agent_containers.agent_credit_containers", "agent_credit_containers"),
    ("agent_containers.agent_participation", "agent_participation"),
    ("metadata_additional_containers_api.titles_containers", "titles_containers"),
    ("metadata_additional_containers_api.notes_containers", "notes_containers"),
    ("metadata_additional_containers_api.labels_containers", "labels_containers"),
    ("metadata_additional_containers_api.genres_containers", "genres_containers"),
    ("metadata_additional_containers_api.subjects_containers", "subjects_containers"),
    ("metadata_additional_containers_api.identifier_containers", "identifier_containers"),
    ("metadata_additional_containers_api.languages_containers", "languages_containers"),
]


@pytest.mark.parametrize(("api_family_module", "impl_family_module"), FAMILY_MODULES)
def test_api_and_impl_family_modules_export_same_public_names(api_family_module: str, impl_family_module: str) -> None:
    api_module = importlib.import_module(
        f"LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.{api_family_module}"
    )
    impl_module = importlib.import_module(
        f"LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.{impl_family_module}"
    )
    assert api_module.__all__ == impl_module.__all__
