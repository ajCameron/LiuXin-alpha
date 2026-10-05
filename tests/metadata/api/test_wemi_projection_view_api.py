"""
Verify WEMI projection views preserve source data and read-only behavior.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test wemi projection view api through its owning regression module::

        python -m pytest -q tests/metadata/api/test_wemi_projection_view_api.py
"""
from __future__ import annotations

from LiuXin_alpha.metadata.api import UnloadedMetadataProjectionError


def test_unloaded_projection_error_records_relation_and_dependency_context() -> None:
    """
    Verify unloaded projection error records relation and dependency context.

    Example:
        Exercise test unloaded projection error records relation and dependency context through its owning regression module::

            python -m pytest -q tests/metadata/api/test_wemi_projection_view_api.py


    :return: None; the function records state or raises through its assertions.
    """
    error = UnloadedMetadataProjectionError("tags", ("languages", "agents"))

    assert error.relation_key == "tags"
    assert error.unloaded_dependencies == ("languages", "agents")
    assert "Metadata projection 'tags' has unloaded lazy data" in str(error)
    assert "Call load('tags')" in str(error)
    assert "Unloaded dependencies: languages, agents." in str(error)
