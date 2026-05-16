from __future__ import annotations

from LiuXin_alpha.metadata.api import UnloadedMetadataProjectionError


def test_unloaded_projection_error_records_relation_and_dependency_context() -> None:
    error = UnloadedMetadataProjectionError("tags", ("languages", "agents"))

    assert error.relation_key == "tags"
    assert error.unloaded_dependencies == ("languages", "agents")
    assert "Metadata projection 'tags' has unloaded lazy data" in str(error)
    assert "Call load('tags')" in str(error)
    assert "Unloaded dependencies: languages, agents." in str(error)
