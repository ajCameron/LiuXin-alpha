from __future__ import annotations

import pytest


def test_resources_are_explicitly_not_implemented_yet() -> None:
    from LiuXin_alpha.utils import resources

    with pytest.raises(NotImplementedError):
        resources.get_image_path("x.png")
    with pytest.raises(NotImplementedError):
        resources.get_path("x")
    with pytest.raises(NotImplementedError):
        resources.resource_to_path("x")
