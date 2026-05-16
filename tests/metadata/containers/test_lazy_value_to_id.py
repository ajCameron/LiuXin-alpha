from __future__ import annotations

from copy import deepcopy

from LiuXin_alpha.metadata.containers.metadata_containers.lazy_value_to_id import (
    LazyValueToID,
)


def test_lazy_value_to_id_materializes_once_and_behaves_like_mapping() -> None:
    calls = 0

    def load_values() -> dict[str, int]:
        nonlocal calls
        calls += 1
        return {"one": 1, "two": 2}

    values = LazyValueToID(load_values, label="tags")

    assert values.label == "tags"
    assert values.loaded is False
    assert str(values) == "<lazy tags>"

    assert values["one"] == 1
    values["three"] = 3
    del values["two"]

    assert values.loaded is True
    assert calls == 1
    assert list(values) == ["one", "three"]
    assert len(values) == 2
    assert bool(values) is True
    assert deepcopy(values) == {"one": 1, "three": 3}
    assert repr(values).startswith("OrderedDict(")
    assert "'one'" in repr(values)
    assert "'three'" in repr(values)
