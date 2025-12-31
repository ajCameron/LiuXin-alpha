from __future__ import annotations

import os
from pathlib import Path


def test_path_helpers_round_trip(tmp_path: Path) -> None:
    from LiuXin_alpha.utils.paths import make_long_path_useable, find_mount_point

    p = tmp_path / "a" / "b"
    p.mkdir(parents=True)
    lp = make_long_path_useable(str(p))
    assert isinstance(lp, str)
    # On non-Windows we expect identity
    assert lp == str(p)

    mp = find_mount_point(str(p))
    assert os.path.isdir(mp)
