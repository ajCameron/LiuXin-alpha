"""
Provide test paths utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test paths through a consuming regression::

        python -m pytest -q tests/utils/test_paths.py
"""
from __future__ import annotations

import os
from pathlib import Path


def test_path_helpers_round_trip(tmp_path: Path) -> None:
    """
    Perform the test path helpers round trip utility operation under explicit compatibility rules.

    Example:
        Exercise test path helpers round trip through a consuming regression::

            python -m pytest -q tests/utils/test_paths.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.paths import make_long_path_useable, find_mount_point

    p = tmp_path / "a" / "b"
    p.mkdir(parents=True)
    lp = make_long_path_useable(str(p))
    assert isinstance(lp, str)
    # On non-Windows we expect identity
    assert lp == str(p)

    mp = find_mount_point(str(p))
    assert os.path.isdir(mp)
