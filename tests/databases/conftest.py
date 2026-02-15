from __future__ import annotations

from pathlib import Path
from typing import Tuple

import pytest

from .calibre_fixture_libraries import (
    CalibreFixtureSpec,
    discover_calibre_fixtures,
    extract_library_zip,
    find_data_repo_root,
)


@pytest.fixture(scope="session")
def liuxin_alpha_data_root() -> Path | None:
    """Return the LiuXin_alpha_data checkout root, if available."""

    return find_data_repo_root()


@pytest.fixture(scope="session")
def calibre_fixture_specs(liuxin_alpha_data_root: Path | None) -> Tuple[CalibreFixtureSpec, ...]:
    """All discovered Calibre fixture libraries in LiuXin_alpha_data."""

    if liuxin_alpha_data_root is None:
        return tuple()
    return tuple(discover_calibre_fixtures(liuxin_alpha_data_root))


@pytest.fixture
def calibre_fixture_library_root(
    tmp_path: Path,
    request: pytest.FixtureRequest,
    calibre_fixture_specs: Tuple[CalibreFixtureSpec, ...],
) -> Path:
    """Extract a Calibre fixture library and return its root path.

    Indirect parametrization is supported:

        @pytest.mark.parametrize("calibre_fixture_library_root", [spec], indirect=True)

    If not parametrized, the first discovered fixture is used (useful for
    interactive debugging).
    """

    if not calibre_fixture_specs:
        pytest.skip("LiuXin_alpha_data/calibre_libraries not available")

    spec = getattr(request, "param", None)
    if not isinstance(spec, CalibreFixtureSpec):
        spec = calibre_fixture_specs[0]

    out_dir = tmp_path / f"{spec.schema_key}_{spec.name}"
    return extract_library_zip(spec, out_dir)
