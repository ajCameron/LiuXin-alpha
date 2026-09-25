"""
Provide session discovery and per-test extraction of Calibre fixture libraries.

The optional external data checkout is resolved once per session. Tests requesting
an extracted library skip when discovery is empty; each extraction uses pytest's
temporary directory.
"""
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
    """
    Resolve the optional LiuXin_alpha_data checkout once per pytest session.

    Example:
        Request liuxin_alpha_data_root as a test argument; a missing checkout yields None.


    :return: Resolved checkout Path or None; this fixture itself does not skip.
    """

    return find_data_repo_root()


@pytest.fixture(scope="session")
def calibre_fixture_specs(liuxin_alpha_data_root: Path | None) -> Tuple[CalibreFixtureSpec, ...]:
    """
    Discover zipped fixture specifications once per pytest session.

    Example:
        Request calibre_fixture_specs as a test argument to enumerate available libraries.


    :param liuxin_alpha_data_root: Session fixture giving the resolved data checkout, or
        None.
    :return: Tuple of specifications in discovery order, or an empty tuple when
        unavailable.
    """

    if liuxin_alpha_data_root is None:
        return tuple()
    return tuple(discover_calibre_fixtures(liuxin_alpha_data_root))


@pytest.fixture
def calibre_fixture_library_root(
    tmp_path: Path,
    request: pytest.FixtureRequest,
    calibre_fixture_specs: Tuple[CalibreFixtureSpec, ...],
) -> Path:
    """
    Extract the requested or first discovered library into the test temporary directory.

    An empty discovered tuple always skips, even if request.param supplies a
    specification. Otherwise a CalibreFixtureSpec parameter selects that fixture; absent
    or differently typed parameters select the first discovered one. Extraction errors
    propagate and pytest owns temporary-directory cleanup.

    Example:
        Select a fixture with indirect parametrization::

            @pytest.mark.parametrize("calibre_fixture_library_root", [spec], indirect=True)
            def test_library(calibre_fixture_library_root):
                assert (calibre_fixture_library_root / "metadata.db").exists()


    :param tmp_path: Pytest-provided per-test temporary directory.
    :param request: Pytest request; optional param may be a CalibreFixtureSpec.
    :param calibre_fixture_specs: Discovered fixture tuple used for availability and
        default selection.
    :return: Extracted library root containing metadata.db.
    """

    if not calibre_fixture_specs:
        pytest.skip("LiuXin_alpha_data/calibre_libraries not available")

    spec = getattr(request, "param", None)
    if not isinstance(spec, CalibreFixtureSpec):
        spec = calibre_fixture_specs[0]

    out_dir = tmp_path / f"{spec.schema_key}_{spec.name}"
    return extract_library_zip(spec, out_dir)
