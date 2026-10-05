"""
Provide test mine types utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test mine types through a consuming regression::

        python -m pytest -q tests/utils/test_mine_types.py
"""
from __future__ import annotations

import mimetypes
from pathlib import Path

import pytest


def test_guess_type_inits_once_and_can_use_custom_mime_types(monkeypatch, tmp_path: Path) -> None:
    """
    Perform the test guess type inits once and can use custom mime types utility operation under explicit compatibility rules.

    Example:
        Exercise test guess type inits once and can use custom mime types through a consuming regression::

            python -m pytest -q tests/utils/test_mine_types.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from LiuXin_alpha.utils import mine_types

    # Make a minimal mime.types file so mimetypes can read from it.
    mt = tmp_path / "mime.types"
    mt.write_text("text/x-foo foo\napplication/x-bar bar\n", encoding="utf-8")

    calls: list[tuple] = []

    def fake_init(files=()):
        """
        Perform the fake init utility operation under explicit compatibility rules.

        Example:
            Exercise test guess type inits once and can use custom mime types.fake init through a consuming regression::

                python -m pytest -q tests/utils/test_mine_types.py


        :param files: Value supplied for files under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        calls.append(tuple(files))
        # Also call real init so guess_type works for common types.
        return mimetypes.init(files)

    monkeypatch.setattr(mine_types, "_mt_inited", False)
    monkeypatch.setattr(mine_types, "_mt_init", fake_init)
    monkeypatch.setattr(mine_types, "resource_to_path", lambda *a, **k: str(mt))

    # First call: triggers init
    assert mine_types.guess_type("thing.foo")[0] == "text/x-foo"
    # Second call: should not re-init
    assert mine_types.guess_type("thing.bar")[0] == "application/x-bar"
    assert len(calls) == 1


def test_guess_type_wrapper_returns_tuple() -> None:
    """
    Perform the test guess type wrapper returns tuple utility operation under explicit compatibility rules.

    Example:
        Exercise test guess type wrapper returns tuple through a consuming regression::

            python -m pytest -q tests/utils/test_mine_types.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.mine_types import guess_type

    t = guess_type("file.txt")
    assert isinstance(t, tuple)
    assert len(t) == 2



import importlib
import mimetypes
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _reset_global_mimetypes() -> None:
    """
    mine_types works by mutating the stdlib mimetypes module; keep tests isolated.

    Example:
        Exercise  reset global mimetypes through a consuming regression::

            python -m pytest -q tests/utils/test_mine_types.py


    :return: An iterator yielding the normalized values described above.
    """
    yield
    importlib.reload(mimetypes)


def test_guess_type_inits_once_and_can_use_custom_mime_types(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """
    Perform the test guess type inits once and can use custom mime types utility operation under explicit compatibility rules.

    Example:
        Exercise test guess type inits once and can use custom mime types through a consuming regression::

            python -m pytest -q tests/utils/test_mine_types.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from LiuXin_alpha.utils import mine_types

    # Minimal mime.types file so mimetypes can read from it.
    mt = tmp_path / "mime.types"
    mt.write_text("text/x-foo foo\napplication/x-bar bar\n", encoding="utf-8")

    calls: list[tuple[str, ...]] = []
    real_init = mimetypes.init

    def recording_init(files=()):
        """
        Perform the recording init utility operation under explicit compatibility rules.

        Example:
            Exercise test guess type inits once and can use custom mime types.recording init through a consuming regression::

                python -m pytest -q tests/utils/test_mine_types.py


        :param files: Value supplied for files under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        calls.append(tuple(str(f) for f in files or ()))
        return real_init(files)

    monkeypatch.setattr(mine_types, "_mt_inited", False)
    monkeypatch.setattr(mine_types, "resource_to_path", lambda *a, **k: str(mt))
    monkeypatch.setattr(mimetypes, "init", recording_init)

    # First call: triggers init
    assert mine_types.guess_type("thing.foo")[0] == "text/x-foo"
    # Second call: should not re-init
    assert mine_types.guess_type("thing.bar")[0] == "application/x-bar"
    assert len(calls) == 1


def test_guess_extension_has_palmreader_special_case(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the test guess extension has palmreader special case utility operation under explicit compatibility rules.

    Example:
        Exercise test guess extension has palmreader special case through a consuming regression::

            python -m pytest -q tests/utils/test_mine_types.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils import mine_types

    monkeypatch.setattr(mine_types, "_mt_inited", False)

    assert mine_types.guess_extension("application/x-palmreader") == ".pdb"


def test_mine_types_integration_uses_calibre_mime_types(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Integration check: the shipped calibre mime.types drives the stdlib mappings.

    Example:
        Exercise test mine types integration uses calibre mime types through a consuming regression::

            python -m pytest -q tests/utils/test_mine_types.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """

    from LiuXin_alpha.utils import mine_types

    monkeypatch.setattr(mine_types, "_mt_inited", False)

    # These are present in calibre's bundled mime.types.
    assert mine_types.guess_type("book.epub")[0] == "application/epub+zip"
    assert mine_types.guess_type("book.pobi")[0] == "application/x-mobipocket-subscription"


def test_guess_type_wrapper_returns_tuple(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the test guess type wrapper returns tuple utility operation under explicit compatibility rules.

    Example:
        Exercise test guess type wrapper returns tuple through a consuming regression::

            python -m pytest -q tests/utils/test_mine_types.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils import mine_types

    monkeypatch.setattr(mine_types, "_mt_inited", False)

    t = mine_types.guess_type("file.txt")
    assert isinstance(t, tuple)
    assert len(t) == 2
