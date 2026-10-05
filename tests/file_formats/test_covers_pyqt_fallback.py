"""
Provide test covers pyqt fallback utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test covers pyqt fallback through a consuming regression::

        python -m pytest -q tests/file_formats/test_covers_pyqt_fallback.py
"""
from __future__ import annotations

import pytest

from LiuXin_alpha.file_formats import covers


def test_create_cover_returns_bytes_without_pyqt(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the test create cover returns bytes without pyqt operation under explicit file-format and conversion rules.

    Example:
        Exercise test create cover returns bytes without pyqt through a consuming regression::

            python -m pytest -q tests/file_formats/test_covers_pyqt_fallback.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    monkeypatch.setattr(covers, "HAS_QT", False)

    data = covers.create_cover(
        title="Fallback Title",
        authors=["Fallback Author"],
        series="Fallback Series",
        series_index=2,
    )

    assert isinstance(data, (bytes, bytearray))
    assert len(data) > 0


def test_generate_cover_survives_template_failures_without_pyqt(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the test generate cover survives template failures without pyqt operation under explicit file-format and conversion rules.

    Example:
        Exercise test generate cover survives template failures without pyqt through a consuming regression::

            python -m pytest -q tests/file_formats/test_covers_pyqt_fallback.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    monkeypatch.setattr(covers, "HAS_QT", False)

    def boom(*_args, **_kwargs):
        """
        Perform the boom operation under explicit file-format and conversion rules.

        Example:
            Exercise test generate cover survives template failures without pyqt.boom through a consuming regression::

                python -m pytest -q tests/file_formats/test_covers_pyqt_fallback.py


        :param _args: Value supplied for args under the utility contract.
        :param _kwargs: Value supplied for kwargs under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise RuntimeError("template formatter boom")

    monkeypatch.setattr(covers, "format_text", boom)
    mi = covers.Metadata("Template Failure", ["Author A", "Author B"])
    mi.series = "Series"
    mi.series_index = 3

    data = covers.generate_cover(mi)

    assert isinstance(data, (bytes, bytearray))
    assert len(data) > 0


def test_create_cover_as_qimage_requires_pyqt(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the test create cover as qimage requires pyqt operation under explicit file-format and conversion rules.

    Example:
        Exercise test create cover as qimage requires pyqt through a consuming regression::

            python -m pytest -q tests/file_formats/test_covers_pyqt_fallback.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    monkeypatch.setattr(covers, "HAS_QT", False)

    with pytest.raises(RuntimeError, match="QImage"):
        covers.create_cover(
            title="Needs Qt",
            authors=["Author"],
            as_qimage=True,
        )


def test_generate_masthead_returns_bytes_without_pyqt(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the test generate masthead returns bytes without pyqt operation under explicit file-format and conversion rules.

    Example:
        Exercise test generate masthead returns bytes without pyqt through a consuming regression::

            python -m pytest -q tests/file_formats/test_covers_pyqt_fallback.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    monkeypatch.setattr(covers, "HAS_QT", False)

    data = covers.generate_masthead("Masthead")

    assert isinstance(data, (bytes, bytearray))
    assert len(data) > 0
