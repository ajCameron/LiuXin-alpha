from __future__ import annotations

import pytest

from LiuXin_alpha.file_formats import covers


def test_create_cover_returns_bytes_without_pyqt(monkeypatch: pytest.MonkeyPatch) -> None:
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
    monkeypatch.setattr(covers, "HAS_QT", False)

    def boom(*_args, **_kwargs):
        raise RuntimeError("template formatter boom")

    monkeypatch.setattr(covers, "format_text", boom)
    mi = covers.Metadata("Template Failure", ["Author A", "Author B"])
    mi.series = "Series"
    mi.series_index = 3

    data = covers.generate_cover(mi)

    assert isinstance(data, (bytes, bytearray))
    assert len(data) > 0


def test_create_cover_as_qimage_requires_pyqt(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(covers, "HAS_QT", False)

    with pytest.raises(RuntimeError, match="QImage"):
        covers.create_cover(
            title="Needs Qt",
            authors=["Author"],
            as_qimage=True,
        )


def test_generate_masthead_returns_bytes_without_pyqt(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(covers, "HAS_QT", False)

    data = covers.generate_masthead("Masthead")

    assert isinstance(data, (bytes, bytearray))
    assert len(data) > 0
