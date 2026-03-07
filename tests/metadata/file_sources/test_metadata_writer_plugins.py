from __future__ import annotations

import io

from LiuXin_alpha.metadata.utils import calibreMetaInformation


def _plugin_map():
    from LiuXin_alpha.customize.builtins.metadata_writers import get_metadata_set_plugins

    return {plugin.__name__: plugin for plugin in get_metadata_set_plugins()}


def test_metadata_writer_plugins_import_and_expose_txtz_htmlz() -> None:
    plugins = _plugin_map()
    assert "HTMLZMetadataWriter" in plugins
    assert "TXTZMetadataWriter" in plugins


def test_htmlz_writer_delegates_to_extz_set_metadata(monkeypatch) -> None:
    import LiuXin_alpha.customize.builtins.metadata_writers as writers_mod

    calls = {}
    monkeypatch.setattr(
        writers_mod,
        "extz_set_metadata",
        lambda stream, mi: calls.update({"stream": stream, "mi": mi}),
    )

    cls = _plugin_map()["HTMLZMetadataWriter"]
    writer = cls(None)
    stream = io.BytesIO(b"zip")
    mi = calibreMetaInformation("HTMLZ Title", ["Author"])
    writer.set_metadata(stream, mi, "htmlz")

    assert calls["stream"] is stream
    assert calls["mi"] is mi


def test_txtz_writer_delegates_to_txtz_set_metadata(monkeypatch) -> None:
    import LiuXin_alpha.customize.builtins.metadata_writers as writers_mod

    calls = {}
    monkeypatch.setattr(
        writers_mod,
        "txtz_set_metadata",
        lambda stream, mi: calls.update({"stream": stream, "mi": mi}),
    )

    cls = _plugin_map()["TXTZMetadataWriter"]
    writer = cls(None)
    stream = io.BytesIO(b"zip")
    mi = calibreMetaInformation("TXTZ Title", ["Author"])
    writer.set_metadata(stream, mi, "txtz")

    assert calls["stream"] is stream
    assert calls["mi"] is mi
