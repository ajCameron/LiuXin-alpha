"""
Verify metadata writer discovery, selection and compatibility adapters.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test metadata writer plugins through its owning regression module::

        python -m pytest -q tests/metadata/file_sources/test_metadata_writer_plugins.py
"""
from __future__ import annotations

import io

from LiuXin_alpha.metadata.utils import calibreMetaInformation


def _plugin_map():
    """
    Perform the plugin map test-helper operation with deterministic inputs.

    Example:
        Exercise plugin map through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_metadata_writer_plugins.py


    :return: The deterministic value, row, identity or collection described above.
    """
    from LiuXin_alpha.customize.builtins.metadata_writers import get_metadata_set_plugins

    return {plugin.__name__: plugin for plugin in get_metadata_set_plugins()}


def test_metadata_writer_plugins_import_and_expose_txtz_htmlz() -> None:
    """
    Verify metadata writer plugins import and expose txtz htmlz.

    Example:
        Exercise test metadata writer plugins import and expose txtz htmlz through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_metadata_writer_plugins.py


    :return: None; the function records state or raises through its assertions.
    """
    plugins = _plugin_map()
    assert "HTMLZMetadataWriter" in plugins
    assert "TXTZMetadataWriter" in plugins


def test_htmlz_writer_delegates_to_extz_set_metadata(monkeypatch) -> None:
    """
    Verify htmlz writer delegates to extz set metadata.

    Example:
        Exercise test htmlz writer delegates to extz set metadata through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_metadata_writer_plugins.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify txtz writer delegates to txtz set metadata.

    Example:
        Exercise test txtz writer delegates to txtz set metadata through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_metadata_writer_plugins.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
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
