from __future__ import annotations

import io

import pytest


def test_registry_exposes_builtin_entries_and_extension_aliases() -> None:
    from LiuXin_alpha.metadata.file_sources import registry

    assert registry.normalize_file_type(".XHTML") == "html"
    assert registry.normalize_file_type("AZW") == "mobi"
    assert registry.normalize_file_type("ODS") == "odt"

    known = registry.known_metadata_file_types()
    assert "epub" in known
    assert "mobi" in known
    assert "pdf" in known

    html_entries = registry.iter_metadata_reader_entries_for_extension("xhtml")
    assert any(entry.name == "HTMLMetadataReader" for entry in html_entries)
    assert all("html" in entry.normalized_file_types for entry in html_entries)


def test_registry_rejects_invalid_runtime_plugins() -> None:
    from LiuXin_alpha.metadata.file_sources import registry

    with pytest.raises(TypeError, match="must be a class"):
        registry.register_metadata_reader_plugin(object())

    class _NoFileTypes:
        def get_metadata(self, stream=None, ftype=None):
            return None

    with pytest.raises(ValueError, match="file type"):
        registry.register_metadata_reader_plugin(_NoFileTypes)

    class _NoGetMetadata:
        file_types = {"bad"}

    with pytest.raises(TypeError, match="get_metadata"):
        registry.register_metadata_reader_plugin(_NoGetMetadata)


def test_runtime_registered_reader_is_visible_to_dispatcher() -> None:
    import LiuXin_alpha.metadata.file_sources as dispatcher
    from LiuXin_alpha.metadata.file_sources import registry

    class _RuntimeReader:
        file_types = frozenset({"zzmeta"})
        inplace_run_cost = "low"
        __module__ = "runtime.plugins"

        def __init__(self, _context) -> None:
            pass

        @staticmethod
        def get_metadata(stream=None, ftype=None):
            return ("runtime", stream.read(), ftype)

    initial_revision = registry.get_metadata_reader_registry_revision()
    try:
        returned = registry.register_metadata_reader_plugin(_RuntimeReader)
        assert returned is _RuntimeReader
        assert registry.get_metadata_reader_registry_revision() > initial_revision

        entries = registry.iter_metadata_reader_entries_for_extension("zzmeta")
        assert [entry.name for entry in entries] == ["_RuntimeReader"]

        plugins = dispatcher.get_plugins_for_extension("zzmeta")
        assert [plugin.module_name for plugin in plugins] == ["_RuntimeReader"]
        assert "ZZMETA" in dispatcher.valid_file_formats
        assert dispatcher.get_metadata(io.BytesIO(b"payload"), force_type="zzmeta") == (
            "runtime",
            b"payload",
            "zzmeta",
        )

        with pytest.raises(ValueError, match="already registered"):
            registry.register_metadata_reader_plugin(_RuntimeReader)
    finally:
        registry.unregister_metadata_reader_plugin(_RuntimeReader)
        dispatcher.valid_plugins.clear()
        dispatcher.valid_file_formats.clear()


def test_runtime_registered_reader_decorator_and_replace() -> None:
    from LiuXin_alpha.metadata.file_sources import registry

    @registry.register_metadata_reader_plugin()
    class _DecoratedReader:
        file_types = frozenset({"decorated"})

        def __init__(self, _context) -> None:
            pass

        @staticmethod
        def get_metadata(stream=None, ftype=None):
            return ("old", ftype)

    try:
        assert registry.iter_metadata_reader_entries_for_extension("decorated")[0].plugin_cls is _DecoratedReader

        @registry.register_metadata_reader_plugin(replace=True)
        class _DecoratedReader:
            file_types = frozenset({"decorated"})
            inplace_run_cost = "medium"

            def __init__(self, _context) -> None:
                pass

            @staticmethod
            def get_metadata(stream=None, ftype=None):
                return ("new", ftype)

        entries = registry.iter_metadata_reader_entries_for_extension("decorated")
        assert len(entries) == 1
        assert entries[0].plugin_cls is _DecoratedReader
        assert entries[0].inplace_run_cost == "medium"
    finally:
        registry.unregister_metadata_reader_plugin(_DecoratedReader)
