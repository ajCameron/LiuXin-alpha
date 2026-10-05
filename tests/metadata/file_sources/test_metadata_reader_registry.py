"""
Verify metadata reader registration, lookup, revision and dispatch behavior.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test metadata reader registry through its owning regression module::

        python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py
"""
from __future__ import annotations

import io

import pytest


def test_registry_exposes_builtin_entries_and_extension_aliases() -> None:
    """
    Verify registry exposes builtin entries and extension aliases.

    Example:
        Exercise test registry exposes builtin entries and extension aliases through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify registry rejects invalid runtime plugins.

    Example:
        Exercise test registry rejects invalid runtime plugins through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources import registry

    with pytest.raises(TypeError, match="must be a class"):
        registry.register_metadata_reader_plugin(object())

    class _NoFileTypes:
        """
        Provide the NoFileTypes test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test registry rejects invalid runtime plugins.NoFileTypes through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py
        """
        def get_metadata(self, stream=None, ftype=None):
            """
            Return metadata from deterministic test state.

            Example:
                Exercise test registry rejects invalid runtime plugins.NoFileTypes.get metadata through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py


            :param stream: Value supplied for stream in the focused test operation.
            :param ftype: Value supplied for ftype in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            return None

    with pytest.raises(ValueError, match="file type"):
        registry.register_metadata_reader_plugin(_NoFileTypes)

    class _NoGetMetadata:
        """
        Provide the NoGetMetadata test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test registry rejects invalid runtime plugins.NoGetMetadata through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py
        """
        file_types = {"bad"}

    with pytest.raises(TypeError, match="get_metadata"):
        registry.register_metadata_reader_plugin(_NoGetMetadata)


def test_runtime_registered_reader_is_visible_to_dispatcher() -> None:
    """
    Verify runtime registered reader remains visible to dispatcher.

    Example:
        Exercise test runtime registered reader is visible to dispatcher through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py


    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.file_sources as dispatcher
    from LiuXin_alpha.metadata.file_sources import registry

    class _RuntimeReader:
        """
        Provide the RuntimeReader test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test runtime registered reader is visible to dispatcher.RuntimeReader through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py
        """
        file_types = frozenset({"zzmeta"})
        inplace_run_cost = "low"
        __module__ = "runtime.plugins"

        def __init__(self, _context) -> None:
            """
            Initialize the RuntimeReader test double.

            Example:
                Exercise test runtime registered reader is visible to dispatcher.RuntimeReader.init through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py


            :param _context: Value supplied for context in the focused test operation.
            :return: None; the function records state or raises through its assertions.
            """
            pass

        @staticmethod
        def get_metadata(stream=None, ftype=None):
            """
            Return metadata from deterministic test state.

            Example:
                Exercise test runtime registered reader is visible to dispatcher.RuntimeReader.get metadata through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py


            :param stream: Value supplied for stream in the focused test operation.
            :param ftype: Value supplied for ftype in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
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
    """
    Verify runtime registered reader decorator and replace.

    Example:
        Exercise test runtime registered reader decorator and replace through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources import registry

    @registry.register_metadata_reader_plugin()
    class _DecoratedReader:
        """
        Provide the DecoratedReader test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test runtime registered reader decorator and replace.DecoratedReader through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py
        """
        file_types = frozenset({"decorated"})

        def __init__(self, _context) -> None:
            """
            Initialize the DecoratedReader test double.

            Example:
                Exercise test runtime registered reader decorator and replace.DecoratedReader.init through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py


            :param _context: Value supplied for context in the focused test operation.
            :return: None; the function records state or raises through its assertions.
            """
            pass

        @staticmethod
        def get_metadata(stream=None, ftype=None):
            """
            Return metadata from deterministic test state.

            Example:
                Exercise test runtime registered reader decorator and replace.DecoratedReader.get metadata through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py


            :param stream: Value supplied for stream in the focused test operation.
            :param ftype: Value supplied for ftype in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            return ("old", ftype)

    try:
        assert registry.iter_metadata_reader_entries_for_extension("decorated")[0].plugin_cls is _DecoratedReader

        @registry.register_metadata_reader_plugin(replace=True)
        class _DecoratedReader:
            """
            Provide the DecoratedReader test fixture or double with explicit deterministic behavior.

            Example:
                Exercise test runtime registered reader decorator and replace.DecoratedReader through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py
            """
            file_types = frozenset({"decorated"})
            inplace_run_cost = "medium"

            def __init__(self, _context) -> None:
                """
                Initialize the DecoratedReader test double.

                Example:
                    Exercise test runtime registered reader decorator and replace.DecoratedReader.init through its owning regression module::

                        python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py


                :param _context: Value supplied for context in the focused test operation.
                :return: None; the function records state or raises through its assertions.
                """
                pass

            @staticmethod
            def get_metadata(stream=None, ftype=None):
                """
                Return metadata from deterministic test state.

                Example:
                    Exercise test runtime registered reader decorator and replace.DecoratedReader.get metadata through its owning regression module::

                        python -m pytest -q tests/metadata/file_sources/test_metadata_reader_registry.py


                :param stream: Value supplied for stream in the focused test operation.
                :param ftype: Value supplied for ftype in the focused test operation.
                :return: The deterministic value, row, identity or collection described above.
                """
                return ("new", ftype)

        entries = registry.iter_metadata_reader_entries_for_extension("decorated")
        assert len(entries) == 1
        assert entries[0].plugin_cls is _DecoratedReader
        assert entries[0].inplace_run_cost == "medium"
    finally:
        registry.unregister_metadata_reader_plugin(_DecoratedReader)
