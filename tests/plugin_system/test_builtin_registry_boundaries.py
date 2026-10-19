"""Verify lightweight built-in namespaces and explicit plugin composition."""

from __future__ import annotations


def test_builtins_namespace_does_not_assemble_plugins() -> None:
    """Importing the package alone must not expose the eager registry."""
    import LiuXin_alpha.plugin_system.builtins as builtins_package

    assert builtins_package.__all__ == []
    assert not hasattr(builtins_package, "plugins")


def test_explicit_registry_preserves_conversion_plugins() -> None:
    """The explicit composition boundary retains representative converters."""
    from LiuXin_alpha.plugin_system.builtins.registry import plugins

    names = {plugin.__name__ for plugin in plugins}

    assert {"EPUBInput", "EPUBOutput", "MOBIInput", "MOBIOutput"} <= names


def test_application_registry_extends_the_standalone_file_catalogue() -> None:
    """File provider membership and tie order have one composition owner."""
    from LiuXin_alpha.plugin_system.builtins.registry import plugins
    from LiuXin_alpha.file_formats.plugin_defaults import builtin_plugins

    file_plugins = builtin_plugins()
    assert plugins[: len(file_plugins)] == file_plugins
    assert all(plugins.count(provider) == 1 for provider in file_plugins)


def test_application_provider_lookup_uses_the_catalogue() -> None:
    """The application takes a catalogue snapshot without owning a second list."""
    from LiuXin_alpha.plugin_system import application
    from LiuXin_alpha.plugin_system.builtins.registry import plugins

    snapshot = application.builtin_plugins()
    assert snapshot == plugins
    assert snapshot is not plugins
