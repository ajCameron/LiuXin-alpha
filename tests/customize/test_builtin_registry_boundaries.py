"""Verify lightweight built-in namespaces and explicit plugin composition."""

from __future__ import annotations


def test_builtins_namespace_does_not_assemble_plugins() -> None:
    """Importing the package alone must not expose the eager registry."""
    import LiuXin_alpha.customize.builtins as builtins_package

    assert builtins_package.__all__ == []
    assert not hasattr(builtins_package, "plugins")


def test_explicit_registry_preserves_conversion_plugins() -> None:
    """The explicit composition boundary retains representative converters."""
    from LiuXin_alpha.customize.builtins.registry import plugins

    names = {plugin.__name__ for plugin in plugins}

    assert {"EPUBInput", "EPUBOutput", "MOBIInput", "MOBIOutput"} <= names
