"""Verify standalone conversion and application-bound file-plugin behavior."""

from __future__ import annotations

import io
import json
import os
import subprocess
import sys
import textwrap
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from LiuXin_alpha.file_formats import plugin_defaults, plugin_runtime
from LiuXin_alpha.file_formats.conversion.plugin import InputFormatPlugin
from LiuXin_alpha.plugins import configuration
from LiuXin_alpha.plugins.file_types import (
    FileTypePlugin,
    MetadataReaderPlugin,
    MetadataWriterPlugin,
)


@pytest.fixture
def isolated_runtime(monkeypatch: pytest.MonkeyPatch):
    """Isolate plugin state and configuration without writing user preferences."""
    settings = {
        "plugins": {},
        "plugin_customization": {},
        "disabled_plugins": set(),
        "enabled_plugins": set(),
    }
    for module in (configuration, plugin_runtime, plugin_defaults):
        monkeypatch.setattr(module, "config", settings)
    previous = plugin_runtime._initialized_plugins
    plugin_runtime.reset_plugins()
    yield settings
    if previous is None:
        plugin_runtime.reset_plugins()
    else:
        plugin_runtime.bind_plugins(previous)


def test_standalone_html_conversion_avoids_application_composition(
    tmp_path: Path,
) -> None:
    """The real pipeline produces an EPUB without loading customization or UI."""
    script = textwrap.dedent(
        """
        import json
        import sys
        import zipfile
        from pathlib import Path
        from LiuXin_alpha.file_formats.conversion.plumber import Plumber
        from LiuXin_alpha.utils.logging import default_log

        workspace = Path(sys.argv[1])
        source = workspace / 'input.html'
        output = workspace / 'output.epub'
        source.write_text('<html><head><title>Native conversion</title></head>'
                          '<body><p>Unicode: café — 你好</p></body></html>', encoding='utf-8')
        Plumber(str(source), str(output), default_log).run()
        with zipfile.ZipFile(output) as archive:
            assert archive.read('mimetype') == b'application/epub+zip'
            assert b'Native conversion' in archive.read('content.opf')
            text = b''.join(archive.read(name) for name in archive.namelist()
                            if name.endswith(('.html', '.xhtml')))
            assert '你好'.encode() in text
        forbidden = ('LiuXin_alpha.customize', 'LiuXin_alpha.plugins.application',
                     'LiuXin_alpha.plugins.contracts', 'LiuXin_alpha.plugins.builtins',
                     'LiuXin_alpha.surfaces',
                     'LiuXin_alpha.library', 'LiuXin_alpha.core')
        assert not [name for name in sys.modules if name.startswith(forbidden)]
        print(json.dumps({'output_size': output.stat().st_size}))
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script, str(tmp_path)],
        capture_output=True,
        text=True,
        timeout=120,
        env={
            **os.environ,
            "LIUXIN_BASE_DIR": str(tmp_path),
            "PYTHONPATH": str(Path(__file__).resolve().parents[2] / "src"),
        },
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout.splitlines()[-1])["output_size"] > 0


def test_bound_plugins_preserve_selection_customization_and_disabled_state(
    isolated_runtime,
) -> None:
    """Selection keeps registration order while availability filters disabled plugins."""

    class FirstInput(InputFormatPlugin):
        name = "First input"
        file_types = {"example"}

    class SecondInput(FirstInput):
        name = "Second input"

    first, second = FirstInput(None), SecondInput(None)
    plugin_runtime.bind_plugins([first, second])
    isolated_runtime["plugin_customization"][first.name] = "configured"
    assert plugin_runtime.plugin_for_input_format("EXAMPLE") is first
    assert first.site_customization == "configured"
    isolated_runtime["disabled_plugins"].update({first.name, second.name})
    assert "example" not in plugin_runtime.available_input_formats()
    assert plugin_runtime.plugin_for_input_format("example") is first
    isolated_runtime["enabled_plugins"].add(first.name)
    assert "example" in plugin_runtime.available_input_formats()


def test_file_hooks_preserve_customization_and_postprocess_destination(
    isolated_runtime, tmp_path: Path
) -> None:
    """A bound file hook receives customization and its postprocess output is copied back."""
    original = tmp_path / "source.example"
    rewritten = tmp_path / "rewritten.example"
    original.write_text("original")

    class Rewrite(FileTypePlugin):
        name = "Rewrite"
        file_types = {"example"}
        on_postprocess = True

        def run(self, path_to_ebook: str) -> str:
            assert self.site_customization == "configured"
            rewritten.write_text(Path(path_to_ebook).read_text() + " rewritten")
            return str(rewritten)

    plugin_runtime.bind_plugins([Rewrite(None)])
    isolated_runtime["plugin_customization"]["Rewrite"] = "configured"
    assert plugin_runtime.run_plugins_on_postprocess(str(original)) == str(original)
    assert original.read_text() == "original rewritten"


def test_metadata_fallback_flags_and_rebinding(isolated_runtime) -> None:
    """Reader failures fall through; metadata contexts and rebuilt writer indexes survive."""
    writes = []

    class FailingReader(MetadataReaderPlugin):
        file_types = {"example"}

        def get_metadata(self, stream, ftype):
            stream.read()
            raise ValueError("try the next reader")

    class Reader(MetadataReaderPlugin):
        file_types = {"example"}

        def get_metadata(self, stream, ftype):
            assert stream.tell() == 0
            return SimpleNamespace(title="read", quick=self.quick)

    class Writer(MetadataWriterPlugin):
        file_types = {"example"}

        def set_metadata(self, stream, mi, ftype):
            writes.append((self.apply_null, self.force_identifiers, mi.title))

    reader, writer = Reader(None), Writer(None)
    plugin_runtime.bind_plugins([FailingReader(None), reader, writer])
    with plugin_runtime.quick_metadata:
        metadata = plugin_runtime.get_file_type_metadata(
            io.BytesIO(b"example"), "EXAMPLE"
        )
    assert metadata.quick is True
    assert plugin_runtime.quick_metadata.quick is False
    with plugin_runtime.apply_null_metadata, plugin_runtime.force_identifiers:
        plugin_runtime.set_file_type_metadata(io.BytesIO(), metadata, "example")
    assert writes == [(True, True, "read")]
    plugin_runtime.bind_plugins([reader])
    assert not plugin_runtime.can_set_metadata("example")
    assert not plugin_runtime.metadata_writers()


def test_installed_zip_converter_is_available_without_application_host(
    isolated_runtime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Persisted ZIP plugins use current conversion imports and precede built-ins."""
    from LiuXin_alpha.plugins.zipplugin import loader

    plugin_name = "lxa_runtime_external"
    archive_path = tmp_path / "external.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr(f"plugin-import-name-{plugin_name}.txt", "")
        archive.writestr(
            "__init__.py",
            "from LiuXin_alpha.file_formats.conversion.plugin import InputFormatPlugin\n"
            "class ExternalInput(InputFormatPlugin):\n"
            "    name = 'External runtime input'\n"
            "    file_types = {'epub'}\n",
        )
    isolated_runtime["plugins"]["External runtime input"] = str(archive_path)
    isolated_runtime["plugin_customization"]["External runtime input"] = "configured"
    monkeypatch.setattr(loader, "loaded_plugins", {})
    try:
        selected = plugin_runtime.plugin_for_input_format("epub")
        assert selected.name == "External runtime input"
        assert selected.site_customization == "configured"
        assert selected.plugin_path == str(archive_path)
        assert isinstance(selected, InputFormatPlugin)
    finally:
        prefix = "calibre_plugins." + plugin_name
        for module_name in tuple(sys.modules):
            if module_name == prefix or module_name.startswith(prefix + "."):
                sys.modules.pop(module_name, None)


def test_rendering_serializer_retains_image_bytes() -> None:
    """The shared serializer emits image bytes instead of silently dropping pixels."""
    from PIL import Image

    from LiuXin_alpha.file_formats.rendering import pixmap_to_data

    encoded = pixmap_to_data(Image.new("RGBA", (4, 3), (20, 30, 40, 128)))
    with Image.open(io.BytesIO(encoded)) as image:
        assert image.size == (4, 3)
        assert image.getpixel((0, 0)) == (20, 30, 40, 128)
    assert pixmap_to_data(None) == b""


def test_stream_import_adapter_uses_bound_file_hooks(isolated_runtime) -> None:
    """The historical stream adapter is now owned by the standalone runtime."""
    seen = []

    class Inspect(FileTypePlugin):
        file_types = {"example"}
        on_import = True

        def run(self, path):
            seen.append(Path(path).read_bytes())
            return path

    plugin_runtime.bind_plugins([Inspect(None)])
    stream = io.BytesIO(b"book contents")
    stream.seek(5)
    path = Path(plugin_runtime.run_import_plugins(stream, "EXAMPLE"))
    try:
        assert path.read_bytes() == b"book contents"
        assert seen == [b"book contents"]
        assert plugin_runtime.run_import_plugins(str(path), "example") == str(path)
        assert seen == [b"book contents", b"book contents"]
    finally:
        path.unlink()


@pytest.mark.parametrize("native_first", (False, True))
def test_application_host_rebinds_shared_runtime(
    tmp_path: Path, native_first: bool
) -> None:
    """Both startup orders expose the host's exact instances and shared settings."""
    script = textwrap.dedent(
        """
        import sys
        from LiuXin_alpha.file_formats import plugin_runtime as runtime
        if sys.argv[1] == 'True':
            assert runtime.plugin_for_input_format('epub') is not None
        from LiuXin_alpha.plugins import application as ui
        # Importing the application API is passive; host startup is explicit.
        ui.initialize_plugins()
        from LiuXin_alpha.plugins.configuration import config
        assert ui.config is config is runtime.config
        assert ui.plugin_for_input_format is runtime.plugin_for_input_format
        assert ui.quick_metadata is runtime.quick_metadata
        plugin = runtime.plugin_for_input_format('epub')
        assert plugin is not None
        assert plugin is ui.find_plugin(plugin.name)
        assert list(runtime.initialized_plugins()) == list(ui.initialized_plugins())
        from LiuXin_alpha.surfaces.gui2 import pixmap_to_data as legacy_serializer
        from LiuXin_alpha.file_formats.rendering import pixmap_to_data
        assert legacy_serializer is pixmap_to_data
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script, str(native_first)],
        capture_output=True,
        text=True,
        timeout=120,
        env={
            **os.environ,
            "PYTHONPATH": str(Path(__file__).resolve().parents[2] / "src"),
            "LIUXIN_BASE_DIR": str(tmp_path),
            "LIUXIN_CONFIG_DIR": str(tmp_path / "config"),
        },
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_builtin_conversion_registry_has_unique_lit_and_lrf_providers() -> None:
    """LRF must not accidentally register a second LIT input provider."""
    from LiuXin_alpha.file_formats.conversion.builtins import get_input_plugins
    from LiuXin_alpha.file_formats.conversion.plugins.lit_input import LITInput
    from LiuXin_alpha.file_formats.conversion.plugins.lrf_input import LRFInput

    providers = get_input_plugins()
    assert providers.count(LITInput) == providers.count(LRFInput) == 1


def test_failed_external_load_restores_streams_and_keeps_builtins(
    isolated_runtime, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A broken external plugin cannot redirect host output or prevent fallback."""
    from LiuXin_alpha.plugins.zipplugin import loader

    stdout, stderr = sys.stdout, sys.stderr
    isolated_runtime["plugins"]["Broken"] = "broken.zip"

    def broken_load(path):
        sys.stdout, sys.stderr = io.StringIO(), io.StringIO()
        raise ValueError("broken external plugin")

    monkeypatch.setattr(loader, "load", broken_load)
    selected = plugin_runtime.plugin_for_input_format("epub")
    assert selected is not None
    assert sys.stdout is stdout and sys.stderr is stderr
