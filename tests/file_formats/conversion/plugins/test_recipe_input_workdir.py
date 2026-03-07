from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path
from zipfile import ZipFile


class _Log:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def _record(self, *parts) -> None:
        self.messages.append(" ".join(str(x) for x in parts))

    def __call__(self, *parts) -> None:
        self._record(*parts)

    def debug(self, *parts) -> None:
        self._record(*parts)

    def info(self, *parts) -> None:
        self._record(*parts)

    def warn(self, *parts) -> None:
        self._record(*parts)

    def warning(self, *parts) -> None:
        self._record(*parts)

    def error(self, *parts) -> None:
        self._record(*parts)

    def exception(self, *parts) -> None:
        self._record(*parts)


def test_recipe_input_downloaded_recipe_does_not_extract_into_project_root(
    tmp_path: Path,
    monkeypatch,
    project_root: Path,
) -> None:
    recipe_input_mod = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.recipe_input")

    archive = tmp_path / "downloaded.recipe.zip"
    leak_name = "__recipe_leak_probe.txt"
    with ZipFile(archive, "w") as zf:
        zf.writestr("download.recipe", b"# fake recipe")
        zf.writestr(leak_name, b"should-not-land-in-project-root")
        zf.writestr("content.opf", b"<package/>")

    fake_recipes = types.ModuleType("LiuXin_alpha.utils.web.feeds.recipes")

    def _compile_recipe(_raw):
        class _Recipe:
            needs_subscription = False
            requires_version = (0, 0, 0)

            def __call__(self, _options, _log, _progress):
                return types.SimpleNamespace(conversion_options={})

        return _Recipe()

    fake_recipes.compile_recipe = _compile_recipe
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.utils.web.feeds.recipes", fake_recipes)

    monkeypatch.chdir(project_root)
    before_exists = (project_root / leak_name).exists()

    options = types.SimpleNamespace(
        output_profile=types.SimpleNamespace(flow_size=1),
        dont_download_recipe=False,
    )

    with archive.open("rb") as stream:
        out = recipe_input_mod.RecipeInput(None).convert(
            stream,
            options,
            "downloaded_recipe",
            _Log(),
            {},
        )

    assert out.endswith(".opf")
    assert (project_root / leak_name).exists() is before_exists
