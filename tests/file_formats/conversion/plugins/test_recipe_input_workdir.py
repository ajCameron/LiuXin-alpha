"""
Provide test recipe input workdir utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test recipe input workdir through a consuming regression::

        python -m pytest -q tests/file_formats/conversion/plugins/test_recipe_input_workdir.py
"""
from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path
from zipfile import ZipFile


class _Log:
    """
    Provide the log contract for validated ebook processing.

    Example:
        Exercise  Log through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/plugins/test_recipe_input_workdir.py
    """
    def __init__(self) -> None:
        """
        Initialize and validate the log state.

        Example:
            Exercise  Log.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_recipe_input_workdir.py


        :return: None; validated state is stored on the receiving object.
        """
        self.messages: list[str] = []

    def _record(self, *parts) -> None:
        """
        Perform the record operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log. record through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_recipe_input_workdir.py


        :param parts: Value supplied for parts under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.messages.append(" ".join(str(x) for x in parts))

    def __call__(self, *parts) -> None:
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_recipe_input_workdir.py


        :param parts: Value supplied for parts under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._record(*parts)

    def debug(self, *parts) -> None:
        """
        Perform the debug operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.debug through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_recipe_input_workdir.py


        :param parts: Value supplied for parts under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._record(*parts)

    def info(self, *parts) -> None:
        """
        Perform the info operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.info through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_recipe_input_workdir.py


        :param parts: Value supplied for parts under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._record(*parts)

    def warn(self, *parts) -> None:
        """
        Perform the warn operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.warn through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_recipe_input_workdir.py


        :param parts: Value supplied for parts under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._record(*parts)

    def warning(self, *parts) -> None:
        """
        Perform the warning operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.warning through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_recipe_input_workdir.py


        :param parts: Value supplied for parts under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._record(*parts)

    def error(self, *parts) -> None:
        """
        Perform the error operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.error through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_recipe_input_workdir.py


        :param parts: Value supplied for parts under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._record(*parts)

    def exception(self, *parts) -> None:
        """
        Perform the exception operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.exception through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_recipe_input_workdir.py


        :param parts: Value supplied for parts under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._record(*parts)


def test_recipe_input_downloaded_recipe_does_not_extract_into_project_root(
    tmp_path: Path,
    monkeypatch,
    project_root: Path,
) -> None:
    """
    Perform the test recipe input downloaded recipe does not extract into project root operation under explicit file-format and conversion rules.

    Example:
        Exercise test recipe input downloaded recipe does not extract into project root through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/plugins/test_recipe_input_workdir.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :param project_root: Value supplied for project root under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    recipe_input_mod = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.recipe_input")

    archive = tmp_path / "downloaded.recipe.zip"
    leak_name = "__recipe_leak_probe.txt"
    with ZipFile(archive, "w") as zf:
        zf.writestr("download.recipe", b"# fake recipe")
        zf.writestr(leak_name, b"should-not-land-in-project-root")
        zf.writestr("content.opf", b"<package/>")

    fake_recipes = types.ModuleType("LiuXin_alpha.utils.web.feeds.recipes")

    def _compile_recipe(_raw):
        """
        Perform the compile recipe operation under explicit file-format and conversion rules.

        Example:
            Exercise test recipe input downloaded recipe does not extract into project root. compile recipe through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_recipe_input_workdir.py


        :param _raw: Value supplied for raw under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        class _Recipe:
            """
            Provide the recipe contract for validated ebook processing.

            Example:
                Exercise test recipe input downloaded recipe does not extract into project root. compile recipe. Recipe through a consuming regression::

                    python -m pytest -q tests/file_formats/conversion/plugins/test_recipe_input_workdir.py
            """
            needs_subscription = False
            requires_version = (0, 0, 0)

            def __call__(self, _options, _log, _progress):
                """
                Perform the call operation under explicit file-format and conversion rules.

                Example:
                    Exercise test recipe input downloaded recipe does not extract into project root. compile recipe. Recipe.  call   through a consuming regression::

                        python -m pytest -q tests/file_formats/conversion/plugins/test_recipe_input_workdir.py


                :param _options: Value supplied for options under the utility contract.
                :param _log: Value supplied for log under the utility contract.
                :param _progress: Value supplied for progress under the utility contract.
                :return: The normalized value, metadata record, path, stream result or collection
                    described above.
                """
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
