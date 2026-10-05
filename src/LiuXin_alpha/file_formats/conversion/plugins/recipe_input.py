#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Convert RECIPE content into the normalized ebook conversion pipeline.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise recipe input through a consuming regression::

        python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py
"""
from __future__ import annotations

import typing as _typing

import os

from LiuXin_alpha.customize.conversion import InputFormatPlugin, OptionRecommendation
from LiuXin_alpha.file_formats.conversion.plugins._workdir import (
    choose_conversion_workdir,
)

from LiuXin_alpha.customize import numeric_version
from LiuXin_alpha.utils.calibre import CurrentDir
from LiuXin_alpha.utils.calibre import walk
from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL v3"
__copyright__ = "2009, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


class RecipeDisabled(Exception):
    """
    Provide the recipedisabled contract for validated ebook processing.

    Example:
        Exercise RecipeDisabled through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py
    """
    pass


class RecipeInput(InputFormatPlugin):

    """
    Convert recipeinput sources into the normalized OEB pipeline model.

    Example:
        Exercise RecipeInput through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py
    """
    name = "Recipe Input"
    author = "Kovid Goyal"
    description = _("Download periodical content from the internet")
    file_types = {"recipe", "downloaded_recipe"}

    recommendations = {
        ("chapter", None, OptionRecommendation.HIGH),
        ("dont_split_on_page_breaks", True, OptionRecommendation.HIGH),
        ("use_auto_toc", False, OptionRecommendation.HIGH),
        ("input_encoding", None, OptionRecommendation.HIGH),
        ("input_profile", "default", OptionRecommendation.HIGH),
        ("page_breaks_before", None, OptionRecommendation.HIGH),
        ("insert_metadata", False, OptionRecommendation.HIGH),
    }

    options = {
        OptionRecommendation(
            name="test",
            recommended_value=False,
            option_help=_(
                "Useful for recipe development. Forces"
                " max_articles_per_feed to 2 and downloads at most 2 feeds."
                " You can change the number of feeds and articles by supplying optional arguments."
                " For example: --test 3 1 will download at most 3 feeds and only 1 article per "
                "feed."
            ),
        ),
        OptionRecommendation(
            name="username",
            recommended_value=None,
            option_help=_("Username for sites that require a login to access content."),
        ),
        OptionRecommendation(
            name="password",
            recommended_value=None,
            option_help=_("Password for sites that require a login to access content."),
        ),
        OptionRecommendation(
            name="dont_download_recipe",
            recommended_value=False,
            option_help=_("Do not download latest version of builtin recipes from the calibre " "reader_server"),
        ),
        OptionRecommendation(
            name="lrf",
            recommended_value=False,
            option_help="Optimize fetching for subsequent conversion to LRF.",
        ),
    }

    def convert(self: _typing.Self, recipe_or_file: _typing.Any, options: _typing.Any, file_ext: _typing.Any, log: _typing.Any, accelerators: _typing.Any) -> _typing.Any:
        """
        Download news from a site. Convert that news to an OEB for later conversion to another type of ebook.

        Example:
            Exercise RecipeInput.convert through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py


        :param recipe_or_file: Value supplied for recipe or file under the utility contract.
        :param options: Value supplied for options under the utility contract.
        :param file_ext: Value supplied for file ext under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :param accelerators: Value supplied for accelerators under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        from LiuXin_alpha.utils.web.feeds.recipes import compile_recipe

        options.output_profile.flow_size = 0
        recipe = None
        work_root = choose_conversion_workdir("_recipe_input")
        with CurrentDir(work_root):
            if file_ext == "downloaded_recipe":

                from LiuXin_alpha.utils.libraries.calibre_zipfile import ZipFile

                zf = ZipFile(recipe_or_file, "r")
                zf.extractall()
                zf.close()
                self.recipe_source = open("download.recipe", "rb").read()
                recipe = compile_recipe(self.recipe_source)
                recipe.needs_subscription = False
                self.recipe_object = recipe(options, log, self.report_progress)

            else:

                if os.access(recipe_or_file, os.R_OK):
                    with open(recipe_or_file, "rb") as r_or_f_pointer:
                        self.recipe_source = r_or_f_pointer.read()
                    recipe = compile_recipe(self.recipe_source)
                    log("Using custom recipe")
                else:
                    from LiuXin_alpha.utils.web.feeds.recipes.collection import (
                        get_builtin_recipe_by_title,
                        get_builtin_recipe_titles,
                    )

                    title = getattr(options, "original_recipe_input_arg", recipe_or_file)
                    title = os.path.basename(title).rpartition(".")[0]
                    titles = frozenset(get_builtin_recipe_titles())
                    if title not in titles:
                        title = getattr(options, "original_recipe_input_arg", recipe_or_file)
                        title = title.rpartition(".")[0]

                    raw = get_builtin_recipe_by_title(title, log=log, download_recipe=not options.dont_download_recipe)
                    builtin = False
                    try:
                        recipe = compile_recipe(raw)
                        self.recipe_source = raw
                        if recipe.requires_version > numeric_version:
                            log.warn(
                                "Downloaded recipe needs calibre version at least: %s"
                                % (".".join(recipe.requires_version))
                            )
                            builtin = True
                    except Exception as e:
                        log.exception(
                            "Failed to compile downloaded recipe. Falling back to builtin one "
                            "- error message: {}".format(e)
                        )
                        builtin = True

                    if builtin:
                        log("Using bundled builtin recipe")
                        raw = get_builtin_recipe_by_title(title, log=log, download_recipe=False)
                        if raw is None:
                            raise ValueError("Failed to find builtin recipe: " + title)
                        recipe = compile_recipe(raw)
                        self.recipe_source = raw
                    else:
                        log("Using downloaded builtin recipe")

                if recipe is None:
                    raise ValueError("%r is not a valid recipe file or builtin recipe" % recipe_or_file)

                disabled = getattr(recipe, "recipe_disabled", None)
                if disabled is not None:
                    raise RecipeDisabled(disabled)
                ro = recipe(options, log, self.report_progress)
                ro.download()
                self.recipe_object = ro

            for key, val in self.recipe_object.conversion_options.items():
                setattr(options, key, val)

            for f in os.listdir("."):
                if f.endswith(".opf"):
                    return os.path.abspath(f)

            for f in walk("."):
                if f.endswith(".opf"):
                    return os.path.abspath(f)

    def postprocess_book(self: _typing.Self, oeb: _typing.Any, opts: _typing.Any, log: _typing.Any) -> None:
        """
        Perform the postprocess book operation under explicit file-format and conversion rules.

        Example:
            Exercise RecipeInput.postprocess book through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py


        :param oeb: Value supplied for oeb under the utility contract.
        :param opts: Value supplied for opts under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.recipe_object is not None:
            self.recipe_object.postprocess_book(oeb, opts, log)

    def specialize(self: _typing.Self, oeb: _typing.Any, opts: _typing.Any, log: _typing.Any, output_fmt: _typing.Any) -> None:
        """
        Perform the specialize operation under explicit file-format and conversion rules.

        Example:
            Exercise RecipeInput.specialize through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py


        :param oeb: Value supplied for oeb under the utility contract.
        :param opts: Value supplied for opts under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :param output_fmt: Value supplied for output fmt under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if opts.no_inline_navbars:
            from LiuXin_alpha.file_formats.oeb.base import XPath

            for item in oeb.spine:
                for div in XPath('//h:div[contains(@class, "calibre_navbar")]')(item.data):
                    div.getparent().remove(div)

    def save_download(self: _typing.Self, zf: _typing.Any) -> None:
        """
        Perform the save download operation under explicit file-format and conversion rules.

        Example:
            Exercise RecipeInput.save download through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py


        :param zf: Value supplied for zf under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raw = self.recipe_source
        if isinstance(raw, str):
            raw = raw.encode("utf-8")
        zf.writestr("download.recipe", raw)
