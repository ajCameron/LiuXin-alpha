"""
Check that every import of a ``LiuXin_alpha`` module names a module that exists.

Modules moved during the port left imports pointing at their old paths
(``utils.calibre_utils.calibre_zipfile``, ``utils.calibre.ptempfile``…). Most sat
inside functions, so nothing failed until the function ran: writing metadata into an
EPUB with a damaged central directory, building a plugin from a folder, or
transliterating Japanese. Those now point at the moved modules.

What is left names calibre modules LiuXin has not ported, or callers that are broken
for other reasons too; each is listed in ``KNOWN_MISSING`` with why. A new import of a
missing module fails this test, and so does a listed module that now exists or is no
longer imported, so the list only shrinks.

Example:
    Run this check::

        python -m pytest -q tests/test_internal_import_paths.py
"""

from __future__ import annotations

import ast
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"

#: Missing modules that are still imported, relative to ``LiuXin_alpha``, by reason.
KNOWN_MISSING: dict[str, frozenset[str]] = {
    "calibre device drivers, not ported": frozenset(
        {
            *(
                f"devices.{name}.driver"
                for name in (
                    "android apple bambook binatone blackberry boeye cybook eb600 edge"
                    " eslick folder_device hanlin hanvon iliad irexdr iriver jetbook kindle"
                    " kobo mtp nokia nook nuut2 prs505 prst1 smart_device_app sne teclast"
                    " user_defined"
                ).split()
            ),
            "devices.interface",
            "devices.misc",
        }
    ),
    "calibre's GUI, server and library-management parts, not ported": frozenset(
        {
            "utils.calibre.gui2.tweak_book.diff.main",
            "utils.calibre.library",
            "utils.calibre.utils.icu_test",
            "utils.serve_coffee",
            "library.catalogs.bibtex",
            "library.catalogs.csv_xml",
            "library.catalogs.epub_mobi",
            "metadata.meta",
            "utils.web.feeds.recipes",
            "utils.web.feeds.recipes.collection",
        }
    ),
    "calibre's compiled extensions and native wrappers, not ported": frozenset(
        {
            "utils.compiled_extensions.linux.icu",
            "utils.fonts.scanner",
            "utils.fonts.sfnt.errors",
            "utils.fonts.sfnt.subset",
            "utils.fonts.utils",
            "utils.magick",
            "utils.magick.draw",
            "utils.mem",
            "utils.podofo",
            "utils.speedups",
            "utils.spell.break_iterator",
            "utils.spell.dictionary",
            "utils.wmf.emf",
            "utils.wmf.parse",
            "utils.wrappers.magick",
            "utils.wrappers.magick.draw",
        }
    ),
    "libraries calibre bundles, not vendored": frozenset(
        {
            "utils.html2text",
            "utils.libraries.liuxin_tinycss.color3",
            "utils.libraries.soupparser",
            "utils.libraries.unsmarten",
            "utils.regex",
        }
    ),
    "LiuXin modules that were removed or never written": frozenset(
        {
            "folder_stores.location",
            "metadata.liuxin_plugins.isbn_extractor",
            "metadata.liuxin_plugins.md_synthesizer",
            "utils.general_ops.io_ops",
            "utils.logger",
            "utils.os_ops",
        }
    ),
    # These moved, but the callers also need modules that do not exist: 7-Zip
    # extraction, the Qt image tools and the Windows locale lookup.
    "moved, in callers broken for other reasons": frozenset(
        {
            "utils.calibre.constants",
            "utils.file_ops.file_ops",
            "utils.filenames",
            "utils.general_ops.python_tools",
            "utils.lx_libraries.liuxin_six",
        }
    ),
    # file_formats.tweak would import file_formats.mobi.tweak, which reaches the
    # conversion plumbing, while oeb.polish.container imports file_formats.tweak: the
    # fix would join the main import cycle, which check_layer_direction only lets shrink.
    "moved, but the fix would grow the import cycle": frozenset(
        {"utils.calibre.ebooks.mobi.tweak"}
    ),
}


def _exists(module: str) -> bool:
    """
    Whether a dotted module path names a module or package under ``src``.

    Example:
        >>> _exists("LiuXin_alpha.utils.ptempfiles"), _exists("LiuXin_alpha.utils.calibre.ptempfile")
        (True, False)


    :param module: The dotted path.
    :return: True if there is a module, package or namespace folder for it.
    """
    path = SRC.joinpath(*module.split("."))
    return path.with_suffix(".py").is_file() or path.is_dir()


def missing_imports() -> dict[str, list[str]]:
    """
    Every imported ``LiuXin_alpha`` module that does not exist, with where it is imported.

    Example:
        >>> "utils.calibre.ptempfile" in missing_imports()  # doctest: +SKIP
        False


    :return: Missing modules (relative to ``LiuXin_alpha``) to ``file:line`` places.
    """
    found: dict[str, list[str]] = {}
    for path in sorted((SRC / "LiuXin_alpha").rglob("*.py")):
        tree = ast.parse(path.read_bytes(), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                modules = [node.module]
            elif isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            else:
                continue
            for module in modules:
                if module.startswith("LiuXin_alpha.") and not _exists(module):
                    place = f"{path.relative_to(SRC.parent)}:{node.lineno}"
                    found.setdefault(module.removeprefix("LiuXin_alpha."), []).append(
                        place
                    )
    return found


def test_no_import_names_a_module_that_does_not_exist() -> None:
    """
    Only the listed, known-missing modules are imported without existing.

    Example:
        >>> test_no_import_names_a_module_that_does_not_exist()


    :return: None after the imports are compared with the list.
    """
    known = frozenset().union(*KNOWN_MISSING.values())
    found = missing_imports()
    new = {module: places for module, places in found.items() if module not in known}
    assert not new, f"imports of modules that do not exist: {new}"
    gone = sorted(known - found.keys())
    assert not gone, (
        f"listed as missing but now present or no longer imported; remove: {gone}"
    )
