"""
Port calibre modules into ``LiuXin_alpha.metadata.upstream_calibre``.

Each module is copied from a calibre source tree with its imports rewritten by
the tables below: calibre modules that are ported whole map to their module in
the package, and smaller helpers map to names in ``_compat``. Nothing else in
the code changes, apart from two mechanical edits so the copies also run before
Python 3.14: ``from __future__ import annotations`` is added, and ``except``
clauses naming several exceptions are parenthesised. A header comment records
the source path and those changes.

Run it again on a newer calibre to bring its changes across. An import the
tables do not cover stops the run, so each new dependency is a deliberate
choice. Modules listed in ``PORTS`` carry no LiuXin edits; the earlier readers
(``txt``, ``rtf``, ``topaz`` and the rest) were ported by hand and some carry
fixes marked ``# LiuXin:``, which a re-port would need to keep.

Example:
    Re-port the OPF modules from a calibre checkout::

        python scripts/port_upstream_calibre.py ~/src/calibre
"""

from __future__ import annotations

import argparse
import ast
import re
from pathlib import Path

DESTINATION = (
    Path(__file__).resolve().parent.parent / "src" / "LiuXin_alpha" / "metadata" / "upstream_calibre"
)
PKG = "LiuXin_alpha.metadata.upstream_calibre"
COMPAT = f"{PKG}._compat"

# Upstream path under src/calibre, module name here, and extra header lines.
PORTS: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("ebooks/metadata/__init__.py", "metadata_base.py", ()),
    ("ebooks/metadata/book/__init__.py", "book_fields.py", ()),
    ("library/field_metadata.py", "field_metadata.py", ()),
    ("ebooks/metadata/book/base.py", "book_base.py", ()),
    ("ebooks/metadata/book/json_codec.py", "json_codec.py", ()),
    (
        "utils/iso8601.py",
        "iso8601.py",
        (
            "The tzlocal package and zoneinfo.ZoneInfo come from _compat, which fall back to UTC",
            "where no zone can be found rather than failing at import.",
        ),
    ),
    ("utils/date.py", "date.py", ()),
    ("utils/cleantext.py", "cleantext.py", ()),
    ("ebooks/chardet.py", "chardet.py", ()),
    ("utils/imghdr.py", "imghdr.py", ()),
    ("ebooks/metadata/toc.py", "toc.py", ()),
    ("ebooks/metadata/utils.py", "metadata_utils.py", ()),
    ("ebooks/metadata/opf2.py", "opf2.py", ()),
    ("ebooks/metadata/opf3.py", "opf3.py", ()),
    ("ebooks/metadata/opf.py", "opf.py", ()),
    ("ebooks/metadata/opf3_test.py", "opf3_test.py", ()),
    ("ebooks/metadata/test_author_sort.py", "author_sort_test.py", ()),
)

# calibre modules ported whole: every name imported from them maps to the same
# name in the ported module.
MODULES = {
    "calibre.ebooks.metadata": f"{PKG}.metadata_base",
    "calibre.ebooks.metadata.book": f"{PKG}.book_fields",
    "calibre.ebooks.metadata.book.base": f"{PKG}.book_base",
    "calibre.ebooks.metadata.book.json_codec": f"{PKG}.json_codec",
    "calibre.library.field_metadata": f"{PKG}.field_metadata",
    "calibre.ebooks.metadata.opf2": f"{PKG}.opf2",
    "calibre.ebooks.metadata.opf3": f"{PKG}.opf3",
    "calibre.ebooks.metadata.opf": f"{PKG}.opf",
    "calibre.ebooks.metadata.toc": f"{PKG}.toc",
    "calibre.ebooks.metadata.utils": f"{PKG}.metadata_utils",
    "calibre.ebooks.metadata.meta": f"{PKG}.meta",
    "calibre.ebooks.metadata.topaz": f"{PKG}.topaz",
    "calibre.ebooks.oeb.polish.pretty": f"{PKG}.pretty",
    "calibre.ebooks.chardet": f"{PKG}.chardet",
    "calibre.utils.cleantext": f"{PKG}.cleantext",
    "calibre.utils.config": f"{PKG}.config_base",
    "calibre.utils.config_base": f"{PKG}.config_base",
    "calibre.utils.date": f"{PKG}.date",
    "calibre.utils.iso8601": f"{PKG}.iso8601",
    "calibre.utils.imghdr": f"{PKG}.imghdr",
}

# Helpers from modules that are not ported whole: the names _compat provides,
# with any renaming.
NAMES: dict[str, dict[str, str]] = {
    "calibre": {
        name: name
        for name in (
            "prints",
            "guess_type",
            "CurrentDir",
            "strftime",
            "xml_replace_entities",
            "replace_entities",
            "force_unicode",
            "prepare_string_for_xml",
            "relpath",
            "isbytestring",
        )
    },
    "calibre.constants": {
        name: name
        for name in (
            "__appname__",
            "__version__",
            "filesystem_encoding",
            "preferred_encoding",
            "DEBUG",
            "iswindows",
            "ismacos",
        )
    },
    "calibre.ebooks": {"escape_xpath_attr": "escape_xpath_attr"},
    "calibre.ebooks.oeb.base": {
        name: name
        for name in ("DC", "OPF", "OPF2_NSMAP", "CALIBRE_NS", "DC11_NS", "OPF2_NS", "XML_NS")
    },
    "calibre.ebooks.oeb.polish.utils": {"guess_type": "polish_guess_type"},
    "calibre.spell": {"parse_lang_code": "parse_lang_code", "DictionaryLocale": "DictionaryLocale"},
    "calibre.utils": {"StreamOrPath": "StreamOrPath"},
    "calibre.utils.icu": {"lower": "icu_lower", "upper": "icu_upper", "sort_key": "sort_key"},
    "calibre.utils.localization": {
        name: name
        for name in (
            "_",
            "ngettext",
            "canonicalize_lang",
            "get_lang",
            "lang_as_iso639_1",
            "lcdata",
            "calibre_langcode_to_name",
            "load_iso3166",
        )
    },
    "calibre.utils.xml_parse": {"safe_xml_fromstring": "safe_xml_fromstring"},
    "calibre.utils.speedups": {"ReadOnlyFileBuffer": "ReadOnlyFileBuffer"},
    "calibre.utils.img": {"save_cover_data_to": "save_cover_data_to"},
    "calibre.utils.zipfile": {
        name: name
        for name in ("ZipFile", "BadZipfile", "safe_replace", "ZIP_DEFLATED", "ZIP_STORED", "ZipInfo")
    },
    "calibre.ptempfile": {
        "TemporaryDirectory": "TemporaryDirectory",
        "SpooledTemporaryFile": "SpooledTemporaryFile",
    },
    "calibre.utils.logging": {"default_log": "default_log"},
    "calibre.utils.formatter": {"TEMPLATE_ERROR": "TEMPLATE_ERROR"},
    "calibre.ebooks.metadata.book.formatter": {"SafeFormat": "SafeFormat"},
    "calibre.utils.filenames": {"is_windows_reserved_filename": "is_windows_reserved_filename"},
    "calibre.gui2": {"is_gui_thread": "is_gui_thread", "UNDEFINED_QDATETIME": "UNDEFINED_QDATETIME"},
    "calibre.gui2.ui": {"get_gui": "get_gui"},
    "polyglot.builtins": {"as_bytes": "as_bytes", "as_unicode": "as_unicode"},
    "polyglot.binary": {
        "as_base64_unicode": "as_base64_unicode",
        "from_base64_bytes": "from_base64_bytes",
    },
}

# Single names from elsewhere: C extensions, polyglot and standard-library
# imports that LiuXin replaces.
EXTERNAL: dict[tuple[str, str], tuple[str, str]] = {
    ("polyglot.urllib", "unquote"): ("urllib.parse", "unquote"),
    ("calibre_extensions", "speedup"): (PKG, "_speedups"),
    ("calibre_extensions.speedup", "clean_xml_chars"): (f"{PKG}._speedups", "clean_xml_chars"),
    ("calibre_extensions.speedup", "parse_iso8601"): (f"{PKG}._speedups", "parse_iso8601"),
    ("calibre_extensions.uchardet", "detect"): (COMPAT, "uchardet_detect"),
    ("calibre_extensions.usbobserver", "date_format"): (COMPAT, "macos_date_format"),
    ("zoneinfo", "ZoneInfo"): (COMPAT, "ZoneInfo"),
}

# Whole-module imports LiuXin replaces.
IMPORT_MODULES = {"tzlocal": f"from {COMPAT} import tzlocal"}

# Imports left as they are: only functions the readers do not call use them.
LEAVE = frozenset({"qt.core", "html5_parser"})

REWRITTEN_ROOTS = frozenset({"calibre", "polyglot", "calibre_extensions", "tzlocal", "zoneinfo"})


def parenthesise_excepts(source: str) -> str:
    """
    Parenthesise ``except A, B:`` clauses (PEP 758) for Python before 3.14.

    Example:
        >>> parenthesise_excepts("try:\\n    x\\nexcept A, B as e:\\n    y\\n").splitlines()[2]
        'except (A, B) as e:'


    :param source: Python source.
    :return: The source with such clauses parenthesised.
    """
    return re.sub(
        r"^(\s*)except ([^():\n]+,[^():\n]+?)( as \w+)?:",
        r"\1except (\2)\3:",
        source,
        flags=re.M,
    )


def rewrite_import(node: ast.Import | ast.ImportFrom, line: str, typing_only: bool) -> list[str]:
    """
    Rewrite one import statement by the tables.

    Imports used only for type checking that the tables do not cover become
    ``typing.Any``.

    Example:
        >>> node = ast.parse("from calibre.utils.icu import lower").body[0]
        >>> rewrite_import(node, "from calibre.utils.icu import lower", False)
        ['from LiuXin_alpha.metadata.upstream_calibre._compat import icu_lower as lower']


    :param node: The import.
    :param line: Its first source line, for the indentation.
    :param typing_only: Whether it sits in an ``if TYPE_CHECKING:`` block.
    :return: The replacement lines.
    :raises SystemExit: For an import the tables do not cover.
    """
    indent = re.match(r"\s*", line).group(0)  # type: ignore[union-attr]
    out = []
    if isinstance(node, ast.Import):
        for alias in node.names:
            if alias.name not in IMPORT_MODULES or alias.asname is not None:
                raise SystemExit(f"no mapping for import {alias.name}")
            out.append(indent + IMPORT_MODULES[alias.name])
        return out
    module = node.module or ""
    for alias in node.names:
        local = alias.asname or alias.name
        if (module, alias.name) in EXTERNAL:
            target_module, target_name = EXTERNAL[(module, alias.name)]
        elif module in MODULES:
            target_module, target_name = MODULES[module], alias.name
        elif module in NAMES and alias.name in NAMES[module]:
            target_module, target_name = COMPAT, NAMES[module][alias.name]
        elif typing_only:
            out.append(f"{indent}from typing import Any as {local}")
            continue
        else:
            raise SystemExit(f"no mapping for {module}.{alias.name}")
        suffix = f" as {local}" if local != target_name else ""
        out.append(f"{indent}from {target_module} import {target_name}{suffix}")
    return out


def port_source(source: str, source_path: str, extra_header: tuple[str, ...] = ()) -> str:
    """
    Turn one calibre module's source into the ported module's source.

    Example:
        >>> print(port_source("from calibre import prints\\n", "x.py").splitlines()[-1])
        from LiuXin_alpha.metadata.upstream_calibre._compat import prints


    :param source: The calibre module's source.
    :param source_path: Its path under ``src/calibre``, for the header.
    :param extra_header: More lines for the header comment.
    :return: The ported source.
    """
    fixed = parenthesise_excepts(source)
    tree = ast.parse(fixed)
    lines = fixed.split("\n")
    typing_lines: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.If) and getattr(node.test, "id", None) == "TYPE_CHECKING":
            for child in node.body:
                typing_lines.update(range(child.lineno, (child.end_lineno or child.lineno) + 1))
    replacements = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module in LEAVE or (node.module or "").split(".")[0] not in REWRITTEN_ROOTS:
                continue
        elif isinstance(node, ast.Import):
            if all(alias.name.split(".")[0] not in REWRITTEN_ROOTS for alias in node.names):
                continue
        else:
            continue
        new = rewrite_import(node, lines[node.lineno - 1], node.lineno in typing_lines)
        replacements.append((node.lineno, node.end_lineno or node.lineno, new))
    for start, end, new in sorted(replacements, reverse=True):
        lines[start - 1 : end] = new
    text = "\n".join(lines)
    if "from __future__ import annotations" not in text:
        tree = ast.parse(text)
        lines = text.split("\n")
        first = tree.body[0] if tree.body else None
        if isinstance(first, ast.Expr) and isinstance(getattr(first, "value", None), ast.Constant):
            lines[first.end_lineno : first.end_lineno] = ["", "from __future__ import annotations"]  # type: ignore[index]
        else:
            at = next((i for i, l in enumerate(lines) if l.strip() and not l.startswith("#")), len(lines))
            lines[at:at] = ["from __future__ import annotations", ""]
        text = "\n".join(lines)
    lines = text.split("\n")
    at = 0
    while at < len(lines) and lines[at].startswith("#"):
        at += 1
    header = [
        f"# Ported from calibre (https://calibre-ebook.com) 9.18: src/calibre/{source_path}",
        "# Changes from upstream: calibre imports point at upstream_calibre and its _compat, and",
        "# 'from __future__ import annotations' is added.",
    ]
    if fixed != source:
        header.append("# Except clauses naming several exceptions are parenthesised, for Python before 3.14.")
    header += [f"# {line}" for line in extra_header]
    header.append("# SPDX-License-Identifier: GPL-3.0-only")
    lines[at:at] = header
    out = "\n".join(lines)
    ast.parse(out)
    return out


def main() -> None:
    """
    Port every module in ``PORTS`` from the calibre tree given.

    Example:
        Run from the command line; see the module docstring.


    :return: None.
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("calibre", type=Path, help="a calibre source checkout")
    args = parser.parse_args()
    root = args.calibre / "src" / "calibre"
    for source_path, name, extra in PORTS:
        source = (root / source_path).read_text(encoding="utf-8")
        (DESTINATION / name).write_text(port_source(source, source_path, extra), encoding="utf-8")
        print(f"ported {source_path} -> {name}")


if __name__ == "__main__":
    main()
