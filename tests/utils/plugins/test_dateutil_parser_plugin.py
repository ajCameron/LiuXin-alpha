"""
Check the ``dateutil_parser`` plugin and LiuXin's vendored python-dateutil.

The plugin serves the installed python-dateutil package, and the vendored copy
where the package is missing. The ported calibre readers parse dates through
it. Runs without the package happen in a child process that refuses to import
it, so this process keeps its own resolved plugins.
"""

from __future__ import annotations

import datetime
import importlib.util
import json
import os
import random
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any

import pytest

from LiuXin_alpha.utils.libraries import python_dateutil
from LiuXin_alpha.utils.libraries.python_dateutil import parser as vendored_parser

# Refuses python-dateutil, then runs the given code, which prints JSON.
BLOCK_DATEUTIL = """
import importlib.abc, json, sys


class _Refuse(importlib.abc.MetaPathFinder):
    def find_spec(self, name, path=None, target=None):
        if name == "dateutil" or name.startswith("dateutil."):
            raise ModuleNotFoundError(f"No module named {name!r}", name=name)
        return None


if BLOCK:
    sys.meta_path.insert(0, _Refuse())
"""

REPORT = """
from LiuXin_alpha.utils.plugins import plugins

module, error = plugins["dateutil_parser"]
print(json.dumps({"source": getattr(module, "SOURCE", None), "error": error}))
"""

OPF = """<?xml version='1.0' encoding='utf-8'?>
<package xmlns="http://www.idpf.org/2007/opf" version="2.0" unique-identifier="id">
 <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
  <dc:title>Date Test</dc:title>
  <dc:identifier id="id">x</dc:identifier>
  <dc:date>3 February 2001</dc:date>
 </metadata>
 <manifest/>
 <spine/>
</package>
"""


def run_child(code: str, cache: Path, *, block: bool) -> dict[str, Any]:
    """
    Run code in a child Python, with or without python-dateutil, and read its JSON.

    Example:
        >>> run_child('print(json.dumps({"ok": True}))', Path("/tmp/cache.json"), block=True)  # doctest: +SKIP
        {'ok': True}


    :param code: Code that prints one JSON object as its last line.
    :param cache: Where the plugin resolver keeps its choices.
    :param block: Whether the child refuses to import python-dateutil.
    :return: The object printed.
    """
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(path for path in sys.path if path),
        "LIUXIN_PLUGIN_CACHE_PATH": str(cache),
    }
    program = f"BLOCK = {block!r}\n" + BLOCK_DATEUTIL + textwrap.dedent(code)
    done = subprocess.run(
        [sys.executable, "-c", program], env=env, capture_output=True, text=True, timeout=120, check=False
    )
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout.strip().splitlines()[-1])


def test_the_installed_package_serves_the_plugin(tmp_path: Path) -> None:
    """
    Serve the plugin from python-dateutil where it is installed.

    Example:
        >>> test_the_installed_package_serves_the_plugin(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: Folder for the resolver's cache.
    :return: None; the assertion is the check.
    """
    pytest.importorskip("dateutil")

    assert run_child(REPORT, tmp_path / "cache.json", block=False)["source"] == "python-dateutil"


def test_the_vendored_copy_serves_without_the_package(tmp_path: Path) -> None:
    """
    Serve the plugin from the vendored copy, and read dates with it, without python-dateutil.

    calibre's own OPF 2 and OPF 3 tests run on the ported code in the same
    child, and the OPF provider reads a written-out date.

    Example:
        >>> test_the_vendored_copy_serves_without_the_package(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: Folder for the resolver's cache and a package file.
    :return: None; the assertions are the check.
    """
    book = tmp_path / "book.opf"
    book.write_text(OPF, encoding="utf-8")
    code = f"""
    import logging, unittest
    from pathlib import Path
    from LiuXin_alpha.file_formats.conversion.report import ConversionReport
    from LiuXin_alpha.file_formats.providers.base import ProviderContext
    from LiuXin_alpha.file_formats.providers.ported_calibre import adapt_ported_reader
    from LiuXin_alpha.metadata.upstream_calibre import opf2, opf3_test
    from LiuXin_alpha.utils.plugins import plugins

    result = unittest.TestResult()
    unittest.TestSuite([opf2.suite(), opf3_test.suite()]).run(result)
    book = Path({str(book)!r})
    context = ProviderContext(book.parent, {{}}, logging.getLogger("child"), ConversionReport())
    values = adapt_ported_reader("Read OPF metadata")().read_calibre_metadata(book, "opf", context).values
    module, error = plugins["dateutil_parser"]
    print(json.dumps({{
        "source": getattr(module, "SOURCE", None),
        "dateutil_imported": any(name.split(".")[0] == "dateutil" for name in sys.modules),
        "tests_run": result.testsRun,
        "problems": [str(test) for test, _ in result.failures + result.errors],
        "pubdate": values.get("pubdate"),
    }}))
    """

    found = run_child(code, tmp_path / "cache.json", block=True)

    assert found["source"] == f"vendored python-dateutil {python_dateutil.__version__}"
    assert not found["dateutil_imported"]
    assert found["tests_run"] > 20
    assert found["problems"] == []
    assert found["pubdate"].startswith("2001-02-03")


def test_installing_the_package_replaces_a_remembered_vendored_choice(tmp_path: Path) -> None:
    """
    Move from the vendored copy to python-dateutil once it can be imported.

    The resolver remembers its choice per machine; the vendored layer's probe
    declines when python-dateutil imports, so the remembered choice does not
    outlast installing it.

    Example:
        >>> test_installing_the_package_replaces_a_remembered_vendored_choice(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: Folder for the shared resolver cache.
    :return: None; the assertions are the check.
    """
    pytest.importorskip("dateutil")
    cache = tmp_path / "cache.json"

    before = run_child(REPORT, cache, block=True)
    after = run_child(REPORT, cache, block=False)

    assert before["source"].startswith("vendored python-dateutil")
    assert after["source"] == "python-dateutil"


def test_the_vendored_parser_agrees_with_the_package() -> None:
    """
    Parse random date-like text exactly as the installed python-dateutil does.

    Example:
        >>> test_the_vendored_parser_agrees_with_the_package()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    installed = pytest.importorskip("dateutil.parser")
    default = datetime.datetime(2026, 11, 15, tzinfo=datetime.UTC)
    chooser = random.Random(696)
    tokens = [
        "2001", "1999", "31", "7", "Jan", "February", "Sept", "juin", "T", ":", "-", "/", ".", " ", ",",
        "Z", "+02:00", "GMT", "PM", "am", "of", "1st", "0101", "12:30", "04:05:06", "Mon", "UTC", "EST",
    ]  # fmt: skip

    def outcome(parse: Any, text: str, dayfirst: bool) -> str:
        """
        Describe what parsing gives: the date and its zone type, or the error's type.

        Example:
            See the test.


        :param parse: The ``parse`` function.
        :param text: The text.
        :param dayfirst: Read ambiguous day and month day first.
        :return: The description.
        """
        try:
            found = parse(text, default=default, dayfirst=dayfirst)
            return f"{found.isoformat()} {type(found.tzinfo).__name__}"
        except Exception as error:
            return type(error).__name__

    for _ in range(5000):
        text = "".join(chooser.choice(tokens) for _ in range(chooser.randint(1, 7)))
        for dayfirst in (False, True):
            assert outcome(vendored_parser.parse, text, dayfirst) == outcome(installed.parse, text, dayfirst), text


def load_vendoring_script() -> Any:
    """
    Load ``scripts/vendor_python_dateutil.py`` as a module.

    Example:
        >>> load_vendoring_script().PKG
        'LiuXin_alpha.utils.libraries.python_dateutil'


    :return: The module.
    """
    path = Path(__file__).resolve().parents[3] / "scripts" / "vendor_python_dateutil.py"
    spec = importlib.util.spec_from_file_location("vendor_python_dateutil", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_re_vendoring_the_same_version_reproduces_the_copy() -> None:
    """
    Reproduce every vendored module from an installed python-dateutil of the same version.

    Example:
        >>> test_re_vendoring_the_same_version_reproduces_the_copy()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    installed = pytest.importorskip("dateutil")
    if getattr(installed, "__version__", None) != python_dateutil.__version__:
        pytest.skip("the installed python-dateutil is not the vendored version")
    script = load_vendoring_script()
    package = Path(installed.__file__).parent
    vendored = Path(python_dateutil.__file__).parent

    sources = sorted(package.rglob("*.py"))
    assert sources
    for source in sources:
        relative = source.relative_to(package).as_posix()
        expected = script.vendor_source(source.read_text(encoding="utf-8"), relative, python_dateutil.__version__)
        assert (vendored / relative).read_text(encoding="utf-8") == expected, relative


def test_the_vendored_copy_imports_neither_six_nor_dateutil() -> None:
    """
    Keep the vendored copy free of imports of ``six`` and of the installed package.

    Example:
        >>> test_the_vendored_copy_imports_neither_six_nor_dateutil()  # doctest: +SKIP


    :return: None; the assertion is the check.
    """
    import ast

    for path in Path(python_dateutil.__file__).parent.rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Import):
                roots = {alias.name.split(".")[0] for alias in node.names}
            elif isinstance(node, ast.ImportFrom) and node.level == 0:
                roots = {(node.module or "").split(".")[0]}
            else:
                continue
            assert not roots & {"six", "dateutil"}, path
