"""
Vendor python-dateutil into ``LiuXin_alpha.utils.libraries.python_dateutil``.

python-dateutil is pure Python and a LiuXin dependency. The vendored copy is
the backup the utils plugin system serves when the package is not installed
(the ``dateutil_parser`` plugin). The copy is the package's own code with two
mechanical changes, recorded in each file's header: its imports of ``six``
point at the small ``_six`` module kept here, as LiuXin runs only on Python 3,
and its absolute ``dateutil`` imports point at the vendored package. The time
zone database archive in ``dateutil/zoneinfo`` is left out; dateutil falls back
to the system's database, and parsing dates does not use it.

Run it again on a newer python-dateutil to bring its changes across. An import
the rewrite does not cover stops the run.

Example:
    Re-vendor from an installed python-dateutil::

        python scripts/vendor_python_dateutil.py /usr/lib/python3/site-packages/dateutil
"""

from __future__ import annotations

import argparse
import ast
import re
import shutil
from pathlib import Path

DESTINATION = (
    Path(__file__).resolve().parent.parent / "src" / "LiuXin_alpha" / "utils" / "libraries" / "python_dateutil"
)
PKG = "LiuXin_alpha.utils.libraries.python_dateutil"

# Files LiuXin keeps here itself, which a re-vendor leaves alone.
OWN_FILES = frozenset({"_six.py"})

# six.moves names and what replaces them on Python 3: a module to import, or
# None for a builtin.
SIX_MOVES = {"_thread": "_thread", "winreg": "winreg", "range": None}


def rewrite_line(line: str) -> str:
    """
    Rewrite one import of ``six`` or ``dateutil``; leave other lines alone.

    Example:
        >>> rewrite_line("from dateutil import tz")
        'from LiuXin_alpha.utils.libraries.python_dateutil import tz'
        >>> rewrite_line("    from six.moves import _thread, range")
        '    import _thread'


    :param line: A source line.
    :return: The line, rewritten if it imports ``six`` or ``dateutil``.
    :raises SystemExit: For an import of either that the rules do not cover.
    """
    match = re.match(r"^(\s*)import six\s*$", line)
    if match:
        return f"{match.group(1)}from {PKG} import _six as six"
    match = re.match(r"^(\s*)from six import (.+)$", line)
    if match:
        return f"{match.group(1)}from {PKG}._six import {match.group(2)}"
    match = re.match(r"^(\s*)from six\.moves import (.+)$", line)
    if match:
        modules = []
        for name in (part.strip() for part in match.group(2).split(",")):
            if name not in SIX_MOVES:
                raise SystemExit(f"no rule for six.moves.{name}")
            if SIX_MOVES[name] is not None:
                modules.append(SIX_MOVES[name])
        return f"{match.group(1)}import {', '.join(modules)}" if modules else f"{match.group(1)}pass"
    match = re.match(r"^(\s*)from dateutil((?:\.\w+)*) import (.+)$", line)
    if match:
        return f"{match.group(1)}from {PKG}{match.group(2)} import {match.group(3)}"
    if re.match(r"^\s*(import|from)\s+(six|dateutil)\b", line):
        raise SystemExit(f"no rule for {line.strip()!r}")
    return line


def vendor_source(source: str, relative: str, version: str) -> str:
    """
    Turn one dateutil module's source into the vendored module's source.

    Example:
        >>> print(vendor_source("import six\\n", "x.py", "2.9.0").splitlines()[-1])
        from LiuXin_alpha.utils.libraries.python_dateutil import _six as six


    :param source: The module's source.
    :param relative: Its path within the ``dateutil`` package, for the header.
    :param version: The python-dateutil version, for the header.
    :return: The vendored source.
    :raises SystemExit: If the result still imports ``six`` or ``dateutil``.
    """
    lines = source.split("\n")
    rewritten = [rewrite_line(line) for line in lines]
    for node in ast.walk(ast.parse("\n".join(rewritten))):
        names = []
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.level == 0:
            names = [node.module or ""]
        if any(name.split(".")[0] in {"six", "dateutil"} for name in names):
            raise SystemExit(f"{relative}: an import of six or dateutil was not rewritten")
    header = [
        f"# Vendored from python-dateutil {version} (https://github.com/dateutil/dateutil): dateutil/{relative}",
        "# Licensed under the Apache License 2.0 or the BSD 3-Clause licence; see LICENSE in this folder.",
    ]
    if rewritten != lines:
        header.append("# Changed: imports of six point at _six here, and absolute dateutil imports at this package.")
    at = 0
    # A coding declaration must stay on the first or second line.
    while at < len(rewritten) and at < 2 and re.match(r"^#.*coding[:=]", rewritten[at]):
        at += 1
    rewritten[at:at] = header
    return "\n".join(rewritten)


def main() -> None:
    """
    Vendor the dateutil package given, with its licence.

    Example:
        Run from the command line; see the module docstring.


    :return: None.
    :raises SystemExit: If the package's licence cannot be found.
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("package", type=Path, help="an installed dateutil package folder")
    parser.add_argument("--licence", type=Path, help="python-dateutil's LICENSE file, if not beside the package")
    args = parser.parse_args()
    package: Path = args.package
    version_source = (package / "_version.py").read_text(encoding="utf-8")
    version = re.search(r"""\bversion\s*=\s*['"]([^'"]+)['"]""", version_source).group(1)  # type: ignore[union-attr]
    licence = args.licence
    if licence is None:
        candidates = list(package.parent.glob("python_dateutil-*.dist-info/LICENSE")) + [package.parent / "LICENSE"]
        licence = next((path for path in candidates if path.exists()), None)
    if licence is None:
        raise SystemExit("python-dateutil's LICENSE was not found; pass --licence")

    for old in DESTINATION.rglob("*.py"):
        if old.name not in OWN_FILES:
            old.unlink()
    for source_path in sorted(package.rglob("*.py")):
        relative = source_path.relative_to(package).as_posix()
        target = DESTINATION / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(vendor_source(source_path.read_text(encoding="utf-8"), relative, version), encoding="utf-8")
        print(f"vendored dateutil/{relative}")
    shutil.copyfile(licence, DESTINATION / "LICENSE")


if __name__ == "__main__":
    main()
