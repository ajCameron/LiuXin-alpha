"""
Capture readability extraction diagnostics and intermediate markup.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise debug through a consuming regression::

        python -m pytest -q tests/file_formats/readability/test_readability_modernized.py
"""
from __future__ import annotations

import typing as _typing
def save_to_file(text: _typing.Any, filename: _typing.Any) -> None:
    """
    Perform the save to file operation under explicit file-format and conversion rules.

    Example:
        Exercise save to file through a consuming regression::

            python -m pytest -q tests/file_formats/readability/test_readability_modernized.py


    :param text: Text parsed, normalized or rendered.
    :param filename: Filename used for type inference or archive output.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    with open(filename, "wt", encoding="utf-8") as f:
        f.write('<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />')
        f.write(text)


uids = {}


def describe(node: _typing.Any, depth: int = 2) -> _typing.Any:
    """
    Perform the describe operation under explicit file-format and conversion rules.

    Example:
        Exercise describe through a consuming regression::

            python -m pytest -q tests/file_formats/readability/test_readability_modernized.py


    :param node: Value supplied for node under the utility contract.
    :param depth: Value supplied for depth under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not hasattr(node, "tag"):
        return "[%s]" % type(node)
    name = node.tag
    if node.get("id", ""):
        name += "#" + node.get("id")
    if node.get("class", ""):
        name += "." + node.get("class").replace(" ", ".")
    if name[:4] in ["div#", "div."]:
        name = name[3:]
    if name in ["tr", "td", "div", "p"]:
        if node not in uids:
            uid = uids[node] = len(uids) + 1
        else:
            uid = uids.get(node)
        name += "%02d" % uid
    if depth and node.getparent() is not None:
        return name + " - " + describe(node.getparent(), depth - 1)
    return name
