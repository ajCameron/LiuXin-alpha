from __future__ import annotations

import typing as _typing
def save_to_file(text: _typing.Any, filename: _typing.Any) -> None:
    with open(filename, "wt", encoding="utf-8") as f:
        f.write('<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />')
        f.write(text)


uids = {}


def describe(node: _typing.Any, depth: int = 2) -> _typing.Any:
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
