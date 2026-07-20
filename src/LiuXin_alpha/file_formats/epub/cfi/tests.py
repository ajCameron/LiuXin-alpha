#!/usr/bin/env python
# vim:fileencoding=utf-8

from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

import unittest

from LiuXin_alpha.file_formats.epub.cfi.parse import parser, cfi_sort_key

# Py2/Py3
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems
from LiuXin_alpha.utils.libraries.liuxin_six import six_map
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode


__license__ = "GPL v3"
__copyright__ = "2014, Kovid Goyal <kovid at kovidgoyal.net>"


class Tests(unittest.TestCase):
    def test_sorting(self: _typing.Self) -> None:
        null_offsets = (0, (0, 0), 0)
        for path, key in [
            ("/1/2/3", ((1, 2, 3), null_offsets)),
            ("/1[id]:34[yyyy]", ((1,), (0, (0, 0), 34))),
            ("/1@1:2", ((1,), (0, (2, 1), 0))),
            ("/1~1.2", ((1,), (1.2, (0, 0), 0))),
        ]:
            self.assertEqual(cfi_sort_key(path), key)

    def test_parsing(self: _typing.Self) -> None:
        p = parser()

        def step(x: _typing.Any) -> dict[_typing.Any, _typing.Any]:
            if isinstance(x, int):
                return {"num": x}
            return {"num": x[0], "id": x[1]}

        def s(*args: _typing.Any) -> dict[_typing.Any, _typing.Any]:
            return {"steps": list(six_map(step, args))}

        def r(*args: _typing.Any) -> _typing.Any:
            idx = args.index("!")
            ans = s(*args[:idx])
            ans["redirect"] = s(*args[idx + 1 :])
            return ans

        def o(*args: _typing.Any) -> _typing.Any:
            ans = s(1)
            local_step = ans["steps"][-1]
            typ, val = args[:2]
            local_step[{"@": "spatial_offset", "~": "temporal_offset", ":": "text_offset"}[typ]] = val
            if len(args) == 4:
                typ, val = args[2:]
                local_step[{"@": "spatial_offset", "~": "temporal_offset"}[typ]] = val
            return ans

        def a(before: _typing.Any = None, after: _typing.Any = None, **params: _typing.Any) -> _typing.Any:
            ans = o(":", 3)
            local_step = ans["steps"][-1]
            ta = {}
            if before is not None:
                ta["before"] = before
            if after is not None:
                ta["after"] = after
            if params:
                ta["params"] = {six_unicode(k): (v,) if isinstance(v, str) else v for k, v in iteritems(params)}
            if ta:
                local_step["text_assertion"] = ta
            return ans

        for raw, path, leftover in [
            # Test parsing of steps
            ("/2", s(2), ""),
            ("/2/3/4", s(2, 3, 4), ""),
            ("/1/2[some^,^^id]/3", s(1, (2, "some,^id"), 3), ""),
            ("/1/2!/3/4", r(1, 2, "!", 3, 4), ""),
            ("/1/2[id]!/3/4", r(1, (2, "id"), "!", 3, 4), ""),
            ("/1!/2[id]/3/4", r(1, "!", (2, "id"), 3, 4), ""),
            # Test parsing of offsets
            ("/1~0", o("~", 0), ""),
            ("/1~7", o("~", 7), ""),
            ("/1~43.1", o("~", 43.1), ""),
            ("/1~0.01", o("~", 0.01), ""),
            ("/1~1.301", o("~", 1.301), ""),
            ("/1@23:34.1", o("@", (23, 34.1)), ""),
            ("/1~3@3.1:2.3", o("~", 3.0, "@", (3.1, 2.3)), ""),
            ("/1:0", o(":", 0), ""),
            ("/1:3", o(":", 3), ""),
            # Test parsing of text assertions
            ("/1:3[aa^,b]", a("aa,b"), ""),
            ("/1:3[aa^,b,c1]", a("aa,b", "c1"), ""),
            ("/1:3[,aa^,b]", a(after="aa,b"), ""),
            ("/1:3[;s=a]", a(s="a"), ""),
            ("/1:3[a;s=a]", a("a", s="a"), ""),
            ("/1:3[a;s=a^,b,c^;d;x=y]", a("a", s=("a,b", "c;d"), x="y"), ""),
        ]:
            self.assertEqual(p.parse_path(raw), (path, leftover))


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(Tests)
    unittest.TextTestRunner(verbosity=2).run(suite)
