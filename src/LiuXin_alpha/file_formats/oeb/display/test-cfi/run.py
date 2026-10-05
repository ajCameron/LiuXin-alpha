#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Run retained CFI display tests against an OEB document.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise run through a consuming regression::

        python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import os

__license__ = "GPL v3"
__copyright__ = "2011, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


# Todo: What does init_calibre do?
try:
    from LiuXin_alpha.utils.serve_coffee import serve
except ImportError:
    import init_calibre

    if False:
        init_calibre, serve
    from LiuXin_alpha.utils.serve_coffee import serve


def run_devel_server() -> None:
    """
    Perform the run devel server operation under explicit file-format and conversion rules.

    Example:
        Exercise run devel server through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    serve(resources={"cfi.coffee": "../cfi.coffee", "/": "index.html"})


if __name__ == "__main__":
    run_devel_server()
