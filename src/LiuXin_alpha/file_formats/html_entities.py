#!/usr/bin/env python
# vim:fileencoding=utf-8

from __future__ import unicode_literals, division, absolute_import, print_function

# Ported from calibre.
# The LiuXin-alpha tree vendors the html5lib constants and a small compat layer.
from LiuXin_alpha.utils.libraries.liuxin_html5lib.constants import entities

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"


html5_entities = {k.replace(";", ""): v for k, v in entities.items()}
