from __future__ import absolute_import, division, unicode_literals

from LiuXin_alpha.utils.libraries.liuxin_html5lib.filters import _base
from LiuXin_alpha.utils.libraries.liuxin_html5lib.sanitizer import HTMLSanitizerMixin


class Filter(_base.Filter, HTMLSanitizerMixin):
    def __iter__(self):
        for token in _base.Filter.__iter__(self):
            token = self.sanitize_token(token)
            if token:
                yield token
