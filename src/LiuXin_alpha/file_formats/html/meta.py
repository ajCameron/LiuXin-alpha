from __future__ import with_statement
from __future__ import annotations

import typing as _typing

__license__ = "GPL 3"
__copyright__ = "2010, Fabian Grassl <fg@jusmeum.de>"
__docformat__ = "restructuredtext en"


class EasyMeta(object):
    def __init__(self: _typing.Self, meta: _typing.Any) -> None:
        self.meta = meta

    def __iter__(self: _typing.Self) -> _typing.Iterator[_typing.Any]:

        from LiuXin_alpha.file_formats.oeb.base import namespace, barename, DC11_NS

        meta = self.meta
        for item_name in meta.items:
            for item in meta[item_name]:
                if namespace(item.term) == DC11_NS:
                    yield {"name": barename(item.term), "value": item.value}

    def __len__(self: _typing.Self) -> _typing.Any:
        return sum(1 for _ in self)

    def titles(self: _typing.Self) -> _typing.Iterator[_typing.Any]:
        for item in self.meta["title"]:
            yield item.value

    def creators(self: _typing.Self) -> _typing.Iterator[_typing.Any]:
        for item in self.meta["creator"]:
            yield item.value
