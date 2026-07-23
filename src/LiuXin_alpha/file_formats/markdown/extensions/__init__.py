from __future__ import unicode_literals
from __future__ import annotations

import typing as _typing

"""
Extensions
-----------------------------------------------------------------------------
"""


class Extension(object):
    """Base class for extensions to subclass."""

    def __init__(self: _typing.Self, configs: _typing.Any = None) -> None:
        """Create an instance of an Extention.

        Keyword arguments:

        * configs: A dict of configuration setting used by an Extension.
        """
        self.config = configs or {}

    def getConfig(self: _typing.Self, key: _typing.Any, default: str = "") -> _typing.Any:
        """Return a setting for the given key or an empty string."""
        if key in self.config:
            return self.config[key][0]
        else:
            return default

    def getConfigs(self: _typing.Self) -> _typing.Any:
        """Return all configs settings as a dict."""
        return dict([(key, self.getConfig(key)) for key in self.config.keys()])

    def getConfigInfo(self: _typing.Self) -> _typing.Any:
        """Return all config descriptions as a list of tuples."""
        return [(key, self.config[key][1]) for key in self.config.keys()]

    def setConfig(self: _typing.Self, key: _typing.Any, value: _typing.Any) -> None:
        """Set a config setting for `key` with the given `value`."""
        self.config[key][0] = value

    def extendMarkdown(self: _typing.Self, md: _typing.Any, md_globals: _typing.Any) -> None:
        """
        Add the various proccesors and patterns to the Markdown Instance.

        This method must be overriden by every extension.

        Keyword arguments:

        * md: The Markdown instance.

        * md_globals: Global variables in the markdown module namespace.

        """
        raise NotImplementedError(
            'Extension "%s.%s" must define an "extendMarkdown"'
            "method." % (self.__class__.__module__, self.__class__.__name__)
        )
