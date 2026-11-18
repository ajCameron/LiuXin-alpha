"""Bridge the pinned template evaluator to LiuXin's existing function registry."""

from contextlib import contextmanager
from contextvars import ContextVar
from enum import Enum, auto
from types import SimpleNamespace

_functions = ContextVar("calibre_search_template_functions", default=None)


@contextmanager
def template_function_context(functions):
    """Keep explicitly injected functions available to nested formatters only."""
    token = _functions.set(functions)
    try:
        yield
    finally:
        _functions.reset(token)


class StoredObjectType(Enum):
    PythonFunction = auto()
    StoredGPMTemplate = auto()
    StoredPythonTemplate = auto()


def function_object_type(value):
    text = value if isinstance(value, str) else value.program_text
    if text.startswith("program:"):
        return StoredObjectType.StoredGPMTemplate
    if text.startswith("python:"):
        return StoredObjectType.StoredPythonTemplate
    return StoredObjectType.PythonFunction


class _Function:
    def __init__(self, value):
        self._value = value
        self.object_type = function_object_type(value)
        self.cached_parse_tree = None

    def __getattr__(self, name):
        return getattr(self._value, name)


def formatter_functions():
    from .upstream_functions import builtin_functions

    def functions():
        return {**builtin_functions(), **(_functions.get() or {})}

    return SimpleNamespace(get_functions=functions)


def get_database(book, function_name):
    database = getattr(book, "_search_database", None)
    if database is None and function_name:
        raise ValueError(
            f"{function_name} requires an explicitly supplied database provider"
        )
    return database


def error_message(error):
    return str(error)
