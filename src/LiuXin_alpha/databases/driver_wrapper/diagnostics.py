"""
Optional operation diagnostics with no timer or callback work when disabled.
"""

import logging
from collections.abc import Callable
from contextvars import ContextVar
from functools import wraps
from inspect import isawaitable
from time import perf_counter
from typing import Literal, ParamSpec, TypeVar, cast

from LiuXin_alpha.databases.db_types.driver_types import RowPage
from LiuXin_alpha.databases.db_types.query_observation import (
    QueryObservation,
    QueryObserver,
)

P = ParamSpec("P")
R = TypeVar("R")
_observing: ContextVar[bool] = ContextVar("liuxin_query_observing", default=False)
_log = logging.getLogger(__name__)


def _row_count(result: object, mode: str) -> int | None:
    """
    Count returned rows only where the operation's result shape proves it.

    :param result:
    :param mode:
    :return:
    """
    if mode == "one":
        return 0 if result is None or result is False else 1
    if mode == "all" and isinstance(result, (list, tuple)):
        return len(result)
    if mode == "page" and isinstance(result, RowPage):
        return len(result.rows)
    return None


# Todo: More context as to what this if for and what it does
def observed(
    operation: str,
    *,
    count: Literal["unknown", "one", "all", "page", "get"] = "unknown",
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """
    Observe the outermost selected wrapper operation; observers cannot recurse.

    :param operation:
    :param count:
    :return:
    """

    def decorate(function: Callable[P, R]) -> Callable[P, R]:
        @wraps(function)
        def run(*args: P.args, **kwargs: P.kwargs) -> R:
            callbacks: tuple[QueryObserver, ...] = getattr(
                args[0], "_query_observers", ()
            )
            if not callbacks or _observing.get():
                return function(*args, **kwargs)
            token = _observing.set(True)
            start = perf_counter()
            row_count = None
            error_type = None
            try:
                result = function(*args, **kwargs)
                mode = (
                    ("all" if kwargs.get("all", True) else "one")
                    if count == "get"
                    else count
                )
                row_count = _row_count(result, mode)
                return result
            except BaseException as error:
                error_type = type(error).__name__
                raise
            finally:
                observation = QueryObservation(
                    operation, perf_counter() - start, row_count, error_type
                )
                try:
                    for callback in callbacks:
                        try:
                            returned = cast(
                                Callable[[QueryObservation], object], callback
                            )(observation)
                            if isawaitable(returned):
                                close = getattr(returned, "close", None)
                                if callable(close):
                                    close()
                                raise TypeError("Query observers must be synchronous")
                        except Exception as observer_error:
                            _log.warning(
                                "Query observer failed (%s)",
                                type(observer_error).__name__,
                            )
                finally:
                    _observing.reset(token)

        return run

    return decorate
