
"""
This is looking to be a common pattern, so a specific event log would be helpful.

This is intended to be embedded in a lot of classes - and provide a common interface
(probably out to the databases, but the advantage of common interface is it doesn't need to be decided now).
"""

from __future__ import annotations


import logging
import sys
import os

from LiuXin_alpha.utils.which_os import iswindows

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode as unicode




def multi_string_print(*args: str) -> None:
    """
    Print when multiple arguments are given.

    :param args:
    :return:
    """
    args = [str(arg) for arg in args]
    print("\n".join(args))



LiuXin_print = multi_string_print
LiuXin_debug_print = multi_string_print
LiuXin_warning_print = multi_string_print


def prints(*args, **kwargs):
    """
    Print Unicode arguments safely by encoding them to preferred_encoding.

    Has the same signature as the print function from Python 3.
    Except for the additional keyword argument safe_encode.
    Which if set to True will cause the function to use repr when encoding fails.

    :param args:
    :param kwargs:
    :return:
    """
    file = kwargs.get("file", sys.stdout)
    sep = kwargs.get("sep", " ")
    end = kwargs.get("end", "\n")
    try:
        enc = preferred_encoding
    except:
        enc = sys.getdefaultencoding()

    safe_encode = kwargs.get("safe_encode", False)

    if "CALIBRE_WORKER" in os.environ:
        enc = "utf-8"

    for i, arg in enumerate(args):

        if isinstance(arg, str):

            if iswindows:
                from LiuXin_alpha.utils.terminal import Detect

                # Todo: This is absolutely not working in any way at all - even a bit
                # Todo: In fact, it is on fire. Right now. Actual flames.
                cs = Detect(file)
                if cs.is_console:
                    cs.write_unicode_text(arg)
                    if i != len(args) - 1:
                        file.write(sep)
                    continue

            try:
                arg = arg.encode(enc)
            except UnicodeEncodeError:
                try:
                    arg = arg.encode("utf-8")
                except:
                    if not safe_encode:
                        raise
                    arg = repr(arg)

            # arg is now in bytes - try turning it back into a utf-8 string
            try:
                arg = arg.decode("utf-8")
            except UnicodeEncodeError:
                if not safe_encode:
                    raise
                arg = repr(arg)

        if isinstance(arg, bytes):
            arg = arg.decode("utf-8")

        if not isinstance(arg, str):
            try:
                arg = str(arg)
            except ValueError:
                arg = unicode(arg)
            if isinstance(arg, unicode):
                try:
                    arg = arg.encode(enc)
                except UnicodeEncodeError:
                    try:
                        arg = arg.encode("utf-8")
                    except:
                        if not safe_encode:
                            raise
                        arg = repr(arg)

        try:
            file.write(arg)
        except:
            import reprlib

            file.write(reprlib.repr(arg))
        if i != len(args) - 1:
            file.write(bytes(sep, "utf-8").decode("utf-8"))

    file.write(bytes(end, "utf-8").decode("utf-8"))




import logging
from dataclasses import dataclass
from itertools import islice
from typing import Any, Iterable, Mapping, MutableMapping, Sequence, Tuple, Union, Optional

LevelLike = Union[int, str]


def _coerce_level(level: LevelLike) -> int:
    """
    Accepts logging level ints, or common strings like:
    'DEBUG', 'INFO', 'WARNING'/'WARN', 'ERROR', 'CRITICAL'/'FATAL'.
    """
    if isinstance(level, int):
        return level

    s = str(level).strip().upper()
    if s == "WARN":
        s = "WARNING"
    if s == "FATAL":
        s = "CRITICAL"

    lvl = logging.getLevelName(s)
    # logging.getLevelName("INFO") returns 20 (int) in recent Python,
    # but can return a string if unknown.
    if isinstance(lvl, int):
        return lvl

    # Fallback: try standard mapping
    mapping = {
        "NOTSET": logging.NOTSET,
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }
    return mapping.get(s, logging.INFO)


def _safe_repr(obj: Any, *, max_len: int = 400, max_items: int = 25) -> str:
    """
    Best-effort repr with truncation + container sampling to avoid huge logs.
    """
    try:
        if isinstance(obj, Mapping):
            items = list(islice(obj.items(), max_items))
            body = ", ".join(f"{_safe_repr(k, max_len=max_len, max_items=max_items)}: "
                             f"{_safe_repr(v, max_len=max_len, max_items=max_items)}"
                             for k, v in items)
            suffix = ", …" if len(obj) > max_items else ""
            s = "{" + body + suffix + "}"
        elif isinstance(obj, (list, tuple, set, frozenset)):
            seq = list(islice(obj, max_items))
            body = ", ".join(_safe_repr(v, max_len=max_len, max_items=max_items) for v in seq)
            more = ", …" if _maybe_has_more(obj, max_items) else ""
            if isinstance(obj, tuple):
                # Keep tuple syntax for 1-element tuples
                if len(seq) == 1 and not more:
                    body = body + ","
                s = "(" + body + more + ")"
            elif isinstance(obj, (set, frozenset)):
                s = "{" + body + more + "}"
            else:
                s = "[" + body + more + "]"
        else:
            s = repr(obj)
    except Exception as e:  # pragma: no cover (rare, but defensive)
        s = f"<unreprable {type(obj).__name__}: {e!r}>"

    if len(s) > max_len:
        s = s[: max(0, max_len - 3)] + "..."
    return s


def _maybe_has_more(container: Any, max_items: int) -> bool:
    try:
        # len() might be expensive or unsupported; best-effort
        return len(container) > max_items  # type: ignore[arg-type]
    except Exception:
        return False


def _coerce_pairs(*pairs: Any) -> dict[str, Any]:
    """
    Accepts:
      - ("k", v) tuples
      - mappings (merged)
    """
    out: dict[str, Any] = {}
    for p in pairs:
        if p is None:
            continue
        if isinstance(p, Mapping):
            for k, v in p.items():
                out[str(k)] = v
            continue
        if isinstance(p, tuple) and len(p) == 2:
            k, v = p
            out[str(k)] = v
            continue
        raise TypeError(
            "log_variables() expects ('key', value) tuples and/or mapping objects; "
            f"got {type(p).__name__}: {p!r}"
        )
    return out


@dataclass(frozen=True)
class LogVariablesFormat:
    """
    Formatting knobs so you can tweak output without changing callsites.
    """
    sep: str = "\n"
    kv_sep: str = " = "
    prefix: str = ""               # e.g. "  " to indent kv lines
    include_empty_base: bool = False
    sort_keys: bool = True
    max_repr_len: int = 400
    max_repr_items: int = 25


class CompatLogger(logging.Logger):
    """
    Backwards-compatible logger with log_variables() that:
      - takes an existing string (or None) and appends key/value context,
      - logs at the requested level,
      - returns the enriched string.
    """

    def __init__(self, name: str, level: int = logging.NOTSET) -> None:
        super().__init__(name, level)
        self._logvars_format = LogVariablesFormat()

    # Optional: let you override formatting at runtime if you like.
    def set_logvars_format(self, fmt: LogVariablesFormat) -> None:
        self._logvars_format = fmt

    def log_variables(
        self,
        base: Optional[str],
        level: LevelLike,
        *pairs: Any,
        emit: bool = True,
        fmt: Optional[LogVariablesFormat] = None,
    ) -> str:
        """
        Backwards-compatible call pattern:

            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("target_table", target_table),
                ("cand_cc_link_table", cand_cc_link_table),
            )

        - base: prior message string (or None)
        - level: int or string level
        - pairs: ('k', v) tuples and/or mapping(s)
        - emit: whether to actually call logger.log()
        - fmt: optional per-call formatting override
        """
        level_int = _coerce_level(level)
        data = _coerce_pairs(*pairs)
        f = fmt or self._logvars_format

        lines: list[str] = []
        if base is None:
            if f.include_empty_base:
                lines.append("")
        else:
            s = str(base)
            if s or f.include_empty_base:
                lines.append(s)

        keys = sorted(data.keys()) if f.sort_keys else list(data.keys())
        for k in keys:
            v = data[k]
            v_str = _safe_repr(v, max_len=f.max_repr_len, max_items=f.max_repr_items)
            lines.append(f"{f.prefix}{k}{f.kv_sep}{v_str}")

        out = f.sep.join(lines)

        if emit:
            # include structured context in `extra` for formatters/filters if desired
            self.log(level_int, out, extra={"vars": data})

        return out

    # Handy single-variable wrapper (optional convenience)
    def log_variable(
        self,
        base: Optional[str],
        level: LevelLike,
        key: str,
        value: Any,
        *,
        emit: bool = True,
        fmt: Optional[LogVariablesFormat] = None,
    ) -> str:
        return self.log_variables(base, level, (key, value), emit=emit, fmt=fmt)


def install_compat_logger_class() -> None:
    """
    Call this once, early in program startup, before any getLogger() calls.
    """
    logging.setLoggerClass(CompatLogger)


def get_compat_logger(name: str) -> CompatLogger:
    """
    Convenience getter when you've installed the logger class.
    """
    logger = logging.getLogger(name)
    if not isinstance(logger, CompatLogger):
        # If someone grabbed a logger before install_compat_logger_class(),
        # you can still wrap by recreating the logger (rare).
        raise TypeError(
            f"Logger for {name!r} is {type(logger).__name__}, not CompatLogger. "
            "Call install_compat_logger_class() before any getLogger()."
        )
    return logger


install_compat_logger_class()

default_log = logging.getLogger("LiuXin_alpha-default-log")


# --- Example wiring ---
if __name__ == "__main__":
    install_compat_logger_class()

    default_log = get_compat_logger(__name__)
    default_log.setLevel(logging.DEBUG)

    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    handler.setFormatter(formatter)
    default_log.addHandler(handler)

    err_str: Optional[str] = "Something went wrong"
    err_str = default_log.log_variables(
        err_str,
        "ERROR",
        ("target_table", "books"),
        ("cand_cc_link_table", {"a": 1, "b": 2, "c": 3}),
    )
    # err_str now contains the enriched multi-line message.
