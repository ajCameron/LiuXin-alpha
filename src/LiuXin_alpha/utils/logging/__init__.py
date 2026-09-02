
"""
This is looking to be a common pattern, so a specific event log would be helpful.

This is intended to be embedded in a lot of classes - and provide a common interface
(probably out to the databases, but the advantage of common interface is it doesn't need to be decided now).
"""

from __future__ import annotations


import logging
import sys
import os
import traceback

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


def multi_string_warning(*args: str) -> None:
    """Print warning text to stderr without contaminating data output."""

    args = [str(arg) for arg in args]
    print("\n".join(args), file=sys.stderr)



LiuXin_print = multi_string_print
LiuXin_debug_print = multi_string_print
LiuXin_warning_print = multi_string_warning


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



# We intentionally store structured vars in logging `extra` so tools like pytest-json-report
# can capture them. Those tools expect JSON-serializable data. When callers pass rich objects
# (Rows, Path, exceptions, etc.), we fall back to a safe string representation.
def _is_json_primitive(obj: Any) -> bool:
    return obj is None or isinstance(obj, (str, int, float, bool))


def _jsonability_probe(obj: Any, *, max_items: int = 25, max_depth: int = 2, _depth: int = 0) -> bool:
    """Best-effort check for JSON-serializability (bounded depth/size).

    We only need a cheap signal to decide whether to attach a helpful note.
    """
    if _is_json_primitive(obj):
        return True

    if _depth >= max_depth:
        return False

    try:
        if isinstance(obj, Mapping):
            for i, (k, v) in enumerate(obj.items()):
                if i >= max_items:
                    break
                if not _is_json_primitive(k):
                    return False
                if not _jsonability_probe(v, max_items=max_items, max_depth=max_depth, _depth=_depth + 1):
                    return False
            return True

        if isinstance(obj, (list, tuple)):
            for i, v in enumerate(obj):
                if i >= max_items:
                    break
                if not _jsonability_probe(v, max_items=max_items, max_depth=max_depth, _depth=_depth + 1):
                    return False
            return True

        # json can’t represent sets by default.
        return False
    except Exception:
        return False


def _fallback_items(data: Mapping[str, Any], *, max_items: int = 25) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for k, v in data.items():
        if not _jsonability_probe(v, max_items=max_items):
            out.append((k, type(v).__name__))
    return out


def _format_fallback_note(items: Sequence[tuple[str, str]], *, max_show: int = 10) -> str:
    shown = items[:max_show]
    sig = ", ".join(f"{k}:{t}" for k, t in shown)
    if len(items) > max_show:
        sig += f" … (+{len(items) - max_show} more)"
    return sig


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

    def __call__(self, *args: Any) -> "CompatLogger":
        """
        Backward-compat callable logger:
        - `log()` returns the logger instance (legacy call sites did `log = Log()`).
        - `log("a", "b")` logs an INFO line with joined arguments.
        """
        if not args:
            return self
        self.info(" ".join(str(arg) for arg in args))
        return self

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
            # NOTE: keep it JSON-serializable (pytest-json-report captures log records).
            safe_vars = {
                k: _safe_repr(v, max_len=f.max_repr_len, max_items=f.max_repr_items) for k, v in data.items()
            }

            fallbacks = _fallback_items(data, max_items=f.max_repr_items)
            extra: dict[str, Any] = {"vars": safe_vars}

            if fallbacks:
                note = _format_fallback_note(fallbacks)
                # Add a human-friendly hint into the message (useful when this string becomes an exception)
                out = out + f.sep + f"{f.prefix}__json_fallback__{f.kv_sep}"                     f"Non-JSON values were stringified for structured logs: {note}. "                     f"Prefer ids / primitives (e.g. row_id) to keep reports clean."
                # Also attach machine-readable detail for reporters.
                extra["liuxin_json_fallback"] = {
                    "count": len(fallbacks),
                    "items": [f"{k}:{t}" for k, t in fallbacks[:25]],
                }

            self.log(level_int, out, extra=extra)

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



    def log_exception(
        self,
        base: Optional[str],
        exc: BaseException,
        level: LevelLike,
        *pairs: Any,
        emit: bool = True,
        fmt: Optional[LogVariablesFormat] = None,
        include_traceback: bool = True,
    ) -> str:
        """
        Log an exception with structured context, returning an enriched message string.

        Compatible with call sites like::

            err_str = default_log.log_exception(
                "sqlite3.OperationalError.",
                e,
                "ERROR",
                ("stmt", stmt),
                ("values", values),
            )

        The returned string is suitable for raising/wrapping errors.
        By default, the log emission includes ``exc_info`` (so tracebacks
        appear in logs) while the returned string stays compact.

        :param base: Base message string to enrich (or None)
        :param exc: The exception instance
        :param level: Logging level (int or common string)
        :param pairs: Optional (key, value) context tuples
        :param emit: If True, emit to the logger; always returns the enriched string
        :param fmt: Optional per-call formatting overrides
        :param include_traceback: If True, log with exc_info (traceback). The returned
                                  string still includes only a concise exception summary.
        """
        level_int = _coerce_level(level)

        exc_type = type(exc).__name__
        exc_msg = str(exc)
        summary = f"{exc_type}: {exc_msg}" if exc_msg else exc_type

        # Add a concise exception summary as a structured variable, then append caller context.
        out = self.log_variables(
            base,
            level_int,
            ("exception", summary),
            *pairs,
            emit=False,
            fmt=fmt,
        )

        if emit:
            pair_data = _coerce_pairs(*pairs)
            safe_vars = {
                k: _safe_repr(v, max_len=(fmt or self._logvars_format).max_repr_len, max_items=(fmt or self._logvars_format).max_repr_items)
                for k, v in pair_data.items()
            }
            fallbacks = _fallback_items(pair_data, max_items=(fmt or self._logvars_format).max_repr_items)
            extra: dict[str, Any] = {
                "vars": safe_vars,
                "exception_type": exc_type,
                "exception": summary,
            }

            if fallbacks:
                note = _format_fallback_note(fallbacks)
                out = out + (fmt or self._logvars_format).sep + f"{(fmt or self._logvars_format).prefix}__json_fallback__{(fmt or self._logvars_format).kv_sep}"                     f"Non-JSON values were stringified for structured logs: {note}. "                     f"Prefer ids / primitives (e.g. row_id) to keep reports clean."
                extra["liuxin_json_fallback"] = {
                    "count": len(fallbacks),
                    "items": [f"{k}:{t}" for k, t in fallbacks[:25]],
                }

            if include_traceback:
                self.log(
                    level_int,
                    out,
                    exc_info=(type(exc), exc, exc.__traceback__),
                    extra=extra,
                )
            else:
                self.log(level_int, out, extra=extra)

        return out

    # Compatibility alias: some codebases prefer plural naming
    def log_exceptions(
        self,
        base: Optional[str],
        exc: BaseException,
        level: LevelLike,
        *pairs: Any,
        emit: bool = True,
        fmt: Optional[LogVariablesFormat] = None,
        include_traceback: bool = True,
    ) -> str:
        return self.log_exception(
            base,
            exc,
            level,
            *pairs,
            emit=emit,
            fmt=fmt,
            include_traceback=include_traceback,
        )

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
