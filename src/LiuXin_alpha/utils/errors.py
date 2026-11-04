"""Render bounded exception diagnostics without invoking a second failure path."""


def exception_message(error: BaseException, *, limit: int = 2000) -> str:
    """Return printable text suitable for durable SQLite/PostgreSQL diagnostics.

    A broken plugin's string formatter must not replace the original error or
    prevent a retry receipt. Escape NUL and replace invalid Unicode surrogates;
    neither can be stored unchanged in PostgreSQL UTF-8 text. This does not make
    arbitrary in-process plugin code safe or bound its execution time.
    """
    try:
        message = str(error)
    except Exception:
        message = f"{type(error).__name__} (exception message unavailable)"
    return (
        message[:limit]
        .replace("\x00", r"\x00")
        .encode("utf-8", "replace")
        .decode("utf-8")[:limit]
    )
