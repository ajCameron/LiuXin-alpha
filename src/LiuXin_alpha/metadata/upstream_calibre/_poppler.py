"""
Run poppler's tools for the ported PDF reader: bounded, in a directory, and replaceable.

calibre's PDF reader (``pdf.py``) forks a worker process, changes the worker's
directory, and runs poppler's ``pdfinfo`` and ``pdftoppm`` there with no limits.
The fixes marked ``# LiuXin:`` in the port call this module instead:

- :func:`check_output` and :func:`check_call` run the tools through
  :func:`LiuXin_alpha.utils.bounded_process.run_tool`, with a timeout and with
  memory, CPU and output limits;
- :func:`in_directory` gives ``read_info`` the same two functions bound to its
  directory, so LiuXin's process, whose threads share one directory, never
  changes it;
- :func:`cover_scale` caps the cover for pages too large to draw at 150 dpi;
- :func:`run_here` runs ``read_info`` in this process in place of calibre's
  worker; inside :func:`without_poppler` it reads the same fields with LiuXin's
  own object reader, for the provider that works when poppler is not installed.

It also stands in for ``calibre.ebooks.pdf.pdftohtml``. That module's
``PDFTOHTML`` tells calibre where poppler's tools are: here, next to the
``pdfinfo`` on the PATH. The limits and the lookup are shared with LiuXin's
own reader, in :mod:`LiuXin_alpha.file_formats.pdf.poppler`.

Example:
    >>> cover_scale({"Page size": "612 x 792 pts (letter)"})
    ()
    >>> cover_scale({"Page size": "14400 x 14400 pts"})
    ('-scale-to', '3000')
"""

from __future__ import annotations

import os
import re
import subprocess
import traceback
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar

from LiuXin_alpha.file_formats.pdf.poppler import (
    PDFINFO_LIMITS,
    PDFTOPPM_LIMITS,
    tool_directory,
)
from LiuXin_alpha.metadata.upstream_calibre._compat import (
    WorkerError,
    iswindows,
    prints,
)
from LiuXin_alpha.utils.bounded_process import ToolLimits, run_tool

__all__ = [
    "COVER_PIXELS",
    "PDFINFO_LIMITS",
    "PDFTOPPM_LIMITS",
    "ToolLimits",
    "check_call",
    "check_output",
    "cover_scale",
    "creationflags",
    "in_directory",
    "iso_date",
    "read_info_without_poppler",
    "run_here",
    "tool_directory",
    "without_poppler",
]

#: The longest side of a cover drawn from a page too large for 150 dpi.
COVER_PIXELS = 3000
#: Windows process-creation flags, as calibre's ``pdftohtml`` module defines them.
creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0) if iswindows else 0
#: The Info fields ``pdfinfo`` prints, which calibre reads.
INFO_FIELDS = ("Title", "Subject", "Keywords", "Author", "Creator", "Producer", "CreationDate", "ModDate")

_PAGE_SIZE = re.compile(r"\s*(\d{1,9}(?:\.\d{0,9})?)\s*x\s*(\d{1,9}(?:\.\d{0,9})?)\s*pts\b")
_PDF_DATE = re.compile(
    r"\s*(?:D:)?(\d{4})(\d{2})?(\d{2})?(\d{2})?(\d{2})?(\d{2})?\s*(?:([Zz])|([+-])(\d{2})(?:'?(\d{2}))?'?)?"
)
_without_poppler: ContextVar[bool] = ContextVar("liuxin_pdf_without_poppler", default=False)


def __getattr__(name: str) -> str:
    """
    Give ``PDFTOHTML``, as ``calibre.ebooks.pdf.pdftohtml`` does, from where poppler is now.

    Example:
        >>> os.path.basename(__getattr__("PDFTOHTML")).startswith("pdftohtml")
        True


    :param name: The attribute.
    :return: The path ``pdftohtml`` would have; calibre takes its directory.
    :raises AttributeError: For any other name.
    """
    if name == "PDFTOHTML":
        return os.path.join(tool_directory() or "", "pdftohtml" + (".exe" if iswindows else ""))
    raise AttributeError(name)


def _limits_for(args: Sequence[str]) -> ToolLimits:
    """
    Choose the limits for a poppler command by the tool it runs.

    Example:
        >>> _limits_for(["/usr/bin/pdftoppm", "x"]) is PDFTOPPM_LIMITS
        True


    :param args: The command.
    :return: The limits.
    """
    tool = os.path.basename(args[0]).lower() if args else ""
    return PDFTOPPM_LIMITS if tool.startswith("pdftoppm") else PDFINFO_LIMITS


def check_output(args: Sequence[str], *, creationflags: int = 0, cwd: str | None = None) -> bytes:
    """
    Run a poppler tool within its limits and return what it printed.

    Example:
        >>> check_output(["pdfinfo", "-v"]) is not None  # doctest: +SKIP
        True


    :param args: The command.
    :param creationflags: Windows process-creation flags.
    :param cwd: The directory to run it in.
    :return: Its output.
    :raises subprocess.CalledProcessError: It failed, timed out, printed too much or is missing.
    """
    return run_tool(args, cwd=cwd, limits=_limits_for(args), creationflags=creationflags)


def check_call(args: Sequence[str], *, creationflags: int = 0, cwd: str | None = None) -> None:
    """
    Run a poppler tool within its limits.

    Example:
        >>> check_call(["pdftoppm", "-v"])  # doctest: +SKIP


    :param args: The command.
    :param creationflags: Windows process-creation flags.
    :param cwd: The directory to run it in.
    :return: None.
    :raises subprocess.CalledProcessError: It failed, timed out, wrote too much or is missing.
    """
    run_tool(args, cwd=cwd, limits=_limits_for(args), creationflags=creationflags)


def in_directory(directory: str) -> tuple[Callable[..., bytes], Callable[..., None]]:
    """
    Give ``read_info`` its ``check_output`` and ``check_call``, running the tools in a directory.

    Example:
        >>> output, call = in_directory("/tmp")
        >>> callable(output) and callable(call)
        True


    :param directory: The directory holding ``src.pdf``.
    :return: The two functions, taking the command's words as calibre's do.
    """

    def output(*args: str) -> bytes:
        """
        Run a tool in the directory and return what it printed.

        Example:
            >>> output("pdfinfo", "src.pdf")  # doctest: +SKIP


        :param args: The command's words.
        :return: Its output.
        """
        return check_output(list(args), creationflags=creationflags, cwd=directory)

    def call(*args: str) -> None:
        """
        Run a tool in the directory.

        Example:
            >>> call("pdftoppm", "src.pdf", "cover")  # doctest: +SKIP


        :param args: The command's words.
        :return: None.
        """
        check_call(list(args), creationflags=creationflags, cwd=directory)

    return output, call


def cover_scale(info: Mapping[str, object]) -> tuple[str, ...]:
    """
    Cap the cover's longest side at :data:`COVER_PIXELS` when the page is too large for 150 dpi.

    ``pdftoppm`` draws at 150 dpi; a 200-inch page would be 30,000 pixels square,
    gigabytes of memory. Pages within the cap are drawn as calibre draws them.

    Example:
        >>> cover_scale({"Page size": "1440 x 1440 pts"}), cover_scale({})
        ((), ('-scale-to', '3000'))


    :param info: What ``pdfinfo`` printed, with ``Page size`` for the first page.
    :return: Extra ``pdftoppm`` options.
    """
    raw = info.get("Page size")
    match = _PAGE_SIZE.match(raw) if isinstance(raw, str) else None
    if match is not None:
        largest = max(float(match.group(1)), float(match.group(2))) * 150 / 72
        if largest <= COVER_PIXELS:
            return ()
    return ("-scale-to", str(COVER_PIXELS))


@contextmanager
def without_poppler() -> Iterator[None]:
    """
    Make :func:`run_here` read the fields with LiuXin's object reader instead of poppler.

    Example:
        >>> with without_poppler():
        ...     _without_poppler.get()
        True


    :return: A context manager.
    """
    token = _without_poppler.set(True)
    try:
        yield
    finally:
        _without_poppler.reset(token)


def run_here(read_info: Callable[[str, bool], object], outputdir: str, get_cover: bool) -> dict[str, object]:
    """
    Stand in for calibre's ``fork_job`` call: run ``read_info`` in this process.

    calibre forks a worker so that ``read_info`` can change directory and so that
    the worker's output is collected. LiuXin's ``read_info`` changes no directory,
    and its messages go straight to standard output, so the output file is empty.

    Example:
        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as folder:
        ...     run_here(lambda folder, cover: {"Title": "T"}, folder, False)["result"]
        {'Title': 'T'}


    :param read_info: The port's ``read_info``.
    :param outputdir: The directory holding ``src.pdf``.
    :param get_cover: Whether to draw the cover.
    :return: ``result`` and ``stdout_stderr``, as ``fork_job`` returns them.
    :raises WorkerError: ``read_info`` raised, as calibre's worker reports it.
    """
    output = os.path.join(outputdir, "worker-output.txt")
    with open(output, "wb"):
        pass
    try:
        if _without_poppler.get():
            result: object = read_info_without_poppler(outputdir, get_cover)
        else:
            result = read_info(outputdir, get_cover)
    except Exception as error:
        raise WorkerError(f"reading the PDF failed: {error}", orig_tb=traceback.format_exc()) from error
    return {"result": result, "stdout_stderr": output}


def iso_date(raw: str) -> str:
    """
    Write a PDF date as ``pdfinfo -isodates`` does, keeping text that is not a date.

    Example:
        >>> iso_date("D:20190601120000+02'00'"), iso_date("D:20200102030405Z"), iso_date("D:2019")
        ('2019-06-01T12:00:00+02', '2020-01-02T03:04:05Z', '2019-01-01T00:00:00Z')
        >>> iso_date("D:20190601120000+05'30'"), iso_date("garbage")
        ('2019-06-01T12:00:00+05:30', 'garbage')


    :param raw: The date as the Info dictionary holds it.
    :return: The ISO 8601 form, or ``raw`` unchanged.
    """
    match = _PDF_DATE.match(raw)
    if match is None:
        return raw
    year, month, day, hour, minute, second, utc, sign, zone_hours, zone_minutes = match.groups()
    text = f"{year}-{month or '01'}-{day or '01'}T{hour or '00'}:{minute or '00'}:{second or '00'}"
    if sign and zone_hours:
        text += f"{sign}{zone_hours}" + (f":{zone_minutes}" if zone_minutes and zone_minutes != "00" else "")
    else:
        text += "Z"
    return text


def read_info_without_poppler(outputdir: str, get_cover: bool) -> dict[str, str | bytes] | None:
    """
    Read what ``read_info`` reads, with LiuXin's own object reader instead of poppler, and no cover.

    The Info fields ``pdfinfo`` prints are taken as text on one line, with dates in
    its ISO form, and the XMP packet as ``xmp_metadata``. calibre's ``get_metadata``
    then treats them as it treats poppler's.

    Example:
        >>> import tempfile
        >>> from LiuXin_alpha.file_formats.pdf.objects import minimal_pdf
        >>> with tempfile.TemporaryDirectory() as folder:
        ...     with open(os.path.join(folder, "src.pdf"), "wb") as handle:
        ...         _ = handle.write(minimal_pdf(b"<< /Title (T) /ModDate (D:20200102030405Z) >>"))
        ...     read_info_without_poppler(folder, True)
        {'Title': 'T', 'ModDate': '2020-01-02T03:04:05Z'}


    :param outputdir: The directory holding ``src.pdf``.
    :param get_cover: Ignored: drawing a page needs poppler.
    :return: The fields, or ``None`` when the file cannot be read, as ``read_info``
        returns when ``pdfinfo`` fails.
    """
    from LiuXin_alpha.file_formats.pdf.objects import PdfReadError, read_metadata

    try:
        with open(os.path.join(outputdir, "src.pdf"), "rb") as stream:
            metadata = read_metadata(stream)
    except PdfReadError as error:
        prints(f"LiuXin's PDF reader could not read the file: {error}")
        return None
    for problem in metadata.problems:
        prints(f"LiuXin's PDF reader lost part of the metadata: {problem}")
    answer: dict[str, str | bytes] = {}
    for field in INFO_FIELDS:
        value = metadata.info.get(field)
        if not isinstance(value, str):
            continue
        value = " ".join(value.splitlines()).strip()
        if field in ("CreationDate", "ModDate"):
            value = iso_date(value)
        if value:
            answer[field] = value
    if metadata.xmp and metadata.xmp.strip():
        answer["xmp_metadata"] = metadata.xmp.strip()
    return answer
