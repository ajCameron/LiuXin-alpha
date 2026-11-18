"""
Read metadata and covers with an installed calibre, as providers.

These providers make current upstream calibre part of the reader pool without
copying its code. Each runs one of calibre's own metadata reader plugins
inside the user's installed calibre, through ``calibre-debug``, and returns
calibre's own serialisation of what it read. Where calibre is not installed,
the providers report themselves unavailable and nothing else changes.

There is one provider per reader plugin in calibre 9.18, listed in
:data:`UPSTREAM_READERS`. Each also extracts covers, because calibre's readers
return the cover with the metadata. One run of calibre answers both, and the
answer is kept briefly so that reading the metadata and then the cover of the
same unchanged file starts calibre once.

The script calibre runs is ``LiuXin_alpha.file_formats.calibre_metadata_probe``.
It is copied to an empty folder under the work folder before each run, because
``calibre-debug`` puts the script's folder first on ``sys.path`` and LiuXin's
own folders would shadow modules calibre needs.

Importing this module starts nothing and loads no calibre code.
"""

from __future__ import annotations

import base64
import json
import os
import shutil
import subprocess
import sys
import tempfile
import types
from collections import OrderedDict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, Self, cast

from LiuXin_alpha.file_formats.conversion.edges import normalize_format_name
from LiuXin_alpha.file_formats.providers.base import (
    ProviderAvailability,
    ProviderContext,
    provider_id_from_name,
)
from LiuXin_alpha.file_formats.providers.metadata import (
    CalibreFields,
    CalibreMetadataReader,
    CoverExtractor,
    CoverImage,
    JSONValue,
)

if TYPE_CHECKING:
    from LiuXin_alpha.file_formats.providers.registry import ProviderRegistry

UPSTREAM_CALIBRE_VERSION = "9.18"
"""The calibre release :data:`UPSTREAM_READERS` was taken from."""

UPSTREAM_READERS: tuple[tuple[str, frozenset[str]], ...] = (
    ("Read comic metadata", frozenset({"cbz", "cb7", "cbc", "cbr"})),
    ("Read CHM metadata", frozenset({"chm"})),
    ("Read EPUB metadata", frozenset({"epub", "kepub"})),
    ("Read FB2 metadata", frozenset({"fb2", "fbz"})),
    ("Read HTML metadata", frozenset({"html"})),
    ("Read HTMLZ metadata", frozenset({"htmlz"})),
    ("Read IMP metadata", frozenset({"imp"})),
    ("Read LIT metadata", frozenset({"lit"})),
    ("Read LRF metadata", frozenset({"lrf"})),
    ("Read LRX metadata", frozenset({"lrx"})),
    ("Read MOBI metadata", frozenset({"azw", "azw3", "azw4", "mobi", "pobi", "prc"})),
    ("Read ODT metadata", frozenset({"odt"})),
    ("Read DOCX metadata", frozenset({"docx"})),
    ("Read OPF metadata", frozenset({"opf"})),
    ("Read PDB metadata", frozenset({"pdb", "updb"})),
    ("Read PDF metadata", frozenset({"pdf"})),
    ("Read PML metadata", frozenset({"pml", "pmlz"})),
    ("Read RAR metadata", frozenset({"rar"})),
    ("Read RB metadata", frozenset({"rb"})),
    ("Read RTF metadata", frozenset({"rtf"})),
    ("Read SNB metadata", frozenset({"snb"})),
    ("Read Topaz metadata", frozenset({"azw1", "tpz"})),
    ("Read TXT metadata", frozenset({"txt"})),
    ("Read TXTZ metadata", frozenset({"txtz"})),
    ("Read ZIP metadata", frozenset({"oebzip", "zip"})),
)
"""calibre's metadata reader plugins: plugin name and the formats it reads."""

UPSTREAM_PRIORITY = -60
"""Priority of every upstream calibre reader.

Above the adapted fork readers, which use the legacy priority of -100, and
below any native provider.
"""

CALIBRE_DEBUG_VARIABLE = "LIUXIN_CALIBRE_DEBUG"
"""Environment variable naming the ``calibre-debug`` program to use."""

PROBE_FILE = Path(__file__).resolve().parent.parent / "calibre_metadata_probe.py"
"""The script calibre runs: ``LiuXin_alpha.file_formats.calibre_metadata_probe``."""

_KNOWN_LOCATIONS: Mapping[str, tuple[str, ...]] = {
    "win32": (
        r"C:\Program Files\Calibre2\calibre-debug.exe",
        r"C:\Program Files (x86)\Calibre2\calibre-debug.exe",
    ),
    "darwin": ("/Applications/calibre.app/Contents/MacOS/calibre-debug",),
}

_CACHE_SIZE = 16
_cache: OrderedDict[tuple[object, ...], CalibreReading] = OrderedDict()


class CalibreRunError(RuntimeError):
    """
    Report that calibre could not be run, or could not read the file.

    The message carries what calibre printed or the traceback it reported, so
    the cause can be seen without running calibre again by hand.

    Example:
        >>> issubclass(CalibreRunError, RuntimeError)
        True
    """


@dataclass(frozen=True, slots=True)
class CalibreReading:
    """
    Hold what one run of calibre said about one file.

    Example:
        >>> reading = CalibreReading("9.18", CalibreFields({"title": "A"}), None, ())
        >>> reading.fields.values["title"], reading.cover is None
        ('A', True)


    :param calibre_version: The version of the calibre that read the file.
    :param fields: The metadata, in calibre's names and shapes.
    :param cover: The cover image, or ``None`` if calibre found none.
    :param text_fields: Places where calibre gave a value with no JSON form,
        which were written as text.
    """

    calibre_version: str
    fields: CalibreFields
    cover: CoverImage | None
    text_fields: tuple[str, ...]


def find_calibre_debug(
    environ: Mapping[str, str] | None = None, platform: str | None = None
) -> Path | None:
    """
    Find the ``calibre-debug`` program to use, if there is one.

    The program named by :data:`CALIBRE_DEBUG_VARIABLE` comes first, then
    ``calibre-debug`` on the ``PATH``, then calibre's standard install
    locations for the platform.

    Example:
        >>> find_calibre_debug({"PATH": ""}, "linux") is None
        True


    :param environ: The environment to read; the process environment if
        ``None``.
    :param platform: ``sys.platform`` to assume; the current one if ``None``.
    :return: The program's path, or ``None`` if calibre was not found.
    """
    env = os.environ if environ is None else environ
    named = env.get(CALIBRE_DEBUG_VARIABLE)
    if named:
        candidate = Path(named)
        return candidate if candidate.is_file() else None
    found = shutil.which("calibre-debug", path=env.get("PATH", ""))
    if found:
        return Path(found)
    for location in _KNOWN_LOCATIONS.get(platform or sys.platform, ()):
        candidate = Path(location)
        if candidate.is_file():
            return candidate
    return None


def probe_source() -> Path:
    """
    Locate the probe script that calibre runs.

    Example:
        >>> probe_source().name
        'calibre_metadata_probe.py'


    :return: The path of the probe module's source file.
    :raises CalibreRunError: If the file is missing from the installation.
    """
    if not PROBE_FILE.is_file():
        raise CalibreRunError(f"the calibre probe script is missing: {PROBE_FILE}")
    return PROBE_FILE


def reading_from_result(result: Mapping[str, object]) -> CalibreReading:
    """
    Turn the probe's result into a reading, or raise the error it reports.

    Example:
        >>> reading = reading_from_result({
        ...     "calibre_version": "9.18",
        ...     "metadata": {"title": "A Book", "pubdate": "2026-11-17T00:00:00"},
        ...     "cover": ["JPG", "/9j/"],
        ...     "text_fields": [],
        ...     "error": None,
        ... })
        >>> reading.fields.values["title"], reading.cover.image_format
        ('A Book', 'jpg')


    :param result: The parsed result file.
    :return: The reading.
    :raises CalibreRunError: If the result reports an error or is malformed.
    """
    error = result.get("error")
    if error:
        raise CalibreRunError(f"calibre could not read the file:\n{error}")
    metadata = result.get("metadata")
    if not isinstance(metadata, Mapping):
        raise CalibreRunError(f"calibre's result has no metadata: {result!r}")
    fields = CalibreFields(cast("Mapping[str, JSONValue]", metadata))
    cover: CoverImage | None = None
    raw_cover = result.get("cover")
    if isinstance(raw_cover, list):
        parts = cast("list[object]", raw_cover)
        if len(parts) == 2 and isinstance(parts[1], str) and parts[1]:
            image_format = normalize_format_name(str(parts[0])) or "unknown"
            cover = CoverImage(base64.b64decode(parts[1]), image_format)
    raw_text_fields = result.get("text_fields")
    text_fields: tuple[str, ...] = ()
    if isinstance(raw_text_fields, list):
        text_fields = tuple(str(item) for item in cast("list[object]", raw_text_fields))
    return CalibreReading(
        str(result.get("calibre_version") or "unknown"), fields, cover, text_fields
    )


def run_calibre_reader(
    executable: Path,
    plugin_name: str,
    source: Path,
    source_format: str,
    work_dir: Path,
    timeout: float,
) -> CalibreReading:
    """
    Read one file with one calibre reader plugin, in a new calibre process.

    Example:
        A provider calls this once per file::

            reading = run_calibre_reader(
                Path("/usr/bin/calibre-debug"), "Read EPUB metadata",
                Path("/books/a.epub"), "epub", work_dir, 120.0,
            )


    :param executable: The ``calibre-debug`` program.
    :param plugin_name: The name of calibre's reader plugin.
    :param source: The file to read.
    :param source_format: Its format.
    :param work_dir: A folder this call may create files in.
    :param timeout: Seconds to wait for calibre before giving up.
    :return: What calibre read.
    :raises CalibreRunError: If calibre fails, times out or reports an error.
    """
    folder = Path(tempfile.mkdtemp(prefix="calibre-probe-", dir=work_dir))
    script = folder / "liuxin_calibre_metadata_probe.py"
    shutil.copyfile(probe_source(), script)
    request_path = folder / "request.json"
    result_path = folder / "result.json"
    request = {
        "plugin": plugin_name,
        "source": str(source.resolve()),
        "format": source_format,
        "result": str(result_path),
    }
    request_path.write_text(json.dumps(request), encoding="utf-8")
    command = [str(executable), "-e", str(script), str(request_path)]
    try:
        completed = subprocess.run(
            command, capture_output=True, text=True, timeout=timeout, check=False
        )
    except subprocess.TimeoutExpired:
        raise CalibreRunError(
            f"calibre took longer than {timeout:g} seconds to read {source}"
        ) from None
    except OSError as error:
        raise CalibreRunError(f"could not start {executable}: {error}") from None
    if not result_path.is_file():
        output = (completed.stderr or completed.stdout or "").strip()
        raise CalibreRunError(
            f"calibre exited with status {completed.returncode} without a "
            f"result:\n{output[-2000:]}"
        )
    loaded: object = json.loads(result_path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise CalibreRunError(f"calibre's result is not a JSON object: {loaded!r}")
    return reading_from_result(cast("dict[str, object]", loaded))


class UpstreamCalibreReader(CalibreMetadataReader, CoverExtractor):
    """
    Read metadata and the cover with one of an installed calibre's readers.

    Subclasses are made by :func:`adapt_upstream_reader`, one per entry of
    :data:`UPSTREAM_READERS`; ``calibre_plugin_name`` names the plugin.

    Example:
        >>> provider = adapt_upstream_reader("Read RTF metadata", frozenset({"rtf"}))
        >>> provider.provider_id, sorted(provider.calibre_read_formats)
        ('calibre.read-rtf-metadata', ['rtf'])
        >>> provider.requires
        ('calibre',)
    """

    calibre_plugin_name: ClassVar[str]
    calibre_timeout: ClassVar[float] = 120.0

    @classmethod
    def availability(cls: type[Self]) -> ProviderAvailability:
        """
        Report whether an installed calibre can be found.

        Example:
            >>> import os
            >>> saved = dict(os.environ)
            >>> os.environ[CALIBRE_DEBUG_VARIABLE] = "/no/such/calibre-debug"
            >>> UpstreamCalibreReader.availability().available
            False
            >>> os.environ.clear(); os.environ.update(saved)


        :return: Ready if ``calibre-debug`` was found, otherwise why not.
        """
        if find_calibre_debug() is None:
            return ProviderAvailability.missing(
                "calibre-debug was not found; install calibre, put calibre-debug "
                f"on the PATH, or set {CALIBRE_DEBUG_VARIABLE}"
            )
        return ProviderAvailability.ready()

    def read_calibre_metadata(
        self: Self, source: Path, source_format: str, context: ProviderContext
    ) -> CalibreFields:
        """
        Return what calibre's reader says about the file.

        Example:
            Called by a route or a metadata run::

                fields = provider().read_calibre_metadata(path, "epub", context)


        :param source: The file to read. It is not changed.
        :param source_format: Which of ``calibre_read_formats`` it is in.
        :param context: Options, log, report and work folder for this call.
        :return: The metadata, in calibre's names and shapes.
        :raises CalibreRunError: If calibre is missing, fails or times out.
        """
        return self._reading(source, source_format, context).fields

    def extract_cover(
        self: Self, source: Path, source_format: str, context: ProviderContext
    ) -> CoverImage | None:
        """
        Return the cover calibre's reader found in the file.

        Example:
            Called by a route or a metadata run::

                cover = provider().extract_cover(path, "epub", context)


        :param source: The file to read. It is not changed.
        :param source_format: Which of ``cover_source_formats`` it is in.
        :param context: Options, log, report and work folder for this call.
        :return: The cover, or ``None``.
        :raises CalibreRunError: If calibre is missing, fails or times out.
        """
        return self._reading(source, source_format, context).cover

    def _reading(
        self: Self, source: Path, source_format: str, context: ProviderContext
    ) -> CalibreReading:
        """
        Run calibre on the file, or reuse the answer for the unchanged file.

        Example:
            Both public methods go through here::

                reading = self._reading(source, "epub", context)


        :param source: The file to read.
        :param source_format: Its format.
        :param context: Options, log, report and work folder for this call.
        :return: What calibre read.
        :raises CalibreRunError: If calibre is missing, fails or times out.
        """
        executable = find_calibre_debug()
        if executable is None:
            raise CalibreRunError(str(type(self).availability().reason))
        status = source.stat()
        key: tuple[object, ...] = (
            str(executable),
            self.calibre_plugin_name,
            str(source.resolve()),
            source_format,
            status.st_mtime_ns,
            status.st_size,
        )
        cached = _cache.get(key)
        if cached is not None:
            _cache.move_to_end(key)
            return cached
        reading = run_calibre_reader(
            executable,
            self.calibre_plugin_name,
            source,
            source_format,
            context.work_dir,
            self.calibre_timeout,
        )
        context.log.info(
            "calibre %s read %s with %r",
            reading.calibre_version,
            source,
            self.calibre_plugin_name,
        )
        for place in reading.text_fields:
            context.report.add_warning(
                f"calibre gave {place} a value with no JSON form; kept as text"
            )
        _cache[key] = reading
        while len(_cache) > _CACHE_SIZE:
            _cache.popitem(last=False)
        return reading


def adapt_upstream_reader(
    plugin_name: str, formats: frozenset[str]
) -> type[UpstreamCalibreReader]:
    """
    Make a provider class for one of calibre's reader plugins.

    Example:
        >>> adapt_upstream_reader("Read PDF metadata", frozenset({"pdf"})).__name__
        'ReadPdfMetadataUpstream'


    :param plugin_name: The plugin's name in calibre.
    :param formats: The formats it reads, in normal form.
    :return: A new provider class; register it to use it.
    """
    provider_id = provider_id_from_name(plugin_name, "calibre")
    class_name = (
        "".join(part.capitalize() for part in provider_id.split(".", 1)[1].split("-"))
        + "Upstream"
    )
    attributes: dict[str, object] = {
        "provider_id": provider_id,
        "version": UPSTREAM_CALIBRE_VERSION,
        "licence": "GPL-3.0-only",
        "priority": UPSTREAM_PRIORITY,
        "requires": ("calibre",),
        "calibre_read_formats": formats,
        "cover_source_formats": formats,
        "calibre_plugin_name": plugin_name,
        "__module__": __name__,
        "__doc__": f"Run calibre's {plugin_name!r} plugin in an installed calibre.",
    }

    def fill(namespace: dict[str, object]) -> None:
        """
        Copy the attributes into the class body.

        Example:
            >>> adapt_upstream_reader("Read TXT metadata", frozenset({"txt"})).priority
            -60


        :param namespace: The class body being built.
        :return: None after the attributes are copied.
        """
        namespace.update(attributes)

    made = types.new_class(class_name, (UpstreamCalibreReader,), exec_body=fill)
    return cast("type[UpstreamCalibreReader]", made)


def register_upstream_calibre_readers(
    registry: ProviderRegistry,
    readers: Sequence[tuple[str, frozenset[str]]] = UPSTREAM_READERS,
) -> tuple[type[UpstreamCalibreReader], ...]:
    """
    Register a provider for each of calibre's metadata reader plugins.

    The providers are registered whether or not calibre is installed; each
    reports its own availability.

    Example:
        >>> from LiuXin_alpha.file_formats.providers.registry import ProviderRegistry
        >>> registry = ProviderRegistry()
        >>> len(register_upstream_calibre_readers(registry))
        25
        >>> registry.calibre_metadata_readers("kepub")[0].provider_id
        'calibre.read-epub-metadata'


    :param registry: The registry to add the providers to.
    :param readers: Plugin names and formats; calibre 9.18's by default.
    :return: The provider classes registered.
    """
    made = tuple(adapt_upstream_reader(name, formats) for name, formats in readers)
    for provider in made:
        registry.register(provider)
    return made
