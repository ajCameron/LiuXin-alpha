"""
Provide small compatibility helpers for the local conversion demonstrations.

Bootstrap checkout imports, supply a minimal logger/metadata stub and option
namespaces, and load or generate OEB input. The scratch context changes a global
setting temporarily; installing the customize.ui shim replaces a process import
entry without restoration. These helpers support example execution rather than
the full plugin registry or metadata-container contracts.
"""

from __future__ import annotations

import sys
import tempfile
import types

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any

EXAMPLES_ROOT = Path(__file__).resolve().parents[1]
if str(EXAMPLES_ROOT) not in sys.path:
    sys.path.insert(0, str(EXAMPLES_ROOT))

from _example_utils import bootstrap_src_path, dump_json

bootstrap_src_path()

from LiuXin_alpha.file_formats.conversion.plugins.oeb_output import OEBOutput
from LiuXin_alpha.file_formats.oeb.base import OEBBook
from LiuXin_alpha.file_formats.oeb.reader import OEBReader
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.ptempfiles import (
    get_base_scratch_folders,
    set_base_scratch_folders,
)


@contextmanager
def isolated_conversion_scratch() -> Iterator[Path]:
    """
    Temporarily redirect the legacy scratch-folder setting to an owned directory. Work begins on
    context entry: read the current setting, create a TemporaryDirectory, and install its path.
    Yield that Path and restore the saved setting in finally before the temporary-directory context
    cleans up. This changes process-global configuration, so overlapping users are not isolated from
    each other's setting changes. The initial setter runs before the restoration try block;
    cleanup/restoration errors can replace a body error. Files left inside the temporary directory
    are removed on normal exit.

    The setter changes LiuXin_scratch_folder only. A previously cached _base_dir or imported
    path alias can still direct legacy temporaries elsewhere; this is not complete filesystem
    isolation for a conversion process.

    Example:
        >>> with isolated_conversion_scratch() as scratch:  # doctest: +SKIP
        ...     scratch_exists = scratch.is_dir()


    :return: Context manager yielding the existing scratch directory during the redirected interval.
    """

    previous = get_base_scratch_folders()
    with tempfile.TemporaryDirectory(
        prefix="liuxin-alpha-conversion-scratch-"
    ) as scratch:
        set_base_scratch_folders(scratch)
        try:
            yield Path(scratch)
        finally:
            set_base_scratch_folders(previous)


class ExampleLog:
    """
    Provide the small logger interface expected by the conversion examples. Truthy verbose enables
    plain print calls to stdout; otherwise methods do nothing. Positional arguments use print's
    ordinary space separation, without logging-style interpolation. Keyword arguments are accepted
    and ignored. Named levels add a bracketed prefix, warn aliases warning, and exception does not
    include a traceback.

    Example:
        >>> log = ExampleLog(verbose=True)
        >>> log.info("ready")
        [info] ready
    """
    def __init__(self, *, verbose: bool = False) -> None:
        """
        Store the verbosity value used by every logging method. No type coercion, output stream,
        formatter, or logging handler is installed; later calls test its truthiness.

        Example:
            >>> ExampleLog().verbose
            False


        :param verbose: Whether calls should print to stdout; false by default and retained as supplied.
        :return: None after storing the verbosity setting.
        """
        self.verbose = verbose

    def __call__(self, *args, **kwargs) -> None:
        """
        Print positional arguments to stdout when verbose is truthy. Use default print
        spacing/newline and ignore all keyword arguments, including formatting or stream overrides.
        Quiet calls do not render their arguments.

        Example:
            >>> ExampleLog(verbose=True)("one", "two", end="ignored")
            one two


        :param args: Values passed positionally to print when enabled.
        :param kwargs: Compatibility keyword arguments accepted but ignored.
        :return: None after printing or taking the quiet path.
        """
        if self.verbose:
            print(*args)

    def info(self, *args, **kwargs) -> None:
        """
        Print the [info] prefix and positional values to stdout when verbose is truthy. Use ordinary
        print spacing/newline, ignore keyword arguments, and perform no logging-style interpolation
        or level filtering.

        Example:
            >>> ExampleLog(verbose=True).info("ready")
            [info] ready


        :param args: Values printed after the fixed prefix when enabled.
        :param kwargs: Compatibility keyword arguments accepted but ignored.
        :return: None after printing or taking the quiet path.
        """
        if self.verbose:
            print("[info]", *args)

    def debug(self, *args, **kwargs) -> None:
        """
        Print the [debug] prefix and positional values to stdout when verbose is truthy. Use
        ordinary print spacing/newline, ignore keyword arguments, and perform no logging-style
        interpolation or level filtering.

        Example:
            >>> ExampleLog(verbose=True).debug("ready")
            [debug] ready


        :param args: Values printed after the fixed prefix when enabled.
        :param kwargs: Compatibility keyword arguments accepted but ignored.
        :return: None after printing or taking the quiet path.
        """
        if self.verbose:
            print("[debug]", *args)

    def warning(self, *args, **kwargs) -> None:
        """
        Print the [warning] prefix and positional values to stdout when verbose is truthy. Use
        ordinary print spacing/newline, ignore keyword arguments, and perform no logging-style
        interpolation or level filtering.

        Example:
            >>> ExampleLog(verbose=True).warning("ready")
            [warning] ready


        :param args: Values printed after the fixed prefix when enabled.
        :param kwargs: Compatibility keyword arguments accepted but ignored.
        :return: None after printing or taking the quiet path.
        """
        if self.verbose:
            print("[warning]", *args)

    warn = warning

    def error(self, *args, **kwargs) -> None:
        """
        Print the [error] prefix and positional values to stdout when verbose is truthy. Use
        ordinary print spacing/newline, ignore keyword arguments, and perform no logging-style
        interpolation or level filtering.

        Example:
            >>> ExampleLog(verbose=True).error("ready")
            [error] ready


        :param args: Values printed after the fixed prefix when enabled.
        :param kwargs: Compatibility keyword arguments accepted but ignored.
        :return: None after printing or taking the quiet path.
        """
        if self.verbose:
            print("[error]", *args)

    def exception(self, *args, **kwargs) -> None:
        """
        Print the [exception] prefix and positional values to stdout when verbose is truthy. Use
        ordinary print spacing/newline, ignore keyword arguments, and perform no logging-style
        interpolation or level filtering. This method does not capture or print exception traceback
        information.

        Example:
            >>> ExampleLog(verbose=True).exception("ready")
            [exception] ready


        :param args: Values printed after the fixed prefix when enabled.
        :param kwargs: Compatibility keyword arguments accepted but ignored.
        :return: None after printing or taking the quiet path.
        """
        if self.verbose:
            print("[exception]", *args)


class MetadataStub:
    """
    Supply mutable metadata attributes needed by the examples' conversion plugins. This is a
    lightweight compatibility object with Unknown title/author defaults, English language, empty
    tags/identifiers, and unset optional publication/cover/identity fields. It does not read source
    metadata, validate identifiers, or provide the complete metadata container API. Helper methods
    expose simple emptiness and display conventions.

    Example:
        >>> metadata = MetadataStub(title="Sample", authors=["An Author"])
        >>> metadata.title, metadata.languages
        ('Sample', ['en'])
    """
    def __init__(
        self,
        *,
        title: str = "Unknown",
        authors: list[str] | None = None,
        cover: str | None = None,
    ) -> None:
        """
        Initialize plugin-facing metadata fields without reading any source bytes. Retain title and
        cover as supplied. Copy a truthy authors iterable with list(); None or an empty list
        produces ["Unknown"]. Allocate fresh tags and identifiers collections and languages=["en"].
        Set sort, publication, series/rating, timestamps, rights, cover data, UUID/application ID,
        and ISBN fields to None.

        Example:
            >>> metadata = MetadataStub(authors=[])
            >>> metadata.authors, metadata.identifiers
            (['Unknown'], {})


        :param title: Title hint retained without normalization; defaults to Unknown.
        :param authors: Author values to copy; a false value selects the Unknown-author fallback.
        :param cover: Optional cover hint retained as supplied, without path validation or loading.
        :return: None after initializing independent mutable metadata fields.
        """
        self.title = title
        self.authors = list(authors or ["Unknown"])
        self.title_sort = None
        self.author_sort = None
        self.book_producer = None
        self.comments = None
        self.publisher = None
        self.series = None
        self.series_index = None
        self.rating = None
        self.tags: list[str] = []
        self.pubdate = None
        self.timestamp = None
        self.rights = None
        self.publication_type = None
        self.languages = ["en"]
        self.cover = cover
        self.cover_data = None
        self.uuid = None
        self.application_id = None
        self.isbn = None
        self.identifiers: dict[str, str] = {}

    def is_null(self, name: str) -> bool:
        """
        Treat missing/None attributes, blank strings, and empty built-in collections as null. Strip
        strings before checking them. Check length only for list, tuple, set, frozenset, and dict
        values; other false values, including zero and False, are not null. Attribute lookup errors
        other than the missing-attribute default can propagate.

        Example:
            >>> metadata = MetadataStub(title="  ")
            >>> metadata.is_null("title"), metadata.is_null("absent")
            (True, True)
            >>> metadata.rating = 0
            >>> metadata.is_null("rating")
            False


        :param name: Attribute name to inspect on this metadata stub.
        :return: True for the explicitly recognized absent/empty values, otherwise false.
        """
        value = getattr(self, name, None)
        if value is None:
            return True
        if isinstance(value, str):
            return value.strip() == ""
        if isinstance(value, (list, tuple, set, frozenset, dict)):
            return len(value) == 0
        return False

    def get_identifiers(self) -> dict[str, str]:
        """
        Copy the identifier mapping and add a truthy ISBN only if its key is absent. Use setdefault
        so an existing isbn entry, even a false value, is retained. Do not mutate the stored mapping
        or normalize/check any identifier.

        Example:
            >>> metadata = MetadataStub()
            >>> metadata.isbn = "example"
            >>> metadata.get_identifiers(), metadata.identifiers
            ({'isbn': 'example'}, {})


        :return: New identifier dictionary, optionally supplemented by the separate ISBN field.
        """
        identifiers = dict(self.identifiers)
        if self.isbn:
            identifiers.setdefault("isbn", self.isbn)
        return identifiers

    def format_series_index(self) -> str:
        """
        Render the series index with str(), using "1" only when it is None. No numeric conversion,
        range check, or special decimal formatting is applied.

        Example:
            >>> metadata = MetadataStub()
            >>> metadata.format_series_index()
            '1'
            >>> metadata.series_index = 0
            >>> metadata.format_series_index()
            '0'


        :return: Literal 1 for an unset index, otherwise its string representation.
        """
        if self.series_index is None:
            return "1"
        return str(self.series_index)


def install_customize_ui_stub(*, html_input_support: bool = False) -> None:
    """
    Install a deliberately narrow customize.ui module in the process import cache. Replace
    sys.modules["LiuXin_alpha.customize.ui"] with a fresh ModuleType exposing the nested
    plugin/metadata helpers and four no-op import-hook lambdas. Support OEB output, a stable CBZ
    input sentinel per installation, and optionally real HTML input; other input formats receive
    fresh generic sentinels. Metadata is inferred from a filename. The previous module is neither
    saved nor restored, and already-bound external references are not rewritten. Call this only
    where that process-wide substitution is intended.

    Example:
        >>> install_customize_ui_stub(html_input_support=True)  # doctest: +SKIP


    :param html_input_support: Whether the html input lookup should lazily construct a real HTMLInput plugin.
    :return: None after replacing the customize.ui entry in sys.modules.
    """
    fake_ui = types.ModuleType("LiuXin_alpha.customize.ui")
    cbz_plugin_sentinel = object()

    def plugin_for_output_format(fmt: str):
        """
        Construct OEBOutput(None) for the exact format string oeb. Reject every other
        spelling/format with ValueError; no case or extension normalization is performed. A new
        plugin instance is made for each supported lookup.

        Example:
            >>> plugin = ui.plugin_for_output_format("oeb")  # doctest: +SKIP


        :param fmt: Exact output-format token, with only oeb supported by this example shim.
        :return: New OEBOutput instance; unsupported tokens raise ValueError.
        """
        if fmt == "oeb":
            return OEBOutput(None)
        raise ValueError(f"Unsupported output format in example stub: {fmt!r}")

    def plugin_for_input_format(fmt: str):
        """
        Return a CBZ sentinel, optionally construct HTMLInput, or return a generic sentinel. Exact
        cbz lookups share one object for this installation. Exact html lookups import and
        instantiate HTMLInput(None) only when html_input_support was enabled. Every other lookup
        returns a fresh object rather than raising or providing a working conversion plugin.

        Example:
            >>> ui.plugin_for_input_format("cbz") is ui.plugin_for_input_format("cbz")  # doctest: +SKIP
            True


        :param fmt: Case-sensitive input-format token used by the shim.
        :return: Shared CBZ sentinel, a new enabled HTMLInput, or a new generic object.
        """
        if fmt == "cbz":
            return cbz_plugin_sentinel
        if html_input_support and fmt == "html":
            from LiuXin_alpha.file_formats.conversion.plugins.html_input import HTMLInput

            return HTMLInput(None)
        return object()

    def get_file_type_metadata(stream: Any, file_ext: str, calibre: bool = True) -> MetadataStub:
        """
        Build placeholder metadata from the basename/stem of stream.name. Default a missing name to
        input and an empty stem to Unknown; use Unknown as the author. Do not read, seek, or close
        the stream. Ignore file_ext and calibre. A present but non-path-like name can fail during
        Path construction.

        Example:
            >>> metadata = ui.get_file_type_metadata(stream, "txt")  # doctest: +SKIP


        :param stream: Object with an optional path-like name used solely to derive a title.
        :param file_ext: Compatibility extension argument; ignored.
        :param calibre: Compatibility metadata-mode flag; ignored.
        :return: New MetadataStub containing a filename-derived title and Unknown author.
        """
        stream_name = Path(getattr(stream, "name", "input")).name
        stem = Path(stream_name).stem or "Unknown"
        return MetadataStub(title=stem, authors=["Unknown"])

    fake_ui.plugin_for_output_format = plugin_for_output_format
    fake_ui.plugin_for_input_format = plugin_for_input_format
    fake_ui.get_file_type_metadata = get_file_type_metadata
    fake_ui.run_plugins_on_import = lambda *args, **kwargs: None
    fake_ui.run_plugins_on_postimport = lambda *args, **kwargs: None
    fake_ui.run_plugins_on_postadd = lambda *args, **kwargs: None
    fake_ui.run_import_plugins = lambda *args, **kwargs: None
    sys.modules["LiuXin_alpha.customize.ui"] = fake_ui


def conversion_profile() -> SimpleNamespace:
    """
    Build a fresh minimal device/profile namespace for the examples. Supply width 600, height 800,
    96 dpi, base font size 16, and the fixed numeric font map. Include the default short name, Sony
    periodical token, and MOBI blockquote setting. This is a mutable compatibility profile, not
    device discovery or a complete profile API.

    Example:
        >>> profile = conversion_profile()
        >>> profile.width, profile.height, profile.fnums[12]
        (600, 800, 5)


    :return: New SimpleNamespace with the fixed example profile values and a fresh font map.
    """
    fnums = {8: 3, 10: 4, 12: 5, 14: 6, 16: 7}
    return SimpleNamespace(
        width=600,
        height=800,
        dpi=96,
        fbase=16,
        fnums=fnums,
        mobi_ems_per_blockquote=2,
        short_name="default",
        epub_periodical_format="sony",
    )


def make_epub_output_opts(*, extract_to: str | None = None) -> SimpleNamespace:
    """
    Build the fixed mutable option namespace used by the EPUB example. Create one profile and share
    it through output_profile, source, and dest. Supply the writer/layout/search defaults expected
    by this example, including flow_size=260 and empty replacement/CSS rules. Retain extract_to
    without creating or validating a path. These values are example defaults rather than every
    installed plugin's recommended options.

    Example:
        >>> options = make_epub_output_opts()
        >>> options.source is options.dest is options.output_profile
        True
        >>> options.flow_size, options.extract_to
        (260, None)


    :param extract_to: Optional extraction destination passed through to EPUB output without local validation.
    :return: Fresh SimpleNamespace of EPUB/output-layout options, sharing one profile within the result.
    """
    profile = conversion_profile()
    return SimpleNamespace(
        epub_inline_toc=False,
        epub_toc_at_end=False,
        epub_flatten=False,
        dont_split_on_page_breaks=False,
        flow_size=260,
        no_default_epub_cover=False,
        no_svg_cover=False,
        preserve_cover_aspect_ratio=False,
        pretty_print=False,
        extract_to=extract_to,
        output_profile=profile,
        mobi_toc_at_start=False,
        mobi_passthrough=False,
        no_inline_toc=False,
        toc_title=None,
        expand_css=False,
        source=profile,
        dest=profile,
        margin_left=5,
        margin_right=5,
        margin_top=5,
        margin_bottom=5,
        line_height=0,
        remove_paragraph_spacing=False,
        remove_paragraph_spacing_indent_size=1.5,
        insert_blank_line=False,
        insert_blank_line_size=0.5,
        keep_ligatures=False,
        subset_embedded_fonts=False,
        embed_all_fonts=False,
        minimum_line_height=120.0,
        change_justification="original",
        html_unwrap_factor=0.4,
        base_font_size=0.0,
        disable_font_rescaling=False,
        font_size_mapping="12,12,12,12,12,12,12,12",
        sr1_search="",
        sr1_replace="",
        sr2_search="",
        sr2_replace="",
        sr3_search="",
        sr3_replace="",
        transform_css_rules="",
        extra_css="",
    )


def make_mobi_output_opts(*, mobi_file_type: str = "old", extract_to: str | None = None) -> SimpleNamespace:
    """
    Build the fixed mutable options used by the MOBI output example. Retain mobi_file_type and
    extract_to without validation. Share a fresh conversion profile between source and dest, and
    supply the fixed compression, image, table, TOC, and periodical flags expected by the example
    writer.

    Example:
        >>> options = make_mobi_output_opts(mobi_file_type="both")
        >>> options.mobi_file_type, options.source is options.dest
        ('both', True)


    :param mobi_file_type: Writer variant token, default old; command-line choices are enforced by the caller.
    :param extract_to: Optional extraction destination forwarded unchanged to the output plugin.
    :return: Fresh SimpleNamespace of MOBI options with one shared source/destination profile.
    """
    profile = conversion_profile()
    return SimpleNamespace(
        mobi_file_type=mobi_file_type,
        prefer_author_sort=False,
        no_inline_toc=False,
        toc_title=None,
        dont_compress=False,
        mobi_ignore_margins=False,
        mobi_toc_at_start=False,
        extract_to=extract_to,
        share_not_sync=False,
        mobi_keep_original_images=False,
        linearize_tables=False,
        pretty_print=False,
        mobi_periodical=False,
        expand_css=False,
        source=profile,
        dest=profile,
    )


def resolve_oeb_input(input_opf: str | None, *, workspace: Path) -> tuple[Path, bool]:
    """
    Use an existing supplied OPF file or create the fixed sample OEB tree. A truthy input string is
    expanded/resolved, checked for existence and is_file, and returned without parsing. A false
    input creates workspace/sample_oeb and overwrites its three fixed sample files through
    write_demo_oeb. The workspace path itself is not expanded or resolved here, and created sample
    files are not removed by this helper.

    Example:
        >>> opf_path, generated = resolve_oeb_input(None, workspace=workspace)  # doctest: +SKIP


    :param input_opf: Optional OPF path text; None or an empty string requests generated sample input.
    :param workspace: Parent for sample_oeb when generating input; unused for a supplied path.
    :return: Pair of OPF Path and whether a sample was generated; missing/non-file supplied paths raise.
    """
    if input_opf:
        resolved = Path(input_opf).expanduser().resolve()
        if not resolved.exists():
            raise FileNotFoundError(f"Input OPF does not exist: {resolved}")
        if not resolved.is_file():
            raise ValueError(f"Input OPF must be a file: {resolved}")
        return resolved, False

    sample_root = workspace / "sample_oeb"
    sample_root.mkdir(parents=True, exist_ok=True)
    return write_demo_oeb(sample_root), True


def write_demo_oeb(root: Path) -> Path:
    """
    Overwrite three UTF-8 sample files in an existing directory and return the OPF path. Write
    multilingual chapter XHTML, an NCX with one navigation point, and an OPF 2.0 package with fixed
    title/author/UUID metadata and chapter/NCX entries. The directory is not created here. Writes
    occur sequentially without rollback or atomic publication, so an error can leave a partial
    sample tree. Source XML strings are fixed example payloads.

    Example:
        >>> opf_path = write_demo_oeb(existing_directory)  # doctest: +SKIP


    :param root: Existing writable directory receiving chapter.xhtml, toc.ncx, and metadata.opf.
    :return: Path to metadata.opf after all three writes succeed.
    """
    title = "Example Conversion Title — Καλημέρα — 你好"
    author = "Example Author"
    chapter = """<html xmlns="http://www.w3.org/1999/xhtml">
  <head><title>Example Conversion</title></head>
  <body>
    <h1 id="top">Example Conversion</h1>
    <p>This file was generated by LiuXin_alpha examples.</p>
    <p>Unicode smoke: naïve café — Здравствуйте — مرحبا — नमस्ते — こんにちは — 🙂</p>
  </body>
</html>
"""
    opf = f"""<?xml version="1.0" encoding="utf-8"?>
<package xmlns="http://www.idpf.org/2007/opf" version="2.0" unique-identifier="BookId">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:title>{title}</dc:title>
    <dc:creator xmlns:opf="http://www.idpf.org/2007/opf" opf:role="aut">{author}</dc:creator>
    <dc:language>en</dc:language>
    <dc:identifier id="BookId">urn:uuid:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee</dc:identifier>
  </metadata>
  <manifest>
    <item id="chap" href="chapter.xhtml" media-type="application/xhtml+xml"/>
    <item id="ncx" href="toc.ncx" media-type="application/x-dtbncx+xml"/>
  </manifest>
  <spine toc="ncx">
    <itemref idref="chap"/>
  </spine>
</package>
"""
    ncx = """<?xml version="1.0"?>
<ncx xmlns="http://www.daisy.org/z3986/2005/ncx/" version="2005-1">
  <head/>
  <docTitle><text>Example Conversion</text></docTitle>
  <navMap>
    <navPoint id="n1" playOrder="1">
      <navLabel><text>Start</text></navLabel>
      <content src="chapter.xhtml#top"/>
    </navPoint>
  </navMap>
</ncx>
"""
    (root / "chapter.xhtml").write_text(chapter, encoding="utf-8")
    (root / "toc.ncx").write_text(ncx, encoding="utf-8")
    opf_path = root / "metadata.opf"
    opf_path.write_text(opf, encoding="utf-8")
    return opf_path


def load_oeb_from_opf(opf_path: Path) -> OEBBook:
    """
    Construct an OEBBook and populate it through OEBReader from the supplied path. Use default_log
    and an identity translation callable, then pass str(opf_path) to the reader. Path/resource
    parsing and validation belong to OEBReader; this helper does not expand paths or catch reader
    errors.

    Example:
        >>> book = load_oeb_from_opf(opf_path)  # doctest: +SKIP


    :param opf_path: OPF input path passed as text to the OEB reader.
    :return: Populated OEBBook when reading succeeds.
    """
    oeb = OEBBook(default_log, lambda x: x)
    OEBReader()(oeb, str(opf_path))
    return oeb
