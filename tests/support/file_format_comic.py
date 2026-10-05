"""
Build deterministic COMIC fixtures and test doubles.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise file format comic through a consuming regression::

        python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py
"""
from __future__ import annotations

import binascii
import io
import struct
import zipfile
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence


COMIC_TITLE = "Comic Καλημέρα 世界"
COMIC_COLLECTION_TITLE = "Comic Collection Καλημέρα 世界"
COMIC_PAGE_MEMBERS = (
    "pages/01_Καλημέρα.png",
    "pages/深/02_世界.png",
    "pages/03_cafe\u0301.png",
)
COMIC_PAGE_FRAGMENTS = (
    "Καλημέρα",
    "世界",
    "深",
    "cafe\u0301",
)
COMIC_CBC_MEMBER_ONE = "comics/volume_Καλημέρα.cbz"
COMIC_CBC_MEMBER_TWO = "comics/深/volume_世界.cbz"
VENDORED_RAR_FIXTURE_DIR = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "LiuXin_alpha"
    / "utils"
    / "decompression"
    / "rarfile"
    / "test"
    / "files"
)


@dataclass(frozen=True)
class CBZFixture:
    """
    Carry the deterministic CBZFixture inputs and expected values used by format tests.

    Example:
        Exercise CBZFixture through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py
    """
    path: Path
    page_members: tuple[str, ...]
    extra_members: tuple[str, ...]
    path_fragments: tuple[str, ...]


@dataclass(frozen=True)
class ComicBookSpec:
    """
    Carry the deterministic ComicBookSpec inputs and expected values used by format tests.

    Example:
        Exercise ComicBookSpec through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py
    """
    member_name: str
    title: str
    page_members: tuple[str, ...]


@dataclass(frozen=True)
class CBCFixture:
    """
    Carry the deterministic CBCFixture inputs and expected values used by format tests.

    Example:
        Exercise CBCFixture through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py
    """
    path: Path
    comics_txt_member: str
    comic_specs: tuple[ComicBookSpec, ...]
    path_fragments: tuple[str, ...]

    @property
    def comic_members(self) -> tuple[str, ...]:
        """
        Perform the comic members step with deterministic fixture inputs.

        Example:
            Exercise CBCFixture.comic members through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        return tuple(spec.member_name for spec in self.comic_specs)

    @property
    def titles(self) -> tuple[str, ...]:
        """
        Perform the titles step with deterministic fixture inputs.

        Example:
            Exercise CBCFixture.titles through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        return tuple(spec.title for spec in self.comic_specs)


@dataclass(frozen=True)
class FakeRarInfo:
    """
    Provide the FakeRarInfo test double with a deliberately bounded compatibility surface.

    Example:
        Exercise FakeRarInfo through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py
    """
    filename: str
    file_size: int | None = 128
    compress_size: int | None = 64
    directory: bool = False
    password: bool = False

    def isdir(self) -> bool:
        """
        Perform the isdir step with deterministic fixture inputs.

        Example:
            Exercise FakeRarInfo.isdir through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        return self.directory

    def needs_password(self) -> bool:
        """
        Perform the needs password step with deterministic fixture inputs.

        Example:
            Exercise FakeRarInfo.needs password through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


        :return: True when the requested fixture condition holds; otherwise False.
        """
        return self.password


class NullLog:
    """
    Record or discard NullLog messages without requiring the production logging stack.

    Example:
        Exercise NullLog through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py
    """
    def __init__(self) -> None:
        """
        Initialize and validate the NullLog test-support state.

        Example:
            Exercise NullLog.  init   through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


        :return: None; completion is expressed through state changes or assertions.
        """
        self.messages: list[str] = []

    def __call__(self, message: str = "", *args) -> None:
        """
        Execute the configured fixture builder or test double operation.

        Example:
            Exercise NullLog.  call   through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


        :param message: Diagnostic message recorded or discarded by the test logger.
        :param args: Positional arguments forwarded to the bounded test double.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self.messages.append(message % args if args else message)

    def debug(self, message: str = "", *args) -> None:
        """
        Record or discard a debug message for assertions without external logging.

        Example:
            Exercise NullLog.debug through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


        :param message: Diagnostic message recorded or discarded by the test logger.
        :param args: Positional arguments forwarded to the bounded test double.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self(message, *args)

    def info(self, message: str = "", *args) -> None:
        """
        Record or discard a info message for assertions without external logging.

        Example:
            Exercise NullLog.info through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


        :param message: Diagnostic message recorded or discarded by the test logger.
        :param args: Positional arguments forwarded to the bounded test double.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self(message, *args)

    def warning(self, message: str = "", *args) -> None:
        """
        Record or discard a warning message for assertions without external logging.

        Example:
            Exercise NullLog.warning through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


        :param message: Diagnostic message recorded or discarded by the test logger.
        :param args: Positional arguments forwarded to the bounded test double.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self(message, *args)

    warn = warning

    def exception(self, message: str = "", *args) -> None:
        """
        Record or discard a exception message for assertions without external logging.

        Example:
            Exercise NullLog.exception through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


        :param message: Diagnostic message recorded or discarded by the test logger.
        :param args: Positional arguments forwarded to the bounded test double.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self(message, *args)


def png_bytes(width: int = 16, height: int = 16, rgb: tuple[int, int, int] = (180, 80, 120)) -> bytes:
    """
    Return deterministic PNG bytes for the requested dimensions and colour.

    Example:
        Exercise png bytes through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


    :param width: Image width in pixels.
    :param height: Image height in pixels.
    :param rgb: RGB colour embedded in the generated image.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    signature = b"\x89PNG\r\n\x1a\n"

    def chunk(tag: bytes, payload: bytes) -> bytes:
        """
        Return the encoded binary chunk required by the fixture container.

        Example:
            Exercise png bytes.chunk through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


        :param tag: Value supplied for tag under the deterministic fixture contract.
        :param payload: Binary or structured payload encoded into the fixture.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        return (
            struct.pack(">I", len(payload))
            + tag
            + payload
            + struct.pack(">I", binascii.crc32(tag + payload) & 0xFFFFFFFF)
        )

    ihdr = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    row = bytes([0]) + bytes(rgb) * width
    raw = row * height
    return signature + chunk(b"IHDR", ihdr) + chunk(b"IDAT", zlib.compress(raw, 9)) + chunk(b"IEND", b"")


def cbr_stub_bytes() -> bytes:
    """
    Perform the cbr stub bytes step with deterministic fixture inputs.

    Example:
        Exercise cbr stub bytes through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return b"Rar!\x1a\x07\x00stub cbr fixture"


def write_stub_cbr(path: Path) -> Path:
    """
    Write stub cbr for deterministic fixture consumers.

    Example:
        Exercise write stub cbr through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


    :param path: Filesystem path read, written or validated by the fixture operation.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(cbr_stub_bytes())
    return path


def vendored_rar_fixture(name: str = "unicode.rar") -> Path:
    """
    Perform the vendored rar fixture step with deterministic fixture inputs.

    Example:
        Exercise vendored rar fixture through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


    :param name: Stable fixture, profile, member or field name.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    path = VENDORED_RAR_FIXTURE_DIR / name
    if not path.exists():
        raise FileNotFoundError(path)
    return path


def patch_rarfile_infolist(monkeypatch, infos: Sequence[FakeRarInfo]) -> None:
    """
    Patch rarfile infolist under the fixture contract.

    Example:
        Exercise patch rarfile infolist through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


    :param monkeypatch: Pytest fixture used to isolate dependencies and mutable process
        state.
    :param infos: Value supplied for infos under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    from LiuXin_alpha.utils.decompression.rarfile import rarfile

    class _FakeRarFile:
        """
        Represent the FakeRarFile state used by deterministic test-support operations.

        Example:
            Exercise patch rarfile infolist. FakeRarFile through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py
        """
        def __init__(self, path):
            """
            Initialize and validate the FakeRarFile test-support state.

            Example:
                Exercise patch rarfile infolist. FakeRarFile.  init   through a consuming regression::

                    python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


            :param path: Filesystem path read, written or validated by the fixture operation.
            :return: None; completion is expressed through state changes or assertions.
            """
            self.path = path

        def __enter__(self):
            """
            Implement the fixture context manager's enter lifecycle step.

            Example:
                Exercise patch rarfile infolist. FakeRarFile.  enter   through a consuming regression::

                    python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


            :return: The deterministic fixture value, path, bytes, record or collection
                described above.
            """
            return self

        def __exit__(self, *args):
            """
            Implement the fixture context manager's exit lifecycle step.

            Example:
                Exercise patch rarfile infolist. FakeRarFile.  exit   through a consuming regression::

                    python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


            :param args: Positional arguments forwarded to the bounded test double.
            :return: The deterministic fixture value, path, bytes, record or collection
                described above.
            """
            return None

        def infolist(self):
            """
            Perform the infolist step with deterministic fixture inputs.

            Example:
                Exercise patch rarfile infolist. FakeRarFile.infolist through a consuming regression::

                    python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


            :return: The deterministic fixture value, path, bytes, record or collection
                described above.
            """
            return list(infos)

    monkeypatch.setattr(rarfile, "RarFile", _FakeRarFile)


def patch_rarfile_failure(monkeypatch, exc: Exception | None = None) -> None:
    """
    Patch rarfile failure under the fixture contract.

    Example:
        Exercise patch rarfile failure through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


    :param monkeypatch: Pytest fixture used to isolate dependencies and mutable process
        state.
    :param exc: Value supplied for exc under the deterministic fixture contract.
    :return: None; fixture state or the supplied destination is updated in place.
    """
    from LiuXin_alpha.utils.decompression.rarfile import rarfile

    failure = exc or RuntimeError("RAR parser unsupported in test")

    class _FailingRarFile:
        """
        Represent the FailingRarFile state used by deterministic test-support operations.

        Example:
            Exercise patch rarfile failure. FailingRarFile through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py
        """
        def __init__(self, path):
            """
            Initialize and validate the FailingRarFile test-support state.

            Example:
                Exercise patch rarfile failure. FailingRarFile.  init   through a consuming regression::

                    python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


            :param path: Filesystem path read, written or validated by the fixture operation.
            :return: None; completion is expressed through state changes or assertions.
            """
            raise failure

    monkeypatch.setattr(rarfile, "RarFile", _FailingRarFile)


def patch_unrar_names(monkeypatch, names: Sequence[str]) -> None:
    """
    Patch unrar names under the fixture contract.

    Example:
        Exercise patch unrar names through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


    :param monkeypatch: Pytest fixture used to isolate dependencies and mutable process
        state.
    :param names: Value supplied for names under the deterministic fixture contract.
    :return: None; fixture state or the supplied destination is updated in place.
    """
    from LiuXin_alpha.utils.decompression import unrar

    monkeypatch.setattr(unrar, "names", lambda stream: iter(tuple(names)))


def patch_unrar_names_failure(monkeypatch, exc: Exception | None = None) -> None:
    """
    Patch unrar names failure under the fixture contract.

    Example:
        Exercise patch unrar names failure through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


    :param monkeypatch: Pytest fixture used to isolate dependencies and mutable process
        state.
    :param exc: Value supplied for exc under the deterministic fixture contract.
    :return: None; fixture state or the supplied destination is updated in place.
    """
    from LiuXin_alpha.utils.decompression import unrar

    failure = exc or RuntimeError("external RAR listing unsupported in test")

    def _raise(_stream):
        """
        Perform the raise step with deterministic fixture inputs.

        Example:
            Exercise patch unrar names failure. raise through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


        :param _stream: Value supplied for stream under the deterministic fixture contract.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        raise failure

    monkeypatch.setattr(unrar, "names", _raise)


def _write_cbz(
    stream,
    *,
    page_members: Sequence[str],
    extra_members: Mapping[str, bytes],
    compression: int,
) -> None:
    """
    Write cbz for deterministic fixture consumers.

    Example:
        Exercise  write cbz through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


    :param stream: Value supplied for stream under the deterministic fixture contract.
    :param page_members: Value supplied for page members under the deterministic fixture
        contract.
    :param extra_members: Value supplied for extra members under the deterministic
        fixture contract.
    :param compression: Value supplied for compression under the deterministic fixture
        contract.
    :return: None; fixture state or the supplied destination is updated in place.
    """
    with zipfile.ZipFile(stream, "w") as zf:
        for index, member_name in enumerate(page_members):
            info = zipfile.ZipInfo(member_name)
            info.compress_type = compression
            zf.writestr(info, png_bytes(rgb=(80 + index * 30, 110, 170)))

        for member_name, data in extra_members.items():
            info = zipfile.ZipInfo(member_name)
            info.compress_type = compression
            zf.writestr(info, data)


def cbz_bytes(
    *,
    page_members: Sequence[str] | None = None,
    extra_members: Mapping[str, bytes] | None = None,
    compression: int = zipfile.ZIP_DEFLATED,
) -> bytes:
    """
    Perform the cbz bytes step with deterministic fixture inputs.

    Example:
        Exercise cbz bytes through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


    :param page_members: Value supplied for page members under the deterministic fixture
        contract.
    :param extra_members: Value supplied for extra members under the deterministic
        fixture contract.
    :param compression: Value supplied for compression under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    stream = io.BytesIO()
    _write_cbz(
        stream,
        page_members=tuple(COMIC_PAGE_MEMBERS if page_members is None else page_members),
        extra_members=dict(extra_members or {}),
        compression=compression,
    )
    return stream.getvalue()


def build_unicode_cbz(
    path: Path,
    *,
    page_members: Sequence[str] | None = None,
    extra_members: Mapping[str, bytes] | None = None,
    compression: int = zipfile.ZIP_DEFLATED,
) -> CBZFixture:
    """
    Build unicode cbz for deterministic fixture consumers.

    Example:
        Exercise build unicode cbz through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


    :param path: Filesystem path read, written or validated by the fixture operation.
    :param page_members: Value supplied for page members under the deterministic fixture
        contract.
    :param extra_members: Value supplied for extra members under the deterministic
        fixture contract.
    :param compression: Value supplied for compression under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    page_members = tuple(COMIC_PAGE_MEMBERS if page_members is None else page_members)
    extra_members = dict(extra_members or {})

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as stream:
        _write_cbz(
            stream,
            page_members=page_members,
            extra_members=extra_members,
            compression=compression,
        )

    return CBZFixture(
        path=path,
        page_members=page_members,
        extra_members=tuple(extra_members),
        path_fragments=COMIC_PAGE_FRAGMENTS,
    )


def build_unicode_cbc(
    path: Path,
    *,
    comic_specs: Sequence[ComicBookSpec] | None = None,
    extra_members: Mapping[str, bytes] | None = None,
    compression: int = zipfile.ZIP_DEFLATED,
) -> CBCFixture:
    """
    Build unicode cbc for deterministic fixture consumers.

    Example:
        Exercise build unicode cbc through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


    :param path: Filesystem path read, written or validated by the fixture operation.
    :param comic_specs: Value supplied for comic specs under the deterministic fixture
        contract.
    :param extra_members: Value supplied for extra members under the deterministic
        fixture contract.
    :param compression: Value supplied for compression under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    comic_specs = tuple(
        comic_specs
        or (
            ComicBookSpec(COMIC_CBC_MEMBER_ONE, COMIC_TITLE, COMIC_PAGE_MEMBERS),
            ComicBookSpec(
                COMIC_CBC_MEMBER_TWO,
                "第二巻 مرحبا",
                (
                    "pages/01_世界.png",
                    "pages/02_مرحبا.png",
                ),
            ),
        )
    )
    extra_members = dict(extra_members or {})

    comics_txt = "".join(f"{spec.member_name}:{spec.title}\n" for spec in comic_specs).encode("utf-8")

    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as zf:
        info = zipfile.ZipInfo("comics.txt")
        info.compress_type = compression
        zf.writestr(info, comics_txt)

        for spec in comic_specs:
            info = zipfile.ZipInfo(spec.member_name)
            info.compress_type = compression
            zf.writestr(
                info,
                cbz_bytes(page_members=spec.page_members, compression=compression),
            )

        for member_name, data in extra_members.items():
            info = zipfile.ZipInfo(member_name)
            info.compress_type = compression
            zf.writestr(info, data)

    return CBCFixture(
        path=path,
        comics_txt_member="comics.txt",
        comic_specs=comic_specs,
        path_fragments=COMIC_PAGE_FRAGMENTS + ("مرحبا", "第二巻"),
    )


def zip_members(path: Path) -> tuple[str, ...]:
    """
    Return the normalized members stored in the generated archive fixture.

    Example:
        Exercise zip members through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


    :param path: Filesystem path read, written or validated by the fixture operation.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    with zipfile.ZipFile(path, "r") as zf:
        return tuple(info.filename for info in zf.infolist())


def read_comic_member(path: Path, member: str) -> bytes:
    """
    Read comic member under the fixture contract.

    Example:
        Exercise read comic member through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


    :param path: Filesystem path read, written or validated by the fixture operation.
    :param member: Archive or container member addressed by the operation.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    with zipfile.ZipFile(path, "r") as zf:
        return zf.read(member)


def rewrite_comic_zip(
    src: Path,
    dst: Path,
    *,
    remove: Sequence[str] = (),
    replace: Mapping[str, bytes] | None = None,
    add: Mapping[str, bytes] | None = None,
    add_compression: int = zipfile.ZIP_STORED,
) -> None:
    """
    Perform the rewrite comic zip step with deterministic fixture inputs.

    Example:
        Exercise rewrite comic zip through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


    :param src: Source path or value copied into the fixture.
    :param dst: Destination path or object receiving generated fixture data.
    :param remove: Value supplied for remove under the deterministic fixture contract.
    :param replace: Value supplied for replace under the deterministic fixture contract.
    :param add: Value supplied for add under the deterministic fixture contract.
    :param add_compression: Value supplied for add compression under the deterministic
        fixture contract.
    :return: None; fixture state or the supplied destination is updated in place.
    """
    replacements = dict(replace or {})
    additions = dict(add or {})
    removed = set(remove)
    with zipfile.ZipFile(src, "r") as zin, zipfile.ZipFile(dst, "w") as zout:
        for info in zin.infolist():
            if info.filename in removed:
                continue
            data = replacements.pop(info.filename, zin.read(info.filename))
            zout.writestr(info, data)
        for name, data in {**replacements, **additions}.items():
            info = zipfile.ZipInfo(name)
            info.compress_type = add_compression
            zout.writestr(info, data)
