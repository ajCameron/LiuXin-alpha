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
    path: Path
    page_members: tuple[str, ...]
    extra_members: tuple[str, ...]
    path_fragments: tuple[str, ...]


@dataclass(frozen=True)
class ComicBookSpec:
    member_name: str
    title: str
    page_members: tuple[str, ...]


@dataclass(frozen=True)
class CBCFixture:
    path: Path
    comics_txt_member: str
    comic_specs: tuple[ComicBookSpec, ...]
    path_fragments: tuple[str, ...]

    @property
    def comic_members(self) -> tuple[str, ...]:
        return tuple(spec.member_name for spec in self.comic_specs)

    @property
    def titles(self) -> tuple[str, ...]:
        return tuple(spec.title for spec in self.comic_specs)


@dataclass(frozen=True)
class FakeRarInfo:
    filename: str
    file_size: int | None = 128
    compress_size: int | None = 64
    directory: bool = False
    password: bool = False

    def isdir(self) -> bool:
        return self.directory

    def needs_password(self) -> bool:
        return self.password


class NullLog:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def __call__(self, message: str = "", *args) -> None:
        self.messages.append(message % args if args else message)

    def debug(self, message: str = "", *args) -> None:
        self(message, *args)

    def info(self, message: str = "", *args) -> None:
        self(message, *args)

    def warning(self, message: str = "", *args) -> None:
        self(message, *args)

    warn = warning

    def exception(self, message: str = "", *args) -> None:
        self(message, *args)


def png_bytes(width: int = 16, height: int = 16, rgb: tuple[int, int, int] = (180, 80, 120)) -> bytes:
    signature = b"\x89PNG\r\n\x1a\n"

    def chunk(tag: bytes, payload: bytes) -> bytes:
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
    return b"Rar!\x1a\x07\x00stub cbr fixture"


def write_stub_cbr(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(cbr_stub_bytes())
    return path


def vendored_rar_fixture(name: str = "unicode.rar") -> Path:
    path = VENDORED_RAR_FIXTURE_DIR / name
    if not path.exists():
        raise FileNotFoundError(path)
    return path


def patch_rarfile_infolist(monkeypatch, infos: Sequence[FakeRarInfo]) -> None:
    from LiuXin_alpha.utils.decompression.rarfile import rarfile

    class _FakeRarFile:
        def __init__(self, path):
            self.path = path

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def infolist(self):
            return list(infos)

    monkeypatch.setattr(rarfile, "RarFile", _FakeRarFile)


def patch_rarfile_failure(monkeypatch, exc: Exception | None = None) -> None:
    from LiuXin_alpha.utils.decompression.rarfile import rarfile

    failure = exc or RuntimeError("RAR parser unsupported in test")

    class _FailingRarFile:
        def __init__(self, path):
            raise failure

    monkeypatch.setattr(rarfile, "RarFile", _FailingRarFile)


def patch_unrar_names(monkeypatch, names: Sequence[str]) -> None:
    from LiuXin_alpha.utils.decompression import unrar

    monkeypatch.setattr(unrar, "names", lambda stream: iter(tuple(names)))


def _write_cbz(
    stream,
    *,
    page_members: Sequence[str],
    extra_members: Mapping[str, bytes],
    compression: int,
) -> None:
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
    with zipfile.ZipFile(path, "r") as zf:
        return tuple(info.filename for info in zf.infolist())


def read_comic_member(path: Path, member: str) -> bytes:
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
