from __future__ import annotations

import shutil

import pytest


def test_stub_modules_import_cleanly() -> None:
    # These are intentionally lightweight stubs: they should at least import.
    from LiuXin_alpha.utils.plugins.fallbacks import chm_extra, pictureflow, qt_hack

    assert chm_extra.__doc__
    assert pictureflow.__doc__
    assert qt_hack.__doc__


def test_chmlib_defines_exception_type() -> None:
    from LiuXin_alpha.utils.plugins.fallbacks import chmlib

    assert isinstance(chmlib.CHMError, type)


def test_podofo_pdfdoc_is_a_clear_runtime_error() -> None:
    from LiuXin_alpha.utils.plugins.fallbacks import podofo

    with pytest.raises(podofo.Error):
        podofo.PDFDoc(object())


def test_woff_and_wpd_define_error_types() -> None:
    from LiuXin_alpha.utils.plugins.fallbacks import woff, wpd

    assert isinstance(woff.WOFFError, type)
    assert isinstance(wpd.WpdError, type)


def test_libusb_and_libmtp_define_error_types() -> None:
    from LiuXin_alpha.utils.plugins.fallbacks import libusb, libmtp

    assert isinstance(libusb.LibUSBError, type)
    assert isinstance(libmtp.LibMTPError, type)


def test_magick_identify_raises_clear_error_if_cli_missing(tmp_path) -> None:
    from LiuXin_alpha.utils.plugins.fallbacks import magick

    exe = shutil.which("magick") or shutil.which("identify")

    if not exe:
        with pytest.raises(RuntimeError, match="ImageMagick"):
            magick.Image(b"not-an-image").identify()
        return

    # If the CLI exists, just assert we get a dict back for a tiny PPM.
    # PPM is extremely simple and commonly supported by ImageMagick.
    ppm = b"P6\n1 1\n255\n" + bytes([0, 0, 0])
    info = magick.Image(ppm).identify()
    assert isinstance(info, dict)


def test_imageops_resize_raises_clear_error_if_cli_missing() -> None:
    from LiuXin_alpha.utils.plugins.fallbacks import imageops

    have = shutil.which("magick") or shutil.which("convert")

    if not have:
        with pytest.raises(RuntimeError, match="ImageMagick"):
            imageops.resize(b"not-an-image", 1, 1)
        return

    ppm = b"P6\n2 2\n255\n" + bytes([0, 0, 0] * 4)
    out = imageops.resize(ppm, 1, 1, fmt="ppm")
    assert out.startswith(b"P6\n")


def test_unrar_raises_clear_error_when_missing_executable(tmp_path) -> None:
    from LiuXin_alpha.utils.plugins.fallbacks import unrar

    exe = shutil.which("unrar") or shutil.which("rar")
    if exe:
        # No reliable RAR fixture bundled; just verify the executable is discoverable.
        # (If you later add a small test rar, we can make this a real integration test.)
        return

    class _CB:
        def __init__(self) -> None:
            self.chunks = []

        def handle_data(self, b: bytes) -> None:
            self.chunks.append(bytes(b))

    with pytest.raises(unrar.UNRARError, match="unrar"):
        unrar.RARArchive(stream=(tmp_path/"empty.rar").open("wb+"), stream_name="empty.rar", callback=_CB())
