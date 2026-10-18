"""Serialize Pillow and Qt images without loading ebook-format backends."""

from __future__ import annotations

from io import BytesIO
from typing import Any


def _qimage_to_png_bytes(qimg: Any) -> bytes:
    """
    Perform the qimage to png bytes utility operation under explicit compatibility rules.

    Example:
        Exercise  qimage to png bytes through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param qimg: Value supplied for qimg under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    for qt_pkg in ("PyQt6", "PyQt5", "PySide6", "PySide2"):
        try:
            QtCore = __import__(f"{qt_pkg}.QtCore", fromlist=["QtCore"])
            QBuffer = QtCore.QBuffer
            QIODevice = QtCore.QIODevice
            buf = QBuffer()
            mode = (
                QIODevice.OpenModeFlag.WriteOnly
                if hasattr(QIODevice, "OpenModeFlag")
                else QIODevice.WriteOnly
            )
            buf.open(mode)
            ok = qimg.save(buf, "PNG")
            if not ok:
                raise ValueError("QImage.save() failed")
            data = bytes(buf.data())
            buf.close()
            return data
        except Exception:
            continue
    raise RuntimeError("Qt bindings not available to serialize QImage")


def image_to_png(image: Any) -> bytes:
    """Serialize an image or pixmap to PNG, retaining its pixels and alpha channel."""
    if hasattr(image, "toImage"):
        image = image.toImage()
    if hasattr(image, "isNull"):
        return _qimage_to_png_bytes(image)
    output = BytesIO()
    image.save(output, format="PNG")
    return output.getvalue()
