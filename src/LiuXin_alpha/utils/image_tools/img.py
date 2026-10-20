"""Image API shared by conversion and metadata callers.

Use the retained Qt implementation when PyQt5 is installed. Headless installs use
``img_fallback``, the existing Pillow implementation. Only absence of PyQt5
selects that backend; an unrelated import failure remains an error.

Pillow is supplied by the ``conversion`` extra and loaded when an operation needs
it. Image objects returned by the headless backend are Pillow images.
"""

from __future__ import annotations

try:
    from . import _img_qt as _backend
except ModuleNotFoundError as exc:
    if exc.name not in {"PyQt5", "PyQt5.Qt"}:
        raise
    from . import img_fallback as _backend

# Explicit aliases keep the public API discoverable without lazy package hooks.
NotImage = _backend.NotImage
normalize_format_name = _backend.normalize_format_name
get_exe_path = _backend.get_exe_path
null_image = _backend.null_image
image_from_data = _backend.image_from_data
image_from_path = _backend.image_from_path
image_from_x = _backend.image_from_x
image_and_format_from_data = _backend.image_and_format_from_data
image_to_data = _backend.image_to_data
save_image = _backend.save_image
save_cover_data_to = _backend.save_cover_data_to
blend_on_canvas = _backend.blend_on_canvas
Canvas = _backend.Canvas
create_canvas = _backend.create_canvas
overlay_image = _backend.overlay_image
texture_image = _backend.texture_image
blend_image = _backend.blend_image
add_borders_to_image = _backend.add_borders_to_image
remove_borders_from_image = _backend.remove_borders_from_image
resize_image = _backend.resize_image
resize_to_fit = _backend.resize_to_fit
clone_image = _backend.clone_image
scale_image = _backend.scale_image
crop_image = _backend.crop_image
grayscale_image = _backend.grayscale_image
set_image_opacity = _backend.set_image_opacity
flip_image = _backend.flip_image
image_has_transparent_pixels = _backend.image_has_transparent_pixels
rotate_image = _backend.rotate_image
gaussian_sharpen_image = _backend.gaussian_sharpen_image
gaussian_blur_image = _backend.gaussian_blur_image
despeckle_image = _backend.despeckle_image
oil_paint_image = _backend.oil_paint_image
normalize_image = _backend.normalize_image
quantize_image = _backend.quantize_image
run_optimizer = _backend.run_optimizer
optimize_jpeg = _backend.optimize_jpeg
optimize_png = _backend.optimize_png
encode_jpeg = _backend.encode_jpeg

__all__ = [
    "NotImage",
    "normalize_format_name",
    "get_exe_path",
    "null_image",
    "image_from_data",
    "image_from_path",
    "image_from_x",
    "image_and_format_from_data",
    "image_to_data",
    "save_image",
    "save_cover_data_to",
    "blend_on_canvas",
    "Canvas",
    "create_canvas",
    "overlay_image",
    "texture_image",
    "blend_image",
    "add_borders_to_image",
    "remove_borders_from_image",
    "resize_image",
    "resize_to_fit",
    "clone_image",
    "scale_image",
    "crop_image",
    "grayscale_image",
    "set_image_opacity",
    "flip_image",
    "image_has_transparent_pixels",
    "rotate_image",
    "gaussian_sharpen_image",
    "gaussian_blur_image",
    "despeckle_image",
    "oil_paint_image",
    "normalize_image",
    "quantize_image",
    "run_optimizer",
    "optimize_jpeg",
    "optimize_png",
    "encode_jpeg",
]
