"""Comic archive extraction and page raster processing helpers."""

from __future__ import annotations

import os
import re
import shutil
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from types import SimpleNamespace

from LiuXin_alpha import prints
from LiuXin_alpha.utils.calibre import walk
from LiuXin_alpha.utils.decompression.archives import extract
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.ptempfiles import PersistentTemporaryDirectory

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal kovid@kovidgoyal.net"
__docformat__ = "restructuredtext en"

# If the specified screen has either dimension larger than this value, no image
# rescaling is done (we assume that it is a tablet output profile)
MAX_SCREEN_SIZE = 3000


def _pillow_modules():
    try:
        from PIL import Image, ImageChops, ImageFilter, ImageOps

        return Image, ImageChops, ImageFilter, ImageOps
    except Exception:
        return None


def _safe_bool(opts, name: str, default: bool = False) -> bool:
    return bool(getattr(opts, name, default))


def _safe_int(value, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _numeric_sort_key(value: str):
    return [int(chunk) if chunk.isdigit() else chunk.lower() for chunk in re.split(r"(\d+)", str(value))]


def _parse_screen_size(opts, fallback_width: int, fallback_height: int) -> tuple[int, int]:
    profile = getattr(opts, "output_profile", None)
    if profile is not None and getattr(profile, "comic_screen_size", None):
        width, height = profile.comic_screen_size
    else:
        width, height = fallback_width, fallback_height

    custom = getattr(opts, "comic_image_size", None)
    if custom:
        try:
            width, height = map(int, [x.strip() for x in str(custom).split("x")])
        except Exception as e:
            default_log.log_exception(
                message="Unable to parse comic_image_size; falling back to output profile size",
                exception=e,
                level="INFO",
            )

    return _safe_int(width, fallback_width), _safe_int(height, fallback_height)


def _trim_uniform_border(image, image_chops_module):
    # Simple, robust trim: compare against a flat image made from the corner pixel.
    try:
        bg = image.copy()
        corner = image.getpixel((0, 0))
        bg.paste(corner, [0, 0, image.size[0], image.size[1]])
        diff = image_chops_module.difference(image, bg)
        box = diff.getbbox()
        if box:
            return image.crop(box)
    except Exception:
        pass
    return image


def _fit_size(width: int, height: int, max_width: int, max_height: int) -> tuple[int, int]:
    if width <= 0 or height <= 0:
        return max(1, max_width), max(1, max_height)
    ratio = min(float(max_width) / float(width), float(max_height) / float(height))
    return max(1, int(width * ratio)), max(1, int(height * ratio))


def _resample_lanczos(Image):
    resampling = getattr(Image, "Resampling", None)
    if resampling is not None and hasattr(resampling, "LANCZOS"):
        return resampling.LANCZOS
    return getattr(Image, "LANCZOS", 1)


def _adaptive_palette(Image):
    palette = getattr(Image, "Palette", None)
    if palette is not None and hasattr(palette, "ADAPTIVE"):
        return palette.ADAPTIVE
    return getattr(Image, "ADAPTIVE", 0)


def extract_comic(path_to_comic_file):
    """Unarchive a comic file to a persistent temp folder."""
    tdir = PersistentTemporaryDirectory(suffix="_comic_extract")

    extract(path_to_comic_file, tdir)
    for x in walk(tdir):
        bn = os.path.basename(x)
        nbn = bn.replace("#", "_")
        if nbn != bn:
            os.rename(x, os.path.join(os.path.dirname(x), nbn))
    return tdir


def find_pages(dir, sort_on_mtime=False, verbose=False):
    """Find image pages in an extracted comic folder."""
    extensions = {"jpeg", "jpg", "gif", "png", "webp"}
    pages = []
    for datum in os.walk(dir):
        for name in datum[-1]:
            path = os.path.abspath(os.path.join(datum[0], name))
            if "__MACOSX" in path:
                continue
            if path.rpartition(".")[-1].lower() in extensions:
                pages.append(path)

    sep_counts = {x.replace(os.sep, "/").count("/") for x in pages}
    # Use the full path to sort unless files are in folder trees of varying depth.
    basename = os.path.basename if len(sep_counts) > 1 else lambda x: x
    if sort_on_mtime:
        key = lambda x: os.stat(x).st_mtime
    else:
        key = lambda x: _numeric_sort_key(basename(x))

    pages.sort(key=key)
    if verbose:
        prints("Found comic pages...")
        if pages:
            try:
                base = os.path.commonpath(pages)
            except Exception:
                base = dir
            prints("\t" + "\n\t".join([os.path.relpath(p, base) for p in pages]))
    return pages


class PageProcessor(list):  # {{{
    """Render and transform a single source page into one or more output pages."""

    def __init__(self, path_to_page, dest, opts, num):
        super().__init__()
        self.path_to_page = path_to_page
        self.opts = opts
        self.num = num
        self.dest = dest
        self.rotate = False
        self.render()

    def _render_passthrough(self):
        output_ext = str(getattr(self.opts, "output_format", "png")).lower()
        output_ext = "jpg" if output_ext in {"jpg", "jpeg"} else output_ext
        if output_ext not in {"png", "jpg", "gif", "webp"}:
            output_ext = "png"

        if self.num == 0:
            thumb_path = os.path.join(self.dest, f"thumbnail.{output_ext}")
            try:
                shutil.copyfile(self.path_to_page, thumb_path)
            except Exception:
                pass

        dest = os.path.join(self.dest, f"{self.num}_0.{output_ext}")
        shutil.copyfile(self.path_to_page, dest)
        self.append(dest)

    def render(self):
        mods = _pillow_modules()
        if mods is None:
            self._render_passthrough()
            return

        Image, _ImageChops, _ImageFilter, _ImageOps = mods

        img = Image.open(self.path_to_page)
        img.load()
        width, height = img.size
        if self.num == 0:  # First image so create a thumbnail from it
            thumb = img.copy()
            thumb.thumbnail((60, 80))
            thumb.save(os.path.join(self.dest, "thumbnail.png"), format="PNG")

        self.pages = [img]
        if width > height:
            if _safe_bool(self.opts, "landscape"):
                self.rotate = True
            else:
                half = int(width / 2)
                split1 = img.crop((0, 0, max(1, half - 1), height))
                split2 = img.crop((half, 0, width, height))
                self.pages = [split2, split1] if _safe_bool(self.opts, "right2left") else [split1, split2]

        self.process_pages()

    def process_pages(self):
        mods = _pillow_modules()
        if mods is None:
            self._render_passthrough()
            return

        Image, ImageChops, ImageFilter, ImageOps = mods
        lanczos = _resample_lanczos(Image)
        adaptive = _adaptive_palette(Image)

        for i, image in enumerate(self.pages):
            wand = image.copy()
            if self.rotate:
                wand = wand.rotate(-90, expand=True, fillcolor="#ffffff")

            if not _safe_bool(self.opts, "disable_trim"):
                wand = _trim_uniform_border(wand, ImageChops)

            # Approximate ImageMagick normalize with Pillow autocontrast.
            if not _safe_bool(self.opts, "dont_normalize"):
                wand = ImageOps.autocontrast(wand)

            sizex, sizey = wand.size
            scrwidth, scrheight = _parse_screen_size(self.opts, sizex, sizey)

            if _safe_bool(self.opts, "keep_aspect_ratio"):
                newsizex, newsizey = _fit_size(sizex, sizey, scrwidth, scrheight)
                if newsizex < MAX_SCREEN_SIZE and newsizey < MAX_SCREEN_SIZE:
                    resized = wand.resize((newsizex, newsizey), resample=lanczos)
                    base_mode = "L" if resized.mode == "L" else "RGB"
                    canvas_color = 255 if base_mode == "L" else (255, 255, 255)
                    canvas = Image.new(base_mode, (scrwidth, scrheight), canvas_color)
                    if resized.mode != base_mode:
                        resized = resized.convert(base_mode)
                    deltax = int((scrwidth - newsizex) / 2)
                    deltay = int((scrheight - newsizey) / 2)
                    canvas.paste(resized, (deltax, deltay))
                    wand = canvas

            elif _safe_bool(self.opts, "wide"):
                screen_aspect = float(scrwidth) / float(max(1, scrheight))
                wscreenx = scrheight + 25
                wscreeny = int(float(wscreenx) / screen_aspect)
                newsizex, newsizey = _fit_size(sizex, sizey, wscreenx, wscreeny)
                if newsizex < MAX_SCREEN_SIZE and newsizey < MAX_SCREEN_SIZE:
                    resized = wand.resize((newsizex, newsizey), resample=lanczos)
                    base_mode = "L" if resized.mode == "L" else "RGB"
                    canvas_color = 255 if base_mode == "L" else (255, 255, 255)
                    canvas = Image.new(base_mode, (wscreenx, wscreeny), canvas_color)
                    if resized.mode != base_mode:
                        resized = resized.convert(base_mode)
                    deltax = int((wscreenx - newsizex) / 2)
                    deltay = int((wscreeny - newsizey) / 2)
                    canvas.paste(resized, (deltax, deltay))
                    wand = canvas

            else:
                if scrwidth < MAX_SCREEN_SIZE and scrheight < MAX_SCREEN_SIZE:
                    wand = wand.resize((scrwidth, scrheight), resample=lanczos)

            if not _safe_bool(self.opts, "dont_sharpen"):
                wand = wand.filter(ImageFilter.UnsharpMask(radius=2, percent=120, threshold=3))

            if not _safe_bool(self.opts, "dont_grayscale"):
                wand = ImageOps.grayscale(wand)

            if _safe_bool(self.opts, "despeckle"):
                wand = wand.filter(ImageFilter.MedianFilter(size=3))

            colors = _safe_int(getattr(self.opts, "colors", 256), 256)
            if 0 < colors <= 256:
                if wand.mode not in {"RGB", "RGBA", "L"}:
                    wand = wand.convert("RGB")
                wand = wand.convert("P", palette=adaptive, colors=colors)

            output_ext = str(getattr(self.opts, "output_format", "png")).lower()
            output_ext = "jpg" if output_ext in {"jpg", "jpeg"} else "png"
            dest = os.path.join(self.dest, f"{self.num}_{i}.{output_ext}")
            if output_ext == "jpg":
                if wand.mode not in {"RGB", "L"}:
                    wand = wand.convert("RGB")
                wand.save(dest, format="JPEG", quality=90)
            else:
                wand.save(dest, format="PNG")
            self.append(dest)


# }}}


def render_pages(tasks, dest, opts, notification=lambda x, y: x):
    """Render all tasks; used by process_pages()."""
    failures, pages = [], []
    for num, path in tasks:
        try:
            pages.extend(PageProcessor(path, dest, opts, num))
            msg = _("Rendered %s") % path
        except Exception as e:
            failures.append(path)
            msg = _("Failed %s") % path
            default_log.log_exception(message=msg, exception=e, level="DEBUG")
            if getattr(opts, "verbose", False):
                msg += "\n" + traceback.format_exc()
        prints(msg)
        notification(0.5, msg)

    return pages, failures


class Progress:
    def __init__(self, total, update):
        self.total = max(1, int(total))
        self.update = update
        self.done = 0

    def __call__(self, percent, msg=""):
        self.done += 1
        self.update(float(self.done) / self.total, msg)


def _opts_to_payload(opts):
    profile = getattr(opts, "output_profile", None)
    screen_size = getattr(profile, "comic_screen_size", None)
    if screen_size is not None:
        try:
            screen_size = (int(screen_size[0]), int(screen_size[1]))
        except Exception:
            screen_size = None

    return {
        "landscape": _safe_bool(opts, "landscape"),
        "right2left": _safe_bool(opts, "right2left"),
        "disable_trim": _safe_bool(opts, "disable_trim"),
        "dont_normalize": _safe_bool(opts, "dont_normalize"),
        "comic_image_size": getattr(opts, "comic_image_size", None),
        "keep_aspect_ratio": _safe_bool(opts, "keep_aspect_ratio"),
        "wide": _safe_bool(opts, "wide"),
        "dont_sharpen": _safe_bool(opts, "dont_sharpen"),
        "dont_grayscale": _safe_bool(opts, "dont_grayscale"),
        "despeckle": _safe_bool(opts, "despeckle"),
        "colors": _safe_int(getattr(opts, "colors", 256), 256),
        "output_format": str(getattr(opts, "output_format", "png")),
        "verbose": _safe_bool(opts, "verbose"),
        "comic_screen_size": screen_size,
    }


def _opts_from_payload(payload):
    payload = dict(payload or {})
    screen_size = payload.pop("comic_screen_size", None)
    output_profile = None
    if screen_size is not None:
        output_profile = SimpleNamespace(comic_screen_size=tuple(screen_size))
    return SimpleNamespace(output_profile=output_profile, **payload)


def _render_pages_job(tasks, dest, opts_payload):
    opts = _opts_from_payload(opts_payload)
    return render_pages(tasks, dest, opts)


def _task_chunks(tasks, size):
    size = max(1, int(size))
    return [tasks[i : i + size] for i in range(0, len(tasks), size)]


def process_pages(pages, opts, update, tdir):
    """Render all identified comic pages."""
    progress = Progress(len(pages), update)
    tasks = list(enumerate(pages))
    if len(tasks) < 2:
        return render_pages(tasks, tdir, opts, notification=progress)

    backend = str(getattr(opts, "comic_job_backend", os.environ.get("LIUXIN_COMIC_JOB_BACKEND", "process"))).lower()
    if backend in {"", "auto", "default"}:
        backend = "process"

    try:
        workers = int(getattr(opts, "comic_job_workers", 0) or 0)
    except Exception:
        workers = 0
    if workers <= 0:
        workers = min(os.cpu_count() or 1, 4)
    workers = max(1, min(workers, len(tasks)))

    if backend == "serial" or workers <= 1:
        return render_pages(tasks, tdir, opts, notification=progress)

    try:
        chunk_size = int(getattr(opts, "comic_job_chunk_size", 0) or 0)
    except Exception:
        chunk_size = 0
    if chunk_size <= 0:
        chunk_size = max(1, (len(tasks) + workers - 1) // workers)

    chunks = _task_chunks(tasks, chunk_size)
    payload = _opts_to_payload(opts)
    timeout = _safe_int(getattr(opts, "comic_job_timeout", 300), 300)

    from LiuXin_alpha.utils.ipc.simple_worker import WorkerError, fork_job

    all_pages = {}
    all_failures = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                fork_job,
                "LiuXin_alpha.file_formats.comic.input",
                "_render_pages_job",
                args=(chunk, tdir, payload),
                no_output=True,
                timeout=timeout,
                backend=backend,
            ): idx
            for idx, chunk in enumerate(chunks)
        }

        for fut in as_completed(futures):
            idx = futures[fut]
            chunk = chunks[idx]
            try:
                result = fut.result()["result"]
            except WorkerError as err:
                message = err.orig_tb or str(err)
                raise Exception(_("Failed to process comic: \n\n%s") % message)
            except Exception:
                raise Exception(_("Failed to process comic: \n\n%s") % traceback.format_exc())

            if not result or len(result) != 2:
                raise Exception(_("Failed to process comic: worker returned invalid result"))

            rendered_pages, failures = result
            all_pages[idx] = list(rendered_pages or ())
            all_failures[idx] = list(failures or ())

            msg = _("Rendered %d pages") % len(chunk)
            for _task in chunk:
                progress(1.0, msg)

    ordered_pages = []
    ordered_failures = []
    for idx in range(len(chunks)):
        ordered_pages.extend(all_pages.get(idx, ()))
        ordered_failures.extend(all_failures.get(idx, ()))
    return ordered_pages, ordered_failures
