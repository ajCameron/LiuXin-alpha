#!/usr/bin/env python3
"""
Example: convert many supported input formats to OEB (directory + OPF/NCX).
"""

from __future__ import annotations

import argparse
import importlib
import shutil

from pathlib import Path
from types import SimpleNamespace

from _conversion_example_utils import (
    ExampleLog,
    conversion_profile,
    dump_json,
    install_customize_ui_stub,
    isolated_conversion_scratch,
    load_oeb_from_opf,
)

from LiuXin_alpha.file_formats.conversion.plugins.oeb_output import OEBOutput


_PLUGIN_MAP: dict[str, tuple[str, str]] = {
    "txt": ("txt_input", "TXTInput"),
    "text": ("txt_input", "TXTInput"),
    "md": ("txt_input", "TXTInput"),
    "markdown": ("txt_input", "TXTInput"),
    "textile": ("txt_input", "TXTInput"),
    "txtz": ("txt_input", "TXTInput"),
    "html": ("html_input", "HTMLInput"),
    "htm": ("html_input", "HTMLInput"),
    "xhtml": ("html_input", "HTMLInput"),
    "xhtm": ("html_input", "HTMLInput"),
    "shtml": ("html_input", "HTMLInput"),
    "shtm": ("html_input", "HTMLInput"),
    "htmlz": ("htmlz_input", "HTMLZInput"),
    "epub": ("epub_input", "EPUBInput"),
    "mobi": ("mobi_input", "MOBIInput"),
    "prc": ("mobi_input", "MOBIInput"),
    "azw": ("mobi_input", "MOBIInput"),
    "azw3": ("mobi_input", "MOBIInput"),
    "pobi": ("mobi_input", "MOBIInput"),
    "azw4": ("azw4_input", "AZW4Input"),
    "pdf": ("pdf_input", "PDFInput"),
    "fb2": ("fb2_input", "FB2Input"),
    "rtf": ("rtf_input", "RTFInput"),
    "odt": ("odt_input", "ODTInput"),
    "docx": ("docx_input", "DOCXInput"),
    "docm": ("docx_input", "DOCXInput"),
    "pdb": ("pdb_input", "PDBInput"),
    "updb": ("pdb_input", "PDBInput"),
    "rb": ("rb_input", "RBInput"),
    "pml": ("pml_input", "PMLInput"),
    "pmlz": ("pml_input", "PMLInput"),
    "tcr": ("tcr_input", "TCRInput"),
    "lit": ("lit_input", "LITInput"),
    "lrf": ("lrf_input", "LRFInput"),
    "snb": ("snb_input", "SNBInput"),
    "chm": ("chm_input", "CHMInput"),
    "djvu": ("djvu_input", "DJVUInput"),
    "djv": ("djvu_input", "DJVUInput"),
    "cbz": ("comic_input", "ComicInput"),
    "cbr": ("comic_input", "ComicInput"),
    "cbc": ("comic_input", "ComicInput"),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert one input file to OEB")
    parser.add_argument("--input", help="Source file path")
    parser.add_argument("--output-dir", help="Target OEB directory path")
    parser.add_argument("--input-format", default=None, help="Source format override (default: infer from extension)")
    parser.add_argument("--clean-output", action="store_true", help="Delete output directory first if it exists")
    parser.add_argument(
        "--no-ui-shim",
        action="store_true",
        help="Do not install examples customize.ui shim (advanced)",
    )
    parser.add_argument("--verbose", action="store_true", help="Print plugin logs")
    parser.add_argument("--list-formats", action="store_true", help="Print supported input formats and exit")
    return parser.parse_args()


def _build_input_options(plugin) -> SimpleNamespace:
    values: dict[str, object] = {}
    for rec in (getattr(plugin, "options", None) or []):
        try:
            values[rec.option.name] = rec.recommended_value
        except Exception:
            continue

    defaults = {
        "input_encoding": None,
        "debug_pipeline": None,
        "pretty_print": False,
        "verbose": 0,
        "breadth_first": False,
        "dont_package": False,
        "max_levels": 5,
        "enable_heuristics": False,
        "dehyphenate": False,
        "output_profile": conversion_profile(),
    }
    for name, value in defaults.items():
        values.setdefault(name, value)
    return SimpleNamespace(**values)


def _load_plugin(input_format: str):
    module_name, class_name = _PLUGIN_MAP[input_format]
    module = importlib.import_module(f"LiuXin_alpha.file_formats.conversion.plugins.{module_name}")
    plugin_cls = getattr(module, class_name)
    return plugin_cls(None)


def _normalize_to_oeb(plugin_result, *, result_label: str):
    if hasattr(plugin_result, "manifest") and hasattr(plugin_result, "spine"):
        return plugin_result, {"input_plugin_result": result_label, "input_plugin_opf_path": None}
    if isinstance(plugin_result, (str, Path)):
        opf_path = Path(plugin_result)
        if not opf_path.is_absolute():
            opf_path = (Path.cwd() / opf_path).resolve()
        if not opf_path.exists():
            raise FileNotFoundError(f"Input plugin produced OPF path that does not exist: {opf_path}")
        return load_oeb_from_opf(opf_path), {"input_plugin_result": "opf_path", "input_plugin_opf_path": str(opf_path)}
    raise TypeError(f"Unsupported plugin result type: {type(plugin_result)!r}")


def main() -> int:
    args = parse_args()

    if args.list_formats:
        print("\n".join(sorted(_PLUGIN_MAP.keys())))
        return 0

    if not args.input or not args.output_dir:
        raise SystemExit("--input and --output-dir are required unless --list-formats is used")

    input_path = Path(args.input).expanduser().resolve()
    if not input_path.exists():
        raise FileNotFoundError(f"Input file does not exist: {input_path}")
    if not input_path.is_file():
        raise ValueError(f"Input path must be a file: {input_path}")

    input_format = (args.input_format or input_path.suffix.lstrip(".")).lower()
    if input_format not in _PLUGIN_MAP:
        supported = ", ".join(sorted(_PLUGIN_MAP.keys()))
        raise ValueError(f"Unsupported input format: {input_format!r}. Supported: {supported}")

    output_dir = Path(args.output_dir).expanduser().resolve()
    if output_dir.exists() and args.clean_output:
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not args.no_ui_shim:
        install_customize_ui_stub(html_input_support=True)

    log = ExampleLog(verbose=args.verbose)
    plugin = _load_plugin(input_format)
    input_opts = _build_input_options(plugin)
    accelerators = {}

    with isolated_conversion_scratch():
        with input_path.open("rb") as stream:
            plugin_result = plugin.convert(
                stream,
                input_opts,
                input_format,
                log,
                accelerators,
            )

        oeb, details = _normalize_to_oeb(
            plugin_result,
            result_label="oeb_object",
        )

        postprocess = getattr(plugin, "postprocess_book", None)
        if callable(postprocess):
            postprocess(oeb, input_opts, log)

        out_opts = SimpleNamespace(
            expand_css=True,
            output_profile=conversion_profile(),
        )
        OEBOutput(None).convert(oeb, str(output_dir), plugin, out_opts, log)

    output_files = sorted(p.relative_to(output_dir).as_posix() for p in output_dir.rglob("*") if p.is_file())
    payload = {
        "input": str(input_path),
        "input_format": input_format,
        "plugin": type(plugin).__name__,
        "output_dir": str(output_dir),
        "output_file_count": len(output_files),
        "output_files_preview": output_files[:40],
        "accelerators": accelerators,
        **details,
    }
    print(dump_json(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
