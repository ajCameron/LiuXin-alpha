"""Core-owned conversion operations and wire translation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from LiuXin_alpha.core.program_services.payloads import (
    _job_submit,
    _mapping,
    _payload,
    _required_text,
    plain,
)

if TYPE_CHECKING:
    from LiuXin_alpha.core.commands import CoreCommand
    from LiuXin_alpha.core.queries import CoreQuery
    from LiuXin_alpha.core.runtime import CoreRuntime


def conversion_formats(
    runtime: CoreRuntime,
    query: CoreQuery,
) -> dict[str, Any]:
    del runtime, query
    from LiuXin_alpha.customize.ui import (
        available_input_formats,
        available_output_formats,
    )

    return {
        "input": sorted(str(item) for item in available_input_formats()),
        "output": sorted(str(item) for item in available_output_formats()),
    }


def conversion_options(
    runtime: CoreRuntime,
    query: CoreQuery,
) -> dict[str, Any]:
    del runtime
    payload = _payload(query)
    input_path = _required_text(payload, "input_path")
    output_path = _required_text(payload, "output_path")
    from LiuXin_alpha.file_formats.conversion.plumber import Plumber
    from LiuXin_alpha.utils.logging import default_log

    plumber = Plumber(input_path, output_path, default_log)
    options: list[dict[str, Any]] = []
    seen: set[str] = set()
    for recommendation in (
        list(getattr(plumber, "input_options", ()) or ())
        + list(getattr(plumber, "output_options", ()) or ())
        + list(getattr(plumber, "pipeline_options", ()) or ())
    ):
        option = getattr(recommendation, "option", None)
        name = str(getattr(option, "name", "") or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        options.append(
            {
                "name": name,
                "recommended_value": plain(
                    getattr(recommendation, "recommended_value", None)
                ),
                "level": plain(getattr(recommendation, "level", None)),
                "choices": plain(getattr(option, "choices", None)),
                "help": str(
                    getattr(option, "help", None)
                    or getattr(option, "option_help", None)
                    or ""
                ),
            }
        )
    return {
        "input_format": getattr(plumber, "input_fmt", None),
        "output_format": getattr(plumber, "output_fmt", None),
        "options": options,
    }


def conversion_start(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    payload = _payload(command)
    return _job_submit(
        runtime,
        payload,
        function_name="run_conversion_job",
        kwargs={
            "input_path": _required_text(payload, "input_path"),
            "output_path": _required_text(payload, "output_path"),
            "options": _mapping(payload, "options", default={}),
        },
        default_label="convert",
    )
