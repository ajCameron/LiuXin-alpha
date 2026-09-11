"""
Expose conversion format discovery, path-selected option descriptions, and background job submission through Core.

Plugin/plumber initialization stays in the conversion subsystem. Starting a job
returns a submission receipt, not a converted file or evidence that conversion
succeeded. Paths and option values are passed to that subsystem without sandboxing
or a separate overwrite-policy check in this adapter.
"""

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
    """
    List conversion input/output formats advertised by the current customization registry.

    Results are stringified and sorted independently without adapter deduplication.
    Discovery imports customization code but does not attempt a sample conversion.

    Example:
        >>> formats = conversion_formats(runtime, query)  # doctest: +SKIP


    :param runtime: Unused runtime; discovery uses the process-wide customization registry.
    :param query: Unused query envelope; no payload filters are consumed.
    :return: input and output format-name lists; discovery/import failures propagate.
    """
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
    """
    Construct a path-selected Plumber and describe its input, output, and pipeline option recommendations.

    The first nonblank stripped option name wins across those three groups; later
    duplicates are ignored. Recommended values, levels, and choices use plain
    conversion, and help falls back from help to option_help. Plumber construction
    may initialize plugins or inspect paths; this handler never calls its run method.

    Example:
        >>> options = conversion_options(runtime, query)  # doctest: +SKIP


    :param runtime: Unused runtime; Plumber is constructed with the shared default logger.
    :param query: Query with required stripped input_path and output_path text selecting conversion formats.
    :return: Plumber-reported input/output formats and ordered unique option descriptions.
    """
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
    """
    Submit a named conversion workflow job with copied options and required input/output paths.

    Options default to an empty Mapping; explicit None is invalid. Validation of
    file existence, conversion options, and overwrite behavior belongs to the
    worker. This handler does not wait for completion or own rollback of worker I/O.

    Example:
        >>> receipt = conversion_start(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime whose job manager accepts the run_conversion_job request.
    :param command: Command with input_path, output_path, optional Mapping options, and shared job submission fields.
    :return: Job ID and submission options, using convert as the fallback label; not a conversion outcome.
    """
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
