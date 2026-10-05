#!/usr/bin/env python3
"""
Forward the historical mixed-tree example entrypoint to the packaged ingest CLI.

Make the checkout importable, then delegate process arguments, reporting, and exit
policy to LiuXin_alpha.surfaces.cli.storage.ingest_main. This wrapper defines no
separate parser or ingest implementation; its --help describes the packaged command.
"""

from __future__ import annotations

import sys
from pathlib import Path

EXAMPLES_ROOT = Path(__file__).resolve().parents[1]
if str(EXAMPLES_ROOT) not in sys.path:
    sys.path.insert(0, str(EXAMPLES_ROOT))

from _example_utils import (
    bootstrap_src_path,  # pyright: ignore[reportImplicitRelativeImport]
)

_ = bootstrap_src_path()

from LiuXin_alpha.surfaces.cli.storage_commands.ingest import ingest_main


def main() -> int:
    """
    Run the packaged mixed-ingest command using the current process arguments. Forward its return
    value unchanged. Parser exits, lifecycle logging, discovery-only behavior, execution, and
    exception policy belong to ingest_main.

    Example:
        >>> exit_code = main()  # doctest: +SKIP


    :return: The packaged ingest_main exit status; exceptions not handled there propagate.
    """
    return ingest_main()


if __name__ == "__main__":
    raise SystemExit(main())
