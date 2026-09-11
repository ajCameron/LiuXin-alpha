"""
Expose the operator CLI through a lazy application dispatcher.

Importing this package does not construct the command grammar or load the
application entry point. Calling main delegates argument handling and error
policy to that application without copying or normalizing the supplied tokens.
"""

from __future__ import annotations


def main(argv: list[str] | None = None) -> int:
    """
    Load the application on demand and forward its arguments and exit status.

    Exceptions, including argparse's SystemExit, propagate unchanged. Importing
    command-family modules need not load the complete application through this
    package initializer.

    Example:
        >>> main(['completion', 'bash'])  # doctest: +SKIP


    :param argv: Argument list passed by identity, or None for process arguments.
    :return: The application dispatcher's integer exit code.
    """

    from LiuXin_alpha.surfaces.cli.app import main as application_main

    return application_main(argv)


__all__ = ["main"]
