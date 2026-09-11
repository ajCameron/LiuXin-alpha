"""
Read visible Store-add wizard input and convert EOF/interrupt into cancellation.

Prompts write to stdout and input is not password-masked. Text defaults and menu
values are displayed verbatim; callers must not treat these helpers as sanitizers.
Only the interactivity probe catches arbitrary ordinary exceptions.
"""

from __future__ import annotations

import sys


class _StorageAddCancelled(Exception):
    """
    Signal that Store-add prompting ended through EOF or KeyboardInterrupt.

    The exception carries no rollback behavior; prompt helpers chain the original
    input exception and the wizard's caller decides how to report cancellation.

    Example:
        >>> isinstance(_StorageAddCancelled(), Exception)
        True
    """


def _storage_stdin_is_interactive() -> bool:
    """
    Ask stdin whether it is a terminal and treat ordinary probe errors as noninteractive.

    Example:
        >>> from unittest.mock import patch
        >>> with patch.object(sys, "stdin", None):
        ...     _storage_stdin_is_interactive()
        False


    :return: Truthiness of stdin.isatty(), or False when that call raises Exception.
    """
    try:
        return bool(sys.stdin.isatty())
    except Exception:
        return False


def _storage_prompt_text(
    label: str,
    *,
    default: str | None = None,
    required: bool = True,
) -> str:
    """
    Read stripped text, falling back to a visible nonempty default or retrying.

    User input is stripped, but a selected default is stringified without stripping.
    Required input with no usable default repeats with guidance; optional blank
    input returns ''. Other input/printing failures propagate unchanged.

    Example:
        >>> from unittest.mock import patch
        >>> with patch("builtins.input", return_value="  "):
        ...     _storage_prompt_text("Name", default="books")
        'books'


    :param label: Prompt text displayed before the optional default and colon.
    :param default: Fallback returned for blank input unless None or the empty string.
    :param required: Whether blank input without a default must be retried.
    :return: Nonempty stripped answer, selected default, or an allowed empty answer.
    :raises _StorageAddCancelled: Reading input raises EOFError or KeyboardInterrupt.
    """
    suffix = "" if default in (None, "") else f" [{default}]"
    while True:
        try:
            value = input(f"{label}{suffix}: ").strip()
        except (EOFError, KeyboardInterrupt) as error:
            raise _StorageAddCancelled from error
        if value:
            return value
        if default not in (None, ""):
            return str(default)
        if not required:
            return ""
        print("A value is required.")


def _storage_prompt_yes_no(label: str, *, default: bool) -> bool:
    """
    Read a case-insensitive yes/no answer, retrying unrecognized text.

    Example:
        >>> from unittest.mock import patch
        >>> with patch("builtins.input", return_value=" YES "):
        ...     _storage_prompt_yes_no("Continue", default=False)
        True


    :param label: Visible question preceding the Y/n or y/N choice suffix.
    :param default: Value returned unchanged for blank input and used to display the default.
    :return: True for y/yes, False for n/no, or the supplied default for blank input.
    :raises _StorageAddCancelled: Input ends with EOFError or KeyboardInterrupt.
    """
    suffix = "Y/n" if default else "y/N"
    while True:
        try:
            answer = input(f"{label} [{suffix}]: ").strip().casefold()
        except (EOFError, KeyboardInterrupt) as error:
            raise _StorageAddCancelled from error
        if not answer:
            return default
        if answer in {"y", "yes"}:
            return True
        if answer in {"n", "no"}:
            return False
        print("Answer yes or no.")


def _storage_prompt_choice(
    label: str,
    choices: tuple[tuple[str, str], ...],
    *,
    default_value: str,
) -> str:
    """
    Display a numbered menu and accept an in-range number or case-folded label/value.

    The last matching value determines the displayed default index; an absent
    default falls back to index one. Duplicate aliases are overwritten by later
    choices. Numeric tokens use index handling, not alias fallback, and an empty
    choices tuple keeps retrying until input is cancelled.

    Example:
        >>> selected = _storage_prompt_choice("Access", (("Read only", "ro"), ("Writable", "rw")), default_value="ro")  # doctest: +SKIP


    :param label: Heading printed before the menu entries.
    :param choices: Ordered display-label/value pairs returned by numeric or alias selection.
    :param default_value: Value whose last matching entry becomes the blank-input choice.
    :return: Selected value without further normalization or validation.
    :raises _StorageAddCancelled: The delegated text prompt receives EOF or an interrupt.
    """
    print(label)
    default_index = 1
    aliases: dict[str, str] = {}
    for index, (choice_label, value) in enumerate(choices, start=1):
        if value == default_value:
            default_index = index
        marker = " (default)" if value == default_value else ""
        print(f"  {index}) {choice_label} [{value}]{marker}")
        aliases[choice_label.casefold()] = value
        aliases[value.casefold()] = value
    while True:
        selected = _storage_prompt_text(
            "Choice",
            default=str(default_index),
        )
        try:
            index = int(selected)
        except ValueError:
            matched = aliases.get(selected.casefold())
            if matched is not None:
                return matched
        else:
            if 1 <= index <= len(choices):
                return choices[index - 1][1]
        print(f"Choose a number from 1 to {len(choices)} or a displayed id.")
