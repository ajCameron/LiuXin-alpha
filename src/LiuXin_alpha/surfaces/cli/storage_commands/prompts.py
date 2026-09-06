"""Storage CLI prompts ownership."""

from __future__ import annotations

import sys


class _StorageAddCancelled(Exception):
    """The operator left the Store-add wizard before persistence."""


def _storage_stdin_is_interactive() -> bool:
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
