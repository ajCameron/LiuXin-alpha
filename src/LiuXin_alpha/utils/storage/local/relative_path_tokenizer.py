"""
Tokenize relative storage paths without permitting root escape.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise relative path tokenizer through a consuming regression::

        python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py
"""
from __future__ import annotations

from pathlib import Path
from typing import Tuple, Union

Pathish = Union[str, Path]

__all__ = ["relative_path_tokens"]


def _is_anchored_path(p: Path) -> bool:
    """
    Return True if *p* has a drive and/or root.

    Example:
        Exercise  is anchored path through a consuming regression::

            python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


    :param p: Path-like value normalized or validated by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return bool(p.drive) or bool(p.root)


def relative_path_tokens(
    base: Pathish,
    target: Pathish,
    base_is_file: bool = False,
) -> Tuple[Path, Tuple[str, ...]]:
    """
    Return (relative_path, tokens) from *base* to *target*.

    Example:
        Exercise relative path tokens through a consuming regression::

            python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


    :param base: Value supplied for base under the utility contract.
    :param target: Value supplied for target under the utility contract.
    :param base_is_file: Value supplied for base is file under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    base_p = Path(base)
    target_p = Path(target)

    if base_is_file:
        base_p = base_p.parent

    base_anchored = _is_anchored_path(base_p)
    target_anchored = _is_anchored_path(target_p)

    if base_anchored != target_anchored:
        raise ValueError(
            "Cannot relativize: one path is anchored (drive/root) and the other is purely relative: "
            f"base={base_p!s}, target={target_p!s}"
        )

    if base_anchored and base_p.anchor != target_p.anchor:
        raise ValueError(
            f"Cannot relativize across different anchors: {base_p.anchor!r} vs {target_p.anchor!r}"
        )

    # Drop the anchor token ("/", "C:\\", "\\\\server\\share\\", etc.) before prefix matching.
    base_parts = base_p.parts[1:] if base_anchored else base_p.parts
    target_parts = target_p.parts[1:] if target_anchored else target_p.parts

    # Find common prefix length.
    i = 0
    for bp, tp in zip(base_parts, target_parts):
        if bp != tp:
            break
        i += 1

    up = ("..",) * (len(base_parts) - i)
    down = target_parts[i:]
    rel_parts = up + down

    rel_path = Path(*rel_parts) if rel_parts else Path(".")
    return rel_path, rel_path.parts
