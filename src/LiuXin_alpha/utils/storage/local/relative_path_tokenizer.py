from __future__ import annotations

from pathlib import Path
from typing import Tuple, Union

Pathish = Union[str, Path]

__all__ = ["relative_path_tokens"]


def _is_anchored_path(p: Path) -> bool:
    """Return True if *p* has a drive and/or root.

    This treats Windows rooted paths like ``\\foo\\bar`` as "anchored" even though
    ``Path("\\foo\\bar").is_absolute()`` is False (because it lacks a drive).

    For relativization, the key distinction is:
      - anchored (drive/root present) vs
      - purely relative (no drive/root)
    """
    return bool(p.drive) or bool(p.root)


def relative_path_tokens(
    base: Pathish,
    target: Pathish,
    base_is_file: bool = False,
) -> Tuple[Path, Tuple[str, ...]]:
    """Return (relative_path, tokens) from *base* to *target*.

    - If base_is_file=True, *base.parent* is used as the starting directory.
    - tokens are the result of ``relative_path.parts``.
      Note: in pathlib, ``Path(".").parts`` is ``()`` (empty tuple).

    Raises ValueError if:
      - one path is anchored (drive/root) and the other is purely relative, OR
      - both are anchored but have different anchors (e.g., different drives/UNC shares).

    Windows note:
      ``Path("/a/b")`` becomes a rooted path like ``\\a\\b`` (rooted) but has no drive,
      so ``is_absolute()`` returns False. We still treat it as anchored via ``root``.
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
