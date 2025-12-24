from __future__ import annotations

from pathlib import Path
from typing import Iterable, Tuple, Union

Pathish = Union[str, Path]

def relative_path_tokens(
    base: Pathish,
    target: Pathish,
    *,
    base_is_file: bool = False,
) -> Tuple[Path, Tuple[str, ...]]:
    """
    Return (relative_path, tokens) where tokens are the path parts.
    If base_is_file=True, base.parent is used as the starting directory.

    Raises ValueError if the paths are on different anchors (e.g., different drives on Windows)
    or one is absolute and the other is relative.
    """
    base_p = Path(base)
    target_p = Path(target)

    if base_is_file:
        base_p = base_p.parent

    # Normalize to "pure" comparison space (no filesystem access required)
    base_abs = base_p.is_absolute()
    target_abs = target_p.is_absolute()
    if base_abs != target_abs:
        raise ValueError(f"Cannot relativize: base is_absolute={base_abs}, target is_absolute={target_abs}")

    # If absolute, anchors must match (e.g., same drive on Windows, same root style)
    if base_abs and base_p.anchor != target_p.anchor:
        raise ValueError(f"Cannot relativize across different anchors: {base_p.anchor!r} vs {target_p.anchor!r}")

    # Compare without the anchor token ("/" or "C:\\") so common-prefix logic works cleanly
    base_parts = base_p.parts[1:] if base_abs else base_p.parts
    target_parts = target_p.parts[1:] if target_abs else target_p.parts

    # Find common prefix length
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
