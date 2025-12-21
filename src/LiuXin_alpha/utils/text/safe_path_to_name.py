from __future__ import annotations

import os
import re
import unicodedata
import hashlib
from pathlib import PurePosixPath, PureWindowsPath
from typing import Union


_PathLikeStr = Union[str, os.PathLike[str]]


# Windows device names (case-insensitive) that cannot be used as a filename.
_WINDOWS_RESERVED = {
    "CON", "PRN", "AUX", "NUL",
    *(f"COM{i}" for i in range(1, 10)),
    *(f"LPT{i}" for i in range(1, 10)),
}


def _looks_like_windows_path(s: str) -> bool:
    # Drive letter, UNC prefix, or backslashes are strong signals.
    return bool(re.match(r"^[a-zA-Z]:", s)) or s.startswith("\\\\") or ("\\" in s)


def _strip_diacritics_to_ascii(s: str) -> str:
    # NFKD splits accents so we can drop combining marks.
    norm = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in norm if not unicodedata.combining(ch)).encode("ascii", "ignore").decode("ascii")


def _sanitize_component(
    s: str,
    *,
    allow_unicode: bool,
    lowercase: bool,
    replacement: str = "-",
) -> str:
    s = s.strip()

    if not allow_unicode:
        s = _strip_diacritics_to_ascii(s)

    if lowercase:
        s = s.lower()

    # Replace path-hostile whitespace with underscores first (more readable),
    # then restrict to a conservative safe set.
    s = re.sub(r"\s+", "_", s)

    # Only allow: alnum, underscore, dash, dot (portable across major filesystems).
    # Everything else becomes the replacement.
    s = re.sub(r"[^A-Za-z0-9_.-]+", replacement, s)

    # Collapse runs of replacement / underscores / dashes a bit.
    s = re.sub(r"[-_]{2,}", lambda m: m.group(0)[0], s)

    # Windows forbids trailing dot/space; generally awkward elsewhere too.
    s = s.rstrip(" .")

    # Avoid special directory names.
    if s in {"", ".", ".."}:
        s = "_"

    return s


def safe_path_to_name(
    path: _PathLikeStr,
    *,
    max_len: int = 120,
    sep: str = "__",
    allow_unicode: bool = False,
    lowercase: bool = False,
    add_hash: bool = True,
    hash_len: int = 10,
) -> str:
    """
    Convert a Windows or POSIX path into a filename-safe name (cross-platform).

    - Works with Windows (drive letters, UNC) and POSIX paths.
    - Produces a mostly human-readable slug.
    - Optionally appends a short hash to avoid collisions.
    - Enforces a maximum length while preserving uniqueness.

    Returns a string that is safe to use as a filename on Windows/macOS/Linux.
    """
    if max_len < 8:
        raise ValueError("max_len must be >= 8")
    if hash_len < 4:
        raise ValueError("hash_len must be >= 4")
    if not sep:
        raise ValueError("sep must be non-empty")

    raw = os.fspath(path)
    raw = raw.strip()

    is_win = _looks_like_windows_path(raw)
    p = PureWindowsPath(raw) if is_win else PurePosixPath(raw)

    tokens: list[str] = []

    if is_win:
        # PureWindowsPath.parts handles UNC anchors like '\\\\server\\share\\'
        parts = list(p.parts)

        # If absolute, encode the "anchor" (drive or UNC root) as a token.
        anchor = p.anchor  # e.g. 'C:\\' or '\\\\server\\share\\'
        if anchor:
            if re.match(r"^[A-Za-z]:\\?$", anchor):
                tokens.append(anchor[0])  # 'C'
            elif anchor.startswith("\\\\"):
                # '\\\\server\\share\\' -> 'UNC_server_share'
                unc = anchor.strip("\\")
                unc_bits = [b for b in unc.split("\\") if b]
                if len(unc_bits) >= 2:
                    tokens.append(f"UNC_{unc_bits[0]}_{unc_bits[1]}")
                else:
                    tokens.append("UNC")

        # Skip the anchor part in parts (it may appear as first element for UNC).
        # Safer approach: remove any leading part that equals the anchor.
        if parts and anchor and parts[0] == anchor:
            parts = parts[1:]

        tokens.extend(parts)
    else:
        if p.is_absolute():
            tokens.append("root")
        tokens.extend(p.parts[1:] if p.is_absolute() else p.parts)

    # Sanitize each token.
    cleaned = [
        _sanitize_component(t, allow_unicode=allow_unicode, lowercase=lowercase)
        for t in tokens
        if t not in {"", os.sep}
    ]

    name = sep.join(cleaned) if cleaned else "_"

    # Avoid Windows reserved device names as the *entire* filename.
    if name.upper() in _WINDOWS_RESERVED:
        name = f"_{name}"

    # Build a stable hash of the raw input (not the cleaned output).
    digest = hashlib.blake2b(raw.encode("utf-8", "ignore"), digest_size=16).hexdigest()
    suffix = digest[:hash_len]

    if add_hash:
        # Only add hash if it's not already present at the end.
        if not name.endswith(f"-{suffix}"):
            name = f"{name}-{suffix}"

    # Enforce max length, keeping the hash at the end when present.
    if len(name) > max_len:
        if add_hash:
            keep = max_len - (1 + hash_len)  # "-{hash}"
            keep = max(1, keep)
            head = name[:keep].rstrip(" .-_")
            if not head:
                head = "_"
            name = f"{head}-{suffix}"
        else:
            name = name[:max_len].rstrip(" .")
            if not name:
                name = "_"

    # Final Windows trailing-dot/space guard.
    name = name.rstrip(" .")
    if not name:
        name = "_"

    return name
