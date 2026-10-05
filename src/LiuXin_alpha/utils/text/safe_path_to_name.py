"""
Convert arbitrary path text into a safe display or filesystem name.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise safe path to name through a consuming regression::

        python -m pytest -q tests/utils/text/test_text_core.py
"""
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
    """
    Perform the looks like windows path utility operation under explicit compatibility rules.

    Example:
        Exercise  looks like windows path through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param s: Value supplied for s under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return bool(re.match(r"^[a-zA-Z]:", s)) or s.startswith("\\\\") or ("\\" in s)


def _strip_diacritics_to_ascii(s: str) -> str:
    # NFKD splits accents so we can drop combining marks.
    """
    Perform the strip diacritics to ascii utility operation under explicit compatibility rules.

    Example:
        Exercise  strip diacritics to ascii through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param s: Value supplied for s under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    norm = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in norm if not unicodedata.combining(ch)).encode("ascii", "ignore").decode("ascii")


def _sanitize_component(
    s: str,
    *,
    allow_unicode: bool,
    lowercase: bool,
    replacement: str = "-",
) -> str:
    """
    Perform the sanitize component utility operation under explicit compatibility rules.

    Example:
        Exercise  sanitize component through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param s: Value supplied for s under the utility contract.
    :param allow_unicode: Value supplied for allow unicode under the utility contract.
    :param lowercase: Value supplied for lowercase under the utility contract.
    :param replacement: Value supplied for replacement under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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
    if allow_unicode:
        s = re.sub(r"[^\w.-]+", replacement, s, flags=re.UNICODE)
    else:
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

    Example:
        Exercise safe path to name through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :param max_len: Value supplied for max len under the utility contract.
    :param sep: Delimiter used to split or join list values.
    :param allow_unicode: Value supplied for allow unicode under the utility contract.
    :param lowercase: Value supplied for lowercase under the utility contract.
    :param add_hash: Value supplied for add hash under the utility contract.
    :param hash_len: Value supplied for hash len under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
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
    tokens: list[str] = []

    if is_win:
        if raw.startswith("\\\\"):
            unc_bits = [bit for bit in raw.lstrip("\\").split("\\") if bit]
            if len(unc_bits) >= 2:
                tokens.append(f"UNC_{unc_bits[0]}_{unc_bits[1]}")
                tokens.extend(unc_bits[2:])
            elif unc_bits:
                tokens.append("UNC")
                tokens.extend(unc_bits)
            else:
                tokens.append("UNC")
        else:
            p = PureWindowsPath(raw)
            parts = list(p.parts)
            anchor = p.anchor  # e.g. 'C:\\'
            if anchor and re.match(r"^[A-Za-z]:\\?$", anchor):
                tokens.append(anchor[0])  # 'C'
            if parts and anchor and parts[0] == anchor:
                parts = parts[1:]
            tokens.extend(parts)
    else:
        p = PurePosixPath(raw)
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
