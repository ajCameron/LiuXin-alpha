"""Open-mode type aliases and async file protocols for storage APIs.

Examples:
    Annotate helpers that accept only text-reading modes::

        def read_mode(mode: OpenTextModeReading = "r") -> OpenTextModeReading:
            return mode
"""

from __future__ import annotations

from typing import TypeAlias, Literal, Callable, Protocol, Any

OpenTextModeUpdating: TypeAlias = Literal[
    "r+", "+r", "rt+", "r+t", "+rt", "tr+", "t+r", "+tr",
    "w+", "+w", "wt+", "w+t", "+wt", "tw+", "t+w", "+tw",
    "a+", "+a", "at+", "a+t", "+at", "ta+", "t+a", "+ta",
    "x+", "+x", "xt+", "x+t", "+xt", "tx+", "t+x", "+tx",
]
OpenTextModeWriting: TypeAlias = Literal["w", "wt", "tw", "a", "at", "ta", "x", "xt", "tx"]
OpenTextModeReading: TypeAlias = Literal["r", "rt", "tr", "U", "rU", "Ur", "rtU", "rUt", "Urt", "trU", "tUr", "Utr"]
OpenTextMode: TypeAlias = OpenTextModeUpdating | OpenTextModeWriting | OpenTextModeReading
OpenBinaryModeUpdating: TypeAlias = Literal[
    "rb+", "r+b", "+rb", "br+", "b+r", "+br",
    "wb+", "w+b", "+wb", "bw+", "b+w", "+bw",
    "ab+", "a+b", "+ab", "ba+", "b+a", "+ba",
    "xb+", "x+b", "+xb", "bx+", "b+x", "+bx",
]
OpenBinaryModeWriting: TypeAlias = Literal["wb", "bw", "ab", "ba", "xb", "bx"]
OpenBinaryModeReading: TypeAlias = Literal["rb", "br", "rbU", "rUb", "Urb", "brU", "bUr", "Ubr"]
OpenBinaryMode: TypeAlias = OpenBinaryModeUpdating | OpenBinaryModeReading | OpenBinaryModeWriting
_Opener: TypeAlias = Callable[[str, int], int]


class AsyncTextFile(Protocol):
    """Protocol for async text file objects returned by location open calls.

    Examples:
        Consume any conforming async text handle::

            async def first_line(file: AsyncTextFile) -> str:
                return (await file.read()).splitlines()[0]
    """
    async def __aenter__(self) -> "AsyncTextFile": ...
    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool | None: ...
    async def read(self, n: int = -1) -> str: ...
    async def write(self, s: str) -> int: ...
    async def flush(self) -> None: ...
    async def close(self) -> None: ...


class AsyncBinaryFile(Protocol):
    """Protocol for async binary file objects returned by location open calls.

    Examples:
        Consume any conforming async binary handle::

            async def header(file: AsyncBinaryFile) -> bytes:
                return await file.read(16)
    """
    async def __aenter__(self) -> "AsyncBinaryFile": ...
    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool | None: ...
    async def read(self, n: int = -1) -> bytes: ...
    async def write(self, b: bytes) -> int: ...
    async def flush(self) -> None: ...
    async def close(self) -> None: ...
