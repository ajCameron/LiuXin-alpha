"""Thin compatibility wrapper for ``tqdm``.

Use the real package when available. Otherwise provide a minimal progress-bar
surface that degrades to plain iteration with no visible progress UI.
"""

from __future__ import annotations


try:
    from tqdm import tqdm as tqdm  # type: ignore
    from tqdm import trange as trange  # type: ignore
except ModuleNotFoundError:
    class _TqdmFallback(object):
        def __init__(self, iterable=None, total=None, **_kwargs):
            self.iterable = iterable
            self.total = total
            self.n = 0

        def __iter__(self):
            if self.iterable is None:
                return iter(())
            for item in self.iterable:
                self.n += 1
                yield item

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return False

        def update(self, n=1):
            self.n += n

        def close(self):
            return None

        def set_description(self, *_args, **_kwargs):
            return None

        def set_postfix(self, *_args, **_kwargs):
            return None

        def refresh(self):
            return None

    def tqdm(iterable=None, total=None, **kwargs):
        return _TqdmFallback(iterable=iterable, total=total, **kwargs)

    def trange(*args, **kwargs):
        return tqdm(range(*args), **kwargs)


__all__ = ["tqdm", "trange"]
