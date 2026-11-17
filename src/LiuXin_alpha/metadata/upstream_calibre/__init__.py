"""
Metadata readers ported from current upstream calibre.

Each reader module here is a copy of one of calibre's metadata reader modules,
from the calibre release in :data:`UPSTREAM_VERSION`. The copies are changed
only where the comment at the top of each says: their ``calibre`` imports point
at :mod:`._compat` instead, and ``from __future__ import annotations`` is added
so they also run on Python versions before 3.14. Keeping the code otherwise
untouched is what makes a later upstream change easy to bring across.

:mod:`._compat` holds ports of the few calibre helpers the readers use. It
takes calibre's ``Metadata`` class from the project's own copy of it, in
``metadata.book.base``.

These modules are used through the providers in
``file_formats.providers.ported_calibre``. Readers not ported yet are covered
by the providers that run an installed calibre.
"""

UPSTREAM_VERSION = "9.18"
"""The calibre release the readers here were copied from."""
