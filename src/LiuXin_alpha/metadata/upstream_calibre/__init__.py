"""
Metadata readers ported from current upstream calibre, and the calibre code they use.

Each module here is a copy of a calibre module, from the calibre release in
:data:`UPSTREAM_VERSION`: the metadata readers (``txt``, ``opf`` and the
rest), and what they import, such as calibre's ``Metadata`` class
(``book_base``), its date handling (``date``, ``iso8601``) and its OPF 2 and
OPF 3 code (``opf2``, ``opf3``). The copies are changed only where the comment
at the top of each says: their ``calibre`` imports point at this package
instead, ``from __future__ import annotations`` is added, and ``except``
clauses naming several exceptions are parenthesised, so they also run on Python
versions before 3.14. Keeping the code otherwise untouched is what makes a later
upstream change easy to bring across; ``scripts/port_upstream_calibre.py``
re-ports the modules it lists.

The one other kind of change fixes a reader that upstream leaves running for
minutes, or forever, on a damaged file. Each such line is marked
``# LiuXin:`` and listed in the module's top comment.

A few modules hold only part of a calibre module (``pretty``, ``config_base``,
``meta``), and say so. :mod:`._compat` holds the small helpers taken from
modules that are not ported, and says where LiuXin differs from calibre;
:mod:`._speedups` ports the C functions the code calls. Three data files come
with them: calibre's ``default_tweaks.py`` and ``mime.types``, and
``iso_codes.json.gz``, the language and country maps calibre builds from
iso-codes, rebuilt by ``scripts/build_upstream_calibre_iso_codes.py``. That last
one lives in ``utils/libraries/iso639``, as LiuXin's own language functions read it.
calibre's own tests for the OPF code and author sorting are ported too
(``opf3_test``, ``author_sort_test``, and the suite in ``opf2``).

These modules are used through the providers in
``file_formats.providers.ported_calibre``. Readers not ported yet are covered
by the providers that run an installed calibre.
"""

UPSTREAM_VERSION = "9.18"
"""The calibre release the modules here were copied from."""
