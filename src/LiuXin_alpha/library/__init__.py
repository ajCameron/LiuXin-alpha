"""
Expose the supported library compatibility surface.

Separation of concerns: Library coordinates user-level workflows that span
Catalog and storage. It does not own collaborator lifecycle, reimplement
bibliographic validation, execute raw persistence for ordinary metadata writes,
or perform backend-specific byte movement. Core-owned ``LibraryServices`` supplies
the collaborators, Catalog guarantees semantic mutations, and storage manages
files and replicas. Library and Core methods delegate ordinary metadata writes
to the public Catalog boundary; they cannot be the sole place that validates a
mutation. Remaining direct compatibility paths are migration debt, not the
contract for new code.

See ``dev-docs/04 - Seperation of Concerns.md`` and
``dev-docs/catalog-write-validation-contract.md``.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/library/test_unified_library.py
"""

from __future__ import annotations

from LiuXin_alpha.library.library import Library

__all__ = ["Library"]
