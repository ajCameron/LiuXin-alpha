"""
Expose the supported LiuXin alpha compatibility surface without composing services.

Core owns application lifecycle; Library coordinates Catalog and storage;
Catalog owns metadata-aware persistence; metadata owns pure value semantics;
databases own generic persistence; and storage owns physical files and replicas.
Importing this root does not transfer those responsibilities or establish a
trusted write path.

See ``dev-docs/04 - Seperation of Concerns.md`` and
``dev-docs/catalog-write-validation-contract.md`` for the canonical boundaries.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/surfaces/test_surface_read_errors.py
"""
