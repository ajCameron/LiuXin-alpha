"""
Reserve the horror leaf-map module path.

This module currently defines no mapping or classifier. Importing it supplies only
the module namespace; horror patterns used elsewhere are maintained in their owning
modules.

Example:
    >>> from LiuXin_alpha.metadata.constants.genre_maps import horror_genres
    >>> [name for name in vars(horror_genres) if not name.startswith('_')]
    []
"""
