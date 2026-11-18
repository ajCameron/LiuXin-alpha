
# Todo: Proposed restructure
#       Expose all these classes through customize - but keep them there if you don't want all this code in customize
#       That way the cache code is stores in cache but the interface is in customize - which seems to be how it should
#       be.


"""
Expose the supported base calibre compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/library/test_unified_library.py
"""
