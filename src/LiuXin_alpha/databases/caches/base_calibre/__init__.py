# Todo: Proposed restructure
#       Expose all these classes through customize - but keep them there if you don't want all this code in customize
#       That way the cache code is stores in cache but the interface is in customize - which seems to be how it should
#       be.


"""
Base for caches which descend from the original calibre cache - though often with some-to-heavy modification.

The interface is defined in customize - but there ended up being enough code that it needed its own file.
Hence, here - where you can find the base classes for the cache and all it's components.
"""
