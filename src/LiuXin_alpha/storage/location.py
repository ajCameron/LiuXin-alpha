
# Todo: KILL THIS. Break and then fix.


"""
Retain the legacy empty Location class used by older storage/cache imports.

This placeholder provides neither path operations nor Store routing. The public
storage API defines a separate Location value carrying a Store UUID and key.
"""



class Location:
    """
    Preserve an instantiable legacy type without implementing location behavior.

    Instances have ordinary object identity and no declared fields, validation, path protocol, or
    I/O methods. This is not an alias or base class for the current storage.api.Location value.

    Example:
        >>> isinstance(Location(), Location)
        True
    """
