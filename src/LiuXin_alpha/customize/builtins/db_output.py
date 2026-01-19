"""
Stores plugins for outputting resources from the database in various ways.

Allows you to easily select and override, for example, how HTML and metadata are generated from the database.
As well as generate selections of the database.
E.g. a class which produces a collection of every book for an author.
E.g. a class which produces a collection of every series in a universe.
E.g. a class which produces a collection of every book with a tag.

You could probably do all this in one class, but there might be other things you want to with outputs
(save to disk, compress, email, e.t.c).
Sufficient variety that it seemed to make sense to spin it off as a plugin.
"""


class DBOutputPlugin:
    """
    Base class for the DB output plugin - which takes an entry on the database and outputs something.
    """

    pass
