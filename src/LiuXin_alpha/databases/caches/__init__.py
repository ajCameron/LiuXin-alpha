
"""
Performance cache for data from the library database.
"""

from __future__ import unicode_literals, division, absolute_import, print_function

# The individual caches are broken down into the following components
# - cache - The main API - responsible for being thread safe and providing a unified interface to everything
# - fields - Provide access apis to the data stored in the tables. A single table can have many fields - for example
#          - titles dor books, which have many
# - tables - represent the tables on the database. Cache data is stored here and they are responsible for updating the
#          - database after changes to it
