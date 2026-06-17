
"""
Catalog macros are macros responsible for changing metadata.

Generic methods below in the database macros.

Module structure
 - every individual main table gets its own macros mixin
 - ancillary metadata tables get link tables macros
    (e.g. "cm_tags_x_links" exists. But "cm_work_x_links" do not)
 - the main WEMI stack (and agents) do not. Things link to them. They do not link.

"""


class CatalogMacros:
    """
    The catalog macros.
    """