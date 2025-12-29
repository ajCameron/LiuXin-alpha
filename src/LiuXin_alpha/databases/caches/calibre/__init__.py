# tables - represent the data stored in a given table on the database.
#          If there are multiple fields for multiple columns in this table then each of them should have their own
#          table instance - pointed to the desired column
#          Has methods to update the cache data stored internally and also update the database - which might require
#          some of the data so stored.

# fields - Represent fields on the view of the database - individual columns on the table.
#          If there are multiple fields corresponding to multiple columns in a single table of the database then there
#          should be multiple fields - each loaded with a table instance appropriate for the column
#          NOTE - the type of field,. in certain circumstances, NEED NOT BE the same as the type of the table. A
#          mismatch in type means that the field is presenting another type of table to the user
#          E.G. If it's a OneToOneField backed with a OneToManyTable will present as a OneToOneField - while actually
#          not being.
#          Sensible behavior for updates e.t.c will be stored here in the field - which is responsible for updating the
#          table in a reasonable manner
#          This is why the update_db methods are stored in the tables - then called from the fields. It would make more
#          sense from a data model prospective to have the updates in the fields - the things responsible for displaying
#          the data - and you can override the update behavior there - but it's a little more convenient if most of the
#          heavy lifting is done in the backend tables.

# cache - Brings everything together. The cache stores all the fields and tables and is responsible for keeping them
#         updated and in sync
