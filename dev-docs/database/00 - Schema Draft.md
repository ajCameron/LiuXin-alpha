# Main Tables

Top level entities within the LiuXin database.

For ADDED CLARITY (tm) every column is prepended with the name of the table it’s in. The aim is to cut down on writes to the wrong table (as several tables will, for example, have a “type” column).

Work

E.g. 

“The Left Hand of Darkness” as a novel, independent of language, edition, or format.

This table represents any creative endevour that could be tracked by this system (because specilization is for insects and people who like compact schema).

Columns include
work_canonical_title - the string the work should be known as
work_sort_title - used to sort the works table for display
work_sort_creator_str - some kinda creator display string
work_original_year - foreign key out to the years table?
work_original_languae - if known. Nullable.

“work_type” is indicated by the “work__work_type_links” - see below 

Expression

A form the work can take.

E.g.

“Left Hand of Darkness, English text as finalized in 1969.”
“Left Hand of Darkness, French translation by X, 1974.”

This table represents expressions of the platonic form. Things which have been written down, or recorded (or, possibly, filmed in the case of an adaption).

This table has a many-to-one link to “work” represented by the “expression__work_links” many to one link table (see below).

Columns include
expression_type - foreign key to the expression_type table - 'text', 'translation', 'revised_text', 'director_cut'
expression_label - some human readable text - "1st English text", "French translation by X"


Manifestation

Manifestations into the physical realm are a particular instance of a piece of text.

E.g.
“2010 Gollancz paperback, ISBN X.”
“2019 Tor ebooks release (EPUB+MOBI).”

This table represents actual manifestations of Expressions - instances and revisions of the text in a form you can actually read.

When you edit, change or correct a text you create a new manifestation.

Columns include
manifestation_expression_id - Foreign key to the manifestation table











Todo: We’re gonna need different UIs for different metadata modes arn’t we?





Stores



Stores can be given a mask of things they can hold, and things they can’t.
These can be types of work, or genres, tags, e.t.c.


