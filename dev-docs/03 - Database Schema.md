
As it transpires, as I'm sure will be familiar to anyone who has engaged in any archival work at all, designing a 
database to comprehensively store metadata is very hard.

LiuXin has taken inspiration from Functional Requirements for Bibliographic Records (FRBR).

As such, LiuXin includes the following top level tables.

Work – “distinct intellectual or artistic creation”
Expression – “intellectual or artistic realization of a work”
Manifestation – “physical embodiment of an expression”
Item (exemplaire [copy]) – “single exemplar of a manifestation”

# Top Level Tables























There are a number of link tables - the reason for the existence of some of them is obvious. Some of them less so.

########################################################################################################################

book_cover_links

Covers from the covers table can be linked to a book - it's assumed that this is the cover for the work in question. Any
number of covers can be linked to the same work - but the priority of that entry must be unique
CONSTRAINTS
- cover_title_link_cover_id & cover_title_link_title_id must be a unique combination (cover can only be assigned to a
- title once and only once)
- as must cover_title_link_title_id & cover_title_link_priority - no two covers linked to the same title can have the
  same priority.
COLUMNS
 - priority - Used to determine the display cover for a book - the one with the highest priority is displayed
 - type - from_file, from_web - where the file comes from

book-file_links

Files can be moved around the FolderStores - they don't need to stay in the books that they where originally entered
into. So some way of finding all the files associated with a book is needed - this link table links every file to one
and only one book.
CONSTRAINTS
- the combination of book_file_link_book_id & book_file_link_file_id must be unique (no file is assigned to the same
  book twice)
- book_file_link_file_id must be unique (each file is assigned to a maximum of one book)
- Todo: priority for a file in a book must be unique (if the priority is not None)
COLUMNS
 - priority - Used to provide a global order for the files and formats

book-folder_links

Books are also linked to folders - which are the default places to put new formats when they are added to a book (so
you can link a single folder to every book in a series and have any new formats added to any of those books turn up in
the series folder instead)
CONSTRAINTS
- the combination of book_folder_link_book_id & book_folder_link_folder_id must be unique (no folder is assigned to the
  same book twice)
COLUMNS
 - priority - Used to order the folders linked to a book - tells you where to preferentially put new formats when
              they're added to the book. Also provides an ordering for the book folders when they're displayed


########################################################################################################################

Comments will be eventually renamed to reviews

comment_creator_links

Reviews for a creator
CONSTRAINTS
 - comment_creator_link_comment_id must be unique - a comment can be assigned to one and only one creator
COLUMNS
 - priority - Provides a good ordering of the comments linked to a creator
 - type - Where did the review come from?

comment_series_links

Reviews for a series
CONSTRAINTS
 - comment_series_link_comment_id must be unique - a comment can be assigned to one and only one series
COLUMNS
 - priority - Provides a good ordering of the comments linked to a series
 - type - Where did the review come from?

comment_title_links

Reviews for a title
CONSTRAINTS
 - comment_title_link_comment_id must be unique - a comment can be assigned to one and only one title
COLUMNS
 - priority - Provides a good ordering for the comments linked to a title
 - type - Where did the review come from?

########################################################################################################################

cover_creator_links

Covers can also be assigned to creators - in this case it might be an image of the creator. Any number of covers can be
assigned to the same creator - but the priority of those covers must be unique
CONSTRAINTS
 - cover_creator_link_cover_id & cover_creator_link_creator_id must be a unique combination
 - cover_creator_link_creator_id & cover_creator_link_priority - ensures that two covers with the same priority are not
   applied to the same creator
COLUMNS
 - priority

cover_series_links

Covers can also be applied to series as well. Because why not? Same restrictions apply.
CONSTRAINTS
 - cover_series_link_series_id & cover_series_link_cover_id must be a unique combination
 - cover_series_link_series_id & cover_series_link_priority must be a unique combination
COLUMNS
 - priority

########################################################################################################################

# Todo: Merge
creator_folder_links

Stores the relationship between the creators table and the folders table - creators can be linked to folders which is
where new books by them will be created by default.
CONSTRAINTS
- creator_folder_link_creator_id & creator_folder_link_folder_id must be a unique combination
COLUMNS
 - priority - Order of the creators linked to the folder. Used to determine the creator folder to be returned.

creator-folder_links

Creators, and combinations of creators (i.e. books with multiple authors) can be assigned to folders. By default books
by these creators are placed inside their corresponding folders.
A single creator can have multiple folders in different folder_stores (but, ideally, not multiple folders in the same
folder store)
CONSTRAINTS
- creator_folder_link_creator_id & creator_folder_link_folder_id must be unique (folders can be assigned to multiple
  creators, but not the same creator twice)
- creator_folder_link_folder_id & creator_folder_link_priority must be unique - folders are linked to a creator with
  unique priority
COLUMNS
 - priority - Used to find the folder with the highest priority link to the creator

creator-language_links

Creators can be linked with a language - to indicate the language that they work in most often.
There is no need for additional details.
CONSTRAINTS
- creator_language_link_creator_id must be unique - Each creator can work in one and only one language
COLUMNS
 - None - No additional columns are needed to parametrize the link

creator_note_links

Creators can be tagged with a note. Can include information such as their bibliography or their biography.
CONSTRAINTS
 - creator_note_link_creator_id & creator_note_link_note_id combination must be unique (the same note can be applied
   once and only once the same creator)
 - creator_note_link_note_id - Each note can be applied to one, and only one, creator
COLUMNS
 - priority - Provides a strict ordering of the notes assigned to a creator
 - type - There are different types of note - bio, e.t.c

creator_series_links

One and only one creator can be linked to a series as the principle creator of that series - this serves to uniquely id
the series - the name and the principle author combination should be unique
CONSTRAINTS
 - creator_series_link_creator_id & creator_series_link_series_id must be a unique combination
 - creator_series_link_series_id must be unique - each series can have one and only one designated creator
COLUMNS
 - None

creator_tag_links

Tags can be applied to a creator which describe them.
CONSTRAINTS
 - creator_tag_link_creator_id & creator_tag_link_tag_id must be a unique combination
COLUMNS
 - None

creator_title_links

The creators of a work - the type informs what role the creator played in the work, for example 'author', 'editor' e.t.c
CONSTRAINTS
 - creator_title_link_creator_id & creator_title_link_title_id must be a unique combination
 - creator_title_link_title_id & creator_title_link_priority must be unique - every
   creator attached to a work MUST have a defined priority.
COLUMNS
 - priority - Order of the creators in the title
 - type - The role the creator played in the work

########################################################################################################################

device_file_links

Notes if a file is present on a given device or not.
CONSTRAINTS
 - device_file_link_file_id & device_file_link_device_id must be a unique combination - the file can either be on the
   device or not.
COLUMNS
 - None

device_note_links

Any device recognized by the devices table can have a note applied to it. One and only one note.
CONSTRAINTS
 - device_note_link_device_id & device_note_link_note_id must be a unique combination
 - device_note_link_note_id - A note can be applied once
 - device_note_link_device_id & device_note_link_priority - Must be a consistent ordering of priority for notes attached
   to a device
COLUMNS
 - Priority - Strict order for any notes assigned to the file

########################################################################################################################

file_folder_links

Files are physically organized into folders on the file system. This indicated where the database believes the file
should be - which allows it to be indexed and retrieved.
CONSTRAINTS
- file_folder_link_file_id & file_folder_link_folder_id must be a unique combination - the file can be in one and only
  one folder at the same time.
COLUMNS
 - None - Just an indicator of which file is in which folder

file_identifier_links

# Todo: Pull the type column from the identifiers table
This is separate from the identifier_title_links because it's possible that there are different versions of the book
present in the files. This marks then with their unique identifiers - thus allowing them to be distinguished.
CONSTRAINTS
- file_identifier_link_identifier_id & file_identifier_link_file_id must be different
COLUMNS
 - type - The type of identifier
 - priority - Used to delineate a primary id for a column (for compatibility)

file_language_links

The language of a file - should be the primary language of a file.
CONSTRAINTS
- file_language_link_file_id & file_language_link_language_id must be a unique combination
COLUMNS
 - None

file_publisher_links

The publisher of a file. The people responsible for actually putting it out the door.
CONSTRAINTS
- file_publisher_link_file_id & file_publisher_link_publisher_id - the file can have one and only one publisher
COLUMNS
 - None

########################################################################################################################

folder_series_links

Folders can be assigned to series - used to represent the series structure on disk.
CONSTRAINTS
 - folder_series_link_folder_id & folder_series_link_series_id - folder can only be linked to a series once
COLUMNS
 - priority - Used to set the order in which series linked to the folder should be retrieved

Todo: Why can;t you put things in alpahbetical order?

series_folder_links

Series can also be associated with folders - this is where books in that series are placed by default.
The usual structure of a folder tree is creators - series tree folders - books
Each series can be associated with multiple folders - they might be in the folders associated with different authors,
for example
A folder can also be linked to multiple series - which might happen if you'd decided to merge the physical location of
two series while retaining their separation on the database.
CONSTRAINTS
- series_folder_link_series_id & series_folder_link_folder_id - One folder cannot be linked to a series more than once.
  It confuses the system.
COLUMNS
 - priority -

########################################################################################################################

folder_store_note_links

Allows you to apply a note to a folder_store.
CONSTRAINTS
- folder_store_note_link_folder_store_id & folder_store_note_link_note_id must be a unique combination - each folder
  store can have one and only one note applied to it.
- folder_store_note_link_note_id - A note can be applied once to a folder_store (and, ideally, universally, but that
  is hard to check in SQL).
COLUMNS
 - priority - Used to order the notes assigned to the folder_store

########################################################################################################################

genre_series_links

The genre a series falls into.
CONSTRAINTS
- genre_series_link_genre_id & genre_series_link_series_id combination must be unique - apply the same genre to a series
  once and only once
COLUMNS
 - priority - Display order for the genre of the series

genre_title_links

Links titles to the genre they fall under.
CONSTRAINTS
- genre_title_link_genre_id & genre_title_link_title_id combination must be unique - apply the same genre to a title
  once and only once
COLUMNS
 - priority - Used to order the genres assigned to the title

########################################################################################################################

identifier_title_links

Links which indicate identifiers for the title - the type of identifier is stores over in the identifiers table.
CONSTRAINTS
- identifier_title_link_identifier_id & identifier_title_link_title_id must be different
- identifier_title_link_identifier_id must be unique - identifiers can only be linked to a single title
COLUMNS
 - priority - Need for compatibility
 - type - The type of identifier linked to the title

########################################################################################################################

language_title_links

The language that the title was originally published in (
CONSTRAINTS
 - language_title_link_language_id & language_title_link_title_id & language_title_link_type must be unique -
   apply the same language to the title in the same way once and only once
COLUMNS
 - type - The way in which the language is involved with the title

########################################################################################################################

note_publisher_links

Make a note about a publisher.
CONSTRAINTS
- note_publisher_link_note_id & note_publisher_link_publisher_id - the same note cannot be applied to the same publisher
  twice
- note_publisher_link_note_id - A note can be applied once and only once (also accomplishes the above)
- note_publisher_link_publisher_id & note_publisher_link_priority - Any notes applied to a given publisher must have a
  unique priority.
COLUMNS
 - priority - Gives an ordering for the notes associated with a publisher
 - type - What does the note represent?

note_series_links

Make a note about a series - could be a synopsis of it - something like that.
CONSTRAINTS
- note_series_link_note_id & note_series_link_series_id - note is applied to a series once and only once
- note_series_link_note_id - Each note is applied once and only once.
- note_series_link_series_id & note_series_link_priority - the priority of a note applied to a given series must be
  unique
COLUMNS
 - priority - Gives an ordering for the notes associated with the series
 - type - What does the note represent?

note_title_links

Make a note about a title - could be a synopsis, or a general note
CONSTRAINTS
- note_title_link_note_id & note_title_link_title_id - note is applied to a title once and only once
- note_title_link_note_id - Each note is applied once and only once
- note_title_link_title_id & note_title_link_priority - the priority of a note applied to a given title must be unique
COLUMNS
 - priority - Gives an ordering for the notes associated with the title
 - type - What does the note represent?

########################################################################################################################

publisher_title_links

Which publishers where responsible for getting the title into print?
Priority is not especially important - thus there are no restrictions placed on the priority column (and it might not
even exist)
CONSTRAINTS
- publisher_title_link_title_id & publisher_title_link_publisher_id - publisher is to be applied once and only once to
- each title
COLUMNS
 - priority - Orders the title associated with the publisher


########################################################################################################################

rating_title_links

Ratings for the title - can include multiple different ratings from multiple different sources.
The ratings table comes pre-filled - what matters is the combination of the rating source and the title_id
CONSTRAINTS
 - rating_title_link_title_id & rating_title_link_type - the combination must be unique (so you can have up to one
   rating of each type associated with a given title record)
COLUMNS
 - type - Priority is not needed - as each type of rating should appear once and only once.

########################################################################################################################

series_synopsis_links
Todo: The three types of comment should be note, review and synopsis - changed comment to be review
Todo: Make synopsis just another type of note
Series can have synopsis assigned to them
CONSTRAINTS
- series_synopsis_link_synopsis_id - synopsis can be linked to one and only one title
COLUMNS
 - priority - used to order the synopsis linked to the series

series_tag_links
Series can have tags applied to them - said tag is then used when searching for any books in the series
CONSTRAINTS
- series_tag_link_series_id & series_tag_link_tag_id - the same tag can't be applied to a series more than once
COLUMNS
 - None - None are needed

series_title_links

Titles can be in series. Did not see this coming.
CONSTRAINTS
 - title_series_link_title_id & title_series_link_series_id - a title cannot appear in the same series twice. Even in
   different positions.
 - title_series_link_title_id & title_series_link_priority - Each title must have a well defined priority order.
COLUMNS
 - priority - Used to order the series linked to the title
 - index - The position of the title in the series

########################################################################################################################

subject_title_links

titles can be linked into the subject tree - they can be linked in in multiple places (as a book may have many different
topics).
CONSTRAINTS
- subject_title_link_title_id & subject_title_link_subject_id - Each title can be linked to a subject a maximum of once
COLUMNS
 - priority - Used to order the subjects in the title


########################################################################################################################

synopsis_title_links
Todo: Synopsis is not just another kind of note - split down into the three note, review and synopsis
Titles can have synopsis. Links titles and synopsis
CONSTRAINTS
 - synopsis_title_link_synopsis_id - synopsis can be linked to one and only one title
COLUMNS
 - priority - Orders the synopsis linked to the title


########################################################################################################################

tag_title_links

Tags can be applied to titles.
CONSTRAINTS
- tag_title_link_tag_id & tag_title_link_title_id - the same tag cannot be applied more than once to the same title
COLUMNS
 - None - None are needed
