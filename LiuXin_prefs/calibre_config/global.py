# calibre wide preferences

### Begin group: DEFAULT
 
# database path
# Path to the database in which books are stored
database_path = 'C:\\Users\\Thane-Winterscale/library1.db'
 
# filename pattern
# Pattern to guess metadata from filenames
filename_pattern = '(?P<title>.+) - (?P<author>[^_]+)'
 
# isbndb com key
# Access key for isbndb.com
isbndb_com_key = ''
 
# network timeout
# Default timeout for network operations (seconds)
network_timeout = 5
 
# library path
# Path to directory in which your library of books is stored
library_path = None
 
# language
# The language in which to display the user interface
language = None
 
# output format
# The default output format for ebook conversions.
output_format = 'EPUB'
 
# input format order
# Ordered list of formats to prefer for input.
input_format_order = cPickle.loads(b'\x80\x05\x95n\x00\x00\x00\x00\x00\x00\x00]\x94(\x8c\x04EPUB\x94\x8c\x04AZW3\x94\x8c\x04MOBI\x94\x8c\x03LIT\x94\x8c\x03PRC\x94\x8c\x03FB2\x94\x8c\x04HTML\x94\x8c\x03HTM\x94\x8c\x04XHTM\x94\x8c\x05SHTML\x94\x8c\x05XHTML\x94\x8c\x03ZIP\x94\x8c\x03ODT\x94\x8c\x03RTF\x94\x8c\x03PDF\x94\x8c\x03TXT\x94e.')
 
# read file metadata
# Read metadata from files
read_file_metadata = True
 
# worker process priority
# The priority of worker processes. A higher priority means they run faster and consume more resources. Most tasks like conversion/news download/adding books/etc. are affected by this setting.
worker_process_priority = 'normal'
 
# swap author names
# Swap author first and last names when reading metadata
swap_author_names = False
 
# add formats to existing
# Add new formats to existing book records
add_formats_to_existing = False
 
# check for dupes on ctl
# Check for duplicates when copying to another library
check_for_dupes_on_ctl = False
 
# installation uuid
# Installation UUID
installation_uuid = 'a0a76cee-4506-46ba-9590-eb37262227f5'
 
# new book tags
# Tags to apply to books added to the library
new_book_tags = cPickle.loads(b'\x80\x05]\x94.')
 
# mark new books
# Mark newly added books. The mark is a temporary mark that is automatically removed when calibre is restarted.
mark_new_books = False
 
# saved searches
# List of named saved searches
saved_searches = cPickle.loads(b'\x80\x05}\x94.')
 
# user categories
# User-created tag browser categories
user_categories = cPickle.loads(b'\x80\x05}\x94.')
 
# manage device metadata
# How and when calibre updates metadata on the device.
manage_device_metadata = 'manual'
 
# limit search columns
# When searching for text without using lookup prefixes, as for example, Red instead of title:Red, limit the columns searched to those named below.
limit_search_columns = False
 
# limit search columns to
# Choose columns to be searched when not using prefixes, as for example, when searching for Red instead of title:Red. Enter a list of search/lookup names separated by commas. Only takes effect if you set the option to limit search columns above.
limit_search_columns_to = cPickle.loads(b'\x80\x05\x953\x00\x00\x00\x00\x00\x00\x00]\x94(\x8c\x05title\x94\x8c\x07authors\x94\x8c\x04tags\x94\x8c\x06series\x94\x8c\tpublisher\x94e.')
 
# use primary find in search
# Characters typed in the search box will match their accented versions, based on the language you have chosen for the calibre interface. For example, in  English, searching for n will match ñ and n, but if your language is Spanish it will only match n. Note that this is much slower than a simple search on very large libraries.
use_primary_find_in_search = True
 
# migrated
# For Internal use. Don't modify.
migrated = False
 


