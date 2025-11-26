from __future__ import print_function

# Todo: Standardize how you deal with extensions
# What it says on the tin really - extensions which LiuXin considers to be books.

# Some books come in the form of .rar files.
# Collection of html pages with associated images.
# However, some files also come packaged for distribution in this way.
# These are the files I'd expect to find in a book which is a page of .html

# .midi - because webpages can include sounds - and these tend to be .midi files
# Broadly a collection of everything which can be included in a webpage


# Files for immediate inclusion in the database

# Files which might want to be included, but probably need more work
# (by default the Grey List is everything which is not black or white. This is just a start).

# Black list - files that will definitely not be included


BOOK_EXTENSIONS = [
    "lrf",
    "rar",
    "zip",
    "rtf",
    "lit",
    "txt",
    "txtz",
    "text",
    "htm",
    "xhtm",
    "html",
    "htmlz",
    "xhtml",
    "pdf",
    "pdb",
    "updb",
    "pdr",
    "prc",
    "mobi",
    "azw",
    "doc",
    "epub",
    "fb2",
    "djv",
    "djvu",
    "lrx",
    "cbr",
    "cbz",
    "cbc",
    "oebzip",
    "rb",
    "imp",
    "odt",
    "chm",
    "tpz",
    "azw1",
    "pml",
    "pmlz",
    "mbp",
    "tan",
    "tif",
    "snb",
    "xps",
    "oxps",
    "azw4",
    "book",
    "zbf",
    "pobi",
    "docx",
    "docm",
    "md",
    "textile",
    "markdown",
    "ibook",
    "ibooks",
    "iba",
    "azw3",
    "ps",
    "kepub",
]

BOOK_EXTENSIONS_DOTTED = []
for be in BOOK_EXTENSIONS:
    BOOK_EXTENSIONS_DOTTED.append(f".{be}")


RAR_BOOK_FILE_CONTENTS = [
    "html",
    "jpg",
    "opf",
    "css",
    "ncx",
    "png",
    "jpeg",
    "htm",
    "gif",
    "midi",
    "js",
]

RAR_BOOK_FILE_CONTENTS_DOTTED = ["." + rbfe for rbfe in RAR_BOOK_FILE_CONTENTS]

COMPRESSED_FILE_EXTENSIONS = [
    "7z",
    "arc",
    "bz2",
    "tar",
    "gz",
    "pea",
    "balz",
    "wim",
    "xz",
    "zip",
    "iso",
    "sit",
    "rar",
    "zip",
]

COMPRESSED_FILE_EXTENSIONS_DOTTED = ["." + cfe for cfe in COMPRESSED_FILE_EXTENSIONS]


IMAGE_EXTENSIONS = [
    ".jpg",
    ".jpeg",
    ".jpe",
    ".jif",
    ".jfif",
    ".jfi",
    ".jp2",
    ".j2k",
    ".jpf",
    ".jpx",
    ".jpm",
    ".mj2",
    ".jxr",
    ".hdp",
    ".wdp",
    ".gif",
    ".png",
    ".apng",
    ".mng",
    ".tiff",
    ".tif",
    ".svg",
    ".svgz",
    ".pdf",
    ".bmp",
    ".dib",
]


IMAGE_EXTENSIONS_DOTTED = ["." + ie for ie in IMAGE_EXTENSIONS]

WHITE_LIST = [
    "lrf",
    "rar",
    "zip",
    "rtf",
    "lit",
    "txt",
    "txtz",
    "text",
    "htm",
    "xhtm",
    "html",
    "htmlz",
    "xhtml",
    "pdf",
    "pdb",
    "updb",
    "pdr",
    "prc",
    "mobi",
    "azw",
    "doc",
    "epub",
    "fb2",
    "djv",
    "djvu",
    "lrx",
    "cbr",
    "cbz",
    "cbc",
    "oebzip",
    "rb",
    "imp",
    "odt",
    "chm",
    "tpz",
    "azw1",
    "pml",
    "pmlz",
    "mbp",
    "tan",
    "tif",
    "snb",
    "xps",
    "oxps",
    "azw4",
    "book",
    "zbf",
    "pobi",
    "docx",
    "docm",
    "md",
    "textile",
    "markdown",
    "ibook",
    "ibooks",
    "iba",
    "azw3",
    "ps",
    "kepub",
    "jpeg",
    "jpg",
    "tif",
    "tiff",
    "png",
]

WHITE_LIST_DOTTED = ["." + wl for wl in WHITE_LIST]

GREY_LIST_DOTTED = [
    ".rar",
    ".7z",
    ".arc",
    ".bz2",
    ".tar",
    ".gz",
    ".pea",
    ".balz",
    ".wim",
    ".xz",
    ".zip",
    ".iso",
]

BLACK_LIST_DOTTED = [
    ".exe",
    ".msi",
    ".py",
    ".pyc",
    ".in",
    ".db",
    ".class",
    ".java",
    ".csk",
    ".cvs",
    ".dbf",
    ".hqx",
    ".mac",
    ".mdb",
    ".mtb",
    ".mtw",
    ".qxd",
    ".xls",
]

METADATA_FILES_DOTTED = [".jpeg", ".jpg", ".opf", ".png", ".gif"]
