__license__ = "GPL v3"
__copyright__ = "2008, Marshall T. Vandegrift <llasram@gmail.com>"

# Todo: Somewhere in this package is a font management library which is currently completely borken - find and fix

# See https://wiki.mobileread.com/wiki/OEB for a primer on the OEB file format
# Also see http://www.idpf.org/epub/20/spec/OPF_2.0.1_draft.htm

# From there
# Basic structure is a zipped together collection files. Including one or more html files and an OPF (Open
# eBook Package File). THe OPF file will contain a list of the files in the build and the metadata for the file contents
# (html files sometimes have an odf extension - as they might not be pure html files)
# Other sections might be included in the
# Manifest - All the files that should be included in the book while it's being built
# Guide - Controls the order of entries in the book - a list of orders in which elements should appear in the book.
#         A set of references to fundamental structural features of the publication, such as table of contents,
#         foreword, bibliography, etc.
#         Guide often includes the following items.
#         toc - If the toc type of subitem is present the Table of Contents icon in the Reader links to the position
#               defined in the href or onclick attribute
#         start - If this type of subitem is present the action defined in the onclick is preformed whenever the ebook
#                 is opened
# Spine - An arrangement of documents providing a linear reading order.
