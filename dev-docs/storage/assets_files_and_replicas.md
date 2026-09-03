
This document serves as an explanation of terms.

# Top level

LiuXin is, at core, a file manager.
It's fundamentally a means of storing, retrieving, transforming and transmitting files.
However, then things get complicated.

# WEMI

The Works, Expressions, Manifestation, Item stack is the core of the metadata representation of the stack.

At the bottom of the stack is the Item.
This represents physical things which may exist on the system.

Items are linked to assets. 
It's at this point that we link over to actual objects.

# Assets

The top level of the physical things stack. An asset can be a number of things.
It can be
 - an epub file
 - an audiobook
 - a text file
 - a cbr comic

e.t.c.

HOWEVER. This leads to another problem.
Some of these assets are composed of multiple files.

So we have

# Digital Assets

These are a SINGLE files.
E.g.
 - epub
 - txt

 e.t.c.
In any case, these are the smallest physical assets.
This container contains a SINGLE file.

# Composite Digital Assets

These are digital assets made of MANY files
E.g.
 - audiobooks (often composed of many chapter mp3s)

# Files

Both composite digital assets and digital assets are composed of files.
Either one, or many.
Files have replicas. Which is the bottom of the files metadata treee.
These are actual files which exist on disc.










