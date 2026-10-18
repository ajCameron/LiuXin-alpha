
# What is LiuXin?

Archival grade calibre.

calibre is a great tool.
It's got a ton of great features, runs on many places, has a very active user base and is heavily used.
It's close to the gold standard for ebook software.

HOWEVER.

It's not intended for archival work.
This gets increasingly obvious as the data sets grow very large.

As such, what I wanted was
 - calibre for large data sets (tens of terrabytes)
 - with needed efficiency improvements
 - and more storage backends
 - with greater modularity for conversion options
 - some capability to handle arbitrary files and audiobooks (as part of the archival effort)
 - better support for incorporating and using external tools

I also wanted something to do the "secure" and "protect" part of archival work.
Such as
 - manage tape backup
 - track file versions
 - tracks and validate backups
 - pack files for distribution into convenient forms

Finally, one of the hardest asks
 - have (most) calibre plugins work

As happens, I couldn't find something which did all of this in a way I wanted, so I started rolling my own.

Due to its origins and objectives, this project shares a LOT of common code with calibre.
If nothing else, this is needed to ensure as much compatibility as possible.

# Philosophy

## NO deletion

At a philosophical level, my belief is that ORIGINAL FILES SHOULD NEVER BE DELETED.
Derived files can be deleted at will, but the original file should never be removed.
There is, in fact, a special flag for original files which must be retained.

## Clarity

As far as possible every aspect of the project - from the code to the output - should be clear.
If there's a choice between a concise name and a clear name, choose the clear one.

## Rich Metadata

LiuXin, under the hood, uses the WEMI stack.
It might present more limited forms of metadata in service of clarity, or calibre compatibility, but the rich metadata
will always be stored and retained. Even if it's being temporarily hidden.

## Robustness

Add logging. Add fallbacks. Add robustness to the code.

The aim here is "minimal functionality in a wide range of environments".
By all means, let's have better and more efficient specialized modules.
But let's have fallbacks as well.
(Or, at the least, decent logging for when everything explodes).