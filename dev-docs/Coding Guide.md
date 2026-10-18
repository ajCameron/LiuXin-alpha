
This is guidance. Not commandments. If there's a good reason to stray from this, do so.
But please keep the reason in mind.

Likewise, every virtue here can be over done.

# Explicit is better than implicit

Go for names which _exactly_ describe what the object is.
There is a space for whimsy, and this isn't it.

# No shims

The instinct towards compatibility shins is an understandable one.
During necessary refactoring, a lot of things can break and it is understandable that you might want to focus on the
task at hand rather than cleaning up a bunch of other references.
However, this bloat technical debt and confuses the intetrface - eventually making the code base much harder to
maintain.

As such _avoid compatibility shims_.

If shims must be inserted temporarily for testing, remove and refactor the affected code at the end.
