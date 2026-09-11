"""
Group state contracts and responsibility-specific owners for the public terminal browser.

Session, command routing, completion, data access, and presentation implementations
live in explicit child modules. This initializer imports no owners or public
browser facade, keeping composition out of the shared package boundary.
"""
