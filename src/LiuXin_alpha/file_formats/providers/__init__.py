"""
Capability contracts for file-format providers.

A provider is a class that does one kind of work on a file format, such as
importing a format into an intermediate book or converting one format straight
to another. Each kind of work is a capability, and each capability has one
base class in this package. A provider subclasses the capabilities it offers;
one class may offer several.

Importing this package or any module in it is passive: nothing is discovered,
registered or instantiated, and no format implementation is imported. The
legacy adapters import the plugins only when asked to register them.

Import names from the module that defines them:

- :mod:`.base`: the shared provider record, availability and call context;
- :mod:`.stages`: the fixed stages that transforms run in;
- :mod:`.conversion`: the conversion capabilities;
- :mod:`.registry`: a registry that files provider classes by capability;
- :mod:`.legacy_conversion`: adapters that run the bundled conversion plugins
  as importers and exporters.
"""

from __future__ import annotations
