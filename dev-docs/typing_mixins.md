# Typing mixins with dependencies on their host class

**Architecture note for LiuXin**  
**Suggested approach:** small capability `Protocol`s, used as explicit `self` types.

## 1. The problem

Splitting a large class into mixins improves organisation, but a mixin may depend on methods or attributes supplied by *other* mixins or by the final class.

For example, `DriverWrapperViewMixin` implements view-related operations, but assumes its host provides:

- `get_tables(force_refresh=...)`
- `get_relation_type(name)`
- `driver`, whose implementation exposes `direct_get_view_column_headings(...)` and `direct_get_view_row_dict_from_id(...)`

These dependencies exist at runtime once the mixins are composed, but a type checker looking at `DriverWrapperViewMixin` alone cannot necessarily infer them.

The design objective is to **tell the type checker which capabilities a mixin requires without making that mixin inherit the whole concrete wrapper**.

## 2. Recommended solution: a host `Protocol`

A `Protocol` describes the *shape* of an object: the methods and attributes it must provide. A class need not inherit the protocol to satisfy it; it just needs compatible members.

The mixin can use the protocol as an explicit annotation for `self`. This is a documented mypy pattern for mixins.

### Define the required capabilities

```python
# driver_wrapper_protocols.py

from typing import Protocol


class SupportsViewDriver(Protocol):
    """The subset of the backend driver needed by view operations."""

    def direct_get_view_column_headings(self, view: str) -> list[str]: ...

    def direct_get_view_row_dict_from_id(
        self,
        view: str,
        row_id: int,
    ) -> dict[str, object]: ...


class SupportsRelationIntrospection(Protocol):
    """Methods for discovering relations in a database."""

    def get_tables(self, force_refresh: bool = False) -> list[str]: ...

    def get_relation_type(self, name: str) -> str | None: ...


class SupportsViewIntrospection(SupportsRelationIntrospection, Protocol):
    """Everything DriverWrapperViewMixin needs from its host."""

    @property
    def driver(self) -> SupportsViewDriver: ...
```

The return types here are *examples*: adjust them to the actual LiuXin driver contracts. In particular, the real row-ID type, column-heading type, and relation-type return value may differ.

The read-only protocol `@property` says the mixin *reads* `driver` but does not need to replace it. A normal concrete instance attribute can satisfy this requirement. This also avoids unnecessarily demanding a writable protocol attribute, which can create type-variance problems with concrete driver subclasses.

### Annotate `self` in the mixin

```python
# driver_wrapper_view_mixin.py

from .driver_wrapper_protocols import SupportsViewIntrospection


class DriverWrapperViewMixin:
    """View operations; requires the SupportsViewIntrospection host API."""

    def get_views(
        self: SupportsViewIntrospection,
        force_refresh: bool = False,
    ) -> list[str]:
        """Return names of relations that are views."""
        names = self.get_tables(force_refresh=force_refresh)
        return [
            name
            for name in names
            if self.get_relation_type(name) == "view"
        ]

    def is_view(
        self: SupportsViewIntrospection,
        name: str,
    ) -> bool:
        return self.get_relation_type(name) == "view"

    def get_view_column_headings(
        self: SupportsViewIntrospection,
        view: str,
    ) -> list[str]:
        return self.driver.direct_get_view_column_headings(view)

    def get_view_row_from_id(
        self: SupportsViewIntrospection,
        view: str,
        row_id: int,
    ) -> dict[str, object]:
        return self.driver.direct_get_view_row_dict_from_id(view, row_id)
```

Nothing here requires `DriverWrapperViewMixin` to inherit a concrete `DriverWrapper` class. It remains a real, reusable mixin; the protocol is a static description of its expected host.

### Compose the wrapper normally

```python
class DriverWrapper(
    DriverWrapperTableMixin,
    DriverWrapperRelationMixin,
    DriverWrapperViewMixin,
):
    def __init__(self, driver: ActualBackendDriver) -> None:
        self.driver = driver
```

Here, `DriverWrapperTableMixin`, `DriverWrapperRelationMixin`, and `ActualBackendDriver` are illustrative names: use the actual implementations. Together they must satisfy `SupportsViewIntrospection` with compatible signatures.

**Important limitation:** mypy's explicit-protocol-`self` pattern checks whether a host satisfies the requirement **when the mixin method is called**, not automatically at the point where mixins are combined into a class. It does not create runtime methods or enforce constructor-time validation.

## 3. Check that the final class really satisfies the protocol

To request an explicit static check during development, add a small check that is examined by the type checker but never needs to run:

```python
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    def _check_view_host(wrapper: DriverWrapper) -> None:
        required_host: SupportsViewIntrospection = wrapper
```

If the final `DriverWrapper` lacks a method, has an incompatible signature, or carries an incompatible driver, the assignment should trigger a type error. This complements call-site checks and catches a broken composition sooner.

You can also use a lightweight type-checking test file for such assertions rather than including them in production modules.

## 4. Organise protocols around capabilities, not entire classes

For LiuXin, a useful directory shape might be:

```text
store/
  driver_wrapper/
    __init__.py
    wrapper.py
    protocols.py
    mixins/
      tables.py
      relations.py
      views.py
      schema.py
      transactions.py
```

Possible capability names:

```text
SupportsDriver
SupportsTableIntrospection
SupportsRelationIntrospection
SupportsViewIntrospection
SupportsScriptExecution
SupportsDirtyQueue
```

These are *examples*, not a requirement to create all six. Prefer just the small contracts that real mixins need. Protocols can extend other protocols, as `SupportsViewIntrospection` extends `SupportsRelationIntrospection` above.

A useful rule: **a mixin should declare what it consumes, not every feature the eventual wrapper happens to expose.** A protocol that gradually comes to describe the whole wrapper has lost much of its value.

## 5. Design cautions

### A. Protocols are not runtime implementations

A protocol does not supply `get_tables()` or a `driver` attribute. The composed class must still provide them. Python's normal method resolution order (MRO) determines which concrete method runs.

### B. Prefer read-only requirements where possible

Use a `@property` in the protocol when the mixin only reads an attribute such as `driver`. Use a writable attribute requirement only if the mixin actually assigns to it. Writable protocol attributes are type-checked more strictly because their types are invariant.

### C. Avoid circular imports

Keep shared capability protocols in a lightweight module that does **not** import concrete wrapper or mixin implementations. Mixins may import protocols; the wrapper may import mixins and protocols.

### D. Do not paper over missing dependencies with `Any` or `cast`

Annotating everything as `Any`, or casting `self` merely to suppress diagnostics, removes the check you wanted. A small explicit host contract tells both the type checker and future maintainers what is required.

### E. Keep signatures faithful to the real code

Do not invent `str`, `int`, or `dict[str, object]` contracts merely for convenience. If `get_tables()` returns an iterable, a row ID accepts other types, or the backend returns a specialised row mapping, describe that accurately. Otherwise the protocol will either reject valid hosts or hide genuine type mismatches.

### F. Be alert to architectural coupling

If a mixin starts requiring many unrelated methods from many other mixins, the problem may be more than typing. Consider relocating that behaviour into a lower-level service or a collaborator passed to the wrapper.

## 6. Alternatives and when to use them

| Approach | Good for | Trade-off |
| --- | --- | --- |
| **Host `Protocol` + explicit `self`** | Mixins that rely on capabilities supplied elsewhere | Usually the best balance: low runtime coupling, clear static contract; compatibility is principally checked at call sites unless asserted separately |
| Abstract base class / abstract methods | A deliberately nominal interface with an enforced abstract contract | Introduces inheritance and MRO considerations; may be awkward for independently composed mixins |
| Constructor-injected collaborator | Large or heavily interdependent behaviours that can be separate objects | More explicit runtime structure, but a larger refactor |
| Suppress errors with `Any`, `cast`, or ignores | Temporary migration work | Loses the protection of static checking; avoid as the final design |

`typing.Self` alone is **not** a replacement for this pattern. `Self` represents the current concrete subclass for return types and related constraints; it does not declare the additional host capabilities that the mixin needs.

## 7. Practical rollout

1. Start with `DriverWrapperViewMixin` and identify every external member it uses (`get_tables`, `get_relation_type`, `driver`).
2. Create a minimal protocol for those dependencies.
3. Annotate `self` on the mixin methods that require the protocol.
4. Add a static host-conformance check for the final `DriverWrapper`.
5. Run your project's type checker (`mypy` and/or `pyright`) and adjust signatures to match reality.
6. Repeat for other mixins, extracting shared capability protocols where there is genuine overlap.
7. If protocol dependencies grow dramatically, revisit the class boundaries rather than building a single enormous protocol.

## 8. Recommendation

For the current LiuXin refactor, **use small host-capability protocols with explicit `self` annotations**. Keep `DriverWrapper` as the composition root and preserve normal multiple inheritance for now.

This documents each mixin's *dependencies*, avoids fake runtime methods, and provides a useful architectural warning when responsibilities become too entangled. Add explicit static conformance checks for your main composed wrappers, since a protocol-based `self` annotation does not necessarily flag an incomplete host at class-definition time.

## References

- [Mypy — Advanced uses of self-types: Mixin classes](https://mypy.readthedocs.io/en/stable/more_types.html#mixin-classes)
- [Python typing specification — Protocols](https://typing.python.org/en/latest/spec/protocol.html)
