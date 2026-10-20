
# LiuXin style guide

Reviewed **2026-10-20**, working tree based on `fc5de7af`. This is the
maintained naming/documentation convention; [coding principles](<Coding Guide.md>)
and [quality gates](../architectural_reviews/maintainability-quality-gates.md)
own implementation guidance and exact enforcement scopes respectively.

Use the ``API`` suffix for API-facing contract classes. Existing naming
exceptions require caller-aware migration; this convention does not claim that
every inherited class already follows it.

## Naming public values

A conceptual thing and a passive Python value describing that thing are not
interchangeable. Reserve an unqualified domain name for the concept itself or
for an object that represents its behaviour. A frozen collection of
manager-maintained facts about a Digital Asset is therefore
``DigitalAssetRecord``, not ``DigitalAsset``; it is neither the byte sequence
nor a persistence adapter's database row.

Use a semantic suffix that tells the caller what kind of value they have:

| Suffix | Meaning |
| --- | --- |
| ``Declaration`` | Validated input or durable intent before a manager assigns identity |
| ``Record`` | Manager-maintained public facts with stable identity; never an exposed ORM row |
| ``Reference`` | A pinned or otherwise explicit reference to another thing |
| ``Observation`` | Evidence obtained by inspecting external or physical state |
| ``Resolution`` | The records, route, or choice selected for a request |
| ``Assessment`` | Calculated interpretation of current facts and policy |
| ``Plan`` | Proposed work that has not yet been applied |
| ``Report`` | Detailed account of completed or inspected work |
| ``Result`` | Direct operational return where the more specific suffixes do not apply |
| ``Configuration`` | Durable intended setup |
| ``Status`` | Dynamic operational condition at a point in time |
| ``Registration`` | Facts created by registering another object with LiuXin |

Conventional narrow values such as ``Location``, ``Digest``, ``FileInfo``, and
``StoreCapabilities`` may keep their established names when those names
already state the value's role precisely. Avoid vague suffixes such as
``Data``, ``Model``, ``DTO``, ``Thing``, and ``Spec``. At a broad public export
boundary, prefer the complete domain term—``CompositeDigitalAssetMembership``
rather than a context-dependent ``CompositeAssetMembership``, and
``DigitalAssetDerivationRecord`` rather than ``AssetDerivationRecord``.

Methods at a broad manager boundary should likewise state when they return
passive records: for example, ``get_digital_asset_record()`` and
``iter_replica_records()``. A narrowly typed repository may retain conventional
``add()``, ``get()``, and ``remove()`` names because its owning protocol and
return annotation already provide that context.

## Python docstrings

The following docstring form is the project-wide target, not a convention
specific to storage. New or substantially edited Python modules, classes, and
functions should use it. Existing packages should adopt it as they are audited;
do not manufacture empty prose or misleading examples merely to make a bulk
check pass.

- For multiline docstrings, put opening and closing triple quotes on their own lines.
- Begin with a concise summary, then a blank line before further prose. Explain
  ownership, side effects, validation and error limits when they matter.
- Give classes and functions a useful ``Example:`` section. Prefer runnable
  doctests where practical; mark illustrative external-object examples with
  ``# doctest: +SKIP`` rather than pretending they run standalone.
- Separate prose/examples from the reST field list with two blank lines.
- List explicit parameters in declaration order as ``:param name:``; omit the
  implicit ``self``/``cls`` receiver. ``*args`` and ``**kwargs`` use their names
  without stars. A bare ``*`` is a signature separator, not a parameter field.
- Follow the parameter fields directly with ``:return:``, including functions
  returning ``None``; keep additional meaningful fields such as ``:raises:``
  afterward. Type annotations remain the authoritative type declarations.
- Describe parameters and results meaningfully, including units, sentinels and
  effects. Empty fields are unfinished prose even when the normalizer can emit them.
- Wrap prose and field continuations readably and leave no trailing whitespace.
  For multiline Python signatures, put parameters and a bare ``*`` on separate
  lines as in the example; this is separate from docstring-field spacing.

For example:

```python
def native_copy(
    self,
    source: DriverObjectAddressT,
    destination: DriverObjectAddressT,
    *,
    mode: WriteMode = WriteMode.CREATE_ONLY,
) -> DriverObjectInfo[DriverObjectAddressT]:
    """
    Copy internally using explicit collision behaviour.

    The returned address must equal ``destination``. Success makes the
    complete destination readable. Failure must not expose a partial object
    that appears successfully published.

    Example:
        >>> info = driver.native_copy(source, destination)  # doctest: +SKIP


    :param source: Address of the complete readable object to copy.
    :param destination: Address at which the copied object should be published.
    :param mode: Collision policy for an object already at the destination.
    :return: Information about the complete copied object at ``destination``.
    """
```

``scripts/normalize_docstrings.py`` performs the structural part of a
migration without changing prose. Use ``--audit-json`` to measure a package,
``--patch`` to emit an ``apply_patch``-compatible patch, and ``--check`` after
migration to verify that no further structural rewrite is needed. The tool
reports missing docstrings and examples but deliberately does not invent them.

## Markdown documentation

Start with purpose, implementation status and a review date/checkpoint where
behaviour can become stale. Separate headings and paragraphs/lists with blank
lines. Keep tables brief and explain substantial contracts in prose sections.
Use relative links, placing angle brackets around destinations containing spaces.
Link new guides from their subsystem index and check heading fragments.

Current contracts, proposed work and dated evidence have different authority.
A task list should state its owner and completion checks, not duplicate a second
independent roadmap. See the [developer navigation rules](../README.md#maintaining-navigation).
