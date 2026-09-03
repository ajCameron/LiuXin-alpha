# LiuXin style guide

Every API class from an API module should end with ``API``. Explicit is better
than implicit.

## Naming public values

A conceptual thing and a passive Python value describing that thing are not
interchangeable. Reserve an unqualified domain name for the concept itself or
for an object that genuinely represents its behaviour. A frozen collection of
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

- Put the opening and closing triple quotes on their own lines.
- Begin with a concise summary, followed by any contract detail needed by a
  caller.
- Give every class and function a small, useful ``Example:`` section. Examples
  should be executable doctests when practical; use ``# doctest: +SKIP`` when
  the example intentionally describes integration with an external object.
- Separate prose and the field list with two blank lines.
- List every function parameter in declaration order as ``:param name:``;
  omit implicit ``self`` and ``cls`` parameters.
- End every function field list with ``:return:``, including functions that
  return ``None``. Type annotations remain the authoritative type declaration.
- Preserve typed exceptions and other meaningful Sphinx fields after the
  parameter and return fields.
- Do not leave trailing whitespace in empty field placeholders.

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


    :param source:
    :param destination:
    :param mode:
    :return:
    """
```

``scripts/normalize_docstrings.py`` performs the structural part of a
migration without changing prose. Use ``--audit-json`` to measure a package,
``--patch`` to emit an ``apply_patch``-compatible patch, and ``--check`` after
migration to verify that no further structural rewrite is needed. The tool
reports missing docstrings and examples but deliberately does not invent them.
