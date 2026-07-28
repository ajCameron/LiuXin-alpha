# DB Property Salvage Split

Date: 2026-03-16

## Outcome

- The DB-property corpus is no longer an active `salvage_existing` bucket.
- All `26` support classes now have live alpha subset coverage.
- The old support-file backlog is fully resolved into:
  - `covered`
  - `rewrite`

## Final DB-Property Split

Reclassified to `covered`:

- `test_db_0_properties.py`
- `test_db_2_properties.py`
- `test_db_3_properties.py`
- `test_db_5_properties.py`
- `test_db_6_properties.py`
- `test_db_7_properties.py`
- `test_db_8_properties.py`
- `test_db_9_properties.py`
- `test_db_11_properties.py`
- `test_db_12_properties.py`
- `test_db_13_properties.py`
- `test_db_22_properties.py`
- `test_db_23_properties.py`
- `test_db_24_properties.py`
- `test_db_25_properties.py`

Reclassified to `rewrite`:

- `test_db_1_properties.py`
- `test_db_4_properties.py`
- `test_db_10_properties.py`
- `test_db_14_properties.py`
- `test_db_15_properties.py`
- `test_db_16_properties.py`
- `test_db_17_properties.py`
- `test_db_18_properties.py`
- `test_db_19_properties.py`
- `test_db_20_properties.py`
- `test_db_21_properties.py`

## Why

Covered rows now have honest alpha-native replacements:

- alpha schema/count subset contracts
- support registry coverage
- blank optional-metadata profile coverage
- empty custom-column profile coverage
- active custom-column cache/emulation coverage

Rewrite rows stay rewrite because their old specialized builder semantics are gone from the live provisioning path:

- author-rich compatibility projection
- secondary UUID / content level / shelf number builders
- identifier-link builders
- rich synthetic-content builders

## Manifest Effect

- `covered`: `76`
- `salvage_existing`: `0`
- `rewrite`: `16`
- `retire`: `16`
- `integration_frozen`: `9`
- `vendor_frozen`: `7`

## References

- [db-property-secondary-uuid-cluster-2026-03-16.md](db-property-secondary-uuid-cluster-2026-03-16.md)
- [db-property-identifier-cluster-2026-03-16.md](db-property-identifier-cluster-2026-03-16.md)
- [db-property-compatibility-projection-cluster-2026-03-16.md](db-property-compatibility-projection-cluster-2026-03-16.md)
- [db-property-rich-content-cluster-2026-03-16.md](db-property-rich-content-cluster-2026-03-16.md)
- [db-property-custom-column-profile-cluster-2026-03-16.md](db-property-custom-column-profile-cluster-2026-03-16.md)
