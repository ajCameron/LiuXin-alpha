# Library examples

`library_facade_example.py` opens or creates a LiuXin database, ensures a
managed filesystem Store is configured, refreshes the storage manager, and
round-trips a file through the application-facing `Library` facade.

```bash
python examples/library/library_facade_example.py \
  --database /tmp/liuxin.sqlite \
  --store-root /tmp/liuxin-store \
  --create-db
```
