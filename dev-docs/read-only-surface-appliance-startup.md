# Read-only Surface Appliance Startup

This note is the operational startup checklist for bringing up the four
read-only LiuXin surfaces against one library database:

- native read-only web UI
- Calibre-style read-only web UI
- standalone OPDS feed
- standalone JSON API

The wrapper scripts run WSGI processes from the repo-local `.venv` and set
`PYTHONPATH=src` for you. Run each service as its own process. The examples
below use explicit ports because the web, Calibre-style web, and OPDS wrappers
all default to `8080`.

## Preflight

1. Start from the repository root:

   ```bash
   cd /mnt/c/dev/LiuXin-alpha
   ```

2. Create or refresh the repo-local virtual environment:

   ```bash
   bash scripts/create_venv.sh
   ```

3. Pick the database path and bind host:

   ```bash
   export LIUXIN_DB=/path/to/library.sqlite
   export LIUXIN_HOST=127.0.0.1
   ```

   Use `0.0.0.0` only when the appliance firewall or reverse proxy is already
   controlling access.

4. Confirm the interpreter and database exist:

   ```bash
   test -x .venv/bin/python
   test -f "$LIUXIN_DB"
   ```

5. Confirm the planned ports are free:

   ```bash
   ss -ltnp | rg ':8080|:8081|:8082|:8083' || true
   ```

6. Decide the metadata read source.

   Use cache-backed reads when startup can afford to build the cache and route
   handlers should avoid repeated live database lookups:

   ```text
   --metadata-read-source cache --cache-type schema_backed
   ```

   Omit those two flags for live database reads. Add
   `--no-cache-db-fallback` when testing strict snapshot behavior.

## Start The Four Processes

Run these in separate terminals, a process supervisor, or separate background
jobs. The examples keep downloads enabled. Add `--no-file-downloads` to any
process that should serve only metadata.

### 1. Native read-only web UI

Start:

```bash
scripts/run_web_readonly.sh \
  --database "$LIUXIN_DB" \
  --host "$LIUXIN_HOST" \
  --port 8080 \
  --metadata-read-source cache \
  --cache-type schema_backed
```

Smoke:

```bash
curl -fsS "http://${LIUXIN_HOST}:8080/" >/dev/null
curl -fsS "http://${LIUXIN_HOST}:8080/tables/works" >/dev/null
```

Expected surface: table-oriented HTML UI, search, row detail pages, and file
download/preview links when files are available.

### 2. Calibre-style read-only web UI

Start:

```bash
scripts/run_web_calibre_readonly.sh \
  --database "$LIUXIN_DB" \
  --host "$LIUXIN_HOST" \
  --port 8081 \
  --metadata-read-source cache \
  --cache-type schema_backed
```

Smoke:

```bash
curl -fsS "http://${LIUXIN_HOST}:8081/" >/dev/null
curl -fsS "http://${LIUXIN_HOST}:8081/ajax-setup" >/dev/null
curl -fsS "http://${LIUXIN_HOST}:8081/opds" >/dev/null
```

Expected surface: Calibre-shaped HTML browse pages, `/mobile`, AJAX endpoints,
interface-data endpoints, OPDS compatibility routes, and legacy `/get` routes.

### 3. Standalone OPDS feed

Start:

```bash
scripts/run_opds_readonly.sh \
  --database "$LIUXIN_DB" \
  --host "$LIUXIN_HOST" \
  --port 8082 \
  --metadata-read-source cache \
  --cache-type schema_backed \
  --page-size 25 \
  --max-page-size 200 \
  --opds-max-ungrouped-items 100
```

Smoke:

```bash
curl -fsS "http://${LIUXIN_HOST}:8082/opds" >/dev/null
curl -fsS "http://${LIUXIN_HOST}:8082/robots.txt" >/dev/null
```

Expected surface: OPDS Atom feeds at `/opds`, a redirect from `/` to `/opds`,
icons, and `/get` compatibility routes for acquisition clients.

### 4. Standalone JSON API

Start:

```bash
scripts/run_api_readonly.sh \
  --database "$LIUXIN_DB" \
  --host "$LIUXIN_HOST" \
  --port 8083 \
  --metadata-read-source cache \
  --cache-type schema_backed
```

Smoke:

```bash
curl -fsS "http://${LIUXIN_HOST}:8083/api" >/dev/null
curl -fsS "http://${LIUXIN_HOST}:8083/api/works?sort=title&limit=5" >/dev/null
```

Expected surface: JSON index at `/api`, work/category/search/file endpoints,
and file download/preview routes when files are available.

## Background Startup Without A Supervisor

For a bare appliance shell, keep logs and pid files under `run/`:

```bash
mkdir -p run/log

scripts/run_web_readonly.sh --database "$LIUXIN_DB" --host "$LIUXIN_HOST" --port 8080 --metadata-read-source cache --cache-type schema_backed \
  > run/log/web-readonly.log 2>&1 &
echo $! > run/web-readonly.pid

scripts/run_web_calibre_readonly.sh --database "$LIUXIN_DB" --host "$LIUXIN_HOST" --port 8081 --metadata-read-source cache --cache-type schema_backed \
  > run/log/web-calibre-readonly.log 2>&1 &
echo $! > run/web-calibre-readonly.pid

scripts/run_opds_readonly.sh --database "$LIUXIN_DB" --host "$LIUXIN_HOST" --port 8082 --metadata-read-source cache --cache-type schema_backed \
  > run/log/opds-readonly.log 2>&1 &
echo $! > run/opds-readonly.pid

scripts/run_api_readonly.sh --database "$LIUXIN_DB" --host "$LIUXIN_HOST" --port 8083 --metadata-read-source cache --cache-type schema_backed \
  > run/log/api-readonly.log 2>&1 &
echo $! > run/api-readonly.pid
```

Check:

```bash
ps -fp "$(cat run/web-readonly.pid)" "$(cat run/web-calibre-readonly.pid)" "$(cat run/opds-readonly.pid)" "$(cat run/api-readonly.pid)"
tail -n 40 run/log/*.log
```

Stop:

```bash
kill "$(cat run/web-readonly.pid)" \
     "$(cat run/web-calibre-readonly.pid)" \
     "$(cat run/opds-readonly.pid)" \
     "$(cat run/api-readonly.pid)"
```

## Notes

- Cache-backed reads use the selected cache backend snapshot. Restart the
  process, or use a future explicit reload path, when the appliance must see
  external database changes immediately.
- The wrappers print the exact Python command before serving. That line is the
  command to copy into a supervisor `ExecStart` if the appliance uses systemd,
  runit, s6, or another process manager.
- Keep one process per port. The maintained appliance port map is:
  `8080` native web, `8081` Calibre-style web, `8082` OPDS, `8083` JSON API.
