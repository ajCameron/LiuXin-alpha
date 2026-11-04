# Optional SQLite progress callbacks

Implemented for review **2026-11-01**. Both `SQLite` and `SQLite_apsw` drivers
leave SQLite's progress handler unset by default. Ordinary queries incur no
Python callback dispatch for VM progress. The existing counter observer remains
available, and callers can register their own observers or cancellation hooks.

## Registering and clearing hooks

Use the concrete SQLite driver, available as `db.driver`:

```python
from threading import Event

cancelled = Event()

def observe():
    # Update cheap local state; do not run SQL or reconfigure the driver here.
    return None

driver = db.driver
driver.set_progress_callbacks(
    (observe, cancelled.is_set),
    instruction_interval=1000,
)
try:
    # Run queries here. Another thread can request cancellation with:
    # cancelled.set()
    rows = driver.conn.execute("SELECT work_id FROM works").fetchall()
finally:
    driver.set_progress_callbacks()
```

Registration replaces the whole callback collection on every live connection
tracked by that driver, including its primary handle. New connections inherit
the current collection after their initialization SQL finishes. Closed handles
are skipped. Calling with no callbacks removes the native handler from live
connections and leaves it unset on future ones. Settings belong to one driver
instance; another driver is unaffected.

Callbacks take no arguments and run in registration order. `None`, `False` or
zero continues execution. The first truthy result interrupts the statement and
skips later callbacks in that dispatch. An exception raised inside a callback
also interrupts through SQLite. The executing query receives a SQLite error;
ordinary transaction error handling still applies. Registration errors are
reported before replacing the existing settings.

The interval counts SQLite VM instructions, not rows, elapsed time or an import
completion percentage. The default interval is 1,000; callers must supply an
integer between 1 and 2,147,483,647. A short statement may never invoke a callback.
Callbacks execute synchronously on the thread running the query: keep them cheap,
avoid SQL/reconfiguration inside them, and serialize registration changes with
connection use. This is a SQLite-specific facility, separate from ingest status
events and test resource telemetry.

## Existing diagnostic counter

The retained observer can be enabled explicitly:

```python
driver.set_progress_callbacks(
    (driver.simple_print_progress_handler,),
    instruction_interval=1000,
)
```

`event_count` counts observer invocations. It counts instructions only at interval
one, which restores the expensive historical behavior. The observer prints its
initial counter and every hundred million invocations thereafter. Clearing hooks
does not reset that counter.

The [regressions](../../tests/databases/test_sqlite_progress.py) exercise both
drivers' real connections, default absence of a handler, existing/future handles,
closed handles, removal, cancellation, validation, instance isolation and opt-in
use of the retained counter.

Return to [Databases](README.md).
