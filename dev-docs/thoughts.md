
# Devices and desires

Be real cool if, when you plugged in a device
 - LiuXin could discover and back up all relevant files
 - It could maintain a library on the device of things you might want to read




Findings (ordered)

  1. Medium: Batch on/off operations are non-atomic, so partial writes can happen if one link fails mid-run.
     src/LiuXin_alpha/interfaces/terminal/commands/on.py:442, src/LiuXin_alpha/interfaces/terminal/commands/off.py:74
  2. Medium: ID selector ranges are unbounded (1-1000000000 can explode work/memory).
     src/LiuXin_alpha/interfaces/terminal/commands/on.py:26
  3. Low: show rewrite logic now recognizes selector-like tokens, but show command execution still accepts only single IDs, so some selector forms fail awkwardly.
     src/LiuXin_alpha/interfaces/terminal/text_browser.py:825, src/LiuXin_alpha/interfaces/terminal/commands/show.py:56
  4. Low: Parsing/validation helpers are duplicated (_safe_int, selector parsing patterns), which increases drift risk.
     src/LiuXin_alpha/interfaces/terminal/text_browser.py:257, src/LiuXin_alpha/interfaces/terminal/commands/on.py:19, src/LiuXin_alpha/interfaces/terminal/commands/show.py:9

  High-value improvements

  1. Add transactional bulk mode for on/off (all-or-nothing), with optional --best-effort.
  2. Add selector guardrails (max targets, max range span).
  3. Decide and enforce one show policy:
      - either support batch targets fully, or
      - reject selectors early with a clear message.
  4. Add dry-run for on/off/link/unlink to preview affected rows.
  5. Improve help UX with concrete examples of compact syntax (work:1, work:1,2-4) in help.
     src/LiuXin_alpha/interfaces/terminal/text_browser.py:1100
  6. Add tab completion for commands/subcommands/tables when readline is available.
     src/LiuXin_alpha/interfaces/terminal/text_browser.py:560

  Test gaps

  1. No stress tests for huge selectors/ranges.
  2. No rollback/atomicity tests for bulk on/off failure paths.
  3. No explicit test for invalid selector forms (1,,2, 5-2, mixed malformed tokens).

  If you want, I can implement the top 3 next in this order: atomic bulk, selector limits, show selector consistency.
  


Yes — your plan is sound, and the proxy/API work you’ve already done is exactly the right foundation.

  I’d tighten it into this architecture:

  Recommended Shape

  1. liuxin_core (in-process domain engine): owns Library, Database, StorageManager, jobs, event bus.
  2. liuxind (daemon wrapper): hosts one liuxin_core instance and exposes RPC.
  3. Clients (terminal/GUI/web/scripts): talk to daemon via client proxies, or embed liuxin_core directly for tests/tools.

  This keeps VLC-style separation without forcing everything through RPC.

  Proxy Strategy (fits your API classes)

  1. Keep API classes as canonical contracts (DatabaseAPI, StoreAPI, etc.).
  2. Add two proxy families implementing the same APIs:
      1. Local*Proxy calls core directly.
      2. Remote*Proxy calls RPC transport.
  3. Make clients depend only on API interfaces, never concrete DB/store classes.

  Important design rule: remote proxies should return DTOs/value objects, not live Row objects with DB handles.

  Threading/Concurrency Model

  1. Single-writer command loop in core for all mutations.
  2. Reads can be concurrent, but start with serialized reads too for correctness; relax later.
  3. Background work (scan/convert/hash/metadata fetch) runs in job workers.
  4. Job workers never mutate DB directly; they submit mutation commands back to writer loop.
  5. Emit events on mutation/job progress so multiple clients stay in sync.

  This avoids race/path corruption and SQLite write contention.

  RPC Transport

  1. Start with HTTP+JSON for commands/queries plus WebSocket (or SSE) for event stream.
  2. Keep transport thin: deserialize -> call core -> serialize.
  3. Keep command IDs + idempotency keys for safe retries.

  You can add gRPC later if needed; don’t start there unless you need strict schemas/perf now.

  API Surface Guidance

  1. Expose coarse “use-case” operations first (add_title, link_tag, publish_squashfs, search).
  2. Keep raw table-level RPC as secondary/admin API only.
  3. Add explicit transactional command endpoints for multi-step operations.

  This prevents a very chatty and fragile remote DB API.

  Concrete Module Layout

  1. src/LiuXin_alpha/core/runtime.py (CoreRuntime, command loop, event bus wiring).
  2. src/LiuXin_alpha/core/commands.py (mutation command DTOs).
  3. src/LiuXin_alpha/core/queries.py (read query DTOs).
  4. src/LiuXin_alpha/core/events.py (typed event DTOs; you already have the file).
  5. src/LiuXin_alpha/core/proxies/ (local_*.py, remote_*.py implementing your API contracts).
  6. src/LiuXin_alpha/core/transport/http.py + ws_events.py.
  7. src/LiuXin_alpha/core/daemon.py (process entrypoint).

  Suggested Implementation Order

  1. Define command/query/event DTOs and CoreRuntime.
  2. Convert terminal interface to use LocalLibraryProxy only.
  3. Add event bus and job progress events.
  4. Add daemon HTTP transport and RemoteLibraryProxy.
  5. Add multi-client tests (two clients mutating + observing events).

  If you want, I can scaffold phase 1 now (CoreRuntime, command/query envelopes, and local proxies) without breaking current interfaces.