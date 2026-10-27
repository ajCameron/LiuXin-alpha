# Tkinter GUI implementation plan

Reviewed **2026-10-20** against `surfaces/tkinter_gui`. This replaces the old
branch-specific phase instructions. The [architecture guide](tkinter-gui-architecture.md)
owns current composition; [surface next tasks](../next_tasks.md) owns priorities.

## Current implementation

| Area | Implemented behaviour | Remaining work |
| --- | --- | --- |
| Session | `TkGuiSession` wraps `SurfaceCoreSession`, with local or remote named Core calls. | Enforce capabilities at the server and surface them as UI affordances. |
| Reads | Schema/table browser, row paging/search/details and metadata hydration through Core adapters. | Richer WEMI presentation and explicit completeness at each projection. |
| Task runner | Serial worker for Core calls; result dispatch and widget updates stay on the Tk thread. | Cancellation/lifecycle coverage; durable work still belongs to Core jobs. |
| Read source | Selection/status/refresh through the shared Core model. | Cache configuration remains Core-owned; do not expose or swap raw handles. |
| Metadata writes | Backend/controller support selected-field replacement via `metadata.write` and render write reports. | Richer field editors, conflict handling and end-to-end failure feedback. |
| Job/OPF/storage panels | Some operations exist in Core and the operational CLI. | Dedicated GUI panels are follow-up work, not completed by the existing backend. |

The public boundary is named Core queries/commands for reads as well as writes.
`CoreDatabaseView` is a transport adapter, not a raw database permission to bypass
Core. Older `invoke_command(target=..., method=...)` examples and GUI-owned
Database/Library composition are not the pattern for new UI code.

## Next slices

### 1. Validate the existing editing and session paths

Exercise local and remote sessions with the same supported metadata edit.
Show validation failures and the authoritative write report, then refresh the
affected view. Preserve the user's pending input when a request fails.
Test close/reconnect while a worker is active so a late callback cannot update a
closed or newly opened session. A client timeout must not be displayed as proof
that the server cancelled a mutation.

### 2. Add capability-aware presentation

Consume an explicit server capability contract when it exists. Disabled controls
can improve feedback but cannot enforce authorization. Use the shared surface
task SURF01; do not implement a GUI-only permissions list.

### 3. Add one durable job panel

Start with job list/detail/progress/result/cancel/retry through named operations.
Reuse job views and structured failures; do not create a second job registry in
the GUI. Keep GUI task cancellation distinct from cancellation of submitted work.
Only claim restart recovery when the selected job workflow persists it.

### 4. Add OPF and storage workflows by existing receipts

Expose one already-supported operation at a time. OPF exchange must explain lost
WEMI structure; file writes need no-clobber publication and verified results.
Storage changes should show manager receipts and explicit destructive intent,
without low-level Store or database calls from widgets.

## Validation

Keep the nonvisual backend/session/task tests in
[test_tkinter_gui.py](../../../tests/surfaces/test_tkinter_gui.py). Add targeted
Core transport parity tests for new operations and the
[surface boundary gate](../../../tests/surfaces/test_core_boundary_enforcement.py)
for dependency direction.

A headless test run does not validate widget layout, keyboard focus or a real
desktop session. Record a separate manual GUI check when visual interactions
change; this documentation review did not run one.
