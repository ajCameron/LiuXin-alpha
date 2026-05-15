# Working Memory

Short, dated handoff notes for active work.

Use this folder for:
- review findings
- debugging notes
- short architectural decisions in progress
- command/test breadcrumbs that are useful across sessions

Do not use this folder for:
- canonical project documentation
- long transcripts
- raw command dumps unless they are genuinely needed

## Commit convention

When the user asks to "commit", treat that as shorthand for:
- update the relevant working-memory note first
- then commit the code/tests/docs/working-memory changes together

## Naming

Use:

```text
<topic>-YYYY-MM-DD.md
```

Examples:
- `surface-findings-2026-03-11.md`
- `rpc-cutover-2026-03-11.md`
- `sync-debug-2026-03-11.md`

## Rule of thumb

Keep each note compact and task-shaped:
- what was looked at
- what was found
- what is still open
- what to read or run next

When adding a new note, update `index.md`.
