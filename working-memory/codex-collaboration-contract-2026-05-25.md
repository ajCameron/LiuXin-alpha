# Codex Collaboration Contract - 2026-05-25

## Purpose

Record the lightweight operating contract that keeps Codex work aligned across
multiple repo copies, branches, and long-running architecture lanes.

## Default Reminder

If a new workstream starts and any of these are missing, Codex should ask or
state the assumption before doing non-trivial work:

- repo path
- branch or worktree
- goal
- scope to touch
- boundaries not to touch
- what "done" means
- verification command or verification policy
- whether to update working memory

The highest-risk omission is repo path. Current known repo copies include:

- Codex/mainline checkout:
  `/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline`
- User working copy seen in prior session:
  `/mnt/c/dev/LiuXin-alpha`

When the prompt does not name a repo and the work could affect files, Codex
should default to the mainline checkout only after saying so. If the user is
clearly talking about their working copy or uncommitted work, Codex should ask
for the path rather than guessing.

## Suggested Workstream Header

```text
Repo: /exact/path
Branch/worktree: name or "current"
Goal: what we are trying to make true
Scope: files/area to touch
Do not touch: boundaries
Done means: tests/docs/behavior/sign-off criteria
Verify with: commands or "choose appropriate"
Memory: update note yes/no
```

## Long-Running Run Contract

For overnight or expensive runs, prefer a stable run id and predictable outputs:

```text
Run id: coverage-YYYY-MM-DD-HHMM
Log: working-memory/test-results/<run-id>.log
JSON: working-memory/test-results/<run-id>.json
Coverage: working-memory/test-results/.coverage-<run-id>
Exit marker: working-memory/test-results/<run-id>.done
```

The exit marker should include at least start time, finish time, command, exit
code, and the artifact paths.

## Assistant Behavior

Codex should nudge when the header or run contract is missing, especially for:

- repo ambiguity
- dirty worktrees
- branch-specific architecture work
- long-running test or coverage commands
- requests to commit

For small questions or read-only checks, Codex can proceed with a stated
assumption instead of forcing ceremony.
