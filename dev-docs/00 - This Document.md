# About the developer documentation

Reviewed: **2026-10-20**.

These notes are for people working on LiuXin-alpha: its code, plugins, data model,
operations and design. Start at the [developer index](README.md); use the
[repository README](../README.md) for environment setup and entry points.

The collection retains the useful parts of a working lab book, but current
contracts need to be distinguishable from experiments and historical conclusions.
A linked design is not automatically an implemented feature.

## What belongs where

| Kind of document | Home and purpose |
| --- | --- |
| Orientation | The numbered top-level guides explain the project, owners and common boundaries. |
| Current contract or runbook | The owning subsystem directory describes supported calls, failure behaviour and checks. Link it from that subsystem's index. |
| Architecture decision or review | [Architecture and reviews](architectural_reviews/README.md) records decisions, dated findings and remaining migrations. |
| Contributor practice | [Hacking](hacking/README.md) owns setup, coding conventions, packaging and verification guidance. |
| Uncommitted idea | [Ideas and experiments](ideas.md) records the problem, first useful experiment and likely owner. |
| Cross-project work | [Cross-project TODOs](global_todo.md) links work that spans subsystem boundaries. Detailed tasks stay with their owner. |
| Session handoff and evidence | [Working memory](../working-memory/index.md) records what was done, checked and left open. Promote durable contracts into these guides. |

Plugin implementation notes belong with the relevant domain or
[plugin composition](plugins/README.md), depending on what they explain. An
experiment with another implementation language should identify its boundary,
build/runtime requirements and result, rather than imply a project-wide migration.

## Keep status and evidence explicit

For a substantial review, record the date and source checkpoint or working-tree
scope. Separate current behaviour, desired policy and remaining implementation.
When quoting a test result, state what ran and when; a historical passing count is
not a current health check. Document optional dependencies and backend limits.

Prefer a small runnable example with resource cleanup over an invented API.
Describe important partial effects, transaction boundaries and failure recovery.
Keep one owner for each contract and link to it rather than copying a long rule
into several guides. Preserve history with a clear superseded label and a link
to its replacement.

## Navigation and review

Use relative Markdown links. Keep local-only logs and generated reports as code
paths unless they are tracked, reproducible artifacts. After moving a guide,
update incoming links, its own relative links and any source/tooling consumers.

The [documentation checks](../tests/scripts/test_developer_documentation_links.py)
verify tracked local targets and reachability from the index. They do not execute
every example, verify every heading fragment or establish that prose agrees with
the implementation. A content review must do those checks where they matter.
