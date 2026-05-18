# Runbook: <alert / condition name>

**Severity:** P1 (page) / P2 (notify) / P3 (log only)

**Owner:**

## What the alert means

One paragraph.  What was observed, what it implies, why it
matters.  Avoid jargon — the reader may be on-call without full
context.

## How to diagnose

Step-by-step commands or dashboard links.  Each step has a
"check for" outcome:

1. `<command>` — check for `<expected pattern>`.
2. `<dashboard link>` — check for `<metric crossing threshold>`.

## How to resolve

Step-by-step.  Each step is reversible or has a rollback.

1.
2.

## How to prevent recurrence

What follow-up work would prevent the alert from firing again?
Link a tracking issue if you opened one.

## Related

- Code: `<path:line>`
- Spec: `docs/<spec>.md`
- Other runbooks: `<links>`
