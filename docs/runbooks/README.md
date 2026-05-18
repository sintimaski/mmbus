# Runbooks

Every operational alert in mmbus has a runbook in this directory.
The template is at `docs/templates/runbook-template.md`.

## Index

| Alert / condition                | File                              |
|----------------------------------|-----------------------------------|
| WAL disk pressure / thrashing    | `wal-disk-pressure.md`            |

## When to add a runbook

If you add a metric, log, or behaviour that ops would page on
(or even just want to investigate), open the spec, add the
metric, and write the runbook in the same PR.  See
`CONTRIBUTING.md` and the harness in `CLAUDE.md`.
