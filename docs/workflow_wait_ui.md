# Workflow waits in the UI

The dashboard visualizes the River **workflow wait family** (signals, timers,
and CEL conditions) added to the
[river](https://github.com/techpartners-asia/river) library. A wait-bearing
task is one that, beyond its dependencies, is held until a signal arrives, a
timer fires, or a CEL condition resolves true. See the library's
`docs/workflow_wait.md` for the underlying model.

## What you see

- **Workflow diagram** — a wait-bearing task renders a **gate** instead of a
  plain dependency handle. The gate's state reflects the task's wait
  `phase` (`not_started`, `waiting`, `resolved`) and its terms (a signal key,
  a timer name, or a CEL term). Hover for a summary tooltip.
- **Gate inspector** — selecting a gated task opens the inspector with its
  conditions, live signals, and a diagnostics panel (the current CEL result
  and per-term status).
- **Approve / emit signal** — for a signal-kind term whose wait is not yet
  resolved, an **Emit signal** action lets you send a signal (key prefilled,
  payload editable) to unblock the task from the dashboard.

## API endpoints

All under the `/api/pro/workflows` prefix (served for OSS workflows too):

| Method & path | Purpose |
|---|---|
| `GET /{id}` | Workflow + tasks, each with `wait` (terms/phase/expr) and `wait_reason` |
| `GET /{id}/task-signals?task_name=&key=&desc=&limit=` | Signal history for a task (newest-first) |
| `POST /{id}/task-signals` | **Emit** a signal: body `{ key, payload, idempotency_key?, source? }` (powers the Approve button) |
| `GET /{id}/task-wait-diagnostics?task_name=` | Read-only wait snapshot (phase, `expr_result`, per-term results) |

The wait/diagnostics data is computed server-side from the job's
`river:workflow_wait` metadata and the `river_workflow_signal` table, using the
river library's `WaitDiagnosticsForExec` helper — no scheduler is started by
the UI.

## You need a worker to actually run workflows

**The dashboard is read-only.** It connects with a plain `river.Client` and
does **not** register workers or run the workflow scheduler. So a seeded
workflow will sit still in the UI — nothing promotes pending/waiting tasks or
executes them.

To make workflows run, run a separate **worker process** that:

1. registers your workers, and
2. uses `riverworkflow.NewClient(driver, cfg).Start(ctx)` so the
   leader-elected workflow scheduler evaluates waits and promotes tasks.

```go
workers := river.NewWorkers()
river.AddWorker(workers, &MyWorker{})

wfClient, _ := riverworkflow.NewClient(driver, &riverworkflow.Config{
    Config: river.Config{
        Queues:  map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 5}},
        Workers: workers,
    },
})
wfClient.Start(ctx) // runs the scheduler + workers; the UI then reflects progress
```

Emitting a satisfying signal (via the Approve button, the `POST` endpoint, or
`Workflow.Signals().Emit`) only *records* the signal; the running scheduler is
what then promotes and executes the task.

## Local Docker setup

```bash
make docker-db/up                          # Postgres in Docker
# apply the FORK's migrations (incl. 009_workflow_signals):
go run ../river/cmd/river migrate-up --database-url postgres://postgres:postgres@localhost:5432/river_dev
DATABASE_URL=postgres://postgres:postgres@localhost:5432/river_dev npm run dev   # UI + backend
```

If a host Postgres already holds `5432`, map the container to another port
(e.g. `5433:5432`) and point `DATABASE_URL` at it. Run a worker process (as
above) against the same database to drive the seeded workflows.

## Known limitations

- **Diagnostics-endpoint inputs** (the live input breakdown from
  `task-wait-diagnostics`) are currently empty — the library's
  `WaitDiagnostics` does not yet return its internal inputs. Phase,
  `expr_result`, and per-term results are populated. (The workflow `GET`
  response *does* populate `wait.inputs` from the spec's terms + deps, which
  is what the gate inspector and the Emit-signal button rely on.)
- **Signal pagination** — the signals endpoint returns the newest page with a
  `has_more` flag, but `cursor_id` is not yet consumed server-side, so "next
  page" re-returns the first page. Fine for typical (small) signal lists.
