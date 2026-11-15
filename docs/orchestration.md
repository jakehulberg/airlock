# Durable agent orchestration

Airlock coordinates queued agent work in PostgreSQL and runs it through the existing Kubernetes
sandbox executor. Interactive conversations remain available from **Sandboxes**; **Overview** opens
the operations console.

## Components

```mermaid
flowchart LR
  Browser -->|OIDC session| Web[Go web application]
  Web -->|administrator token and user identity| Orchestrator[Go orchestrator]
  Orchestrator --> PostgreSQL
  Orchestrator -->|SandboxClaim and pods/exec| Sandbox[gVisor sandbox]
  Sandbox -->|attempt-scoped credential| Orchestrator
  Sandbox --> Vault[Agent Vault proxy]
  Flux -->|homelab manifests| Orchestrator
  Flux --> PostgreSQL
```

- `internal/operations`: schema, state transitions, queue, workflows, scheduling, messaging
- `internal/orchestrator/automation.go`: provisioning, heartbeat, output capture, cleanup
- `internal/orchestrator/history.go`: durable interactive history and restart protection
- `internal/web/operations.go`, templates and `operations.js`: authenticated console and SSE relay
- Homelab `apps/airlock`: PostgreSQL, R2 backup configuration, monitoring, application deployment

No new CRD, Kubernetes controller, frontend framework, or deployment system is introduced.

## PostgreSQL and migrations

`AIRLOCK_DATABASE_URL` is required by the orchestrator. The homelab deployment obtains the CNPG
application URI from `airlock-db-app`, not the Infisical database. `AIRLOCK_DATABASE_CA` points to
CNPG's `ca.crt`; when set, pgx requires TLS 1.2 or later, validates the server certificate and
hostname, and disables plaintext fallbacks. Local integration tests use a disposable PostgreSQL
instance with TLS disabled explicitly.

Embedded, numbered SQL migrations run at startup inside a transaction under an advisory lock.
Checksums prevent editing applied migrations. Newer or incompatible schemas stop startup. Add a
new migration instead of changing one already deployed. PostgreSQL remains the source of truth;
notifications only reduce queue latency.

The schema separates tasks, attempts, agents, workflows, runs, dependencies, schedules, fires,
messages, approvals, activity, and interactive history. Task and attempt metadata are retained for 365 days;
attempt output is capped at 1 MiB and cleared after 30 days. Finished interactive output is cleared
after 30 days. Activity and messages expire after 90 days. Each task may send at most 100 messages. Structured
results are capped at 64 KiB and retained with the task. Finished tasks are pruned after 365 days
only when no dependent task or delegated child still references them and sandbox cleanup is done.
Terminal workflows and schedule fire history expire after 365 days. Active work is never pruned.
External artifact storage is not implemented; export reports before their retention period ends.

## Execution and recovery

A logical task has independent execution attempts. Claiming uses a transactional capacity lock and
`FOR UPDATE SKIP LOCKED`. At most four automated tasks or tasks awaiting sandbox cleanup consume
capacity across replicas. An owner can have at most 1,000 nonterminal tasks. Priorities range from
-10 to 10; attempts from 1 to 10; timeouts from one second to four hours, bounded by the sandbox TTL.

Each attempt receives a distinct worker identity, a 60-second lease, and a deterministic claim name
before provisioning. Heartbeats renew the lease every ten seconds. Provisioning has a five-minute
limit. Known unsuccessful command exits retry with exponential backoff, after cleanup. Every
attempt records its output, pod name and UID, exit status, error, and cleanup state.

A lost exec connection does not prove process termination. Transport errors and expired leases
enter `uncertain`. Reconciliation requests claim deletion and waits for the recorded pod UID to
disappear before marking the task failed or cancelled. It never automatically retries an uncertain
attempt. A human may select **Retry** after cleanup if an attempt remains. Review external side
effects first: execution is at least once, not exactly once, and deleting a sandbox cannot undo an
external action. Use idempotency keys in downstream systems.

Interactive execution persists before dispatch and after output updates. After restart, interrupted
interactive tasks display an uncertain-outcome error and keep a durable claim lock. Release the
sandbox before starting new work. Existing conversation follow-ups, pool commands, TTLs, ownership
checks, and SSE are preserved. Keep one orchestrator replica for interactive SSE routing; automated
queue coordination is replica-safe. The interactive page restores the latest 500 records; the
console shows the latest 100 interactive histories, including released claims.

## Workflows and approvals

Workflow definitions contain up to 32 named steps. `needs` references step keys; invalid references
and cycles are rejected. Independent steps run in parallel within the global execution limit.
Dependent steps receive a predecessor structured result, or bounded output, as untrusted data. Required dependencies must
complete successfully. A failed or cancelled dependency fails its waiting descendants by default. Set a step's
`dependencyPolicy` to `block` to leave it blocked for human cancellation or upstream recovery.
Steps can also set `maxAttempts` and `timeoutSeconds`. Workflow
completion waits for every task, including delegated tasks, to reach a terminal state.

Create logical agents first, then paste a workflow's steps into the workflow editor. The supplied
[security research example](../examples/daily-kubernetes-security.json) uses three logical agents
with the existing `claude` pool. Replace its agent IDs with IDs from **Agents**. Running it stores the
final report in the reporting task's output and optional structured result. Scheduled and manual
workflow runs use the same execution infrastructure.

Task or agent settings can require approval before execution. Only an authenticated human owner
can approve or reject a pending task. Decisions are durable and audited. Agents cannot approve work.
Approval is a task-start gate, not interception of individual shell commands. Actual tools, model
options, network destinations, and resource permissions remain controlled by the Kubernetes pool
and Agent Vault access bundle. Instructions alone are not a security boundary. Configure a separate
restricted pool for read-only or privileged operations.

## Scheduling

Schedules support five-field cron expressions and IANA timezones, or one future RFC3339 timestamp.
`missed: once` coalesces missed executions into one run; `missed: skip` skips runs more than one
minute late. `concurrency: forbid` skips a scheduled fire while its workflow is running and rejects
run-now in that situation. `allow` permits overlapping runs within execution capacity.

Schedule rows are locked while the fire record, workflow run, and next occurrence are committed.
Multiple replicas cannot produce the same scheduled fire. Run-now records history. Failed schedule
starts appear in activity; one invalid workflow does not block other due schedules. A two-second
poll recovers after missed notifications and restarts. Disabling a schedule does not cancel an
already-created run; use **Cancel workflow** for that.

## Agent interfaces

The orchestrator generates a random credential for each attempt and stores only its SHA-256 hash.
The credential arrives through exec stdin and is exported as `AIRLOCK_AGENT_TOKEN`; it is never
placed in a command argument, stored prompt, or Kubernetes Secret. It expires with the task timeout
and is rejected after lease expiry, cancellation, or completion. Authentication checks the active
attempt and recorded pod identity. Tokens are bearer credentials scoped to an execution, not proof
of the calling network process; keep them inside that sandbox.

The sandbox's API base is `AIRLOCK_AGENT_URL`. Use `curl --noproxy airlock-orchestrator.airlock.svc.cluster.local` when Agent Vault proxy environment settings are
active. The shared web token is never given to agents.

| Endpoint under `/v1/agent` | Request |
|---|---|
| `GET /messages` | Inbox scoped to this workflow or parent/child delegation |
| `POST /messages` | `recipient`, `type`, JSON `payload`, optional `correlationId` |
| `POST /ack` | Message `id`; only the intended recipient can acknowledge |
| `POST /result` | Structured JSON result, at most 64 KiB |
| `POST /delegate` | `title`, `prompt`, `agentId` |

Delegation targets must be explicitly listed on the sending agent and belong to the same owner.
Depth is limited to three, each root task can spawn at most eight descendants, and child executions
have one attempt and at most 30 minutes. Delegated work always requires human approval. Agent input
cannot alter permissions, ownership, workflow membership, dependencies, or these limits.

Known execution credentials are redacted from retained output, and agent APIs reject the execution
credential in submitted content. This is not a general secret detector. Do not paste credentials
into prompts or messages; use Agent Vault. API errors and logs do not print database URLs or bearer
tokens. Sandbox egress only adds the labeled orchestrator on TCP 8081; it does not open internet or
SQL access.

## Operational API

All human routes below use the existing shared-token and `X-Airlock-User` authentication. The web
application supplies these after OIDC login; its browser-facing proxy retains session and CSRF
checks. Every database query is scoped to that owner.

| Endpoint under `/v1/operations` | Purpose |
|---|---|
| `GET /` (without trailing slash) | Bounded overview, task board, histories, counts |
| `GET /stream` | Authoritative SSE snapshots; reconnect replays current state |
| `POST /agents` | Create or edit an agent by optional `id` |
| `POST /tasks` | Create task; optional owner-scoped `idempotencyKey` |
| `GET /tasks/{id}` | Instructions, attempts, logs, dependencies, messages |
| `POST /tasks/{id}/assign` | Reassign unstarted work; invalidates prior approval |
| `POST /tasks/{id}/{action}` | `queue`, `cancel`, `retry`, `approve`, `reject` |
| `POST /workflows` | Create or edit a DAG |
| `POST /workflows/{id}/run` | JSON workflow input |
| `POST /runs/{id}/cancel` | Cancel pending tasks and request cancellation of active tasks |
| `POST /schedules` | Create or edit schedule |
| `POST /schedules/{id}/enabled` | `{"enabled":false}` |
| `POST /schedules/{id}/run` | Run now with concurrency enforcement |
| `GET /interactive/{id}` | Durable interactive history after claim release |

For example, from a trusted administrative environment:

```sh
curl "$AIRLOCK_ORCHESTRATOR_URL/v1/operations/tasks" \
  -H "Authorization: Bearer $AIRLOCK_API_TOKEN" -H "X-Airlock-User: $OIDC_SUBJECT" \
  -H 'Content-Type: application/json' \
  -d '{"title":"Research","agentId":"AGENT_ID","prompt":"Summarize Kubernetes advisories","maxAttempts":3,"timeoutSeconds":1800,"approvalRequired":true}'
```

## Monitoring and development

`/healthz` is process liveness. `/readyz` checks PostgreSQL with a two-second deadline. `/metrics`
exports bounded state counts, attempt and retry counts, failure counts, cumulative task and workflow
duration, provisioning failures, and scheduler success age. No metric labels contain task IDs.
Prometheus discovers the homelab PodMonitors. Compare scheduler age and database readiness when work
stops progressing; inspect **Activity** and task attempts before retrying.

```sh
AIRLOCK_TEST_DATABASE_URL='postgres://postgres:airlock-test-only@localhost:55439/airlock?sslmode=disable' go test -race ./...
go vet ./...
go build ./cmd/orchestrator ./cmd/web
```

Integration tests create and drop uniquely named schemas in the supplied test database. Never point
this variable at a production database. CI provides PostgreSQL and runs the same integration tests
alongside the existing format, vet, race, and multi-architecture image workflow.

`TestBrowserPreview` is an opt-in test-only harness with a separate schema, fake Kubernetes, and no
execution workers. It supplies a test identity for browser checks and is never compiled into the
production binaries. Use `AIRLOCK_VISUAL_TEST=1` with the test database variable and run
`go test ./internal/web -run TestBrowserPreview -v -timeout 16m`; it listens on loopback port 18080
for 15 minutes. Before starting a developer preview, check existing Tailscale Serve routes, add a
private route without replacing unrelated routes, and verify it. Never enable Funnel.

Live OIDC, gVisor, Agent Vault, CNPG failover, R2 backup restoration, and production NetworkPolicy
behavior require cluster acceptance testing. The Kubernetes health example additionally needs a
reviewed read-only integration or pool; this change grants no Kubernetes credentials to agents.
