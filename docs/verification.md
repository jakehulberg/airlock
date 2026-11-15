# Implementation verification

This change is prepared for review, not deployed. Airlock and homelab changes must be reviewed as
a coordinated release.

## Implemented

- PostgreSQL schema with transactional, checksum-checked migrations and startup locking
- Interactive task persistence, output retention, restart locks, and retained released history
- Queue priorities, dependencies, independent attempts, leases, heartbeats, backoff, cancellation,
  capacity limits, idempotency keys, and uncertain-outcome reconciliation
- Sandbox provisioning and execution using the existing Sandboxes and Executor abstractions
- Agent definitions, bounded delegation, durable messaging, recipient acknowledgements, structured
  results, and human approval gates
- DAG workflows, immutable run definitions, dependency failure policies, structured input, output
  transfer, cancellation, and execution history
- Cron and one-time scheduling, timezones, missed-run and concurrency policies, run-now and history
- Operations navigation, real overview counts, task board, filtering, details, assignments,
  agent/workflow/schedule editors, approvals, activity, and SSE state updates
- Dedicated CNPG database, TLS connectivity, R2 backup configuration, scoped networking, PodMonitors,
  readiness and liveness, without changing Flux image names, policy markers, public hostname or RBAC

## Checks performed

| Check | Result |
|---|---|
| `gofmt` | Passed |
| `go vet ./...` | Passed |
| `go test -race ./...` with PostgreSQL | Passed |
| Transactional migrations, concurrent startup and checksum rejection | Passed |
| Concurrent queue claims, global capacity, leases and stale-worker fencing | Passed |
| Retry backoff, idempotency, cancellation, approval isolation | Passed |
| DAG dependencies, predecessor results, failure policy and workflow completion | Passed |
| Timezone validation, duplicate schedule prevention and run-now concurrency | Passed |
| Agent token lifecycle, delegation allowlist and message authorization | Passed |
| Fake Kubernetes execution, transport uncertainty, pod-aware cleanup | Passed |
| Interactive restart history and authorization | Passed |
| Web-to-orchestrator-to-PostgreSQL task/workflow/schedule/approval/cancel journeys | Passed |
| Existing web authentication, CSRF, cookies, sandbox and SSE tests | Passed |
| JavaScript syntax check | Passed |
| Orchestrator and web Docker builds, Linux AMD64 and ARM64 | Passed |
| Kustomize rendering: `apps/airlock`, `apps/agents`, full homelab `apps` | Passed |
| Whitespace and targeted credential-pattern scan | Passed; no production credentials added |
| Private preview endpoint | HTTP 200 through Tailscale Serve using explicit hostname resolution |
| Browser UI | Initial local page loaded; browser connection subsequently unavailable |

Tests use disposable PostgreSQL schemas and fake Kubernetes clients. They do not prove production
OIDC, sandbox runtime, Agent Vault, egress policy, failover or backup behavior. The preview is a
test-only harness with an injected test identity and no execution workers.

## Required acceptance checks

- Complete browser journeys and responsive/accessibility inspection in a working browser session
- Validate manifests against the installed CRDs and run the coordinated Flux rollout
- Verify live OIDC login, conversational follow-ups, SSE reconnect and sandbox release
- Run the supplied daily security workflow against the actual pool and Agent Vault access bundle
- Interrupt a non-sensitive sandbox task and verify uncertain state, pod termination and manual retry
- Confirm CNPG certificate validation, failover, WAL archiving, a completed R2 backup and a restore
  drill; no successful backup or restore is claimed from manifest inspection
- Review the separate read-only Kubernetes integration needed by the cluster-health scenario;
  this change intentionally grants no Kubernetes credentials to sandbox workloads

## Deliberate boundaries

Execution is at least once. Task approval gates execution start; it cannot approve or intercept an
individual command inside an already running agent. Pool definitions and Agent Vault bundles enforce
capabilities and resources. Agent credentials are short-lived bearer capabilities tied to a recorded
active attempt, not Kubernetes projected identity tokens. Interactive SSE remains single-replica.
The workflow editor accepts bounded JSON steps; drag-and-drop editing is not implemented. Output
uses bounded PostgreSQL storage; external artifacts remain a future extension.
