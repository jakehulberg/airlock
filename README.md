# Airlock

Airlock is a web UI for claiming agent sandboxes
([kubernetes-sigs/agent-sandbox](https://github.com/kubernetes-sigs/agent-sandbox)) and running
agent tasks in them. Each user gets their own short-lived sandbox, and the agent's output streams
back to the browser.

This repo holds the code and CI. The deploy manifests live in the homelab repo, and Flux applies
them.

## How it fits together

```
browser ──1. OIDC (Authentik)──▶ web ──2. bearer token + user headers──▶ orchestrator
                                                                              │
                                                3. SandboxClaim, pods/exec    │
                                                                              ▼
                                                                       Kubernetes API
```

1. `web` signs the user in with OIDC and serves the UI. It connects only to the orchestrator
2. `web` calls the [orchestrator API](#orchestrator-api) with the shared token and the user's
   OIDC subject
3. The orchestrator creates and deletes `SandboxClaim`s and runs tasks through `pods/exec`

### Claims

- The user picks a `SandboxWarmPool`, a name and a TTL. The orchestrator creates a `SandboxClaim`
  with `shutdownTime` and `shutdownPolicy: Delete`, so every sandbox expires
- Each claim is labeled with a hash of the user's OIDC subject. Users can see and release only
  their own claims

### Tasks

- A task runs the pool's command in the agent container, with the prompt on stdin. The default
  command sources `/session/env` (the Agent Vault proxy settings that the session sidecar writes)
  and runs `claude -p`. Each pool can set its own command ([Pools](#pools))
- Each sandbox runs one task at a time
- The sandbox page shows each task's prompt and output in order, streams the running task, and
  shows a reply box once the task finishes
- If follow-ups should build on earlier tasks, the pool's command has to continue the agent's
  session (for Claude Code, `claude -p --continue`)
- The orchestrator keeps task output in memory: 1 MiB per task, 500 tasks. Output is lost on
  restart, so run one replica

## Images

Every push to `main` builds `linux/amd64` and `linux/arm64` images:

- `ghcr.io/jakehulberg/airlock-orchestrator:<tag>`
- `ghcr.io/jakehulberg/airlock-web:<tag>`

Tags are `YYYYMMDDTHHMMSS-<sha7>` (UTC build time), so a Flux `ImagePolicy` can pick the newest
tag by its timestamp. Both images built from one commit share the tag.

## Configuration

### Orchestrator

| Variable | Default | What |
|---|---|---|
| `AIRLOCK_API_TOKEN` | required | Shared with `web` |
| `AIRLOCK_NAMESPACE` | `agents` | Namespace for pools and claims |
| `AIRLOCK_POOL_SELECTOR` | all pools | Label selector for the pools users may claim from |
| `AIRLOCK_DEFAULT_TTL` | `1h` | Claim TTL if the request doesn't set one |
| `AIRLOCK_MAX_TTL` | `8h` | Longest TTL a user can request |
| `AIRLOCK_AGENT_CONTAINER` | `agent` | Container that tasks run in, unless the pool sets one |
| `AIRLOCK_TASK_COMMAND` | `. /session/env 2>/dev/null; exec claude -p` | Task command, unless the pool sets one. Run with `sh -c`, prompt on stdin |
| `AIRLOCK_TASK_TIMEOUT` | `30m` | Longest a task can run |
| `AIRLOCK_LISTEN` | `:8081` | Listen address |

The orchestrator's ServiceAccount needs this Role in `AIRLOCK_NAMESPACE`:

```yaml
rules:
  - apiGroups: [extensions.agents.x-k8s.io]
    resources: [sandboxclaims]
    verbs: [get, list, create, delete]
  - apiGroups: [extensions.agents.x-k8s.io]
    resources: [sandboxwarmpools]
    verbs: [get, list]
  - apiGroups: [agents.x-k8s.io]
    resources: [sandboxes]
    verbs: [get]
  - apiGroups: [""]
    resources: [pods]
    verbs: [get, list]
  - apiGroups: [""]
    resources: [pods/exec]
    verbs: [get, create]
```

### Pools

Optional annotations on a `SandboxWarmPool`:

| Annotation | What |
|---|---|
| `airlock.jakehulberg.com/description` | Shown in the claim form |
| `airlock.jakehulberg.com/agent-container` | Container that tasks run in. Default `AIRLOCK_AGENT_CONTAINER` |
| `airlock.jakehulberg.com/task-command` | Run with `sh -c`, prompt on stdin. Default `AIRLOCK_TASK_COMMAND` |

With these, pools for different agents can run side by side. For example, a Codex pool:

```yaml
metadata:
  annotations:
    airlock.jakehulberg.com/description: Codex
    airlock.jakehulberg.com/task-command: . /session/env 2>/dev/null; exec codex exec -
```

The orchestrator can only read pools, so only cluster admins can change them.

### Web

| Variable | Default | What |
|---|---|---|
| `AIRLOCK_PUBLIC_URL` | required | For example `https://airlock.home.jakehulberg.com`. The redirect URI is `<this>/callback` |
| `OIDC_ISSUER_URL` | required | For Authentik: `https://<authentik>/application/o/<app-slug>/` |
| `OIDC_CLIENT_ID` | required | |
| `OIDC_CLIENT_SECRET` | required | |
| `OIDC_ALLOWED_GROUPS` | anyone | Comma-separated. The user's `groups` claim must contain one |
| `AIRLOCK_SESSION_KEY` | required | 32 or more random characters. Encrypts the session cookie |
| `AIRLOCK_ORCHESTRATOR_URL` | required | For example `http://airlock-orchestrator:8081` |
| `AIRLOCK_API_TOKEN` | required | Same value as the orchestrator's |
| `AIRLOCK_SESSION_TTL` | `12h` | How long a sign-in lasts |
| `AIRLOCK_DEFAULT_TTL` | `1h` | Prefilled in the claim form |
| `AIRLOCK_LISTEN` | `:8080` | Listen address |

### Exposure

- Both components serve `GET /healthz`
- Expose only `web`. Restrict the orchestrator's ingress to `web` with a NetworkPolicy

## Orchestrator API

Every `/v1` request needs two headers:

- `Authorization: Bearer $AIRLOCK_API_TOKEN`
- `X-Airlock-User`: the user's OIDC subject

`X-Airlock-User-Name` is optional. The orchestrator shows it as the claim's owner.

| Endpoint | What |
|---|---|
| `GET /v1/pools` | Pools users may claim from |
| `GET /v1/claims` | The user's claims |
| `POST /v1/claims` | Create a claim: `{"pool", "name"?, "ttlMinutes"?}` |
| `GET /v1/claims/{name}` | One claim |
| `DELETE /v1/claims/{name}` | Release a claim |
| `GET /v1/claims/{name}/tasks` | The claim's tasks, newest first, with output so far |
| `POST /v1/claims/{name}/tasks` | Start a task: `{"prompt"}` |
| `GET /v1/claims/{name}/tasks/{id}` | One task, with output so far |
| `GET /v1/claims/{name}/tasks/{id}/stream` | Server-sent events: `output` chunks, then `done` |

The request and response types are in [internal/api/api.go](internal/api/api.go).

## Develop

```sh
go test -race ./...

# orchestrator, against your current kubectl context
AIRLOCK_API_TOKEN=dev go run ./cmd/orchestrator

# web (needs an OIDC client with http://localhost:8080/callback as a redirect URI)
AIRLOCK_PUBLIC_URL=http://localhost:8080 AIRLOCK_ORCHESTRATOR_URL=http://localhost:8081 \
AIRLOCK_API_TOKEN=dev AIRLOCK_SESSION_KEY=$(openssl rand -hex 32) \
OIDC_ISSUER_URL=... OIDC_CLIENT_ID=... OIDC_CLIENT_SECRET=... \
  go run ./cmd/web
```
