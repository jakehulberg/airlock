# Airlock

A web UI for claiming agent sandboxes ([kubernetes-sigs/agent-sandbox](https://github.com/kubernetes-sigs/agent-sandbox)) and running agent tasks in them.

This repo is the code and CI only. The deploy manifests live in the homelab repo and are applied by Flux.

## How it fits together

```
browser ──OIDC──▶ web ──bearer token + user headers──▶ orchestrator ──▶ Kubernetes API
                (Authentik)                                             SandboxClaim, pods/exec
```

| Component | Does | Talks to |
|---|---|---|
| `web` | Signs users in with OIDC, serves the UI | the orchestrator only |
| `orchestrator` | Creates and deletes `SandboxClaim`s, runs tasks with `pods/exec`, streams output | the Kubernetes API |

- A user picks a `SandboxWarmPool`, a name, and a TTL. The orchestrator creates a `SandboxClaim` with `shutdownTime` and `shutdownPolicy: Delete`, so every sandbox expires.
- Claims are labeled with a hash of the user's OIDC subject. Users only see and touch their own.
- A task runs a command in the agent container with the prompt on stdin. The default sources `/session/env` (the Agent Vault proxy settings the session sidecar writes) and runs `claude -p`; each pool can name its own agent (see [Pools](#pools)). One task per sandbox at a time.
- Task output is kept in the orchestrator's memory (1 MiB per task, 500 tasks). It's lost on restart, so run one replica.

## Images

Every push to `main` builds `linux/amd64` and `linux/arm64` images:

- `ghcr.io/jakehulberg/airlock-orchestrator:<tag>`
- `ghcr.io/jakehulberg/airlock-web:<tag>`

Tags are `YYYYMMDDTHHMMSS-<sha7>` (UTC build time), the same scheme as life-dashboard, so a Flux `ImagePolicy` can pick the newest by the timestamp. Both images from one commit share the tag.

## Configuration

### orchestrator

| Variable | Default | |
|---|---|---|
| `AIRLOCK_API_TOKEN` | required | Shared with `web` |
| `AIRLOCK_NAMESPACE` | `agents` | Where pools and claims live |
| `AIRLOCK_POOL_SELECTOR` | all pools | Label selector for the pools users may claim from |
| `AIRLOCK_DEFAULT_TTL` | `1h` | |
| `AIRLOCK_MAX_TTL` | `8h` | |
| `AIRLOCK_AGENT_CONTAINER` | `agent` | Default; a pool can override it |
| `AIRLOCK_TASK_COMMAND` | `. /session/env 2>/dev/null; exec claude -p` | Default; a pool can override it. Run with `sh -c`, prompt on stdin |
| `AIRLOCK_TASK_TIMEOUT` | `30m` | |
| `AIRLOCK_LISTEN` | `:8081` | |

Needs a ServiceAccount with this Role in `AIRLOCK_NAMESPACE`:

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

Annotations on a `SandboxWarmPool`, all optional:

| Annotation | |
|---|---|
| `airlock.jakehulberg.com/description` | Shown in the claim form |
| `airlock.jakehulberg.com/agent-container` | The container tasks run in. Default `AIRLOCK_AGENT_CONTAINER` |
| `airlock.jakehulberg.com/task-command` | Run with `sh -c`, prompt on stdin. Default `AIRLOCK_TASK_COMMAND` |

So pools for different agents sit side by side, e.g. a Codex pool:

```yaml
metadata:
  annotations:
    airlock.jakehulberg.com/description: Codex
    airlock.jakehulberg.com/task-command: . /session/env 2>/dev/null; exec codex exec -
```

Only cluster admins can edit pools; the orchestrator can only read them.

### web

| Variable | Default | |
|---|---|---|
| `AIRLOCK_PUBLIC_URL` | required | e.g. `https://airlock.home.jakehulberg.com`. The redirect URI is `<this>/callback` |
| `OIDC_ISSUER_URL` | required | Authentik: `https://<authentik>/application/o/<app-slug>/` |
| `OIDC_CLIENT_ID` | required | |
| `OIDC_CLIENT_SECRET` | required | |
| `OIDC_ALLOWED_GROUPS` | anyone | Comma-separated; checked against the `groups` claim |
| `AIRLOCK_SESSION_KEY` | required | 32+ random characters; encrypts the session cookie |
| `AIRLOCK_ORCHESTRATOR_URL` | required | e.g. `http://airlock-orchestrator:8081` |
| `AIRLOCK_API_TOKEN` | required | Same as the orchestrator's |
| `AIRLOCK_SESSION_TTL` | `12h` | |
| `AIRLOCK_DEFAULT_TTL` | `1h` | Prefilled in the claim form |
| `AIRLOCK_LISTEN` | `:8080` | |

Both serve `GET /healthz`. Only `web` should be exposed; restrict the orchestrator's ingress to `web` with a NetworkPolicy.

## Orchestrator API

Every `/v1` request needs `Authorization: Bearer $AIRLOCK_API_TOKEN` and `X-Airlock-User` (the OIDC subject). `X-Airlock-User-Name` is shown as the owner.

| | |
|---|---|
| `GET /v1/pools` | Pools users may claim from |
| `GET /v1/claims` | The user's claims |
| `POST /v1/claims` | `{"pool", "name"?, "ttlMinutes"?}` |
| `GET /v1/claims/{name}` | |
| `DELETE /v1/claims/{name}` | Release |
| `GET /v1/claims/{name}/tasks` | |
| `POST /v1/claims/{name}/tasks` | `{"prompt"}` |
| `GET /v1/claims/{name}/tasks/{id}` | With output so far |
| `GET /v1/claims/{name}/tasks/{id}/stream` | Server-sent events: `output` chunks, then `done` |

Types are in [internal/api/api.go](internal/api/api.go).

## Develop

```sh
go test -race ./...

# orchestrator against your current kubectl context
AIRLOCK_API_TOKEN=dev go run ./cmd/orchestrator

# web (needs an OIDC client with http://localhost:8080/callback as a redirect URI)
AIRLOCK_PUBLIC_URL=http://localhost:8080 AIRLOCK_ORCHESTRATOR_URL=http://localhost:8081 \
AIRLOCK_API_TOKEN=dev AIRLOCK_SESSION_KEY=$(openssl rand -hex 32) \
OIDC_ISSUER_URL=... OIDC_CLIENT_ID=... OIDC_CLIENT_SECRET=... \
  go run ./cmd/web
```
