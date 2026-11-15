# Orchestration implementation checklist

- [x] Inspect Airlock execution, web, authentication, CI and ownership
- [x] Inspect homelab Flux, CNPG, R2, sandbox security and monitoring
- [x] PostgreSQL migrations and transactional integration tests
- [x] Durable interactive history and restart protection
- [x] Queue, attempts, leases, retries, dependencies, cancellation and recovery
- [x] Automated execution through existing SandboxClaims and Executor
- [x] Agents, workflows, schedules, messaging, approvals and bounded delegation
- [x] Operations UI and authoritative live state
- [x] Homelab database, backups, connectivity, monitoring and readiness manifests
- [x] Tests, race checks, multi-architecture builds and Kustomize rendering
- [x] Documentation and operational recovery procedures
- [ ] Full browser journey, responsive and accessibility verification (browser connection unavailable)
- [ ] Live-cluster, Agent Vault, CNPG failover and backup/restore acceptance (not deployed)
- [x] Feature branch pushes and cross-linked draft pull requests
  ([Airlock #16](https://github.com/jakehulberg/airlock/pull/16),
  [homelab #33](https://github.com/jakehulberg/homelab/pull/33))

See [verification evidence and acceptance requirements](verification.md). Unchecked verification is
not represented as complete or production-tested.
