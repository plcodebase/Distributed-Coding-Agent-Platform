# Local development

## Prerequisites

- `uv` with Python 3.12 support
- Podman 6 or newer with a running Podman machine on macOS
- `podman-compose` (installed by `make bootstrap`)
- GNU Make

## Setup

1. Copy `.env.example` to `.env`.
2. Replace the local-only placeholder passwords if the services will be reachable by
   another user or machine.
3. Run `make bootstrap`.
4. Run `make check`.
5. Run `make compose-config`.
6. On macOS, initialize and start the Podman machine if needed.
7. Run `make compose-up`.
8. Run `make migrate`.
9. Run `make compose-smoke`.
10. Run `make api` in a separate terminal.

`make compose-up` stops waiting after 180 seconds by default. Override
`COMPOSE_WAIT_TIMEOUT` when a slower machine needs more startup time. The smoke check
uses the same bound and never prints the configured gateway key. After long-running
services are healthy, setup runs the one-shot MinIO bucket initializer separately so
Podman's service wait cannot mistake its successful exit for an unhealthy dependency.

The default LiteLLM configuration targets two deterministic fake upstreams. To use real
providers, set `LITELLM_CONFIG_FILE=/config/litellm-config.yaml`, provide model IDs with
provider prefixes such as `openai/...` and `anthropic/...`, and inject both provider keys
only into the gateway environment.

## Useful endpoints

- PostgreSQL: `127.0.0.1:5432`
- Redis: `127.0.0.1:6379`
- MinIO S3 API: `http://127.0.0.1:9000`
- MinIO console: `http://127.0.0.1:9001`
- LiteLLM: `http://127.0.0.1:4000`
- Agent API: `http://127.0.0.1:8000`
- Prometheus: `http://127.0.0.1:9090`
- Grafana: `http://127.0.0.1:3000`

All session, run, approval, event, and WebSocket routes require the bearer credential
configured by `AGENT_PLATFORM_API_CREDENTIALS_JSON`. The local example contains one
non-production token. Do not put a real identity-provider credential in source control.

Use `make migration-check` after changing persistence models. Run
`make postgres-security` for the opt-in real-PostgreSQL migration, idempotency,
shared-policy, concurrent-sequence, and replay suite.
Run `make redis-security ENV_FILE=.env.example` to verify real wake-up delivery and prove a Redis
outage does not prevent durable PostgreSQL run creation or polling-based claims.

For the complete deterministic local acceptance matrix, start a healthy rootless Podman machine and
run `make local-acceptance ENV_FILE=.env.example`. Generated evaluation evidence is written beneath
`.cache/acceptance/<revision>/`; the checked-in simulation baseline is not modified.

## Local CLI

Render persisted or streamed JSON-Line events without trusting their terminal contents:

```console
agent-platform render < events.jsonl
```

Run a bounded, non-durable local session against the configured LiteLLM endpoint. Repository reads
are enabled by default and occur in a private detached worktree:

```console
agent-platform local --workspace ./sample-repository --task "Inspect the failing test"
```

Enable edits explicitly and save the final patch to a new path:

```console
agent-platform local \
  --workspace ./sample-repository \
  --task "Fix the failing test" \
  --allow-edit \
  --patch-output ./agent-result.patch
```

Commands require an additional capability and always use the rootless Podman sandbox. Build the
sandbox image with `make podman-images` first:

```console
agent-platform local \
  --workspace ./sample-repository \
  --task "Fix and test the project" \
  --allow-edit \
  --allow-commands \
  --sandbox-image localhost/agent-platform-sandbox:sequence-10 \
  --patch-output ./agent-result.patch
```

The CLI never mutates the source checkout and never exposes a Docker-compatible execution path.
Provider credentials are read from the existing `AGENT_PLATFORM_*` environment configuration;
do not pass secrets as command-line arguments. Repeat `--task` to retain bounded conversation
context across multiple runs in the same process.
