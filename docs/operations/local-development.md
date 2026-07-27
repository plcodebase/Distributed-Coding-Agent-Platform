# Local development

## Prerequisites

- `uv` with Python 3.12 support
- Podman 5 or newer with a running Podman machine on macOS
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
8. Run `make compose-smoke`.

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
- Prometheus: `http://127.0.0.1:9090`
- Grafana: `http://127.0.0.1:3000`
