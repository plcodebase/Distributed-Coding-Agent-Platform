# Coding-agent happy-path E2E

The opt-in happy-path test proves that a deterministic coding task crosses the real model,
tool, workspace, and command-isolation boundaries. It is intentionally separate from the fast
unit and integration suites because it builds images and starts local Podman services.

## Scenario

The checked-in fixture contains an `add()` implementation that subtracts and a locked
`unittest` that expects addition. The fake upstream recognizes only the explicit
`[fixture:calculator-bug-v1]` marker and streams this bounded sequence through LiteLLM:

1. read `calculator.py`;
2. read `test_calculator.py`;
3. edit `calculator.py` using the fixture's exact SHA-256 precondition;
4. execute `python -m unittest -v`;
5. return a final completion.

This is not a mocked `ModelGateway`: the test uses the OpenAI Agents SDK adapter and
`GatewayClient`, with fragmented function-call arguments delivered by the OpenAI-compatible
fake upstream through LiteLLM.

## Run it

Prerequisites are a healthy rootless Podman machine, `uv`, and the local values in
`.env.example` or an equivalent private environment file.

```shell
cp .env.example .env
make e2e-happy ENV_FILE=.env
```

The target builds the fake-provider and sandbox images, starts only the two fake upstreams and
LiteLLM, runs the E2E test, and tears the services down through a shell trap on success, failure,
or interruption. It invokes the locked `podman-compose` executable with the native Podman CLI.

For a manual preflight:

```shell
podman info --format 'version={{.Version.Version}} rootless={{.Host.Security.Rootless}}'
```

The value of `rootless` must be `true`. On macOS, use the Podman-maintained installer when the
host package is missing its selected VM-provider helper; do not weaken the sandbox or substitute
host command execution.

## Assertions

The test fails unless all of the following are true:

- the original test fails inside the Podman sandbox before the agent runs;
- tool calls arrive in the expected read/read/edit/command sequence;
- all tool arguments pass the production schemas and the edit hash matches;
- the edit and command each create a checkpoint before execution;
- the sandboxed command succeeds and emits `OK` through the bounded output stream;
- event sequence numbers are contiguous and the run has the expected final text;
- the final test passes again in a new disposable command container;
- only `calculator.py` changes and the test file remains byte-identical;
- the source checkout's HEAD, status, and files remain unchanged;
- the binary-safe final patch applies cleanly to a fresh clone;
- a canary secret is absent from all serialized events and the final patch;
- two independent runs produce byte-identical patches; and
- gateway and worktree cleanup paths run even after failures, and no private worktree allocation
  remains after either execution.

## Distributed production-boundary journey

The dedicated rootless-Podman CI runner also executes:

```console
make distributed-e2e ENV_FILE=.env.example
```

On a remote macOS Podman machine, the target normally discovers the forwarded rootless socket. It
can be supplied explicitly when an enclosing filesystem sandbox prevents machine inspection:

```console
AGENT_PLATFORM_PODMAN_SOCKET=/absolute/path/to/podman-machine-api.sock \
  make distributed-e2e ENV_FILE=.env.example
```

This opt-in journey uses real PostgreSQL and MinIO services, uploads and validates a source archive
through the authenticated API, starts the production node application behind generated one-use mTLS
identities, and creates actual rootless-Podman sandboxes through the remote node protocol. The run
suspends for edit approval, resumes on a different worker, persists the completed edit checkpoint to
S3, suspends for command approval, then resumes on a third worker from that object. It verifies that
project instructions and an explicitly referenced file enter bounded model context, a restored
tracked change appears in non-mutating current-diff context, the repository tests pass only after
restore, each side-effecting tool executes once, events replay in order, and the tenant-scoped final
patch downloads with its recorded checksum. It then rewinds to the first pre-tool checkpoint, runs a
different approved edit on a new execution epoch, and proves active and abandoned branch artifacts
remain distinct and auditable.

The target removes only its derived `agent-platform-distributed` containers, network, and volumes.
It also rehearses a full Alembic downgrade-to-base and upgrade-to-head against that disposable
PostgreSQL database before the journey.
It does not use or invoke any Docker-compatible command-line tool.

Run the deterministic target on a dedicated rootless-Podman CI runner. Add a separate live-model
evaluation gate; model drift must not make the release smoke test nondeterministic.

This journey proves the local production boundary and arbitrary branch-safe rewind, not production
scale. Remaining acceptance work includes concurrent workers, real provider failover, Redis-loss
polling, API reconnect through a real network server, a rendered site Kubernetes overlay, and
immutable revision/image/report evidence from the release environment. See
`docs/architecture/production-readiness.md` for the complete closure plan.
