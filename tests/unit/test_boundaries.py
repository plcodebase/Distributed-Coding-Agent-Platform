import ast
from pathlib import Path

import pytest

CORE_ROOTS = (
    Path("apps/agent-worker/src"),
    Path("apps/scheduler/src"),
    Path("packages/agent-core/src"),
    Path("packages/sandbox-runtime/src"),
    Path("packages/telemetry/src"),
)
TRUSTED_COMPOSITION_ROOTS = frozenset(
    {
        Path("apps/agent-worker/src/agent_worker/production.py"),
        Path("apps/scheduler/src/agent_scheduler/production.py"),
    }
)
PERSISTENCE_ADAPTER_ROOTS = (
    Path("packages/persistence/src"),
    Path("packages/event-store/src"),
)
API_CORE_ROOTS = (
    Path("apps/agent-api/src/agent_api/app.py"),
    Path("apps/agent-api/src/agent_api/auth.py"),
    Path("apps/agent-api/src/agent_api/dependencies.py"),
    Path("apps/agent-api/src/agent_api/schemas.py"),
)
FORBIDDEN_IMPORT_ROOTS = frozenset(
    {
        "agents",
        "agents_sdk_adapter",
        "alembic",
        "asyncpg",
        "docker",
        "fastapi",
        "httpx",
        "kubernetes",
        "litellm",
        "openai",
        "podman",
        "psycopg",
        "redis",
        "sqlalchemy",
    }
)


def imported_roots(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name.partition(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module.partition(".")[0])
    return imports


@pytest.mark.parametrize(
    "path",
    sorted(
        (
            path
            for root in CORE_ROOTS
            for path in root.rglob("*.py")
            if path not in TRUSTED_COMPOSITION_ROOTS
        ),
        key=str,
    ),
    ids=str,
)
def test_foundation_packages_do_not_import_provider_adapters(path: Path) -> None:
    assert imported_roots(path).isdisjoint(FORBIDDEN_IMPORT_ROOTS)


@pytest.mark.parametrize(
    "path",
    sorted(
        (path for root in PERSISTENCE_ADAPTER_ROOTS for path in root.rglob("*.py")),
        key=str,
    ),
    ids=str,
)
def test_persistence_adapters_do_not_import_api_provider_or_runtime_layers(path: Path) -> None:
    forbidden = {
        "agents",
        "agents_sdk_adapter",
        "fastapi",
        "httpx",
        "litellm",
        "openai",
        "podman",
    }
    assert imported_roots(path).isdisjoint(forbidden)


@pytest.mark.parametrize("path", API_CORE_ROOTS, ids=str)
def test_api_handlers_depend_on_protocols_not_persistence_adapters(path: Path) -> None:
    forbidden = {
        "agents",
        "agents_sdk_adapter",
        "alembic",
        "asyncpg",
        "openai",
        "platform_persistence",
        "podman",
        "sqlalchemy",
    }
    assert imported_roots(path).isdisjoint(forbidden)
