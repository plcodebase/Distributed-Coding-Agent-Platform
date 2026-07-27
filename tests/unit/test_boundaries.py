import ast
from pathlib import Path

import pytest

CORE_ROOTS = (
    Path("packages/agent-core/src"),
    Path("packages/telemetry/src"),
)
FORBIDDEN_IMPORT_ROOTS = frozenset(
    {
        "alembic",
        "asyncpg",
        "docker",
        "fastapi",
        "kubernetes",
        "litellm",
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
        (path for root in CORE_ROOTS for path in root.rglob("*.py")),
        key=str,
    ),
    ids=str,
)
def test_foundation_packages_do_not_import_provider_adapters(path: Path) -> None:
    assert imported_roots(path).isdisjoint(FORBIDDEN_IMPORT_ROOTS)
