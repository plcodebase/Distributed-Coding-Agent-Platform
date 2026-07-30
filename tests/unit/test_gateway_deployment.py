from pathlib import Path
from typing import Any, cast

import yaml

ROOT = Path(__file__).parents[2]
ROUTES = {
    "coding-default",
    "coding-fast",
    "coding-strong",
    "summarization",
    "code-review",
}


def _load_yaml(path: Path) -> dict[str, Any]:
    value = yaml.safe_load(path.read_text())
    assert isinstance(value, dict)
    return cast("dict[str, Any]", value)


def _models(configuration: dict[str, Any]) -> dict[str, dict[str, Any]]:
    model_list = configuration["model_list"]
    assert isinstance(model_list, list)
    return {
        str(item["model_name"]): cast("dict[str, Any]", item["litellm_params"])
        for item in model_list
    }


def test_podman_compose_gateway_is_local_hardened_and_secret_scoped() -> None:
    compose_path = ROOT / "compose.yaml"
    raw = compose_path.read_text()
    compose = _load_yaml(compose_path)
    services = cast("dict[str, dict[str, Any]]", compose["services"])

    assert "dockerfile" not in raw.casefold()
    for name in ("fake-llm-primary", "fake-llm-secondary"):
        service = services[name]
        assert service["image"] == "localhost/agent-platform-fake-llm:local"
        assert "build" not in service
        assert service["read_only"] is True
        assert service["cap_drop"] == ["ALL"]
        assert service["security_opt"] == ["no-new-privileges"]
        assert service["networks"] == ["llm-upstreams"]

    gateway = services["litellm"]
    assert gateway["image"] == "${LITELLM_IMAGE:-ghcr.io/berriai/litellm:v1.89.4}"
    assert gateway["ports"] == ["127.0.0.1:4000:4000"]
    assert gateway["networks"] == ["llm-egress", "llm-upstreams"]
    assert gateway["read_only"] is True
    assert gateway["cap_drop"] == ["ALL"]
    assert gateway["security_opt"] == ["no-new-privileges"]
    assert "./services/llm-gateway:/config:ro" in gateway["volumes"]
    assert compose["networks"] == {
        "default": {},
        "llm-egress": {},
        "llm-upstreams": {"internal": True},
    }

    provider_keys = {"OPENAI_API_KEY", "ANTHROPIC_API_KEY"}
    for name, service in services.items():
        environment = service.get("environment", {})
        assert isinstance(environment, dict)
        if name == "litellm":
            assert provider_keys <= environment.keys()
        else:
            assert provider_keys.isdisjoint(environment.keys())


def test_fake_gateway_exposes_all_routes_across_two_deployments() -> None:
    configuration = _load_yaml(ROOT / "services" / "llm-gateway" / "litellm-config.fake.yaml")
    models = _models(configuration)

    assert set(models) == ROUTES
    assert {params["api_base"] for params in models.values()} == {
        "http://fake-llm-primary:8080/v1",
        "http://fake-llm-secondary:8080/v1",
    }
    assert {params["timeout"] for params in models.values()} == {2}
    assert models["coding-default"]["api_base"].endswith("fake-llm-primary:8080/v1")
    assert models["coding-fast"]["api_base"].endswith("fake-llm-primary:8080/v1")
    assert models["summarization"]["api_base"].endswith("fake-llm-primary:8080/v1")
    assert models["coding-strong"]["api_base"].endswith("fake-llm-secondary:8080/v1")
    assert models["code-review"]["api_base"].endswith("fake-llm-secondary:8080/v1")
    assert configuration["general_settings"]["master_key"] == ("os.environ/LITELLM_MASTER_KEY")


def test_production_gateway_routes_across_two_provider_families() -> None:
    configuration = _load_yaml(ROOT / "services" / "llm-gateway" / "litellm-config.yaml")
    models = _models(configuration)

    assert set(models) == ROUTES
    assert {(params["model"], params["api_key"]) for params in models.values()} == {
        ("os.environ/OPENAI_MODEL", "os.environ/OPENAI_API_KEY"),
        ("os.environ/ANTHROPIC_MODEL", "os.environ/ANTHROPIC_API_KEY"),
    }
    assert configuration["general_settings"]["master_key"] == ("os.environ/LITELLM_MASTER_KEY")


def test_gateway_router_policy_is_explicit_and_bounded() -> None:
    for name in ("litellm-config.yaml", "litellm-config.fake.yaml"):
        configuration = _load_yaml(ROOT / "services" / "llm-gateway" / name)
        router = configuration["router_settings"]

        assert router["num_retries"] == 2
        assert router["retry_after"] == 1
        assert router["allowed_fails"] == 5
        assert router["cooldown_time"] == 60
        assert router["fallbacks"] == [
            {"coding-default": ["coding-strong"]},
            {"coding-fast": ["coding-default"]},
            {"code-review": ["coding-strong"]},
        ]
