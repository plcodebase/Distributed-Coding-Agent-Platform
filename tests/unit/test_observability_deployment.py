from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, cast

import yaml

from platform_telemetry import PlatformMetrics

ROOT = Path(__file__).parents[2]
DEPLOYMENT = ROOT / "deployments" / "podman-compose"
DASHBOARD_ROOT = DEPLOYMENT / "grafana" / "dashboards"
DATASOURCE_UID = "agent-platform-prometheus"
_METRIC_REFERENCE = re.compile(r"\bagent_platform(?:_|:)[a-zA-Z0-9_:]+")
_PROMETHEUS_SUFFIXES = ("_bucket", "_count", "_sum", "_created")


def _yaml(path: Path) -> dict[str, Any]:
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return cast("dict[str, Any]", value)


def _dashboards() -> tuple[dict[str, Any], ...]:
    values: list[dict[str, Any]] = []
    for path in sorted((DASHBOARD_ROOT / "json").glob("*.json")):
        value = json.loads(path.read_text(encoding="utf-8"))
        assert isinstance(value, dict)
        values.append(cast("dict[str, Any]", value))
    return tuple(values)


def _expressions(value: object) -> tuple[str, ...]:
    expressions: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "expr" and isinstance(item, str):
                expressions.append(item)
            else:
                expressions.extend(_expressions(item))
    elif isinstance(value, list):
        for item in value:
            expressions.extend(_expressions(item))
    return tuple(expressions)


def _metric_bases() -> set[str]:
    payload = PlatformMetrics().render().decode("utf-8")
    return {
        match.group(1)
        for match in re.finditer(r"^# HELP (agent_platform_[a-zA-Z0-9_]+) ", payload, re.M)
    }


def _is_exported(reference: str, bases: set[str]) -> bool:
    if reference in bases:
        return True
    return any(reference.removesuffix(suffix) in bases for suffix in _PROMETHEUS_SUFFIXES)


def test_prometheus_loads_versioned_platform_rules_from_read_only_mount() -> None:
    compose = _yaml(ROOT / "compose.yaml")
    prometheus = cast("dict[str, Any]", compose["services"])["prometheus"]
    volumes = cast("list[str]", prometheus["volumes"])
    assert "./deployments/podman-compose/prometheus-rules:/etc/prometheus/rules:ro" in volumes

    configuration = _yaml(DEPLOYMENT / "prometheus.yml")
    assert configuration["rule_files"] == ["/etc/prometheus/rules/*.yml"]
    jobs = {
        item["job_name"] for item in cast("list[dict[str, Any]]", configuration["scrape_configs"])
    }
    assert {"prometheus", "litellm", "agent-platform-host"} <= jobs


def test_grafana_provisioning_is_immutable_and_uses_stable_uids() -> None:
    compose = _yaml(ROOT / "compose.yaml")
    grafana = cast("dict[str, Any]", compose["services"])["grafana"]
    environment = cast("dict[str, str]", grafana["environment"])
    assert environment["GF_AUTH_ANONYMOUS_ENABLED"] == "false"
    assert environment["GF_USERS_ALLOW_SIGN_UP"] == "false"
    datasource = _yaml(DASHBOARD_ROOT.parent / "datasources" / "prometheus.yaml")
    configured = cast("list[dict[str, Any]]", datasource["datasources"])
    assert configured == [
        {
            "name": "Prometheus",
            "uid": DATASOURCE_UID,
            "type": "prometheus",
            "access": "proxy",
            "url": "http://prometheus:9090",
            "isDefault": True,
            "editable": False,
        }
    ]
    provider = _yaml(DASHBOARD_ROOT / "provider.yaml")
    providers = cast("list[dict[str, Any]]", provider["providers"])
    assert len(providers) == 1
    assert providers[0]["folderUid"] == "agent-platform"
    assert providers[0]["disableDeletion"] is True
    assert providers[0]["allowUiUpdates"] is False
    assert providers[0]["options"]["path"] == ("/etc/grafana/provisioning/dashboards/json")


def test_dashboards_cover_required_operational_views_without_content_fields() -> None:
    dashboards = _dashboards()
    assert len(dashboards) == 2
    assert len({dashboard["uid"] for dashboard in dashboards}) == len(dashboards)
    panels = [
        panel
        for dashboard in dashboards
        for panel in cast("list[dict[str, Any]]", dashboard["panels"])
    ]
    titles = {str(panel["title"]) for panel in panels}
    assert {
        "Active runs",
        "Queue depth",
        "Oldest queued run",
        "Worker utilization",
        "Sandbox startup p95",
        "Provider success rate",
        "Model latency",
        "Token rate",
        "Cost by opaque tenant",
        "Gateway retries",
        "Gateway fallbacks",
        "Circuit state",
    } <= titles
    for panel in panels:
        assert panel["datasource"] == {"type": "prometheus", "uid": DATASOURCE_UID}
        assert cast("list[dict[str, Any]]", panel["targets"])
    rendered = json.dumps(dashboards, sort_keys=True).casefold()
    for prohibited in ("prompt", "source_code", "tool_arguments", "tool_result", "api_key"):
        assert prohibited not in rendered


def test_dashboard_and_rule_queries_reference_exported_or_recorded_metrics() -> None:
    rules = _yaml(DEPLOYMENT / "prometheus-rules" / "platform.yml")
    groups = cast("list[dict[str, Any]]", rules["groups"])
    records = {
        str(rule["record"])
        for group in groups
        for rule in cast("list[dict[str, Any]]", group["rules"])
        if "record" in rule
    }
    bases = _metric_bases()
    expressions = (*_expressions(rules), *(_expressions(item) for item in _dashboards()))
    flattened = tuple(
        expression
        for value in expressions
        for expression in ((value,) if isinstance(value, str) else value)
    )
    for expression in flattened:
        for reference in _METRIC_REFERENCE.findall(expression):
            assert reference in records or _is_exported(reference, bases), reference


def test_alerts_have_stable_severity_duration_and_content_free_annotations() -> None:
    rules = _yaml(DEPLOYMENT / "prometheus-rules" / "platform.yml")
    alerts = [
        rule
        for group in cast("list[dict[str, Any]]", rules["groups"])
        for rule in cast("list[dict[str, Any]]", group["rules"])
        if "alert" in rule
    ]
    assert {rule["alert"] for rule in alerts} == {
        "AgentPlatformQueueBacklogOld",
        "AgentPlatformWorkersSaturated",
        "AgentPlatformProviderSuccessLow",
        "AgentPlatformApiErrorsHigh",
        "AgentPlatformGatewayCircuitOpen",
        "AgentPlatformSandboxStartupSlow",
    }
    for alert in alerts:
        assert re.fullmatch(r"[1-9][0-9]*[ms]", str(alert["for"]))
        assert alert["labels"]["severity"] in {"warning", "critical"}
        annotation = json.dumps(alert["annotations"], sort_keys=True).casefold()
        assert "secret" not in annotation
        assert "source" not in annotation
