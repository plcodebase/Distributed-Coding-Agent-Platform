"""Fail-closed static validation for the platform Kubernetes deployment contract."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

MAX_MANIFEST_BYTES = 4 * 1024 * 1024
REQUIRED_WORKLOADS = frozenset(
    {"agent-api", "event-gateway", "scheduler", "agent-worker", "litellm"}
)
PROVIDER_BUNDLE_REFERENCE = "agent-platform-provider"
PROVIDER_VARIABLES = frozenset(
    {
        "OPENAI_API_KEY",
        "ANTHROPIC_API_KEY",
        "AZURE_API_KEY",
        "GEMINI_API_KEY",
        "LITELLM_MASTER_KEY",
    }
)
EXPECTED_SECRET_ENV = {
    "agent-api": {
        "AGENT_PLATFORM_API_CREDENTIALS_JSON": (
            "agent-platform-api-runtime",
            "api-credentials-json",
        ),
        "AGENT_PLATFORM_DATABASE_URL": ("agent-platform-api-runtime", "database-url"),
    },
    "event-gateway": {
        "AGENT_PLATFORM_EVENT_CREDENTIALS_JSON": (
            "agent-platform-event-runtime",
            "event-credentials-json",
        ),
        "AGENT_PLATFORM_DATABASE_URL": ("agent-platform-event-runtime", "database-url"),
    },
    "scheduler": {
        "AGENT_PLATFORM_SCHEDULER_FACTORY": (
            "agent-platform-scheduler-runtime",
            "scheduler-factory",
        ),
        "AGENT_PLATFORM_DATABASE_URL": ("agent-platform-scheduler-runtime", "database-url"),
        "AGENT_PLATFORM_REDIS_URL": ("agent-platform-scheduler-runtime", "redis-url"),
    },
    "agent-worker": {
        "AGENT_PLATFORM_WORKER_FACTORY": ("agent-platform-worker-runtime", "worker-factory"),
        "AGENT_PLATFORM_DATABASE_URL": ("agent-platform-worker-runtime", "database-url"),
        "AGENT_PLATFORM_REDIS_URL": ("agent-platform-worker-runtime", "redis-url"),
        "AGENT_PLATFORM_OBJECT_STORE_URL": (
            "agent-platform-worker-runtime",
            "object-store-url",
        ),
        "AGENT_PLATFORM_OBJECT_STORE_ACCESS_KEY": (
            "agent-platform-worker-runtime",
            "object-store-access-key",
        ),
        "AGENT_PLATFORM_OBJECT_STORE_SECRET_KEY": (
            "agent-platform-worker-runtime",
            "object-store-secret-key",
        ),
        "AGENT_PLATFORM_GATEWAY_API_KEY": (
            "agent-platform-worker-runtime",
            "gateway-api-key",
        ),
        "AGENT_PLATFORM_WORKSPACE_SNAPSHOT_URL": (
            "agent-platform-worker-runtime",
            "workspace-snapshot-url",
        ),
    },
    "litellm": {
        "OPENAI_API_KEY": (PROVIDER_BUNDLE_REFERENCE, "openai-api-key"),
        "OPENAI_MODEL": (PROVIDER_BUNDLE_REFERENCE, "openai-model"),
        "ANTHROPIC_API_KEY": (PROVIDER_BUNDLE_REFERENCE, "anthropic-api-key"),
        "ANTHROPIC_MODEL": (PROVIDER_BUNDLE_REFERENCE, "anthropic-model"),
        "LITELLM_MASTER_KEY": (PROVIDER_BUNDLE_REFERENCE, "litellm-master-key"),
    },
}
REQUIRED_MODEL_ROUTES = frozenset(
    {"coding-default", "coding-fast", "coding-strong", "summarization", "code-review"}
)
MIN_REPLICAS = 2
MIN_TERMINATION_GRACE_SECONDS = 30


class KubernetesContractError(ValueError):
    """One static deployment invariant was violated."""


@dataclass(frozen=True, slots=True)
class KubernetesContractSummary:
    """Bounded summary emitted after all deployment invariants pass."""

    documents: int
    deployments: int
    services: int
    service_accounts: int
    network_policies: int
    disruption_budgets: int
    autoscalers: int


def load_manifest_documents(root: Path) -> tuple[dict[str, Any], ...]:
    """Load ordinary Kubernetes documents from a contained directory."""

    resolved = root.resolve(strict=True)
    if not resolved.is_dir():
        raise KubernetesContractError("manifest root must be a directory")
    files = tuple(sorted((*resolved.glob("*.yaml"), *resolved.glob("*.yml"))))
    if not files:
        raise KubernetesContractError("manifest root contains no YAML files")
    total_bytes = 0
    documents: list[dict[str, Any]] = []
    for path in files:
        if path.name == "kustomization.yaml":
            continue
        if path.resolve().parent != resolved:
            raise KubernetesContractError("manifest file escaped its root")
        payload = path.read_bytes()
        total_bytes += len(payload)
        if total_bytes > MAX_MANIFEST_BYTES:
            raise KubernetesContractError("manifest set exceeds the byte limit")
        try:
            loaded = yaml.safe_load_all(payload)
            for document in loaded:
                if not isinstance(document, dict):
                    raise KubernetesContractError(f"{path.name} contains a non-object document")
                documents.append(document)
        except yaml.YAMLError as error:
            raise KubernetesContractError(f"{path.name} contains invalid YAML") from error
    if not documents:
        raise KubernetesContractError("manifest set contains no Kubernetes documents")
    return tuple(documents)


def validate_kubernetes_contract(root: Path) -> KubernetesContractSummary:
    """Validate identity, containment, availability, and credential boundaries."""

    documents = load_manifest_documents(root)
    indexed: dict[tuple[str, str], dict[str, Any]] = {}
    for document in documents:
        kind = _text(document, "kind")
        metadata = _mapping(document, "metadata")
        name = _text(metadata, "name")
        key = (kind, name)
        if key in indexed:
            raise KubernetesContractError(f"duplicate Kubernetes object {kind}/{name}")
        indexed[key] = document

    deployments = {
        name: document for (kind, name), document in indexed.items() if kind == "Deployment"
    }
    if set(deployments) != REQUIRED_WORKLOADS:
        raise KubernetesContractError(
            "deployment set does not match the required platform services"
        )

    accounts = {
        name for kind, name in indexed if kind == "ServiceAccount" and name in REQUIRED_WORKLOADS
    }
    if accounts != REQUIRED_WORKLOADS:
        raise KubernetesContractError("every workload requires its own ServiceAccount")

    for name, deployment in deployments.items():
        _validate_deployment(name, deployment)
    _validate_provider_boundary(deployments)
    _validate_services(indexed)
    _validate_disruption_budgets(indexed)
    _validate_network_policies(indexed)
    _validate_rbac(indexed)
    _validate_autoscaling(indexed)
    _validate_gateway_routes(indexed)

    return KubernetesContractSummary(
        documents=len(documents),
        deployments=len(deployments),
        services=sum(kind == "Service" for kind, _ in indexed),
        service_accounts=sum(kind == "ServiceAccount" for kind, _ in indexed),
        network_policies=sum(kind == "NetworkPolicy" for kind, _ in indexed),
        disruption_budgets=sum(kind == "PodDisruptionBudget" for kind, _ in indexed),
        autoscalers=sum(kind == "HorizontalPodAutoscaler" for kind, _ in indexed),
    )


def _validate_deployment(name: str, deployment: Mapping[str, Any]) -> None:
    spec = _mapping(deployment, "spec")
    if _integer(spec, "replicas") < MIN_REPLICAS:
        raise KubernetesContractError(f"{name} must start with at least two replicas")
    template = _mapping(spec, "template")
    pod = _mapping(template, "spec")
    _validate_pod_contract(name, pod)
    containers = _sequence(pod, "containers")
    if len(containers) != 1 or not isinstance(containers[0], dict):
        raise KubernetesContractError(f"{name} must have one primary container")
    _validate_container_contract(name, containers[0])


def _validate_pod_contract(name: str, pod: Mapping[str, Any]) -> None:
    if _text(pod, "serviceAccountName") != name:
        raise KubernetesContractError(f"{name} must use its dedicated ServiceAccount")
    if pod.get("automountServiceAccountToken") is not False:
        raise KubernetesContractError(f"{name} must disable Kubernetes API token mounting")
    if pod.get("enableServiceLinks") is not False:
        raise KubernetesContractError(f"{name} must disable service-link environment injection")
    for host_setting in ("hostNetwork", "hostPID", "hostIPC"):
        if pod.get(host_setting) is not False:
            raise KubernetesContractError(f"{name} must disable {host_setting}")
    if _integer(pod, "terminationGracePeriodSeconds") < MIN_TERMINATION_GRACE_SECONDS:
        raise KubernetesContractError(f"{name} termination grace period is too short")
    pod_security = _mapping(pod, "securityContext")
    if pod_security.get("runAsNonRoot") is not True:
        raise KubernetesContractError(f"{name} must run as non-root")
    if _mapping(pod_security, "seccompProfile").get("type") != "RuntimeDefault":
        raise KubernetesContractError(f"{name} must use RuntimeDefault seccomp")
    if not _sequence(pod, "topologySpreadConstraints"):
        raise KubernetesContractError(f"{name} requires topology spread constraints")
    anti_affinity = _mapping(_mapping(pod, "affinity"), "podAntiAffinity")
    if not any(
        isinstance(anti_affinity.get(key), list) and anti_affinity[key]
        for key in (
            "preferredDuringSchedulingIgnoredDuringExecution",
            "requiredDuringSchedulingIgnoredDuringExecution",
        )
    ):
        raise KubernetesContractError(f"{name} requires pod anti-affinity")


def _validate_container_contract(name: str, container: Mapping[str, Any]) -> None:
    for probe in ("readinessProbe", "livenessProbe"):
        _mapping(container, probe)
    resources = _mapping(container, "resources")
    for section in ("requests", "limits"):
        values = _mapping(resources, section)
        if not {"cpu", "memory"}.issubset(values):
            raise KubernetesContractError(f"{name} must bound CPU and memory {section}")
    security = _mapping(container, "securityContext")
    if security.get("allowPrivilegeEscalation") is not False:
        raise KubernetesContractError(f"{name} may not allow privilege escalation")
    if security.get("readOnlyRootFilesystem") is not True:
        raise KubernetesContractError(f"{name} requires a read-only root filesystem")
    dropped = _sequence(_mapping(security, "capabilities"), "drop")
    if "ALL" not in dropped:
        raise KubernetesContractError(f"{name} must drop all Linux capabilities")
    image = _text(container, "image")
    if image.endswith(":latest") or ":main-latest" in image:
        raise KubernetesContractError(f"{name} image must use a fixed version")

    if name == "agent-worker":
        lifecycle = _mapping(container, "lifecycle")
        command = _sequence(_mapping(_mapping(lifecycle, "preStop"), "exec"), "command")
        if not any(isinstance(item, str) and "/drain" in item for item in command):
            raise KubernetesContractError("worker requires a graceful drain preStop hook")


def _validate_worker_scheduling(deployment: Mapping[str, Any]) -> None:
    pod = _mapping(_mapping(_mapping(deployment, "spec"), "template"), "spec")
    selector = _mapping(pod, "nodeSelector")
    if selector.get("agent-platform.openai.com/sandbox-worker") != "true":
        raise KubernetesContractError("workers must target dedicated sandbox nodes")


def _validate_provider_boundary(deployments: Mapping[str, Mapping[str, Any]]) -> None:
    _validate_worker_scheduling(deployments["agent-worker"])
    for name, deployment in deployments.items():
        template = _mapping(_mapping(deployment, "spec"), "template")
        container = _sequence(_mapping(template, "spec"), "containers")[0]
        if not isinstance(container, dict):
            raise KubernetesContractError(f"{name} container is invalid")
        env_from = container.get("envFrom", [])
        if not isinstance(env_from, list):
            raise KubernetesContractError(f"{name} envFrom must be an array")
        if any(isinstance(item, dict) and "secretRef" in item for item in env_from):
            raise KubernetesContractError(f"{name} may not import a complete Secret")
        observed_secret_env: dict[str, tuple[str, str]] = {}
        for item in container.get("env", []):
            if not isinstance(item, dict) or not isinstance(item.get("name"), str):
                raise KubernetesContractError(f"{name} environment entry is invalid")
            variable = item["name"]
            value_from = item.get("valueFrom")
            if not isinstance(value_from, dict):
                continue
            secret_ref = value_from.get("secretKeyRef")
            if not isinstance(secret_ref, dict):
                continue
            secret_name = secret_ref.get("name")
            secret_key = secret_ref.get("key")
            if not isinstance(secret_name, str) or not isinstance(secret_key, str):
                raise KubernetesContractError(f"{name} Secret reference is invalid")
            observed_secret_env[variable] = (secret_name, secret_key)
        if observed_secret_env != EXPECTED_SECRET_ENV[name]:
            raise KubernetesContractError(f"{name} Secret key references do not match its role")
        if name != "litellm" and set(observed_secret_env) & PROVIDER_VARIABLES:
            raise KubernetesContractError(f"provider credentials leaked into {name}")

    event_container = _sequence(
        _mapping(
            _mapping(_mapping(deployments["event-gateway"], "spec"), "template"),
            "spec",
        ),
        "containers",
    )[0]
    if not isinstance(event_container, dict) or not any(
        isinstance(argument, str)
        and "event_factory:create_production_event_gateway_app" in argument
        for argument in event_container.get("args", [])
    ):
        raise KubernetesContractError("event gateway must use the event-only application factory")


def _validate_services(indexed: Mapping[tuple[str, str], Mapping[str, Any]]) -> None:
    services = {name: document for (kind, name), document in indexed.items() if kind == "Service"}
    if set(services) != REQUIRED_WORKLOADS:
        raise KubernetesContractError("every workload requires one matching Service")
    for name, service in services.items():
        spec = _mapping(service, "spec")
        if _mapping(spec, "selector").get("app.kubernetes.io/name") != name:
            raise KubernetesContractError(f"{name} Service selector does not match its workload")
        ports = _sequence(spec, "ports")
        if len(ports) != 1 or not isinstance(ports[0], dict):
            raise KubernetesContractError(f"{name} Service must expose one named port")
        if not isinstance(ports[0].get("targetPort"), str):
            raise KubernetesContractError(f"{name} Service must target a named container port")


def _validate_disruption_budgets(indexed: Mapping[tuple[str, str], Mapping[str, Any]]) -> None:
    budgets = {name for kind, name in indexed if kind == "PodDisruptionBudget"}
    if budgets != REQUIRED_WORKLOADS:
        raise KubernetesContractError("every workload requires one disruption budget")


def _validate_network_policies(indexed: Mapping[tuple[str, str], Mapping[str, Any]]) -> None:
    policies = {
        name: document for (kind, name), document in indexed.items() if kind == "NetworkPolicy"
    }
    default_deny = policies.get("default-deny")
    if default_deny is None:
        raise KubernetesContractError("a namespace default-deny policy is required")
    spec = _mapping(default_deny, "spec")
    if _mapping(spec, "podSelector") or set(_sequence(spec, "policyTypes")) != {
        "Ingress",
        "Egress",
    }:
        raise KubernetesContractError(
            "default-deny must select all pods and both traffic directions"
        )
    for required in (
        "allow-dns",
        "allow-platform-to-litellm",
        "allow-worker-to-litellm",
        "allow-litellm-provider-egress",
        "allow-operations-scrape",
        "allow-api-database",
        "allow-scheduler-data-services",
        "allow-worker-data-services",
    ):
        if required not in policies:
            raise KubernetesContractError(f"missing NetworkPolicy {required}")

    litellm_ingress = _sequence(_mapping(policies["allow-platform-to-litellm"], "spec"), "ingress")
    if "agent-api" in str(litellm_ingress) or "event-gateway" in str(litellm_ingress):
        raise KubernetesContractError("only workers may enter the model gateway")


def _validate_rbac(indexed: Mapping[tuple[str, str], Mapping[str, Any]]) -> None:
    role = indexed.get(("Role", "workload-no-kubernetes-api"))
    if role is None or role.get("rules") != []:
        raise KubernetesContractError("workloads must have no Kubernetes API permissions")


def _validate_autoscaling(indexed: Mapping[tuple[str, str], Mapping[str, Any]]) -> None:
    expected = {
        "agent-api": {
            "resource:cpu",
            "external:agent_platform_api_requests_per_second",
        },
        "agent-worker": {
            "external:agent_platform_queue_depth_total",
            "external:agent_platform_oldest_queued_seconds",
        },
        "litellm": {
            "external:litellm_active_requests",
            "external:litellm_request_latency_p95_seconds",
        },
    }
    autoscalers = {
        name: document
        for (kind, name), document in indexed.items()
        if kind == "HorizontalPodAutoscaler"
    }
    if set(autoscalers) != set(expected):
        raise KubernetesContractError(
            "autoscaler set does not match the required scalable services"
        )
    for name, required_metrics in expected.items():
        spec = _mapping(autoscalers[name], "spec")
        if _integer(spec, "minReplicas") < MIN_REPLICAS:
            raise KubernetesContractError(f"{name} HPA minimum is too low")
        if _integer(spec, "maxReplicas") <= _integer(spec, "minReplicas"):
            raise KubernetesContractError(f"{name} HPA range is invalid")
        observed: set[str] = set()
        for metric in _sequence(spec, "metrics"):
            if not isinstance(metric, dict):
                raise KubernetesContractError(f"{name} HPA metric must be an object")
            metric_type = _text(metric, "type").lower()
            details = _mapping(metric, metric_type)
            metric_name = (
                _text(_mapping(details, "metric"), "name")
                if metric_type == "external"
                else _text(details, "name")
            )
            observed.add(f"{metric_type}:{metric_name}")
        if observed != required_metrics:
            raise KubernetesContractError(f"{name} HPA metrics do not match the scaling contract")

    deployments = {
        name: document for (kind, name), document in indexed.items() if kind == "Deployment"
    }
    for name, deployment in deployments.items():
        metadata = _mapping(_mapping(_mapping(deployment, "spec"), "template"), "metadata")
        annotations = _mapping(metadata, "annotations")
        if annotations.get("prometheus.io/scrape") != "true":
            raise KubernetesContractError(f"{name} must opt into metrics discovery")
        path = annotations.get("prometheus.io/path")
        port = annotations.get("prometheus.io/port")
        if path != "/metrics" or not isinstance(port, str) or not port.isdecimal():
            raise KubernetesContractError(f"{name} metrics discovery annotation is invalid")
        container = _sequence(
            _mapping(_mapping(_mapping(deployment, "spec"), "template"), "spec"),
            "containers",
        )[0]
        if not isinstance(container, dict):
            raise KubernetesContractError(f"{name} container is invalid")
        container_ports = {
            item.get("containerPort")
            for item in container.get("ports", [])
            if isinstance(item, dict)
        }
        if int(port) not in container_ports:
            raise KubernetesContractError(f"{name} metrics port is not exposed by its container")


def _validate_gateway_routes(indexed: Mapping[tuple[str, str], Mapping[str, Any]]) -> None:
    config = indexed.get(("ConfigMap", "litellm-config"))
    if config is None:
        raise KubernetesContractError("LiteLLM configuration is missing")
    raw = _mapping(config, "data").get("config.yaml")
    if not isinstance(raw, str):
        raise KubernetesContractError("LiteLLM configuration must be text")
    try:
        parsed = yaml.safe_load(raw)
    except yaml.YAMLError as error:
        raise KubernetesContractError("LiteLLM configuration is invalid") from error
    if not isinstance(parsed, dict) or not isinstance(parsed.get("model_list"), list):
        raise KubernetesContractError("LiteLLM model list is invalid")
    routes = {
        item.get("model_name")
        for item in parsed["model_list"]
        if isinstance(item, dict) and isinstance(item.get("model_name"), str)
    }
    if routes != REQUIRED_MODEL_ROUTES:
        raise KubernetesContractError("LiteLLM routes do not match the platform contract")


def _mapping(value: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    item = value.get(key)
    if not isinstance(item, dict):
        raise KubernetesContractError(f"{key} must be an object")
    return item


def _sequence(value: Mapping[str, Any], key: str) -> Sequence[Any]:
    item = value.get(key)
    if not isinstance(item, list):
        raise KubernetesContractError(f"{key} must be an array")
    return item


def _text(value: Mapping[str, Any], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise KubernetesContractError(f"{key} must be non-empty text")
    return item


def _integer(value: Mapping[str, Any], key: str) -> int:
    item = value.get(key)
    if type(item) is not int:
        raise KubernetesContractError(f"{key} must be an integer")
    return item


def _format_summary(summary: KubernetesContractSummary) -> str:
    return (
        f"validated {summary.documents} documents: "
        f"{summary.deployments} deployments, {summary.services} services, "
        f"{summary.service_accounts} service accounts, "
        f"{summary.network_policies} network policies, "
        f"{summary.disruption_budgets} disruption budgets, and "
        f"{summary.autoscalers} autoscalers"
    )


def main(arguments: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "root",
        nargs="?",
        type=Path,
        default=Path("deployments/kubernetes/base"),
    )
    parsed = parser.parse_args(arguments)
    print(_format_summary(validate_kubernetes_contract(parsed.root)))  # noqa: T201


if __name__ == "__main__":
    main()
