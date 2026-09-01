from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import pytest
import yaml
from scripts.kubernetes_contracts import (
    KubernetesContractError,
    load_manifest_documents,
    main,
    validate_kubernetes_contract,
    validate_production_admission_contract,
)

ROOT = Path("deployments/kubernetes/base")
PRODUCTION = Path("deployments/kubernetes/production")


def test_base_kubernetes_contract_is_complete_and_hardened() -> None:
    summary = validate_kubernetes_contract(ROOT)

    assert summary.deployments == 5
    assert summary.daemonsets == 1
    assert summary.services == 6
    assert summary.service_accounts == 6
    assert summary.disruption_budgets == 5
    assert summary.network_policies >= 7


def test_production_admission_contract_is_fail_closed() -> None:
    validate_production_admission_contract(PRODUCTION)


def test_production_admission_contract_rejects_audit_only_binding(tmp_path: Path) -> None:
    documents = [copy.deepcopy(item) for item in load_manifest_documents(PRODUCTION)]
    binding = next(item for item in documents if item["kind"] == "ValidatingAdmissionPolicyBinding")
    binding["spec"]["validationActions"] = ["Audit"]
    _write_documents(tmp_path, documents)

    with pytest.raises(KubernetesContractError, match="must deny"):
        validate_production_admission_contract(tmp_path)


def test_manifest_validator_cli_reports_bounded_summary(capsys: pytest.CaptureFixture[str]) -> None:
    main((str(ROOT),))

    assert capsys.readouterr().out.startswith("validated 47 documents")


def test_secrets_are_referenced_by_exact_keys_and_provider_keys_are_litellm_only() -> None:
    documents = load_manifest_documents(ROOT)
    deployments = {
        document["metadata"]["name"]: document
        for document in documents
        if document["kind"] == "Deployment"
    }

    for name, deployment in deployments.items():
        container = deployment["spec"]["template"]["spec"]["containers"][0]
        assert all("secretRef" not in item for item in container.get("envFrom", []))
        secret_names = {
            item["valueFrom"]["secretKeyRef"]["name"]
            for item in container.get("env", [])
            if "secretKeyRef" in item.get("valueFrom", {})
        }
        assert ("agent-platform-provider" in secret_names) is (name == "litellm")


def test_no_secret_objects_or_literal_provider_credentials_are_committed() -> None:
    documents = load_manifest_documents(ROOT)

    assert all(document["kind"] != "Secret" for document in documents)
    manifest_text = "\n".join(path.read_text() for path in ROOT.glob("*.yaml"))
    assert "sk-" not in manifest_text
    assert "ANTHROPIC_API_KEY:" not in manifest_text
    assert "OPENAI_API_KEY:" not in manifest_text


@pytest.mark.parametrize(
    ("mutation", "expected"),
    [
        (lambda deployment: deployment["spec"].update(replicas=1), "at least two replicas"),
        (
            lambda deployment: deployment["spec"]["template"]["spec"].update(
                automountServiceAccountToken=True
            ),
            "disable Kubernetes API token",
        ),
        (
            lambda deployment: deployment["spec"]["template"]["spec"]["containers"][0][
                "securityContext"
            ].update(readOnlyRootFilesystem=False),
            "read-only root filesystem",
        ),
        (
            lambda deployment: deployment["spec"]["template"]["spec"]["containers"][0].update(
                image="example:latest"
            ),
            "fixed version",
        ),
    ],
)
def test_validator_rejects_weakened_workload_contract(
    tmp_path: Path,
    mutation: Any,
    expected: str,
) -> None:
    documents = [copy.deepcopy(item) for item in load_manifest_documents(ROOT)]
    deployment = next(item for item in documents if item["kind"] == "Deployment")
    mutation(deployment)
    _write_documents(tmp_path, documents)

    with pytest.raises(KubernetesContractError, match=expected):
        validate_kubernetes_contract(tmp_path)


def test_validator_rejects_provider_secret_in_worker(tmp_path: Path) -> None:
    documents = [copy.deepcopy(item) for item in load_manifest_documents(ROOT)]
    worker = next(
        item
        for item in documents
        if item["kind"] == "Deployment" and item["metadata"]["name"] == "agent-worker"
    )
    worker["spec"]["template"]["spec"]["containers"][0]["envFrom"].append(
        {"secretRef": {"name": "agent-platform-provider"}}
    )
    _write_documents(tmp_path, documents)

    with pytest.raises(KubernetesContractError, match="may not import a complete Secret"):
        validate_kubernetes_contract(tmp_path)


def test_validator_rejects_role_secret_key_and_event_factory_drift(tmp_path: Path) -> None:
    documents = [copy.deepcopy(item) for item in load_manifest_documents(ROOT)]
    event_gateway = next(
        item
        for item in documents
        if item["kind"] == "Deployment" and item["metadata"]["name"] == "event-gateway"
    )
    container = event_gateway["spec"]["template"]["spec"]["containers"][0]
    container["env"][0]["valueFrom"]["secretKeyRef"]["name"] = "agent-platform-api-runtime"
    _write_documents(tmp_path, documents)
    with pytest.raises(KubernetesContractError, match="Secret key references"):
        validate_kubernetes_contract(tmp_path)

    container["env"][0]["valueFrom"]["secretKeyRef"]["name"] = "agent-platform-event-runtime"
    container["args"][0] = "agent_api.factory:create_production_app"
    _write_documents(tmp_path, documents)
    with pytest.raises(KubernetesContractError, match="event-only application factory"):
        validate_kubernetes_contract(tmp_path)


def test_validator_rejects_missing_metrics_discovery_and_anti_affinity(tmp_path: Path) -> None:
    documents = [copy.deepcopy(item) for item in load_manifest_documents(ROOT)]
    api = next(
        item
        for item in documents
        if item["kind"] == "Deployment" and item["metadata"]["name"] == "agent-api"
    )
    del api["spec"]["template"]["metadata"]["annotations"]["prometheus.io/scrape"]
    _write_documents(tmp_path, documents)
    with pytest.raises(KubernetesContractError, match="metrics discovery"):
        validate_kubernetes_contract(tmp_path)

    api["spec"]["template"]["metadata"]["annotations"]["prometheus.io/scrape"] = "true"
    api["spec"]["template"]["spec"]["affinity"]["podAntiAffinity"] = {}
    _write_documents(tmp_path, documents)
    with pytest.raises(KubernetesContractError, match="anti-affinity"):
        validate_kubernetes_contract(tmp_path)


def test_validator_rejects_node_workspace_namespace_mismatch(tmp_path: Path) -> None:
    documents = [copy.deepcopy(item) for item in load_manifest_documents(ROOT)]
    node = next(
        item
        for item in documents
        if item["kind"] == "DaemonSet" and item["metadata"]["name"] == "sandbox-node-agent"
    )
    container = node["spec"]["template"]["spec"]["containers"][0]
    workspace_mount = next(
        item for item in container["volumeMounts"] if item["name"] == "workspaces"
    )
    workspace_mount["mountPath"] = "/workspaces"
    _write_documents(tmp_path, documents)

    with pytest.raises(KubernetesContractError, match="identical host and container path"):
        validate_kubernetes_contract(tmp_path)


def test_validator_rejects_shared_worker_identity(tmp_path: Path) -> None:
    documents = [copy.deepcopy(item) for item in load_manifest_documents(ROOT)]
    worker = next(
        item
        for item in documents
        if item["kind"] == "Deployment" and item["metadata"]["name"] == "agent-worker"
    )
    container = worker["spec"]["template"]["spec"]["containers"][0]
    instance = next(item for item in container["env"] if item["name"] == "AGENT_WORKER_INSTANCE_ID")
    instance["valueFrom"] = {"fieldRef": {"fieldPath": "metadata.name"}}
    _write_documents(tmp_path, documents)

    with pytest.raises(KubernetesContractError, match="must come from the pod UID"):
        validate_kubernetes_contract(tmp_path)


def test_loader_rejects_non_object_and_oversized_sets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (tmp_path / "invalid.yaml").write_text("- not-an-object\n")
    with pytest.raises(KubernetesContractError, match="non-object"):
        load_manifest_documents(tmp_path)

    (tmp_path / "invalid.yaml").write_text("kind: ConfigMap\nmetadata: {name: bounded}\n")
    monkeypatch.setattr("scripts.kubernetes_contracts.MAX_MANIFEST_BYTES", 1)
    with pytest.raises(KubernetesContractError, match="byte limit"):
        load_manifest_documents(tmp_path)


def _write_documents(root: Path, documents: list[dict[str, Any]]) -> None:
    (root / "all.yaml").write_text(yaml.safe_dump_all(documents, sort_keys=False))
