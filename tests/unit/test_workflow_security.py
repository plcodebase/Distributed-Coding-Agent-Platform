from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
COMMIT_SHA = re.compile(r"^[0-9a-f]{40}$")


def test_all_remote_workflow_actions_use_immutable_commit_shas() -> None:
    mutable: list[str] = []
    for workflow in sorted((ROOT / ".github" / "workflows").glob("*.y*ml")):
        for line_number, raw_line in enumerate(
            workflow.read_text(encoding="utf-8").splitlines(),
            start=1,
        ):
            line = raw_line.strip()
            if not line.startswith("- uses:"):
                continue
            reference = line.removeprefix("- uses:").split("#", maxsplit=1)[0].strip()
            if reference.startswith("./"):
                continue
            action, separator, revision = reference.rpartition("@")
            if not action or separator != "@" or COMMIT_SHA.fullmatch(revision) is None:
                mutable.append(f"{workflow.relative_to(ROOT)}:{line_number}: {reference}")

    assert mutable == []


def test_release_workflow_is_identity_bound_scanned_and_reverified() -> None:
    workflow = (ROOT / ".github/workflows/release-supply-chain.yml").read_text(encoding="utf-8")

    assert "runs-on: [self-hosted, linux, podman, rootless, release]" in workflow
    assert "id-token: write" in workflow
    assert "packages: write" in workflow
    assert "persist-credentials: false" in workflow
    assert "make release-check" in workflow
    assert "make kubernetes-contract" in workflow
    assert "make release-tool-contract" in workflow
    assert "make release-evidence\n" in workflow
    assert "make release-evidence-verify" in workflow
    assert "--password-stdin" in workflow
    assert "RELEASE_COSIGN_SHA256" in workflow
    assert "RELEASE_PYTHON_SLIM_IMAGE" in workflow
    assert "RELEASE_PYTHON_ALPINE_IMAGE" in workflow
    assert "RELEASE_LITELLM_IMAGE" in workflow
    assert "cancel-in-progress: false" in workflow
    assert "refs/tags/v*)" in workflow
    assert workflow.index("make release-tool-contract") < workflow.index("make podman-preflight")
