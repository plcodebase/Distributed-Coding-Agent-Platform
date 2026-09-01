from __future__ import annotations

import asyncio
import base64
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock

import pytest
import scripts.release_supply_chain as release
from scripts.release_supply_chain import (
    ReleaseManifest,
    SupplyChainError,
    build_release,
    validate_release_inventory,
    verify_release,
)

from agent_core.tools import ToolOutputChannel
from sandbox_runtime import ProcessChunk, ProcessResult

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

REVISION = "a" * 40
IDENTITY = (
    "https://github.com/example/agent-platform/.github/workflows/"
    "release-supply-chain.yml@refs/tags/v1.0.0"
)
ISSUER = "https://token.actions.githubusercontent.com"
SOURCE_URI = "https://github.com/example/agent-platform"
MATERIALS = {
    "python-slim": f"registry.example/library/python@sha256:{'1' * 64}",
    "python-alpine": f"registry.example/library/python@sha256:{'2' * 64}",
    "litellm": f"registry.example/upstream/litellm@sha256:{'3' * 64}",
}
NOW = datetime(2026, 8, 21, 12, tzinfo=UTC)


class _ReleaseRunner:
    def __init__(
        self,
        *,
        revision: str = REVISION,
        status_output: str = "",
        fail_scan: bool = False,
        ignored_scan: bool = False,
    ) -> None:
        self.revision = revision
        self.status_output = status_output
        self.fail_scan = fail_scan
        self.ignored_scan = ignored_scan
        self.calls: list[tuple[str, ...]] = []
        self.attestations: dict[tuple[str, str], Path] = {}

    async def run(  # noqa: PLR0912 - deterministic external-tool protocol fixture
        self,
        argv: Sequence[str],
        *,
        cwd: Path,
        timeout_seconds: float,
        max_output_bytes: int,
        environment: Mapping[str, str] | None = None,
    ) -> ProcessResult:
        del cwd, timeout_seconds, max_output_bytes, environment
        call = tuple(argv)
        self.calls.append(call)
        tool = Path(call[0]).name
        operation = call[1]
        output = ""
        exit_code = 0

        if operation == "--version":
            output = f"{tool} 1.0.0\n"
        elif tool == "git" and operation == "rev-parse":
            output = f"{self.revision}\n"
        elif tool == "git" and operation == "status":
            output = self.status_output
        elif tool == "podman" and operation == "image":
            output = f"sha256:{hashlib.sha256(call[-1].encode()).hexdigest()}\n"
        elif tool == "podman" and operation == "save":
            await asyncio.to_thread(_write, Path(call[call.index("--output") + 1]), b"oci-archive")
        elif tool == "syft" and operation == "scan":
            target = next(
                item.removeprefix("spdx-json=") for item in call if item.startswith("spdx-json=")
            )
            await asyncio.to_thread(
                _write_json,
                Path(target),
                {
                    "spdxVersion": "SPDX-2.3",
                    "documentNamespace": "https://example.invalid/spdx/test",
                    "packages": [],
                },
            )
        elif tool == "grype":
            scan = Path(call[call.index("--file") + 1])
            await asyncio.to_thread(
                _write_json,
                scan,
                {
                    "matches": [],
                    "ignoredMatches": ([{"reason": "test"}] if self.ignored_scan else []),
                    "source": {},
                },
            )
            if self.fail_scan:
                exit_code = 1
        elif tool == "podman" and operation == "push":
            digest_path = Path(call[call.index("--digestfile") + 1])
            digest = hashlib.sha256(call[-1].encode()).hexdigest()
            await asyncio.to_thread(_write, digest_path, f"sha256:{digest}\n".encode())
        elif tool == "cosign" and operation == "attest":
            self.attestations[(call[-1], call[call.index("--type") + 1])] = Path(
                call[call.index("--predicate") + 1]
            )
        elif tool == "cosign" and operation == "verify":
            output = json.dumps([{"critical": {"identity": {"issuer": ISSUER}}}])
        elif tool == "cosign" and operation == "verify-attestation":
            output = await asyncio.to_thread(self._attestation_output, call)
        elif tool == "cosign" and operation == "sign-blob":
            bundle = Path(call[call.index("--bundle") + 1])
            await asyncio.to_thread(
                _write_json, bundle, {"mediaType": "application/vnd.dev.sigstore.bundle+json"}
            )
        elif tool == "cosign" and operation == "verify-blob":
            output = "Verified OK\n"

        return ProcessResult(
            chunks=(ProcessChunk(channel=ToolOutputChannel.STDOUT, text=output),),
            exit_code=exit_code,
        )

    def _attestation_output(self, call: tuple[str, ...]) -> str:
        attestation_type = call[call.index("--type") + 1]
        image_reference = call[-1]
        predicate_path = self.attestations[(image_reference, attestation_type)]
        predicate = json.loads(predicate_path.read_bytes())
        predicate_type = (
            "https://spdx.dev/Document"
            if attestation_type == "spdxjson"
            else "https://slsa.dev/provenance/v0.2"
        )
        statement = {
            "_type": "https://in-toto.io/Statement/v0.1",
            "subject": [
                {
                    "name": image_reference.rsplit("@", maxsplit=1)[0],
                    "digest": {"sha256": image_reference.rsplit("@sha256:", maxsplit=1)[1]},
                }
            ],
            "predicateType": predicate_type,
            "predicate": predicate,
        }
        payload = base64.b64encode(
            json.dumps(statement, sort_keys=True, separators=(",", ":")).encode()
        ).decode()
        return json.dumps([{"payload": payload}])


class _StaticRunner:
    def __init__(self, result: ProcessResult) -> None:
        self.result = result

    async def run(
        self,
        argv: Sequence[str],
        *,
        cwd: Path,
        timeout_seconds: float,
        max_output_bytes: int,
        environment: Mapping[str, str] | None = None,
    ) -> ProcessResult:
        del argv, cwd, timeout_seconds, max_output_bytes, environment
        return self.result


@pytest.mark.asyncio
async def test_release_build_scans_signs_and_reverifies(tmp_path: Path) -> None:
    repository, tools, checksums = _repository(tmp_path)
    runner = _ReleaseRunner()
    evidence = tmp_path / "evidence"

    manifest = await build_release(
        repository,
        inventory_path=Path("release/images.yaml"),
        output_directory=evidence,
        registry_prefix="registry.example/acme/agent-platform",
        source_revision=REVISION,
        source_uri=SOURCE_URI,
        certificate_identity=IDENTITY,
        certificate_oidc_issuer=ISSUER,
        material_references=MATERIALS,
        executable_paths=tools,
        executable_sha256=checksums,
        runner=runner,
        now=lambda: NOW,
    )

    assert {image.name for image in manifest.images} == {"platform", "node", "sandbox", "litellm"}
    assert all("@sha256:" in image.image_reference for image in manifest.images)
    assert all(image.vulnerability_scan.size_bytes > 0 for image in manifest.images)
    assert all(
        image.containerfile is not None for image in manifest.images if image.name != "litellm"
    )
    assert next(image for image in manifest.images if image.name == "litellm").containerfile is None
    assert not (evidence / "INCOMPLETE").exists()
    assert (
        ReleaseManifest.model_validate_json((evidence / "release-manifest.json").read_bytes())
        == manifest
    )

    with pytest.raises(ValueError, match="timezone-aware"):
        manifest.model_copy(update={"created_at": NOW.replace(tzinfo=None)})
    image = manifest.images[0]
    with pytest.raises(ValueError, match="reference and digest disagree"):
        image.model_copy(
            update={
                "image_reference": f"registry.example/acme/changed@sha256:{'f' * 64}",
            }
        )
    with pytest.raises(ValueError, match="material requirements must be unique"):
        image.model_copy(update={"materials": (*image.materials, image.materials[0])})
    with pytest.raises(ValueError, match="artifact paths must be unique"):
        image.model_copy(update={"vulnerability_scan": image.sbom})
    with pytest.raises(ValueError, match="every trusted release tool"):
        manifest.model_copy(update={"tools": tuple(manifest.tools[0] for _ in manifest.tools)})
    with pytest.raises(ValueError, match="every production image"):
        manifest.model_copy(update={"images": tuple(manifest.images[0] for _ in manifest.images)})
    with pytest.raises(ValueError, match="source URI must use HTTPS"):
        manifest.model_copy(update={"source_uri": "http://github.example/source"})

    duplicate_reference = manifest.images[1].model_copy(
        update={
            "image_reference": image.image_reference,
            "digest": image.digest,
        }
    )
    with pytest.raises(ValueError, match="image references must be unique"):
        manifest.model_copy(update={"images": (image, duplicate_reference, *manifest.images[2:])})
    duplicate_artifact = manifest.images[1].model_copy(update={"sbom": image.sbom})
    with pytest.raises(ValueError, match="artifact paths must be globally unique"):
        manifest.model_copy(update={"images": (image, duplicate_artifact, *manifest.images[2:])})

    builds = [
        call for call in runner.calls if Path(call[0]).name == "podman" and call[1] == "build"
    ]
    scans = [
        call for call in runner.calls if Path(call[0]).name == "grype" and call[1] != "--version"
    ]
    assert len(builds) == 3
    assert all("--pull=never" in call for call in builds)
    assert all(any(value.startswith("PYTHON_BASE_IMAGE=") for value in call) for call in builds)
    assert len(scans) == 4
    assert all(call[call.index("--fail-on") + 1] == "high" for call in scans)
    assert all(call[call.index("--config") + 1].endswith("grype-config.yaml") for call in scans)
    syft_scans = [
        call for call in runner.calls if Path(call[0]).name == "syft" and call[1] == "scan"
    ]
    assert all(call[call.index("--config") + 1].endswith("syft-config.yaml") for call in syft_scans)
    assert all("-c" not in call for call in runner.calls)

    verified = await verify_release(
        evidence,
        expected_source_revision=REVISION,
        certificate_identity=IDENTITY,
        certificate_oidc_issuer=ISSUER,
        cosign_executable=tools["cosign"],
        cosign_sha256=checksums["cosign"],
        runner=runner,
    )
    assert verified == manifest

    await asyncio.to_thread((evidence / manifest.images[0].sbom.path).write_text, "{}")
    with pytest.raises(SupplyChainError, match=r"size mismatch|checksum mismatch"):
        await verify_release(
            evidence,
            expected_source_revision=REVISION,
            certificate_identity=IDENTITY,
            certificate_oidc_issuer=ISSUER,
            cosign_executable=tools["cosign"],
            cosign_sha256=checksums["cosign"],
            runner=runner,
        )


@pytest.mark.asyncio
async def test_release_fails_closed_before_push_or_sign_when_scan_fails(tmp_path: Path) -> None:
    repository, tools, checksums = _repository(tmp_path)
    runner = _ReleaseRunner(fail_scan=True)
    evidence = tmp_path / "evidence"

    with pytest.raises(SupplyChainError, match="release command failed: grype"):
        await build_release(
            repository,
            inventory_path=Path("release/images.yaml"),
            output_directory=evidence,
            registry_prefix="registry.example/acme/agent-platform",
            source_revision=REVISION,
            source_uri=SOURCE_URI,
            certificate_identity=IDENTITY,
            certificate_oidc_issuer=ISSUER,
            material_references=MATERIALS,
            executable_paths=tools,
            executable_sha256=checksums,
            runner=runner,
            now=lambda: NOW,
        )

    assert (evidence / "INCOMPLETE").is_file()
    assert not any(Path(call[0]).name == "podman" and call[1] == "push" for call in runner.calls)
    assert not any(Path(call[0]).name == "cosign" and call[1] == "sign" for call in runner.calls)


@pytest.mark.asyncio
async def test_release_rejects_suppressed_vulnerabilities(tmp_path: Path) -> None:
    repository, tools, checksums = _repository(tmp_path)
    runner = _ReleaseRunner(ignored_scan=True)

    with pytest.raises(SupplyChainError, match="may not suppress matches"):
        await build_release(
            repository,
            inventory_path=Path("release/images.yaml"),
            output_directory=tmp_path / "evidence",
            registry_prefix="registry.example/acme/agent-platform",
            source_revision=REVISION,
            source_uri=SOURCE_URI,
            certificate_identity=IDENTITY,
            certificate_oidc_issuer=ISSUER,
            material_references=MATERIALS,
            executable_paths=tools,
            executable_sha256=checksums,
            runner=runner,
            now=lambda: NOW,
        )

    assert not any(Path(call[0]).name == "podman" and call[1] == "push" for call in runner.calls)


@pytest.mark.asyncio
async def test_release_rejects_mutable_material_and_untrusted_tool(tmp_path: Path) -> None:
    repository, tools, checksums = _repository(tmp_path)
    mutable = {**MATERIALS, "python-slim": "registry.example/library/python:latest"}
    with pytest.raises(SupplyChainError, match="material reference is not immutable"):
        await build_release(
            repository,
            inventory_path=Path("release/images.yaml"),
            output_directory=tmp_path / "mutable",
            registry_prefix="registry.example/acme/agent-platform",
            source_revision=REVISION,
            source_uri=SOURCE_URI,
            certificate_identity=IDENTITY,
            certificate_oidc_issuer=ISSUER,
            material_references=mutable,
            executable_paths=tools,
            executable_sha256=checksums,
            runner=_ReleaseRunner(),
        )

    bad_checksums = {**checksums, "cosign": "f" * 64}
    with pytest.raises(SupplyChainError, match="tool checksum mismatch: cosign"):
        await build_release(
            repository,
            inventory_path=Path("release/images.yaml"),
            output_directory=tmp_path / "untrusted",
            registry_prefix="registry.example/acme/agent-platform",
            source_revision=REVISION,
            source_uri=SOURCE_URI,
            certificate_identity=IDENTITY,
            certificate_oidc_issuer=ISSUER,
            material_references=MATERIALS,
            executable_paths=tools,
            executable_sha256=bad_checksums,
            runner=_ReleaseRunner(),
        )


def test_container_contexts_are_deny_first_allowlists() -> None:
    root = Path(".containerignore").read_text().splitlines()
    sandbox = Path("services/sandbox/.containerignore").read_text().splitlines()
    fake = Path("services/fake-llm/.containerignore").read_text().splitlines()

    assert root[0] == sandbox[0] == fake[0] == "**"
    assert not any(line in {"!.env", "!**"} for line in (*root, *sandbox, *fake))
    assert "!pyproject.toml" in root and "!uv.lock" in root
    assert "!Containerfile" in sandbox
    assert {"!Containerfile", "!app.py", "!coding_scenario.py"}.issubset(fake)


def test_checked_in_release_inventory_has_digest_injectable_bases() -> None:
    inventory = validate_release_inventory(Path())

    assert {image.name for image in inventory.images} == {
        "platform",
        "node",
        "sandbox",
        "litellm",
    }


def test_release_models_revalidate_copy_and_reject_ambiguous_inventory() -> None:
    build = release.ReleaseImageSpec(
        name="platform",
        repository="agent-platform",
        kind=release.ImageKind.BUILD,
        context=".",
        containerfile="services/platform/Containerfile",
        build_args={"PYTHON_BASE_IMAGE": "python-slim"},
    )
    with pytest.raises(ValueError, match="context, containerfile, and build arguments"):
        build.model_copy(update={"context": None})
    with pytest.raises(ValueError, match="may not declare a mirrored source"):
        build.model_copy(update={"source_requirement": "python-slim"})
    with pytest.raises(ValueError, match="only one source requirement"):
        release.ReleaseImageSpec(
            name="litellm",
            repository="agent-platform-litellm",
            kind=release.ImageKind.MIRROR,
            context=".",
            source_requirement="litellm",
        )

    images = (
        build,
        build.model_copy(update={"name": "node", "repository": "agent-platform-node"}),
        build.model_copy(
            update={
                "name": "sandbox",
                "repository": "agent-platform-sandbox",
                "context": "services/sandbox",
                "containerfile": "services/sandbox/Containerfile",
                "build_args": {"PYTHON_BASE_IMAGE": "python-alpine"},
            }
        ),
        release.ReleaseImageSpec(
            name="litellm",
            repository="agent-platform-litellm",
            kind=release.ImageKind.MIRROR,
            source_requirement="litellm",
        ),
    )
    release.ReleaseInventory(images=images)
    with pytest.raises(ValueError, match="every production image"):
        release.ReleaseInventory(images=(build, build, images[2], images[3]))
    with pytest.raises(ValueError, match="repositories must be unique"):
        release.ReleaseInventory(
            images=(
                build,
                images[1].model_copy(update={"repository": build.repository}),
                *images[2:],
            )
        )


def test_inventory_validation_is_internal_and_exact(tmp_path: Path) -> None:
    repository, _tools, _checksums = _repository(tmp_path)
    release_file = repository / "release"
    syft = release_file / "syft-config.yaml"
    grype = release_file / "grype-config.yaml"
    containerfile = repository / "services" / "platform" / "Containerfile"
    context_ignore = repository / ".containerignore"

    syft.write_text("invalid: [\n")
    with pytest.raises(SupplyChainError, match="scanner configuration is invalid"):
        validate_release_inventory(repository)
    syft.write_text("{}\n")
    grype.write_text("{}\n")
    with pytest.raises(SupplyChainError, match="weakens the fixed policy"):
        validate_release_inventory(repository)
    _write_grype_configuration(grype)

    containerfile.write_text("FROM python:latest\n")
    with pytest.raises(SupplyChainError, match="does not declare every release build argument"):
        validate_release_inventory(repository)
    containerfile.write_text("ARG PYTHON_BASE_IMAGE\nFROM python:latest\n")
    with pytest.raises(SupplyChainError, match="mutable production base"):
        validate_release_inventory(repository)
    containerfile.write_text("ARG PYTHON_BASE_IMAGE\nFROM ${PYTHON_BASE_IMAGE}\n")

    original_ignore = context_ignore.read_text()
    context_ignore.write_text(original_ignore.removeprefix("**\n"))
    with pytest.raises(SupplyChainError, match="deny-first"):
        validate_release_inventory(repository)
    context_ignore.write_text(original_ignore + "!.env\n")
    with pytest.raises(SupplyChainError, match="exact source allowlist"):
        validate_release_inventory(repository)


@pytest.mark.asyncio
async def test_release_command_boundary_fails_closed() -> None:
    chunk = ProcessChunk(channel=ToolOutputChannel.STDOUT, text="bounded output")
    with pytest.raises(SupplyChainError, match="timed out"):
        await release._run(
            _StaticRunner(ProcessResult(chunks=(chunk,), exit_code=0, timed_out=True)),
            ("/trusted/grype", "scan"),
            Path(),
            {},
        )
    with pytest.raises(SupplyChainError, match="output exceeded"):
        await release._run(
            _StaticRunner(ProcessResult(chunks=(chunk,), exit_code=0, output_truncated=True)),
            ("/trusted/syft", "scan"),
            Path(),
            {},
        )
    with pytest.raises(SupplyChainError, match="command failed"):
        await release._run(
            _StaticRunner(ProcessResult(chunks=(chunk,), exit_code=7)),
            ("/trusted/cosign", "verify"),
            Path(),
            {},
        )
    assert (
        await release._run(
            _StaticRunner(ProcessResult(chunks=(chunk,), exit_code=0)),
            ("/trusted/git", "status"),
            Path(),
            {},
        )
        == b"bounded output"
    )


@pytest.mark.asyncio
async def test_source_and_external_tool_protocol_validation(tmp_path: Path) -> None:
    repository, tools, _checksums = _repository(tmp_path)
    with pytest.raises(SupplyChainError, match="checked-out commit"):
        await release._verify_clean_revision(
            _ReleaseRunner(revision="b" * 40),
            tools["git"],
            repository,
            {},
            REVISION,
        )
    with pytest.raises(SupplyChainError, match="clean source checkout"):
        await release._verify_clean_revision(
            _ReleaseRunner(status_output=" M changed.py\n"),
            tools["git"],
            repository,
            {},
            REVISION,
        )
    invalid_id = _StaticRunner(
        ProcessResult(
            chunks=(ProcessChunk(channel=ToolOutputChannel.STDOUT, text="latest\n"),),
            exit_code=0,
        )
    )
    with pytest.raises(SupplyChainError, match="SHA-256 image ID"):
        await release._podman_image_id(
            invalid_id,
            tools["podman"],
            repository,
            {},
            "registry.example/acme/image:tag",
        )
    invalid_version = _StaticRunner(
        ProcessResult(
            chunks=(ProcessChunk(channel=ToolOutputChannel.STDOUT, text="\n"),),
            exit_code=0,
        )
    )
    with pytest.raises(SupplyChainError, match="invalid version string"):
        await release._collect_tool_evidence(
            invalid_version,
            {"git": (tools["git"], "a" * 64)},
            repository,
            {},
        )


def test_scanner_and_attestation_payloads_fail_closed(tmp_path: Path) -> None:
    document = tmp_path / "document.json"
    document.write_text("{}")
    with pytest.raises(SupplyChainError, match="not an SPDX"):
        release._validate_spdx(document)
    document.write_text('{"spdxVersion":"SPDX-2.3"}')
    with pytest.raises(SupplyChainError, match="SPDX document is incomplete"):
        release._validate_spdx(document)

    document.write_text('{"matches":[]}')
    with pytest.raises(SupplyChainError, match="Grype JSON contract"):
        release._validate_grype(document)
    document.write_text('{"matches":[],"source":{},"ignoredMatches":[{}]}')
    with pytest.raises(SupplyChainError, match="may not suppress matches"):
        release._validate_grype(document)

    with pytest.raises(SupplyChainError, match="invalid JSON"):
        release._validate_cosign_payload(b"not-json")
    with pytest.raises(SupplyChainError, match="no signed payload"):
        release._validate_cosign_payload(b"[]")
    document.write_text("[]")
    with pytest.raises(SupplyChainError, match="must contain an object"):
        release._json_object(document, 100)
    document.write_text('{"value":NaN}')
    with pytest.raises(SupplyChainError, match="artifact is invalid"):
        release._json_value(document, 100)

    predicate = tmp_path / "predicate.json"
    predicate.write_text("{}")
    reference = f"registry.example/acme/image@sha256:{'d' * 64}"
    for payload, message in (
        (b"[]", "returned no payload"),
        (b'[{"payload":"not-base64"}]', "does not bind"),
        (
            json.dumps(
                [
                    {
                        "payload": base64.b64encode(
                            json.dumps({"predicateType": "wrong"}).encode()
                        ).decode()
                    }
                ]
            ).encode(),
            "does not bind",
        ),
    ):
        with pytest.raises(SupplyChainError, match=message):
            release._validate_attestation_payload(
                payload,
                predicate,
                reference,
                release.SPDX_PREDICATE_TYPE,
            )


def test_release_filesystem_helpers_are_bounded_and_contained(tmp_path: Path) -> None:
    repository, _tools, _checksums = _repository(tmp_path)
    empty = tmp_path / "empty"
    empty.touch()
    with pytest.raises(SupplyChainError, match="file size is invalid"):
        release._read_bounded_file(empty, 10)
    with pytest.raises(SupplyChainError, match="artifact size is invalid"):
        release._artifact(tmp_path, empty, 10)
    with pytest.raises(SupplyChainError, match="file cannot be hashed safely"):
        release._hash_file(tmp_path, 10)

    target = tmp_path / "target"
    target.write_text("payload")
    link = tmp_path / "link"
    link.symlink_to(target)
    with pytest.raises(SupplyChainError, match="symbolic links"):
        release._read_bounded_file(link, 100)
    with pytest.raises(SupplyChainError, match="symbolic links"):
        release._hash_file(link, 100)
    with pytest.raises(SupplyChainError, match="may not be a symbolic link"):
        release._contained_file(tmp_path, Path("link"))
    with pytest.raises(SupplyChainError, match="inventory paths may not be symbolic links"):
        release._contained_path(tmp_path, "link", directory=False)

    destination = tmp_path / "artifact"
    with pytest.raises(SupplyChainError, match="invalid release artifact"):
        release._write_bytes(destination, b"", 10)
    release._write_bytes(destination, b"ok", 10)
    with pytest.raises(SupplyChainError, match="invalid release artifact"):
        release._write_bytes(destination, b"again", 10)

    artifact = release._artifact(tmp_path, destination, 10)
    destination.write_bytes(b"no")
    with pytest.raises(SupplyChainError, match="checksum mismatch"):
        release._verify_artifact(tmp_path, artifact)

    incomplete = tmp_path / "incomplete"
    incomplete.mkdir()
    release._mark_incomplete(incomplete)
    release._mark_incomplete(incomplete)
    with pytest.raises(SupplyChainError, match="invalid or incomplete"):
        release._evidence_root(incomplete)
    with pytest.raises(SupplyChainError, match="repository root is invalid"):
        release._repository_root(tmp_path)
    with pytest.raises(SupplyChainError, match="must not already exist"):
        release._create_output_directory(repository, incomplete)
    with pytest.raises(ValueError, match="contained relative path"):
        release._relative_path("../escape")


def test_release_identity_tool_and_assignment_inputs_are_closed(tmp_path: Path) -> None:
    repository, tools, checksums = _repository(tmp_path)
    assert release.validate_release_tools(tools, checksums)["git"][0] == tools["git"]
    with pytest.raises(SupplyChainError, match="name every required tool"):
        release.validate_release_tools({"git": tools["git"]}, {"git": checksums["git"]})
    with pytest.raises(SupplyChainError, match="checksum is invalid"):
        release._validated_executable("git", tools["git"], "bad")
    with pytest.raises(SupplyChainError, match="unavailable"):
        release._validated_executable("git", str(tmp_path / "missing"), "a" * 64)
    non_executable = repository / "not-executable"
    non_executable.write_text("x")
    with pytest.raises(SupplyChainError, match="not a regular executable"):
        release._validated_executable("git", str(non_executable), "a" * 64)

    with pytest.raises(SupplyChainError, match="registry prefix is invalid"):
        release._registry_prefix("HTTPS://REGISTRY/UPPER")
    with pytest.raises(SupplyChainError, match="full lowercase Git SHA-1"):
        release._source_revision("short")
    with pytest.raises(SupplyChainError, match="source URI is invalid"):
        release._source_identity("http://example.invalid/source", IDENTITY, ISSUER)
    with pytest.raises(ValueError, match="full registry SHA-256 digest"):
        release._require_digest_reference("registry.example/acme/image:latest")
    with pytest.raises(SupplyChainError, match="unique NAME=VALUE"):
        release._parse_assignments(("A=1", "A=2"), label="test")
    assert release._parse_assignments(("A=1", "B=2"), label="test") == {
        "A": "1",
        "B": "2",
    }

    artifact = release.ArtifactEvidence(path="artifact", size_bytes=1, sha256="a" * 64)
    image = release.ReleaseImageSpec(
        name="platform",
        repository="agent-platform",
        kind=release.ImageKind.BUILD,
        context=".",
        containerfile="services/platform/Containerfile",
        build_args={"PYTHON_BASE_IMAGE": "python-slim"},
    )
    with pytest.raises(SupplyChainError, match="naive timestamp"):
        release._slsa_provenance(
            image,
            source_revision=REVISION,
            source_uri=SOURCE_URI,
            certificate_identity=IDENTITY,
            materials=MATERIALS,
            local_image_id=f"sha256:{'a' * 64}",
            archive=artifact,
            sbom=artifact.model_copy(update={"path": "sbom"}),
            scan=artifact.model_copy(update={"path": "scan"}),
            timestamp=NOW.replace(tzinfo=None),
        )


def test_release_cli_validates_contract_and_tools(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repository, tools, checksums = _repository(tmp_path)
    assert release.main(("validate", "--repository", str(repository))) == 0
    assert "validated 4" in capsys.readouterr().out
    tool_arguments = tuple(value for item in tools.items() for value in ("--tool", "=".join(item)))
    checksum_arguments = tuple(
        value for item in checksums.items() for value in ("--tool-sha256", "=".join(item))
    )
    assert release.main(("verify-tools", *tool_arguments, *checksum_arguments)) == 0
    assert "verified 5" in capsys.readouterr().out
    with pytest.raises(SystemExit, match="name every required tool"):
        release.main(("verify-tools", "--tool", f"git={tools['git']}"))


@pytest.mark.asyncio
async def test_release_cli_routes_build_and_verify(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = SimpleNamespace(source_revision=REVISION, images=(1, 2, 3, 4))
    build = AsyncMock(return_value=result)
    verify = AsyncMock(return_value=result)
    monkeypatch.setattr(release, "build_release", build)
    monkeypatch.setattr(release, "verify_release", verify)

    assert (
        await release._main(
            (
                "build",
                "--output",
                "evidence",
                "--registry-prefix",
                "registry.example/acme/release",
                "--source-revision",
                REVISION,
                "--source-uri",
                SOURCE_URI,
                "--certificate-identity",
                IDENTITY,
                "--certificate-oidc-issuer",
                ISSUER,
                "--material",
                f"python-slim={MATERIALS['python-slim']}",
                "--tool",
                "git=/trusted/git",
                "--tool-sha256",
                f"git={'a' * 64}",
            )
        )
        == 0
    )
    assert build.await_count == 1
    assert "verified release" in capsys.readouterr().out

    assert (
        await release._main(
            (
                "verify",
                "--evidence",
                "evidence",
                "--expected-source-revision",
                REVISION,
                "--certificate-identity",
                IDENTITY,
                "--certificate-oidc-issuer",
                ISSUER,
                "--cosign",
                "/trusted/cosign",
                "--cosign-sha256",
                "a" * 64,
            )
        )
        == 0
    )
    assert verify.await_count == 1


def _repository(tmp_path: Path) -> tuple[Path, dict[str, str], dict[str, str]]:
    root = tmp_path / "repository"
    root.mkdir()
    _write(root / "pyproject.toml", b"[project]\nname='release-test'\nversion='0'\n")
    _write(root / "uv.lock", b"version = 1\n")
    inventory = root / "release" / "images.yaml"
    inventory.parent.mkdir()
    _write(inventory.parent / "syft-config.yaml", b"{}\n")
    _write_grype_configuration(inventory.parent / "grype-config.yaml")
    _write(
        inventory,
        b"""version: agent-release-images-v1
images:
  - name: platform
    repository: agent-platform
    kind: build
    context: .
    containerfile: services/platform/Containerfile
    build_args: {PYTHON_BASE_IMAGE: python-slim}
  - name: node
    repository: agent-platform-node
    kind: build
    context: .
    containerfile: services/node/Containerfile
    build_args: {PYTHON_BASE_IMAGE: python-slim}
  - name: sandbox
    repository: agent-platform-sandbox
    kind: build
    context: services/sandbox
    containerfile: services/sandbox/Containerfile
    build_args: {PYTHON_BASE_IMAGE: python-alpine}
  - name: litellm
    repository: agent-platform-litellm
    kind: mirror
    source_requirement: litellm
""",
    )
    for name in ("platform", "node", "sandbox"):
        directory = root / "services" / name
        directory.mkdir(parents=True)
        _write(directory / "Containerfile", b"ARG PYTHON_BASE_IMAGE\nFROM ${PYTHON_BASE_IMAGE}\n")
    _write(
        root / ".containerignore",
        b"""**
!pyproject.toml
!uv.lock
!apps/
!apps/**
!packages/
!packages/**
!services/
!services/platform/
!services/platform/Containerfile
!services/node/
!services/node/Containerfile
""",
    )
    _write(root / "services" / "sandbox" / ".containerignore", b"**\n!Containerfile\n")
    binary = root / ".tools"
    binary.mkdir()
    tools: dict[str, str] = {}
    checksums: dict[str, str] = {}
    for name in ("git", "podman", "syft", "grype", "cosign"):
        path = binary / name
        _write(path, f"{name}-test-executable\n".encode())
        path.chmod(0o700)
        tools[name] = str(path)
        checksums[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    return root, tools, checksums


def _write_grype_configuration(path: Path) -> None:
    _write(
        path,
        b"""check-for-app-update: false
only-fixed: false
only-notfixed: false
ignore-wontfix: ""
ignore: []
exclude: []
vex-documents: []
vex-add: []
db:
  auto-update: true
  validate-by-hash-on-start: true
  validate-age: true
  max-allowed-built-age: 48h0m0s
  require-update-check: true
""",
    )


def _write(path: Path, payload: bytes) -> None:
    path.write_bytes(payload)


def _write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, sort_keys=True, separators=(",", ":")))
