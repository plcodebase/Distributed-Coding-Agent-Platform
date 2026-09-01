"""Build, scan, attest, and verify immutable Podman release images."""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import os
import re
import shutil
import stat
import sys
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Annotated, Literal, Protocol, Self
from urllib.parse import urlsplit

import yaml
from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

from sandbox_runtime import BoundedProcessRunner, ProcessResult

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from typing import Any

MAX_INVENTORY_BYTES = 64 * 1024
MAX_PROCESS_OUTPUT_BYTES = 8 * 1024 * 1024
MAX_JSON_ARTIFACT_BYTES = 64 * 1024 * 1024
MAX_ARCHIVE_BYTES = 20 * 1024 * 1024 * 1024
MAX_MANIFEST_BYTES = 2 * 1024 * 1024
MAX_TEXT_LENGTH = 1_000
MIN_FROM_PARTS = 2
PROCESS_TIMEOUT_SECONDS = 1_800
IMAGE_NAMES = frozenset({"platform", "node", "sandbox", "litellm"})
TOOL_NAMES = frozenset({"git", "podman", "syft", "grype", "cosign"})
SPDX_PREDICATE_TYPE = "https://spdx.dev/Document"
SLSA_PREDICATE_TYPE = "https://slsa.dev/provenance/v0.2"
SLSA_ATTESTATION_TYPE = "slsaprovenance02"
SPDX_ATTESTATION_TYPE = "spdxjson"
GRYPE_RELEASE_CONFIG: dict[str, Any] = {
    "check-for-app-update": False,
    "only-fixed": False,
    "only-notfixed": False,
    "ignore-wontfix": "",
    "ignore": [],
    "exclude": [],
    "vex-documents": [],
    "vex-add": [],
    "db": {
        "auto-update": True,
        "validate-by-hash-on-start": True,
        "validate-age": True,
        "max-allowed-built-age": "48h0m0s",
        "require-update-check": True,
    },
}
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_DIGEST_PATTERN = re.compile(r"^sha256:([0-9a-f]{64})$")
_REVISION_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_REGISTRY_PATTERN = re.compile(
    r"^(?=.{3,300}$)[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?(?::[0-9]{1,5})?"
    r"(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)+$"
)
_DIGEST_REFERENCE_PATTERN = re.compile(
    r"^(?=.{10,512}$)[A-Za-z0-9](?:[A-Za-z0-9._:-]*[A-Za-z0-9])?"
    r"(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)+@sha256:[0-9a-f]{64}$"
)
type Clock = Callable[[], datetime]


class SupplyChainError(RuntimeError):
    """A release input or external evidence failed closed validation."""


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    def model_copy(
        self,
        *,
        update: Mapping[str, Any] | None = None,
        deep: bool = False,
    ) -> Self:
        del deep
        values = self.model_dump(mode="python", round_trip=True)
        if update is not None:
            values.update(update)
        return type(self).model_validate(values)


class ImageKind(StrEnum):
    BUILD = "build"
    MIRROR = "mirror"


class ReleaseImageSpec(_Model):
    name: Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9-]{1,39}$")]
    repository: Annotated[str, StringConstraints(pattern=r"^[a-z0-9]+(?:[._-][a-z0-9]+)*$")]
    kind: ImageKind
    context: str | None = None
    containerfile: str | None = None
    build_args: dict[
        Annotated[str, StringConstraints(pattern=r"^[A-Z][A-Z0-9_]{0,63}$")],
        Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9-]{1,39}$")],
    ] = Field(default_factory=dict, max_length=16)
    source_requirement: (
        Annotated[
            str,
            StringConstraints(pattern=r"^[a-z][a-z0-9-]{1,39}$"),
        ]
        | None
    ) = None

    @model_validator(mode="after")
    def validate_kind(self) -> Self:
        if self.kind is ImageKind.BUILD:
            if self.context is None or self.containerfile is None or not self.build_args:
                raise ValueError("build images require context, containerfile, and build arguments")
            if self.source_requirement is not None:
                raise ValueError("build images may not declare a mirrored source")
            _relative_path(self.context, allow_dot=True)
            _relative_path(self.containerfile)
        elif (
            self.context is not None
            or self.containerfile is not None
            or self.build_args
            or self.source_requirement is None
        ):
            raise ValueError("mirror images require only one source requirement")
        return self


class ReleaseInventory(_Model):
    version: Literal["agent-release-images-v1"] = "agent-release-images-v1"
    images: tuple[ReleaseImageSpec, ...] = Field(min_length=4, max_length=4)

    @model_validator(mode="after")
    def validate_images(self) -> Self:
        names = [image.name for image in self.images]
        repositories = [image.repository for image in self.images]
        if set(names) != IMAGE_NAMES or len(names) != len(set(names)):
            raise ValueError("release inventory must contain every production image exactly once")
        if len(repositories) != len(set(repositories)):
            raise ValueError("release image repositories must be unique")
        return self


type Sha256Text = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
type RelativeArtifactPath = Annotated[
    str,
    StringConstraints(min_length=1, max_length=240, pattern=r"^[A-Za-z0-9][A-Za-z0-9._/-]*$"),
]


class ArtifactEvidence(_Model):
    path: RelativeArtifactPath
    size_bytes: int = Field(ge=1, le=MAX_ARCHIVE_BYTES)
    sha256: Sha256Text

    @model_validator(mode="after")
    def validate_path(self) -> Self:
        _relative_path(self.path)
        return self


class ToolEvidence(_Model):
    name: Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9-]{1,39}$")]
    executable_sha256: Sha256Text
    version_output_sha256: Sha256Text
    version: Annotated[str, StringConstraints(min_length=1, max_length=1_000)]


class MaterialEvidence(_Model):
    requirement: Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9-]{1,39}$")]
    reference: Annotated[str, StringConstraints(min_length=10, max_length=512)]

    @model_validator(mode="after")
    def validate_reference(self) -> Self:
        _require_digest_reference(self.reference)
        return self


class ImageReleaseEvidence(_Model):
    name: Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9-]{1,39}$")]
    image_reference: Annotated[str, StringConstraints(min_length=10, max_length=512)]
    digest: Annotated[str, StringConstraints(pattern=r"^sha256:[0-9a-f]{64}$")]
    local_image_id: Annotated[str, StringConstraints(pattern=r"^sha256:[0-9a-f]{64}$")]
    materials: tuple[MaterialEvidence, ...] = Field(min_length=1, max_length=16)
    containerfile: ArtifactEvidence | None
    sbom: ArtifactEvidence
    vulnerability_scan: ArtifactEvidence
    provenance: ArtifactEvidence
    signature_verification: ArtifactEvidence
    sbom_attestation_verification: ArtifactEvidence
    provenance_attestation_verification: ArtifactEvidence

    @model_validator(mode="after")
    def validate_image(self) -> Self:
        _require_digest_reference(self.image_reference)
        if not self.image_reference.endswith(f"@{self.digest}"):
            raise ValueError("image reference and digest disagree")
        requirements = [material.requirement for material in self.materials]
        if len(requirements) != len(set(requirements)):
            raise ValueError("image material requirements must be unique")
        paths = [
            self.sbom.path,
            self.vulnerability_scan.path,
            self.provenance.path,
            self.signature_verification.path,
            self.sbom_attestation_verification.path,
            self.provenance_attestation_verification.path,
        ]
        if self.containerfile is not None:
            paths.append(self.containerfile.path)
        if len(paths) != len(set(paths)):
            raise ValueError("image evidence artifact paths must be unique")
        return self


class ReleaseManifest(_Model):
    manifest_version: Literal["agent-release-evidence-v1"] = "agent-release-evidence-v1"
    result_claim: Literal["release-evidence"] = "release-evidence"
    source_revision: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
    source_uri: Annotated[str, StringConstraints(min_length=8, max_length=500)]
    source_dirty: Literal[False] = False
    created_at: datetime
    vulnerability_threshold: Literal["high"] = "high"
    inventory: ArtifactEvidence
    lockfile: ArtifactEvidence
    syft_configuration: ArtifactEvidence
    grype_configuration: ArtifactEvidence
    tools: tuple[ToolEvidence, ...] = Field(min_length=5, max_length=5)
    images: tuple[ImageReleaseEvidence, ...] = Field(min_length=4, max_length=4)

    @model_validator(mode="after")
    def validate_manifest(self) -> Self:
        if self.created_at.tzinfo is None or self.created_at.utcoffset() is None:
            raise ValueError("release manifest timestamp must be timezone-aware")
        if {tool.name for tool in self.tools} != TOOL_NAMES:
            raise ValueError("release manifest must identify every trusted release tool")
        if {image.name for image in self.images} != IMAGE_NAMES:
            raise ValueError("release manifest must contain every production image")
        references = [image.image_reference for image in self.images]
        if len(references) != len(set(references)):
            raise ValueError("release image references must be unique")
        if not self.source_uri.startswith("https://"):
            raise ValueError("release source URI must use HTTPS")
        paths = [
            self.inventory.path,
            self.lockfile.path,
            self.syft_configuration.path,
            self.grype_configuration.path,
        ]
        for image in self.images:
            paths.extend(
                (
                    image.sbom.path,
                    image.vulnerability_scan.path,
                    image.provenance.path,
                    image.signature_verification.path,
                    image.sbom_attestation_verification.path,
                    image.provenance_attestation_verification.path,
                )
            )
            if image.containerfile is not None:
                paths.append(image.containerfile.path)
        if len(paths) != len(set(paths)):
            raise ValueError("release manifest artifact paths must be globally unique")
        return self


class ReleaseCommandRunner(Protocol):
    async def run(
        self,
        argv: Sequence[str],
        *,
        cwd: Path,
        timeout_seconds: float,
        max_output_bytes: int,
        environment: Mapping[str, str] | None = None,
    ) -> ProcessResult: ...


def validate_release_tools(
    executable_paths: Mapping[str, str],
    executable_sha256: Mapping[str, str],
) -> Mapping[str, tuple[str, str]]:
    """Resolve every release binary and verify its configured SHA-256 checksum."""

    return _validated_tools(executable_paths, executable_sha256)


def validate_release_inventory(
    repository: Path,
    *,
    inventory_path: Path = Path("release/images.yaml"),
) -> ReleaseInventory:
    """Validate the checked-in image inventory and deny-first build contexts."""

    root = _repository_root(repository)
    inventory_file = _contained_file(root, inventory_path)
    inventory = _load_inventory(inventory_file)
    syft_configuration = _contained_file(root, Path("release/syft-config.yaml"))
    grype_configuration = _contained_file(root, Path("release/grype-config.yaml"))
    try:
        syft_config = yaml.safe_load(_read_bounded_file(syft_configuration, MAX_INVENTORY_BYTES))
        grype_config = yaml.safe_load(_read_bounded_file(grype_configuration, MAX_INVENTORY_BYTES))
    except yaml.YAMLError as error:
        raise SupplyChainError("release scanner configuration is invalid") from error
    if syft_config != {} or grype_config != GRYPE_RELEASE_CONFIG:
        raise SupplyChainError("release scanner configuration weakens the fixed policy")
    required_context_entries = {
        "platform": {
            "!pyproject.toml",
            "!uv.lock",
            "!apps/",
            "!apps/**",
            "!packages/",
            "!packages/**",
            "!services/",
            "!services/platform/",
            "!services/platform/Containerfile",
            "!services/node/",
            "!services/node/Containerfile",
        },
        "node": {
            "!pyproject.toml",
            "!uv.lock",
            "!apps/",
            "!apps/**",
            "!packages/",
            "!packages/**",
            "!services/",
            "!services/platform/",
            "!services/platform/Containerfile",
            "!services/node/",
            "!services/node/Containerfile",
        },
        "sandbox": {"!Containerfile"},
    }
    for image in inventory.images:
        if image.kind is ImageKind.MIRROR:
            continue
        if image.context is None or image.containerfile is None:
            raise SupplyChainError("validated build image lost its source paths")
        context = _contained_path(root, image.context, directory=True)
        containerfile = _contained_file(root, Path(image.containerfile))
        _validate_containerfile(containerfile, frozenset(image.build_args))
        _validate_context_allowlist(context, required_context_entries[image.name])
    return inventory


async def build_release(
    repository: Path,
    *,
    inventory_path: Path,
    output_directory: Path,
    registry_prefix: str,
    source_revision: str,
    source_uri: str,
    certificate_identity: str,
    certificate_oidc_issuer: str,
    material_references: Mapping[str, str],
    executable_paths: Mapping[str, str],
    executable_sha256: Mapping[str, str],
    runner: ReleaseCommandRunner | None = None,
    now: Clock | None = None,
) -> ReleaseManifest:
    """Create signed release evidence from exact OCI inputs without a shell."""

    root = await asyncio.to_thread(_repository_root, repository)
    inventory_file = await asyncio.to_thread(_contained_file, root, inventory_path)
    inventory = await asyncio.to_thread(
        validate_release_inventory,
        root,
        inventory_path=inventory_path,
    )
    syft_configuration = await asyncio.to_thread(
        _contained_file,
        root,
        Path("release/syft-config.yaml"),
    )
    grype_configuration = await asyncio.to_thread(
        _contained_file,
        root,
        Path("release/grype-config.yaml"),
    )
    revision = _source_revision(source_revision)
    registry = _registry_prefix(registry_prefix)
    _source_identity(source_uri, certificate_identity, certificate_oidc_issuer)
    materials = _validated_materials(inventory, material_references)
    tools = await asyncio.to_thread(
        _validated_tools,
        executable_paths,
        executable_sha256,
    )
    actual_runner = runner or BoundedProcessRunner()
    environment = _release_environment(root)

    await _verify_clean_revision(
        actual_runner,
        tools["git"][0],
        root,
        environment,
        revision,
    )

    evidence_root = await asyncio.to_thread(_create_output_directory, root, output_directory)
    try:
        tool_evidence = await _collect_tool_evidence(actual_runner, tools, root, environment)
        inventory_evidence = await asyncio.to_thread(
            _capture_source_artifact,
            evidence_root,
            inventory_file,
            "release-inventory.yaml",
        )
        lockfile_evidence = await asyncio.to_thread(
            _capture_source_artifact,
            evidence_root,
            root / "uv.lock",
            "uv.lock",
        )
        syft_evidence = await asyncio.to_thread(
            _capture_source_artifact,
            evidence_root,
            syft_configuration,
            "syft-config.yaml",
        )
        grype_evidence = await asyncio.to_thread(
            _capture_source_artifact,
            evidence_root,
            grype_configuration,
            "grype-config.yaml",
        )
        for reference in sorted(set(materials.values())):
            await _run(
                actual_runner,
                (tools["podman"][0], "pull", reference),
                root,
                environment,
            )

        image_evidence = [
            await _build_image_evidence(
                root,
                evidence_root,
                image,
                registry=registry,
                source_revision=revision,
                source_uri=source_uri,
                certificate_identity=certificate_identity,
                certificate_oidc_issuer=certificate_oidc_issuer,
                syft_configuration=evidence_root / syft_evidence.path,
                grype_configuration=evidence_root / grype_evidence.path,
                materials=materials,
                tools=tools,
                runner=actual_runner,
                environment=environment,
                now=now,
            )
            for image in sorted(inventory.images, key=lambda item: item.name)
        ]
        await _verify_clean_revision(
            actual_runner,
            tools["git"][0],
            root,
            environment,
            revision,
        )

        manifest = ReleaseManifest(
            source_revision=revision,
            source_uri=source_uri,
            created_at=(now or (lambda: datetime.now(UTC)))(),
            inventory=inventory_evidence,
            lockfile=lockfile_evidence,
            syft_configuration=syft_evidence,
            grype_configuration=grype_evidence,
            tools=tuple(tool_evidence),
            images=tuple(image_evidence),
        )
        manifest_path = evidence_root / "release-manifest.json"
        await asyncio.to_thread(_write_model, manifest_path, manifest, MAX_MANIFEST_BYTES)
        bundle_path = evidence_root / "release-manifest.sigstore.json"
        await _run(
            actual_runner,
            (
                tools["cosign"][0],
                "sign-blob",
                "--yes",
                "--bundle",
                str(bundle_path),
                str(manifest_path),
            ),
            root,
            environment,
        )
        await asyncio.to_thread(_artifact, evidence_root, bundle_path, MAX_JSON_ARTIFACT_BYTES)
        verify_output = await _verify_manifest_signature(
            actual_runner,
            tools["cosign"][0],
            root=root,
            environment=environment,
            manifest=manifest_path,
            bundle=bundle_path,
            identity=certificate_identity,
            issuer=certificate_oidc_issuer,
        )
        await asyncio.to_thread(
            _write_bytes,
            evidence_root / "release-manifest-verification.txt",
            verify_output,
            MAX_PROCESS_OUTPUT_BYTES,
        )
    except BaseException:
        await asyncio.to_thread(_mark_incomplete, evidence_root)
        raise
    else:
        return manifest


async def verify_release(
    evidence_directory: Path,
    *,
    expected_source_revision: str,
    certificate_identity: str,
    certificate_oidc_issuer: str,
    cosign_executable: str,
    cosign_sha256: str,
    runner: ReleaseCommandRunner | None = None,
) -> ReleaseManifest:
    """Revalidate local hashes and remote signatures before promotion."""

    root = await asyncio.to_thread(_evidence_root, evidence_directory)
    _source_identity(
        "https://release-verification.invalid/source", certificate_identity, certificate_oidc_issuer
    )
    executable = await asyncio.to_thread(
        _validated_executable,
        "cosign",
        cosign_executable,
        cosign_sha256,
    )
    revision = _source_revision(expected_source_revision)
    manifest_path = root / "release-manifest.json"
    payload = await asyncio.to_thread(_read_bounded_file, manifest_path, MAX_MANIFEST_BYTES)
    try:
        manifest = ReleaseManifest.model_validate_json(payload)
    except ValueError as error:
        raise SupplyChainError("release manifest is invalid") from error
    if manifest.source_revision != revision:
        raise SupplyChainError("release manifest source revision does not match promotion input")
    manifest_cosign = next(tool for tool in manifest.tools if tool.name == "cosign")
    if manifest_cosign.executable_sha256 != cosign_sha256:
        raise SupplyChainError("promotion Cosign checksum differs from release evidence")
    await asyncio.to_thread(_verify_local_artifacts, root, manifest)

    actual_runner = runner or BoundedProcessRunner()
    environment = _release_environment(root)
    await _verify_manifest_signature(
        actual_runner,
        executable,
        root=root,
        environment=environment,
        manifest=manifest_path,
        bundle=root / "release-manifest.sigstore.json",
        identity=certificate_identity,
        issuer=certificate_oidc_issuer,
    )
    for image in manifest.images:
        await _verify_remote_image(
            actual_runner,
            executable,
            root=root,
            environment=environment,
            image=image,
            identity=certificate_identity,
            issuer=certificate_oidc_issuer,
        )
    return manifest


async def _build_image_evidence(
    root: Path,
    evidence_root: Path,
    image: ReleaseImageSpec,
    *,
    registry: str,
    source_revision: str,
    source_uri: str,
    certificate_identity: str,
    certificate_oidc_issuer: str,
    syft_configuration: Path,
    grype_configuration: Path,
    materials: Mapping[str, str],
    tools: Mapping[str, tuple[str, str]],
    runner: ReleaseCommandRunner,
    environment: Mapping[str, str],
    now: Clock | None,
) -> ImageReleaseEvidence:
    local_reference = f"{registry}/{image.repository}:sha-{source_revision[:12]}"
    selected_materials, containerfile_evidence = await _prepare_local_image(
        root,
        image,
        evidence_root=evidence_root,
        local_reference=local_reference,
        source_revision=source_revision,
        source_uri=source_uri,
        materials=materials,
        podman=tools["podman"][0],
        runner=runner,
        environment=environment,
    )
    local_image_id = await _podman_image_id(
        runner,
        tools["podman"][0],
        root,
        environment,
        local_reference,
    )

    archive_path = evidence_root / f"{image.name}.oci.tar"
    sbom_path = evidence_root / f"{image.name}.spdx.json"
    try:
        await _run(
            runner,
            (
                tools["podman"][0],
                "save",
                "--format=oci-archive",
                "--output",
                str(archive_path),
                local_reference,
            ),
            root,
            environment,
        )
        archive = await asyncio.to_thread(
            _artifact,
            evidence_root,
            archive_path,
            MAX_ARCHIVE_BYTES,
        )
        await _run(
            runner,
            (
                tools["syft"][0],
                "scan",
                f"oci-archive:{archive_path}",
                "--config",
                str(syft_configuration),
                "--output",
                f"spdx-json={sbom_path}",
            ),
            root,
            environment,
        )
    finally:
        await asyncio.to_thread(archive_path.unlink, missing_ok=True)
    await asyncio.to_thread(_validate_spdx, sbom_path)
    sbom = await asyncio.to_thread(_artifact, evidence_root, sbom_path, MAX_JSON_ARTIFACT_BYTES)

    scan_path = evidence_root / f"{image.name}.grype.json"
    await _run(
        runner,
        (
            tools["grype"][0],
            f"sbom:{sbom_path}",
            "--config",
            str(grype_configuration),
            "--fail-on",
            "high",
            "--output",
            "json",
            "--file",
            str(scan_path),
        ),
        root,
        environment,
    )
    await asyncio.to_thread(_validate_grype, scan_path)
    scan = await asyncio.to_thread(_artifact, evidence_root, scan_path, MAX_JSON_ARTIFACT_BYTES)

    if (
        await _podman_image_id(
            runner,
            tools["podman"][0],
            root,
            environment,
            local_reference,
        )
        != local_image_id
    ):
        raise SupplyChainError("local image changed between scanning and publication")

    digest_path = evidence_root / f"{image.name}.digest"
    await _run(
        runner,
        (
            tools["podman"][0],
            "push",
            "--digestfile",
            str(digest_path),
            local_reference,
        ),
        root,
        environment,
    )
    digest = await asyncio.to_thread(_read_digest, digest_path)
    await asyncio.to_thread(digest_path.unlink, missing_ok=True)
    image_reference = f"{registry}/{image.repository}@{digest}"

    provenance_path = evidence_root / f"{image.name}.slsa.json"
    timestamp = (now or (lambda: datetime.now(UTC)))().astimezone(UTC)
    provenance = _slsa_provenance(
        image,
        source_revision=source_revision,
        source_uri=source_uri,
        certificate_identity=certificate_identity,
        materials=selected_materials,
        local_image_id=local_image_id,
        archive=archive,
        sbom=sbom,
        scan=scan,
        timestamp=timestamp,
    )
    await asyncio.to_thread(_write_json, provenance_path, provenance, MAX_JSON_ARTIFACT_BYTES)
    provenance_evidence = await asyncio.to_thread(
        _artifact,
        evidence_root,
        provenance_path,
        MAX_JSON_ARTIFACT_BYTES,
    )

    await _run(
        runner,
        (tools["cosign"][0], "sign", "--yes", image_reference),
        root,
        environment,
    )
    await _run(
        runner,
        (
            tools["cosign"][0],
            "attest",
            "--yes",
            "--type",
            SPDX_ATTESTATION_TYPE,
            "--predicate",
            str(sbom_path),
            image_reference,
        ),
        root,
        environment,
    )
    await _run(
        runner,
        (
            tools["cosign"][0],
            "attest",
            "--yes",
            "--type",
            SLSA_ATTESTATION_TYPE,
            "--predicate",
            str(provenance_path),
            image_reference,
        ),
        root,
        environment,
    )

    signature_path = evidence_root / f"{image.name}.signature-verification.json"
    signature_output = await _verify_cosign_image(
        runner,
        tools["cosign"][0],
        root=root,
        environment=environment,
        image_reference=image_reference,
        identity=certificate_identity,
        issuer=certificate_oidc_issuer,
    )
    await asyncio.to_thread(
        _write_bytes,
        signature_path,
        signature_output,
        MAX_PROCESS_OUTPUT_BYTES,
    )
    await asyncio.to_thread(_validate_cosign_json, signature_path)

    sbom_verification_path = evidence_root / f"{image.name}.sbom-attestation.json"
    sbom_output = await _verify_cosign_attestation(
        runner,
        tools["cosign"][0],
        root=root,
        environment=environment,
        image_reference=image_reference,
        attestation_type=SPDX_ATTESTATION_TYPE,
        identity=certificate_identity,
        issuer=certificate_oidc_issuer,
    )
    await asyncio.to_thread(
        _write_bytes,
        sbom_verification_path,
        sbom_output,
        MAX_PROCESS_OUTPUT_BYTES,
    )
    await asyncio.to_thread(
        _validate_attestation,
        sbom_verification_path,
        sbom_path,
        image_reference,
        SPDX_PREDICATE_TYPE,
    )

    provenance_verification_path = evidence_root / f"{image.name}.slsa-attestation.json"
    provenance_output = await _verify_cosign_attestation(
        runner,
        tools["cosign"][0],
        root=root,
        environment=environment,
        image_reference=image_reference,
        attestation_type=SLSA_ATTESTATION_TYPE,
        identity=certificate_identity,
        issuer=certificate_oidc_issuer,
    )
    await asyncio.to_thread(
        _write_bytes,
        provenance_verification_path,
        provenance_output,
        MAX_PROCESS_OUTPUT_BYTES,
    )
    await asyncio.to_thread(
        _validate_attestation,
        provenance_verification_path,
        provenance_path,
        image_reference,
        SLSA_PREDICATE_TYPE,
    )

    return ImageReleaseEvidence(
        name=image.name,
        image_reference=image_reference,
        digest=digest,
        local_image_id=local_image_id,
        materials=tuple(
            MaterialEvidence(requirement=requirement, reference=reference)
            for requirement, reference in sorted(selected_materials.items())
        ),
        containerfile=containerfile_evidence,
        sbom=sbom,
        vulnerability_scan=scan,
        provenance=provenance_evidence,
        signature_verification=await asyncio.to_thread(
            _artifact,
            evidence_root,
            signature_path,
            MAX_JSON_ARTIFACT_BYTES,
        ),
        sbom_attestation_verification=await asyncio.to_thread(
            _artifact,
            evidence_root,
            sbom_verification_path,
            MAX_JSON_ARTIFACT_BYTES,
        ),
        provenance_attestation_verification=await asyncio.to_thread(
            _artifact,
            evidence_root,
            provenance_verification_path,
            MAX_JSON_ARTIFACT_BYTES,
        ),
    )


async def _prepare_local_image(
    root: Path,
    image: ReleaseImageSpec,
    *,
    evidence_root: Path,
    local_reference: str,
    source_revision: str,
    source_uri: str,
    materials: Mapping[str, str],
    podman: str,
    runner: ReleaseCommandRunner,
    environment: Mapping[str, str],
) -> tuple[dict[str, str], ArtifactEvidence | None]:
    if image.kind is ImageKind.MIRROR:
        requirement = image.source_requirement
        if requirement is None:
            raise SupplyChainError("validated mirror image lost its source requirement")
        selected = {requirement: materials[requirement]}
        await _run(
            runner,
            (podman, "tag", materials[requirement], local_reference),
            root,
            environment,
        )
        return selected, None

    if image.context is None or image.containerfile is None:
        raise SupplyChainError("validated build image lost its source paths")
    context = _contained_path(root, image.context, directory=True)
    containerfile = _contained_file(root, Path(image.containerfile))
    selected = {requirement: materials[requirement] for requirement in image.build_args.values()}
    command = [
        podman,
        "build",
        "--pull=never",
        "--format=oci",
        "--label",
        f"org.opencontainers.image.revision={source_revision}",
        "--label",
        f"org.opencontainers.image.source={source_uri}",
        "--tag",
        local_reference,
        "--file",
        str(containerfile),
    ]
    for argument, requirement in sorted(image.build_args.items()):
        command.extend(("--build-arg", f"{argument}={materials[requirement]}"))
    command.append(str(context))
    await _run(runner, tuple(command), root, environment)
    containerfile_evidence = await asyncio.to_thread(
        _capture_source_artifact,
        evidence_root,
        containerfile,
        f"{image.name}.Containerfile",
    )
    return selected, containerfile_evidence


async def _verify_clean_revision(
    runner: ReleaseCommandRunner,
    git: str,
    root: Path,
    environment: Mapping[str, str],
    revision: str,
) -> None:
    head = (
        (await _run(runner, (git, "rev-parse", "HEAD"), root, environment)).decode("utf-8").strip()
    )
    if head != revision:
        raise SupplyChainError("source revision does not match the checked-out commit")
    status_output = await _run(
        runner,
        (git, "status", "--porcelain=v1", "--untracked-files=all"),
        root,
        environment,
    )
    if status_output.strip():
        raise SupplyChainError("release builds require a clean source checkout")


async def _podman_image_id(
    runner: ReleaseCommandRunner,
    podman: str,
    root: Path,
    environment: Mapping[str, str],
    image_reference: str,
) -> str:
    value = (
        (
            await _run(
                runner,
                (podman, "image", "inspect", "--format", "{{.Id}}", image_reference),
                root,
                environment,
            )
        )
        .decode("ascii")
        .strip()
    )
    if _DIGEST_PATTERN.fullmatch(value) is None:
        raise SupplyChainError("Podman image inspection did not return a SHA-256 image ID")
    return value


async def _verify_remote_image(
    runner: ReleaseCommandRunner,
    cosign: str,
    *,
    root: Path,
    environment: Mapping[str, str],
    image: ImageReleaseEvidence,
    identity: str,
    issuer: str,
) -> None:
    signature = await _verify_cosign_image(
        runner,
        cosign,
        root=root,
        environment=environment,
        image_reference=image.image_reference,
        identity=identity,
        issuer=issuer,
    )
    _validate_cosign_payload(signature)
    attestations = (
        (
            SPDX_ATTESTATION_TYPE,
            root / image.sbom.path,
            SPDX_PREDICATE_TYPE,
        ),
        (
            SLSA_ATTESTATION_TYPE,
            root / image.provenance.path,
            SLSA_PREDICATE_TYPE,
        ),
    )
    for attestation_type, predicate_path, predicate_type in attestations:
        output = await _verify_cosign_attestation(
            runner,
            cosign,
            root=root,
            environment=environment,
            image_reference=image.image_reference,
            attestation_type=attestation_type,
            identity=identity,
            issuer=issuer,
        )
        _validate_attestation_payload(
            output,
            predicate_path,
            image.image_reference,
            predicate_type,
        )


async def _verify_cosign_image(
    runner: ReleaseCommandRunner,
    cosign: str,
    *,
    root: Path,
    environment: Mapping[str, str],
    image_reference: str,
    identity: str,
    issuer: str,
) -> bytes:
    return await _run(
        runner,
        (
            cosign,
            "verify",
            "--certificate-identity",
            identity,
            "--certificate-oidc-issuer",
            issuer,
            "--output",
            "json",
            image_reference,
        ),
        root,
        environment,
    )


async def _verify_cosign_attestation(
    runner: ReleaseCommandRunner,
    cosign: str,
    *,
    root: Path,
    environment: Mapping[str, str],
    image_reference: str,
    attestation_type: str,
    identity: str,
    issuer: str,
) -> bytes:
    return await _run(
        runner,
        (
            cosign,
            "verify-attestation",
            "--certificate-identity",
            identity,
            "--certificate-oidc-issuer",
            issuer,
            "--type",
            attestation_type,
            "--output",
            "json",
            image_reference,
        ),
        root,
        environment,
    )


async def _verify_manifest_signature(
    runner: ReleaseCommandRunner,
    cosign: str,
    *,
    root: Path,
    environment: Mapping[str, str],
    manifest: Path,
    bundle: Path,
    identity: str,
    issuer: str,
) -> bytes:
    await asyncio.to_thread(_read_bounded_file, bundle, MAX_JSON_ARTIFACT_BYTES)
    return await _run(
        runner,
        (
            cosign,
            "verify-blob",
            "--bundle",
            str(bundle),
            "--certificate-identity",
            identity,
            "--certificate-oidc-issuer",
            issuer,
            str(manifest),
        ),
        root,
        environment,
    )


async def _collect_tool_evidence(
    runner: ReleaseCommandRunner,
    tools: Mapping[str, tuple[str, str]],
    root: Path,
    environment: Mapping[str, str],
) -> tuple[ToolEvidence, ...]:
    evidence: list[ToolEvidence] = []
    for name in sorted(tools):
        executable, digest = tools[name]
        output = await _run(runner, (executable, "--version"), root, environment)
        version = output.decode("utf-8").strip()
        if not version or len(version) > MAX_TEXT_LENGTH or "\x00" in version:
            raise SupplyChainError(f"{name} returned an invalid version string")
        evidence.append(
            ToolEvidence(
                name=name,
                executable_sha256=digest,
                version_output_sha256=hashlib.sha256(output).hexdigest(),
                version=version,
            )
        )
    return tuple(evidence)


async def _run(
    runner: ReleaseCommandRunner,
    argv: tuple[str, ...],
    cwd: Path,
    environment: Mapping[str, str],
) -> bytes:
    result = await runner.run(
        argv,
        cwd=cwd,
        timeout_seconds=PROCESS_TIMEOUT_SECONDS,
        max_output_bytes=MAX_PROCESS_OUTPUT_BYTES,
        environment=environment,
    )
    output = "".join(chunk.text for chunk in result.chunks).encode("utf-8")
    if result.timed_out:
        raise SupplyChainError(f"release command timed out: {Path(argv[0]).name} {argv[1]}")
    if result.output_truncated:
        raise SupplyChainError(f"release command output exceeded its limit: {Path(argv[0]).name}")
    if result.exit_code != 0:
        raise SupplyChainError(f"release command failed: {Path(argv[0]).name} {argv[1]}")
    return output


def _load_inventory(path: Path) -> ReleaseInventory:
    payload = _read_bounded_file(path, MAX_INVENTORY_BYTES)
    try:
        loaded = yaml.safe_load(payload)
        inventory = ReleaseInventory.model_validate(loaded)
    except (yaml.YAMLError, ValueError) as error:
        raise SupplyChainError("release image inventory is invalid") from error
    return inventory


def _validate_containerfile(path: Path, build_arguments: frozenset[str]) -> None:
    payload = _read_bounded_file(path, MAX_INVENTORY_BYTES).decode("utf-8")
    lines = [line.strip() for line in payload.splitlines() if line.strip()]
    declared = {
        line.removeprefix("ARG ").split("=", maxsplit=1)[0]
        for line in lines
        if line.startswith("ARG ")
    }
    if not build_arguments.issubset(declared):
        raise SupplyChainError(
            f"containerfile does not declare every release build argument: {path}"
        )
    allowed_bases = {f"${{{argument}}}" for argument in build_arguments}
    from_lines = [line.split() for line in lines if line.upper().startswith("FROM ")]
    if not from_lines or any(
        len(parts) < MIN_FROM_PARTS or parts[1] not in allowed_bases for parts in from_lines
    ):
        raise SupplyChainError(f"containerfile contains a mutable production base: {path}")


def _validate_context_allowlist(context: Path, required_entries: set[str]) -> None:
    ignore_file = _contained_file(context, Path(".containerignore"))
    lines = [
        line.strip()
        for line in _read_bounded_file(ignore_file, MAX_INVENTORY_BYTES)
        .decode("utf-8")
        .splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    if not lines or lines[0] != "**" or "!**" in lines:
        raise SupplyChainError("release build context must use a deny-first allowlist")
    expected = {"**", *required_entries}
    if set(lines) != expected or len(lines) != len(expected):
        raise SupplyChainError("release build context must use the exact source allowlist")


def _validated_materials(
    inventory: ReleaseInventory,
    references: Mapping[str, str],
) -> dict[str, str]:
    expected = {
        requirement for image in inventory.images for requirement in image.build_args.values()
    } | {
        image.source_requirement
        for image in inventory.images
        if image.source_requirement is not None
    }
    if set(references) != expected:
        raise SupplyChainError("material references do not match the release inventory")
    validated: dict[str, str] = {}
    for name, reference in references.items():
        try:
            _require_digest_reference(reference)
        except ValueError as error:
            raise SupplyChainError(f"material reference is not immutable: {name}") from error
        validated[name] = reference
    return validated


def _validated_tools(
    paths: Mapping[str, str],
    expected_hashes: Mapping[str, str],
) -> dict[str, tuple[str, str]]:
    if set(paths) != TOOL_NAMES or set(expected_hashes) != TOOL_NAMES:
        raise SupplyChainError("release tool paths and checksums must name every required tool")
    return {
        name: (
            _validated_executable(name, paths[name], expected_hashes[name]),
            expected_hashes[name],
        )
        for name in sorted(TOOL_NAMES)
    }


def _validated_executable(name: str, value: str, expected_sha256: str) -> str:
    if name not in TOOL_NAMES or not _SHA256_PATTERN.fullmatch(expected_sha256):
        raise SupplyChainError("release tool checksum is invalid")
    located = value if Path(value).is_absolute() else shutil.which(value)
    if located is None:
        raise SupplyChainError(f"required release tool is unavailable: {name}")
    try:
        path = Path(located).resolve(strict=True)
        metadata = path.stat()
    except OSError as error:
        raise SupplyChainError(f"required release tool is unavailable: {name}") from error
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_mode & 0o111 == 0:
        raise SupplyChainError(f"release tool is not a regular executable: {name}")
    try:
        actual = _hash_file(path, 1024 * 1024 * 1024)
    except OSError as error:
        raise SupplyChainError(f"release tool could not be inspected safely: {name}") from error
    if actual != expected_sha256:
        raise SupplyChainError(f"release tool checksum mismatch: {name}")
    return str(path)


def _slsa_provenance(
    image: ReleaseImageSpec,
    *,
    source_revision: str,
    source_uri: str,
    certificate_identity: str,
    materials: Mapping[str, str],
    local_image_id: str,
    archive: ArtifactEvidence,
    sbom: ArtifactEvidence,
    scan: ArtifactEvidence,
    timestamp: datetime,
) -> dict[str, Any]:
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise SupplyChainError("release clock returned a naive timestamp")
    instant = timestamp.astimezone(UTC).isoformat().replace("+00:00", "Z")
    return {
        "builder": {"id": certificate_identity},
        "buildType": f"{source_uri.rstrip('/')}/.github/workflows/release-supply-chain.yml",
        "invocation": {
            "configSource": {
                "uri": f"git+{source_uri}",
                "digest": {"sha1": source_revision},
                "entryPoint": image.containerfile or "mirrored-image",
            },
            "parameters": {
                "image": image.name,
                "localImageId": local_image_id,
                "archiveSha256": archive.sha256,
                "sbomSha256": sbom.sha256,
                "scanSha256": scan.sha256,
            },
            "environment": {"networkPolicy": "release-runner"},
        },
        "buildConfig": {"inventoryVersion": "agent-release-images-v1"},
        "metadata": {
            "buildInvocationId": f"{source_revision}:{image.name}",
            "buildStartedOn": instant,
            "buildFinishedOn": instant,
            "completeness": {"parameters": True, "environment": True, "materials": True},
            "reproducible": False,
        },
        "materials": [
            {
                "uri": reference.rsplit("@", maxsplit=1)[0],
                "digest": {"sha256": reference.rsplit("@sha256:", maxsplit=1)[1]},
            }
            for _, reference in sorted(materials.items())
        ],
    }


def _validate_spdx(path: Path) -> None:
    value = _json_object(path, MAX_JSON_ARTIFACT_BYTES)
    if not str(value.get("spdxVersion", "")).startswith("SPDX-"):
        raise SupplyChainError("generated SBOM is not an SPDX document")
    if not isinstance(value.get("documentNamespace"), str) or not isinstance(
        value.get("packages"), list
    ):
        raise SupplyChainError("generated SPDX document is incomplete")


def _validate_grype(path: Path) -> None:
    value = _json_object(path, MAX_JSON_ARTIFACT_BYTES)
    if not isinstance(value.get("matches"), list) or not isinstance(value.get("source"), dict):
        raise SupplyChainError("vulnerability scan output does not match the Grype JSON contract")
    if value.get("ignoredMatches") != []:
        raise SupplyChainError("release vulnerability scans may not suppress matches")


def _validate_cosign_json(path: Path) -> None:
    _validate_cosign_payload(_read_bounded_file(path, MAX_JSON_ARTIFACT_BYTES))


def _validate_cosign_payload(payload: bytes) -> None:
    value = _decode_json(payload, "Cosign verification")
    if (
        not isinstance(value, list)
        or not value
        or not all(isinstance(item, dict) for item in value)
    ):
        raise SupplyChainError("Cosign verification returned no signed payload")


def _validate_attestation(
    verification_path: Path,
    predicate_path: Path,
    image_reference: str,
    predicate_type: str,
) -> None:
    _validate_attestation_payload(
        _read_bounded_file(verification_path, MAX_JSON_ARTIFACT_BYTES),
        predicate_path,
        image_reference,
        predicate_type,
    )


def _validate_attestation_payload(
    payload: bytes,
    predicate_path: Path,
    image_reference: str,
    predicate_type: str,
) -> None:
    value = _decode_json(payload, "Cosign attestation verification")
    expected_predicate = _json_value(predicate_path, MAX_JSON_ARTIFACT_BYTES)
    digest = image_reference.rsplit("@sha256:", maxsplit=1)[1]
    if not isinstance(value, list) or not value:
        raise SupplyChainError("Cosign attestation verification returned no payload")
    for item in value:
        if not isinstance(item, dict) or not isinstance(item.get("payload"), str):
            continue
        try:
            statement = json.loads(base64.b64decode(item["payload"], validate=True))
        except (ValueError, json.JSONDecodeError):
            continue
        if not isinstance(statement, dict) or statement.get("predicateType") != predicate_type:
            continue
        subjects = statement.get("subject")
        if not isinstance(subjects, list) or not any(
            isinstance(subject, dict)
            and isinstance(subject.get("digest"), dict)
            and subject["digest"].get("sha256") == digest
            for subject in subjects
        ):
            continue
        if statement.get("predicate") == expected_predicate:
            return
    raise SupplyChainError("verified attestation does not bind the expected predicate and image")


def _decode_json(payload: bytes, label: str) -> Any:
    try:
        return json.loads(payload, parse_constant=_reject_json_constant)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        raise SupplyChainError(f"{label} returned invalid JSON") from error


def _json_object(path: Path, maximum: int) -> dict[str, Any]:
    value = _json_value(path, maximum)
    if not isinstance(value, dict):
        raise SupplyChainError(f"JSON artifact must contain an object: {path.name}")
    return value


def _json_value(path: Path, maximum: int) -> Any:
    payload = _read_bounded_file(path, maximum)
    try:
        return json.loads(payload, parse_constant=_reject_json_constant)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        raise SupplyChainError(f"JSON artifact is invalid: {path.name}") from error


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"nonfinite JSON constant: {value}")


def _verify_local_artifacts(root: Path, manifest: ReleaseManifest) -> None:
    for artifact in (
        manifest.inventory,
        manifest.lockfile,
        manifest.syft_configuration,
        manifest.grype_configuration,
    ):
        _verify_artifact(root, artifact)
    for image in manifest.images:
        artifacts = (
            image.sbom,
            image.vulnerability_scan,
            image.provenance,
            image.signature_verification,
            image.sbom_attestation_verification,
            image.provenance_attestation_verification,
        ) + ((image.containerfile,) if image.containerfile is not None else ())
        for artifact in artifacts:
            _verify_artifact(root, artifact)
        _validate_spdx(root / image.sbom.path)
        _validate_grype(root / image.vulnerability_scan.path)
        _validate_cosign_json(root / image.signature_verification.path)
        _validate_attestation(
            root / image.sbom_attestation_verification.path,
            root / image.sbom.path,
            image.image_reference,
            SPDX_PREDICATE_TYPE,
        )
        _validate_attestation(
            root / image.provenance_attestation_verification.path,
            root / image.provenance.path,
            image.image_reference,
            SLSA_PREDICATE_TYPE,
        )


def _verify_artifact(root: Path, artifact: ArtifactEvidence) -> None:
    path = _contained_file(root, Path(artifact.path))
    if path.stat().st_size != artifact.size_bytes:
        raise SupplyChainError(f"release artifact size mismatch: {artifact.path}")
    if _hash_file(path, MAX_ARCHIVE_BYTES) != artifact.sha256:
        raise SupplyChainError(f"release artifact checksum mismatch: {artifact.path}")


def _artifact(root: Path, path: Path, maximum: int) -> ArtifactEvidence:
    contained = _contained_file(root, path)
    size = contained.stat().st_size
    if size <= 0 or size > maximum:
        raise SupplyChainError(f"release artifact size is invalid: {contained.name}")
    return ArtifactEvidence(
        path=contained.relative_to(root).as_posix(),
        size_bytes=size,
        sha256=_hash_file(contained, maximum),
    )


def _capture_source_artifact(
    evidence_root: Path,
    source: Path,
    destination_name: str,
) -> ArtifactEvidence:
    _relative_path(destination_name)
    before = _hash_file(source, MAX_JSON_ARTIFACT_BYTES)
    destination = evidence_root / destination_name
    _write_bytes(
        destination,
        _read_bounded_file(source, MAX_JSON_ARTIFACT_BYTES),
        MAX_JSON_ARTIFACT_BYTES,
    )
    evidence = _artifact(evidence_root, destination, MAX_JSON_ARTIFACT_BYTES)
    if evidence.sha256 != before or _hash_file(source, MAX_JSON_ARTIFACT_BYTES) != before:
        raise SupplyChainError("source artifact changed while release evidence was captured")
    return evidence


def _read_digest(path: Path) -> str:
    value = _read_bounded_file(path, 256).decode("ascii").strip()
    if _DIGEST_PATTERN.fullmatch(value) is None:
        raise SupplyChainError("Podman push did not return an OCI SHA-256 digest")
    return value


def _hash_file(path: Path, maximum: int) -> str:
    if path.is_symlink():
        raise SupplyChainError(f"symbolic links are not valid release artifacts: {path.name}")
    metadata = path.stat()
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > maximum:
        raise SupplyChainError(f"file cannot be hashed safely: {path.name}")
    digest = hashlib.sha256()
    total = 0
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            total += len(chunk)
            if total > maximum:
                raise SupplyChainError(f"file grew beyond its limit: {path.name}")
            digest.update(chunk)
    if total != metadata.st_size:
        raise SupplyChainError(f"file changed while being hashed: {path.name}")
    return digest.hexdigest()


def _read_bounded_file(path: Path, maximum: int) -> bytes:
    if path.is_symlink():
        raise SupplyChainError(f"symbolic links are not valid release artifacts: {path.name}")
    resolved = path.resolve(strict=True)
    metadata = resolved.stat()
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_size <= 0 or metadata.st_size > maximum:
        raise SupplyChainError(f"file size is invalid: {path.name}")
    with resolved.open("rb") as source:
        payload = source.read(maximum + 1)
    if len(payload) != metadata.st_size or len(payload) > maximum:
        raise SupplyChainError(f"file changed or exceeded its limit: {path.name}")
    return payload


def _write_model(path: Path, value: BaseModel, maximum: int) -> None:
    _write_bytes(path, value.model_dump_json(indent=2).encode("utf-8") + b"\n", maximum)


def _write_json(path: Path, value: Any, maximum: int) -> None:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    _write_bytes(path, payload + b"\n", maximum)


def _write_bytes(path: Path, payload: bytes, maximum: int) -> None:
    if not payload or len(payload) > maximum or path.exists():
        raise SupplyChainError(f"refusing to write invalid release artifact: {path.name}")
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def _mark_incomplete(root: Path) -> None:
    marker = root / "INCOMPLETE"
    if marker.exists():
        return
    try:
        _write_bytes(marker, b"release evidence generation failed\n", 1_024)
    except (OSError, SupplyChainError):
        return


def _repository_root(path: Path) -> Path:
    root = path.resolve(strict=True)
    if (
        not root.is_dir()
        or not (root / "pyproject.toml").is_file()
        or not (root / "uv.lock").is_file()
    ):
        raise SupplyChainError("release repository root is invalid")
    return root


def _evidence_root(path: Path) -> Path:
    root = path.resolve(strict=True)
    if not root.is_dir() or root.is_symlink() or (root / "INCOMPLETE").exists():
        raise SupplyChainError("release evidence directory is invalid or incomplete")
    return root


def _create_output_directory(repository: Path, path: Path) -> Path:
    candidate = path if path.is_absolute() else repository / path
    if candidate.exists():
        raise SupplyChainError("release evidence output must not already exist")
    parent = candidate.parent.resolve(strict=True)
    if parent == Path(parent.anchor) or parent.is_symlink():
        raise SupplyChainError("release evidence output parent is unsafe")
    candidate.mkdir(mode=0o700)
    return candidate.resolve(strict=True)


def _contained_path(root: Path, value: str, *, directory: bool) -> Path:
    relative = _relative_path(value, allow_dot=True)
    candidate = root / relative
    if candidate.is_symlink():
        raise SupplyChainError("release inventory paths may not be symbolic links")
    path = candidate.resolve(strict=True)
    if not path.is_relative_to(root) or (directory and not path.is_dir()):
        raise SupplyChainError("release inventory path escaped its repository")
    return path


def _contained_file(root: Path, value: Path) -> Path:
    candidate = value if value.is_absolute() else root / value
    if candidate.is_symlink():
        raise SupplyChainError("release artifact may not be a symbolic link")
    path = candidate.resolve(strict=True)
    if not path.is_relative_to(root) or not path.is_file() or path.is_symlink():
        raise SupplyChainError("release artifact is not a contained regular file")
    return path


def _relative_path(value: str, *, allow_dot: bool = False) -> PurePosixPath:
    path = PurePosixPath(value)
    if (
        not value
        or "\x00" in value
        or path.is_absolute()
        or ".." in path.parts
        or (path == PurePosixPath(".") and not allow_dot)
    ):
        raise ValueError("path must be a contained relative path")
    return path


def _registry_prefix(value: str) -> str:
    normalized = value.rstrip("/")
    if _REGISTRY_PATTERN.fullmatch(normalized) is None:
        raise SupplyChainError("release registry prefix is invalid")
    return normalized


def _source_revision(value: str) -> str:
    if _REVISION_PATTERN.fullmatch(value) is None:
        raise SupplyChainError("release source revision must be a full lowercase Git SHA-1")
    return value


def _source_identity(source_uri: str, identity: str, issuer: str) -> None:
    for name, value in (
        ("source URI", source_uri),
        ("certificate identity", identity),
        ("OIDC issuer", issuer),
    ):
        parsed = urlsplit(value)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or len(value) > MAX_TEXT_LENGTH
            or any(character in value for character in ("\x00", "\r", "\n"))
        ):
            raise SupplyChainError(f"release {name} is invalid")


def _require_digest_reference(value: str) -> None:
    if _DIGEST_REFERENCE_PATTERN.fullmatch(value) is None:
        raise ValueError("OCI image references must use a full registry SHA-256 digest")


def _release_environment(repository: Path) -> dict[str, str]:
    allowed = (
        "ACTIONS_ID_TOKEN_REQUEST_TOKEN",
        "ACTIONS_ID_TOKEN_REQUEST_URL",
        "HOME",
        "PATH",
        "REGISTRY_AUTH_FILE",
        "SIGSTORE_ID_TOKEN",
        "SSL_CERT_DIR",
        "SSL_CERT_FILE",
        "TMPDIR",
        "XDG_RUNTIME_DIR",
    )
    environment = {name: os.environ[name] for name in allowed if name in os.environ}
    environment.update(
        {
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_TERMINAL_PROMPT": "0",
            "LC_ALL": "C.UTF-8",
            "PYTHONPATH": str(repository),
        }
    )
    return environment


def _parse_assignments(values: Sequence[str], *, label: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values:
        name, separator, assigned = value.partition("=")
        if not separator or not name or not assigned or name in result:
            raise SupplyChainError(f"{label} assignments must use unique NAME=VALUE entries")
        result[name] = assigned
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    validate = subparsers.add_parser("validate", help="validate immutable image build contracts")
    validate.add_argument("--repository", type=Path, default=Path())
    validate.add_argument("--inventory", type=Path, default=Path("release/images.yaml"))
    tools = subparsers.add_parser("verify-tools", help="verify trusted release executables")
    tools.add_argument("--tool", action="append", default=[])
    tools.add_argument("--tool-sha256", action="append", default=[])
    build = subparsers.add_parser("build", help="build and sign a complete release evidence set")
    build.add_argument("--repository", type=Path, default=Path())
    build.add_argument("--inventory", type=Path, default=Path("release/images.yaml"))
    build.add_argument("--output", type=Path, required=True)
    build.add_argument("--registry-prefix", required=True)
    build.add_argument("--source-revision", required=True)
    build.add_argument("--source-uri", required=True)
    build.add_argument("--certificate-identity", required=True)
    build.add_argument("--certificate-oidc-issuer", required=True)
    build.add_argument("--material", action="append", default=[])
    build.add_argument("--tool", action="append", default=[])
    build.add_argument("--tool-sha256", action="append", default=[])

    verify = subparsers.add_parser("verify", help="verify evidence before promotion")
    verify.add_argument("--evidence", type=Path, required=True)
    verify.add_argument("--expected-source-revision", required=True)
    verify.add_argument("--certificate-identity", required=True)
    verify.add_argument("--certificate-oidc-issuer", required=True)
    verify.add_argument("--cosign", required=True)
    verify.add_argument("--cosign-sha256", required=True)
    return parser


async def _main(arguments: Sequence[str] | None = None) -> int:
    options = _parser().parse_args(arguments)
    if options.command == "validate":
        inventory = validate_release_inventory(
            options.repository,
            inventory_path=options.inventory,
        )
        sys.stdout.write(f"validated {len(inventory.images)} immutable release image contracts\n")
        return 0
    if options.command == "verify-tools":
        tools = validate_release_tools(
            _parse_assignments(options.tool, label="tool"),
            _parse_assignments(options.tool_sha256, label="tool checksum"),
        )
        sys.stdout.write(f"verified {len(tools)} trusted release executables\n")
        return 0
    if options.command == "build":
        manifest = await build_release(
            options.repository,
            inventory_path=options.inventory,
            output_directory=options.output,
            registry_prefix=options.registry_prefix,
            source_revision=options.source_revision,
            source_uri=options.source_uri,
            certificate_identity=options.certificate_identity,
            certificate_oidc_issuer=options.certificate_oidc_issuer,
            material_references=_parse_assignments(options.material, label="material"),
            executable_paths=_parse_assignments(options.tool, label="tool"),
            executable_sha256=_parse_assignments(options.tool_sha256, label="tool checksum"),
        )
    else:
        manifest = await verify_release(
            options.evidence,
            expected_source_revision=options.expected_source_revision,
            certificate_identity=options.certificate_identity,
            certificate_oidc_issuer=options.certificate_oidc_issuer,
            cosign_executable=options.cosign,
            cosign_sha256=options.cosign_sha256,
        )
    sys.stdout.write(
        f"verified release {manifest.source_revision} with "
        f"{len(manifest.images)} immutable images\n"
    )
    return 0


def main(arguments: Sequence[str] | None = None) -> int:
    """Synchronous command entrypoint."""

    try:
        return asyncio.run(_main(arguments))
    except SupplyChainError as error:
        raise SystemExit(str(error)) from error


if __name__ == "__main__":
    raise SystemExit(main())
