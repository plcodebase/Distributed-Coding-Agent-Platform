# Production safety layer

This Kustomize layer installs the base together with a fail-closed `ValidatingAdmissionPolicy`.
It deliberately does not contain organization-specific registries, image digests, OIDC URLs,
managed-service CIDRs, ingress certificates, DNS names, or external-secret store identifiers.

Build a site overlay on top of this directory. Replace those values, install the admission policy,
and only then label the target namespace:

```console
kubectl label namespace agent-platform agent-platform.openai.com/production=true
```

Once the label exists, admission denies mutable images, weakened pod/container security contexts,
unexpected host paths, Podman-socket access outside the node agent, and provider-secret access
outside LiteLLM. Use a cluster version that serves `admissionregistration.k8s.io/v1`
`ValidatingAdmissionPolicy`, and verify that the admission plugin is enabled before rollout.
