# Evaluation and evidence

The evaluation system has four independently versioned inputs:

1. deployment scenarios for queue scale-up, graceful drain, and abrupt recovery;
2. load profiles for API, events, workers, sandboxes, gateway, and data-store contention;
3. ten chaos scenarios with explicit expected outcomes and cleanup;
4. thirty deterministic coding tasks spanning the ten categories in the design.

Every harness uses bounded typed reports, opaque errors, atomic output, and explicit simulation/live
modes. Coding fixtures are private temporary repositories. Their verification commands are
declarative; the host harness never executes model-authored commands. A trusted live driver must run
verification through the hardened Podman sandbox.

`scripts.final_benchmark_report` validates all supplied artifacts, rejects duplicate JSON keys,
enforces one source revision, optionally checks a trusted SHA-256 index, and records every artifact
digest. Simulation can mark a correctness contract `verified_simulation`; only live artifacts can
populate measured metrics. Missing live evidence makes the report `incomplete`, never zero or
implicitly successful.

The committed baseline report is therefore intentionally incomplete. It demonstrates the evaluation
pipeline and identifies external acceptance work; it is not a résumé-performance report.
