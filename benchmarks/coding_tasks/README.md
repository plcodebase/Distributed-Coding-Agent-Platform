# Coding-task evaluation corpus

`tasks.yaml` defines 30 deterministic tasks: three variants in each of the ten categories required
by the implementation design. `scripts.coding_evaluation` materializes each fixture in a private
temporary directory and passes it to an injected driver.

The default driver is a deterministic simulation of the harness contract. It validates corpus
materialization and metric/report aggregation, but it is not evidence of coding ability. Run a real
campaign only with a trusted driver that submits the task to the deployed platform, runs verification
inside the platform's hardened Podman sandbox, and returns `mode=live` observations:

```console
python -m scripts.coding_evaluation \
  --mode live \
  --driver your_package.evaluation:create_driver \
  --campaign-id production-eval-001 \
  --source-revision <git-sha> \
  --output benchmarks/reports/coding-production-eval-001.json
```

The harness never executes the manifest's verification commands on the host. They are declarative
inputs to the trusted live driver. Reports from the built-in simulation are always labeled
`simulation_only` and cannot be used for résumé or production-performance claims.
