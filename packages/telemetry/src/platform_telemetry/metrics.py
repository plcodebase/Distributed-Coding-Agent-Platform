"""Bounded-cardinality Prometheus metrics for platform control loops."""

from __future__ import annotations

import hashlib
import math
import threading
from dataclasses import dataclass

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

_LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60)
_QUEUE_BUCKETS = (0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900)
_SANDBOX_BUCKETS = (0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 60)
_ALLOWED_PRIORITIES = frozenset({"interactive", "background", "evaluation"})
_ALLOWED_CIRCUIT_STATES = frozenset({"closed", "open", "half_open"})
_ALLOWED_RUN_STATES = frozenset(
    {
        "queued",
        "leased",
        "running",
        "waiting_approval",
        "retry_pending",
        "lost",
        "completed",
        "failed",
        "cancelled",
    }
)
_ALLOWED_HTTP_METHODS = frozenset(
    {"CONNECT", "DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT", "TRACE"}
)
_MAX_LABEL_BYTES = 128


def _positive_limit(value: int, *, name: str, maximum: int = 100_000) -> int:
    if type(value) is not int or not 1 <= value <= maximum:
        raise ValueError(f"{name} must be an integer in [1, {maximum}]")
    return value


def _finite_nonnegative(value: float, *, name: str) -> float:
    numeric = float(value)
    if not math.isfinite(numeric) or numeric < 0:
        raise ValueError(f"{name} must be finite and nonnegative")
    return numeric


@dataclass(frozen=True, slots=True)
class MetricsSettings:
    """Cardinality and payload bounds for one metrics registry."""

    max_tenant_labels: int = 1_000
    max_route_labels: int = 100
    max_tool_labels: int = 100
    max_component_labels: int = 100

    def __post_init__(self) -> None:
        _positive_limit(self.max_tenant_labels, name="max_tenant_labels")
        _positive_limit(self.max_route_labels, name="max_route_labels")
        _positive_limit(self.max_tool_labels, name="max_tool_labels")
        _positive_limit(self.max_component_labels, name="max_component_labels")


class _BoundedLabels:
    def __init__(self, maximum: int) -> None:
        self._maximum = maximum
        self._values: set[str] = set()
        self._lock = threading.Lock()

    def resolve(self, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized.encode("utf-8")) > _MAX_LABEL_BYTES:
            return "overflow"
        with self._lock:
            if normalized in self._values:
                return normalized
            if len(self._values) >= self._maximum:
                return "overflow"
            self._values.add(normalized)
        return normalized


class PlatformMetrics:
    """Owned Prometheus registry with methods that enforce label policy."""

    content_type = "text/plain; version=0.0.4; charset=utf-8"

    def __init__(
        self,
        settings: MetricsSettings | None = None,
        *,
        registry: CollectorRegistry | None = None,
    ) -> None:
        resolved = settings or MetricsSettings()
        self.registry = registry or CollectorRegistry(auto_describe=True)
        self._tenants = _BoundedLabels(resolved.max_tenant_labels)
        self._routes = _BoundedLabels(resolved.max_route_labels)
        self._tools = _BoundedLabels(resolved.max_tool_labels)
        self._components = _BoundedLabels(resolved.max_component_labels)

        self.api_requests = Counter(
            "agent_platform_api_requests_total",
            "Accepted API requests by bounded route, method, and status.",
            ("route", "method", "status"),
            registry=self.registry,
        )
        self.api_duration = Histogram(
            "agent_platform_api_request_duration_seconds",
            "API request latency by bounded route and method.",
            ("route", "method"),
            buckets=_LATENCY_BUCKETS,
            registry=self.registry,
        )
        self.active_runs = Gauge(
            "agent_platform_active_runs",
            "Runs currently owned by workers.",
            registry=self.registry,
        )
        self.runs_accepted = Counter(
            "agent_platform_runs_accepted_total",
            "New durable runs accepted by the API; idempotent replays are excluded.",
            registry=self.registry,
        )
        self.run_state_transitions = Counter(
            "agent_platform_run_state_transitions_total",
            "Durable worker completion transitions by bounded run state.",
            ("state",),
            registry=self.registry,
        )
        self.run_recoveries = Counter(
            "agent_platform_run_recoveries_total",
            "Expired run attempts durably recovered by the scheduler.",
            registry=self.registry,
        )
        self.event_reconnects = Counter(
            "agent_platform_event_reconnects_total",
            "Authenticated event streams resumed after a durable sequence.",
            registry=self.registry,
        )
        self.idempotent_replays = Counter(
            "agent_platform_idempotent_replays_total",
            "Previously committed results reused by bounded component.",
            ("component",),
            registry=self.registry,
        )
        self.queue_depth = Gauge(
            "agent_platform_queue_depth",
            "Queued runs by priority class.",
            ("priority",),
            registry=self.registry,
        )
        self.oldest_queued_seconds = Gauge(
            "agent_platform_oldest_queued_seconds",
            "Age of the oldest queued run.",
            registry=self.registry,
        )
        self.worker_slots = Gauge(
            "agent_platform_worker_slots",
            "Worker slots by state.",
            ("state",),
            registry=self.registry,
        )
        self.worker_utilization = Gauge(
            "agent_platform_worker_utilization_ratio",
            "Fraction of configured worker slots in use.",
            registry=self.registry,
        )
        self.queue_wait = Histogram(
            "agent_platform_queue_wait_seconds",
            "Time from queue admission to worker lease acquisition.",
            buckets=_QUEUE_BUCKETS,
            registry=self.registry,
        )
        self.context_build = Histogram(
            "agent_platform_context_build_duration_seconds",
            "Bounded context construction latency.",
            buckets=_LATENCY_BUCKETS,
            registry=self.registry,
        )
        self.gateway_wait = Histogram(
            "agent_platform_gateway_wait_duration_seconds",
            "Gateway admission and upstream wait latency by route.",
            ("route",),
            buckets=_LATENCY_BUCKETS,
            registry=self.registry,
        )
        self.model_requests = Counter(
            "agent_platform_model_requests_total",
            "Model requests by route and terminal outcome.",
            ("route", "outcome"),
            registry=self.registry,
        )
        self.model_latency = Histogram(
            "agent_platform_model_request_duration_seconds",
            "Model request latency by route.",
            ("route",),
            buckets=_LATENCY_BUCKETS,
            registry=self.registry,
        )
        self.model_first_token = Histogram(
            "agent_platform_model_time_to_first_token_seconds",
            "Model time to first output by route.",
            ("route",),
            buckets=_LATENCY_BUCKETS,
            registry=self.registry,
        )
        self.model_tokens = Counter(
            "agent_platform_model_tokens_total",
            "Model tokens by route and direction.",
            ("route", "direction"),
            registry=self.registry,
        )
        self.model_cost = Counter(
            "agent_platform_model_cost_usd_total",
            "Reported model cost by opaque bounded tenant label and route.",
            ("tenant", "route"),
            registry=self.registry,
        )
        self.provider_requests = Counter(
            "agent_platform_provider_requests_total",
            "Upstream provider attempts by route and outcome.",
            ("route", "outcome"),
            registry=self.registry,
        )
        self.gateway_retries = Counter(
            "agent_platform_gateway_retries_total",
            "Gateway retries by route and category.",
            ("route", "category"),
            registry=self.registry,
        )
        self.gateway_fallbacks = Counter(
            "agent_platform_gateway_fallbacks_total",
            "Gateway fallbacks by route and category.",
            ("route", "category"),
            registry=self.registry,
        )
        self.circuit_state = Gauge(
            "agent_platform_gateway_circuit_state",
            "One-hot circuit state by route and state.",
            ("route", "state"),
            registry=self.registry,
        )
        self.tool_calls = Counter(
            "agent_platform_tool_calls_total",
            "Tool calls by bounded tool name and outcome.",
            ("tool", "outcome"),
            registry=self.registry,
        )
        self.tool_duration = Histogram(
            "agent_platform_tool_duration_seconds",
            "Tool execution latency by bounded tool name.",
            ("tool",),
            buckets=_LATENCY_BUCKETS,
            registry=self.registry,
        )
        self.sandbox_startup = Histogram(
            "agent_platform_sandbox_startup_duration_seconds",
            "Sandbox creation latency by outcome.",
            ("outcome",),
            buckets=_SANDBOX_BUCKETS,
            registry=self.registry,
        )
        self.checkpoints = Counter(
            "agent_platform_checkpoints_total",
            "Checkpoint operations by outcome.",
            ("outcome",),
            registry=self.registry,
        )
        self.checkpoint_duration = Histogram(
            "agent_platform_checkpoint_duration_seconds",
            "Checkpoint creation latency.",
            buckets=_LATENCY_BUCKETS,
            registry=self.registry,
        )
        self.errors = Counter(
            "agent_platform_errors_total",
            "Structured errors by stable category and component.",
            ("category", "component"),
            registry=self.registry,
        )

    def render(self) -> bytes:
        return generate_latest(self.registry)

    def route(self, value: str) -> str:
        return self._routes.resolve(value)

    def tool(self, value: str) -> str:
        return self._tools.resolve(value)

    def tenant(self, value: str) -> str:
        digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]
        return self._tenants.resolve(f"tenant_{digest}")

    @staticmethod
    def method(value: str) -> str:
        normalized = value.upper()
        return normalized if normalized in _ALLOWED_HTTP_METHODS else "OTHER"

    def component(self, value: str) -> str:
        return self._components.resolve(value)

    def observe_queue(self, *, depth: dict[str, int], oldest_seconds: float) -> None:
        for priority in _ALLOWED_PRIORITIES:
            value = depth.get(priority, 0)
            if type(value) is not int or value < 0:
                raise ValueError("queue depth values must be nonnegative integers")
            self.queue_depth.labels(priority=priority).set(value)
        self.oldest_queued_seconds.set(_finite_nonnegative(oldest_seconds, name="oldest_seconds"))

    def observe_worker(self, *, active: int, total: int) -> None:
        _positive_limit(total, name="total", maximum=1_000_000)
        if type(active) is not int or not 0 <= active <= total:
            raise ValueError("active must be an integer between zero and total")
        self.active_runs.set(active)
        self.worker_slots.labels(state="active").set(active)
        self.worker_slots.labels(state="available").set(total - active)
        self.worker_utilization.set(active / total)

    def record_cost(self, *, tenant_id: str, route: str, usd: float) -> None:
        self.model_cost.labels(
            tenant=self.tenant(tenant_id),
            route=self.route(route),
        ).inc(_finite_nonnegative(usd, name="usd"))

    def record_run_state(self, state: str) -> None:
        if state not in _ALLOWED_RUN_STATES:
            raise ValueError("state is not a supported run state")
        self.run_state_transitions.labels(state=state).inc()

    def record_replay(self, component: str) -> None:
        self.idempotent_replays.labels(component=self.component(component)).inc()

    def set_circuit(self, *, route: str, state: str) -> None:
        if state not in _ALLOWED_CIRCUIT_STATES:
            raise ValueError("state must be closed, open, or half_open")
        route_label = self.route(route)
        for candidate in _ALLOWED_CIRCUIT_STATES:
            self.circuit_state.labels(route=route_label, state=candidate).set(
                1 if candidate == state else 0
            )


__all__ = ["MetricsSettings", "PlatformMetrics"]
