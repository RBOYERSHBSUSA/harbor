"""
Observability Interface Layer - Phase 5.3

Abstract interfaces for metrics and tracing instrumentation.
Provides no-op default implementations with zero overhead.

Usage:
    from shared.observability_interface import get_metrics_collector, get_tracing_provider

    metrics = get_metrics_collector()
    metrics.increment_counter("sync_started", workspace_id, {"status": "started"})

    tracer = get_tracing_provider()
    with tracer.span("workspace_sync", workspace_id):
        # ... operation ...

Contract:
- All instrumentation MUST include workspace_id (required parameter)
- Default implementations are no-op (zero overhead when disabled)
- No external dependencies or backend assumptions
- Interfaces designed for future backend integration (Prometheus, OpenTelemetry, etc.)
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from contextlib import contextmanager


class MetricsCollector(ABC):
    """
    Abstract interface for metrics collection.

    All methods MUST include workspace_id for proper multitenancy isolation.
    """

    @abstractmethod
    def increment_counter(
        self,
        name: str,
        workspace_id: str,
        labels: Optional[Dict[str, str]] = None,
        value: int = 1
    ):
        """
        Increment a counter metric.

        Args:
            name: Metric name (e.g., "sync_started", "oauth_refresh")
            workspace_id: Workspace ID (required for isolation)
            labels: Additional labels/dimensions (e.g., {"status": "success"})
            value: Amount to increment by (default: 1)
        """
        pass

    @abstractmethod
    def record_histogram(
        self,
        name: str,
        value: float,
        workspace_id: str,
        labels: Optional[Dict[str, str]] = None
    ):
        """
        Record a histogram value (e.g., duration, size).

        Args:
            name: Metric name (e.g., "qbo_api_duration", "sync_duration")
            value: Value to record (e.g., duration in seconds)
            workspace_id: Workspace ID (required for isolation)
            labels: Additional labels/dimensions (e.g., {"operation": "create_deposit"})
        """
        pass

    @abstractmethod
    def set_gauge(
        self,
        name: str,
        value: float,
        workspace_id: str,
        labels: Optional[Dict[str, str]] = None
    ):
        """
        Set a gauge value (e.g., current state, health score).

        Args:
            name: Metric name (e.g., "workspace_health_score", "pending_syncs")
            value: Current value
            workspace_id: Workspace ID (required for isolation)
            labels: Additional labels/dimensions
        """
        pass


class TracingProvider(ABC):
    """
    Abstract interface for distributed tracing.

    All spans MUST include workspace_id for proper multitenancy isolation.
    """

    @abstractmethod
    @contextmanager
    def span(
        self,
        name: str,
        workspace_id: str,
        attributes: Optional[Dict[str, Any]] = None
    ):
        """
        Create a trace span context manager.

        Args:
            name: Span name (e.g., "workspace_sync", "qbo_create_deposit")
            workspace_id: Workspace ID (required for isolation)
            attributes: Additional span attributes (e.g., {"sync_run_id": "..."})

        Usage:
            with tracer.span("operation_name", workspace_id, {"attr": "value"}):
                # ... operation ...
        """
        pass


# =============================================================================
# No-Op Implementations (Default - Zero Overhead)
# =============================================================================


class NoOpMetricsCollector(MetricsCollector):
    """
    No-op metrics collector (default when observability disabled).

    Zero overhead - all methods are empty.
    """

    def increment_counter(
        self,
        name: str,
        workspace_id: str,
        labels: Optional[Dict[str, str]] = None,
        value: int = 1
    ):
        """No-op: does nothing."""
        pass

    def record_histogram(
        self,
        name: str,
        value: float,
        workspace_id: str,
        labels: Optional[Dict[str, str]] = None
    ):
        """No-op: does nothing."""
        pass

    def set_gauge(
        self,
        name: str,
        value: float,
        workspace_id: str,
        labels: Optional[Dict[str, str]] = None
    ):
        """No-op: does nothing."""
        pass


class NoOpTracingProvider(TracingProvider):
    """
    No-op tracing provider (default when observability disabled).

    Zero overhead - span context manager does nothing.
    """

    @contextmanager
    def span(
        self,
        name: str,
        workspace_id: str,
        attributes: Optional[Dict[str, Any]] = None
    ):
        """No-op: yields immediately without creating a span."""
        yield None


# =============================================================================
# Global Singleton Configuration
# =============================================================================

# Default to no-op implementations (zero overhead)
_metrics_collector: MetricsCollector = NoOpMetricsCollector()
_tracing_provider: TracingProvider = NoOpTracingProvider()


def get_metrics_collector() -> MetricsCollector:
    """
    Get the configured metrics collector.

    Returns:
        MetricsCollector instance (default: NoOpMetricsCollector)
    """
    return _metrics_collector


def get_tracing_provider() -> TracingProvider:
    """
    Get the configured tracing provider.

    Returns:
        TracingProvider instance (default: NoOpTracingProvider)
    """
    return _tracing_provider


def configure_observability(
    metrics_collector: Optional[MetricsCollector] = None,
    tracing_provider: Optional[TracingProvider] = None
):
    """
    Configure observability providers at application startup.

    Args:
        metrics_collector: MetricsCollector implementation (None to keep current)
        tracing_provider: TracingProvider implementation (None to keep current)

    Usage:
        # At application startup (future):
        from observability_backends.prometheus import PrometheusMetricsCollector
        from observability_backends.opentelemetry import OpenTelemetryTracingProvider

        configure_observability(
            metrics_collector=PrometheusMetricsCollector(),
            tracing_provider=OpenTelemetryTracingProvider()
        )

    Note:
        Called once during application initialization.
        Defaults to no-op implementations if not configured.
    """
    global _metrics_collector, _tracing_provider

    if metrics_collector is not None:
        _metrics_collector = metrics_collector

    if tracing_provider is not None:
        _tracing_provider = tracing_provider
