"""
Phase 6.6: Metrics Collection Infrastructure

Provides read-only observability to prove Phase 6.5 invariants.

Design Constraints:
- Non-blocking (< 10 microseconds per call)
- Failure-tolerant (never raises exceptions)
- Side-effect free (no control flow)
- Disable-able (feature flag default OFF)
- After-commit only (emitted after database commits)

Author: Claude Code
Date: 2025-12-30
"""

import os
import time
from typing import Dict, Optional, List, Tuple
from threading import Lock
from collections import defaultdict


# ============================================================================
# Metrics Configuration
# ============================================================================

def metrics_enabled() -> bool:
    """
    Check if metrics collection is enabled.

    Returns:
        True if ENABLE_METRICS=true, False otherwise
    """
    return os.getenv('ENABLE_METRICS', 'false').lower() == 'true'


# ============================================================================
# Metrics Collector
# ============================================================================

class MetricsCollector:
    """
    Non-blocking metrics collector with in-memory buffering.

    Contract:
    - All methods are non-blocking (< 10 microseconds)
    - Never raises exceptions (failures silently ignored)
    - Thread-safe (uses lock for buffer updates)
    - Disabled by default (ENABLE_METRICS=false)

    Supported Metric Types:
    - Counter: Monotonically increasing value (increments only)
    - Gauge: Point-in-time value (can go up or down)
    - Histogram: Distribution of values (stores samples)
    """

    def __init__(self):
        """Initialize metrics collector."""
        self._enabled = metrics_enabled()

        # In-memory buffers (protected by lock)
        self._lock = Lock()
        self._counters: Dict[Tuple[str, ...], float] = defaultdict(float)
        self._gauges: Dict[Tuple[str, ...], float] = {}
        self._histograms: Dict[Tuple[str, ...], List[float]] = defaultdict(list)

        # Cardinality limits
        self.MAX_CARDINALITY = 10000  # Total metric series limit

    def increment(self, metric_name: str, value: float = 1.0, tags: Optional[Dict[str, str]] = None):
        """
        Increment a counter metric.

        Args:
            metric_name: Metric name (e.g., 'charles_jobs_total')
            value: Increment amount (default: 1.0)
            tags: Optional labels (e.g., {'status': 'COMPLETED'})

        Performance: < 10 microseconds (non-blocking)
        Failure: Never raises (silent no-op on error)
        """
        if not self._enabled:
            return

        try:
            key = self._make_key(metric_name, tags)

            with self._lock:
                # Check cardinality limit
                if key not in self._counters and self._total_series() >= self.MAX_CARDINALITY:
                    # Drop metric to prevent cardinality explosion
                    return

                self._counters[key] += value
        except Exception:
            # Never raise - metrics failures don't impact execution
            pass

    def gauge(self, metric_name: str, value: float, tags: Optional[Dict[str, str]] = None):
        """
        Set a gauge metric to a specific value.

        Args:
            metric_name: Metric name (e.g., 'charles_jobs_current')
            value: Current value
            tags: Optional labels (e.g., {'status': 'RUNNING'})

        Performance: < 10 microseconds (non-blocking)
        Failure: Never raises (silent no-op on error)
        """
        if not self._enabled:
            return

        try:
            key = self._make_key(metric_name, tags)

            with self._lock:
                # Check cardinality limit
                if key not in self._gauges and self._total_series() >= self.MAX_CARDINALITY:
                    return

                self._gauges[key] = value
        except Exception:
            pass

    def histogram(self, metric_name: str, value: float, tags: Optional[Dict[str, str]] = None):
        """
        Record a histogram sample.

        Args:
            metric_name: Metric name (e.g., 'charles_job_age_seconds')
            value: Sample value
            tags: Optional labels (e.g., {'status': 'QUEUED'})

        Performance: < 10 microseconds (non-blocking)
        Failure: Never raises (silent no-op on error)
        """
        if not self._enabled:
            return

        try:
            key = self._make_key(metric_name, tags)

            with self._lock:
                # Check cardinality limit
                if key not in self._histograms and self._total_series() >= self.MAX_CARDINALITY:
                    return

                # Store sample (limit buffer size to 10000 samples per histogram)
                if len(self._histograms[key]) < 10000:
                    self._histograms[key].append(value)
        except Exception:
            pass

    def get_prometheus_text(self) -> str:
        """
        Export metrics in Prometheus text format.

        Returns:
            Prometheus-formatted metrics string

        Format:
            # TYPE metric_name counter
            metric_name{label="value"} 42.0
        """
        if not self._enabled:
            return ""

        lines = []

        with self._lock:
            # Export counters
            for key, value in sorted(self._counters.items()):
                metric_name, labels = self._parse_key(key)
                lines.append(f"# TYPE {metric_name} counter")
                lines.append(f"{metric_name}{{{labels}}} {value}")

            # Export gauges
            for key, value in sorted(self._gauges.items()):
                metric_name, labels = self._parse_key(key)
                lines.append(f"# TYPE {metric_name} gauge")
                lines.append(f"{metric_name}{{{labels}}} {value}")

            # Export histograms (as summary with count and sum)
            for key, samples in sorted(self._histograms.items()):
                if not samples:
                    continue

                metric_name, labels = self._parse_key(key)
                count = len(samples)
                total = sum(samples)

                lines.append(f"# TYPE {metric_name} summary")
                lines.append(f"{metric_name}_count{{{labels}}} {count}")
                lines.append(f"{metric_name}_sum{{{labels}}} {total}")

        return "\n".join(lines) + "\n" if lines else ""

    def reset(self):
        """
        Reset all metrics (for testing only).

        WARNING: Do not use in production.
        """
        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._histograms.clear()

    # ========================================================================
    # Internal Helpers
    # ========================================================================

    def _make_key(self, metric_name: str, tags: Optional[Dict[str, str]]) -> Tuple[str, ...]:
        """
        Create unique key for metric series.

        Args:
            metric_name: Metric name
            tags: Optional labels

        Returns:
            Tuple of (metric_name, tag1_key, tag1_value, tag2_key, tag2_value, ...)
        """
        key_parts = [metric_name]

        if tags:
            # Sort tags for consistent key ordering
            for tag_key, tag_value in sorted(tags.items()):
                key_parts.append(tag_key)
                key_parts.append(tag_value)

        return tuple(key_parts)

    def _parse_key(self, key: Tuple[str, ...]) -> Tuple[str, str]:
        """
        Parse key back into metric name and Prometheus labels.

        Args:
            key: Tuple from _make_key

        Returns:
            (metric_name, prometheus_labels_string)

        Example:
            ('charles_jobs_total', 'status', 'COMPLETED')
            → ('charles_jobs_total', 'status="COMPLETED"')
        """
        metric_name = key[0]

        if len(key) == 1:
            return metric_name, ""

        # Build Prometheus labels: status="COMPLETED",workspace_id="ws_test"
        labels = []
        for i in range(1, len(key), 2):
            if i + 1 < len(key):
                label_key = key[i]
                label_value = key[i + 1]
                labels.append(f'{label_key}="{label_value}"')

        return metric_name, ",".join(labels)

    def _total_series(self) -> int:
        """
        Get total number of metric series (for cardinality limit).

        Returns:
            Total unique metric series across all types
        """
        return len(self._counters) + len(self._gauges) + len(self._histograms)


# ============================================================================
# Global Metrics Instance
# ============================================================================

# Global metrics collector (singleton)
# Initialized on module import
metrics = MetricsCollector()
