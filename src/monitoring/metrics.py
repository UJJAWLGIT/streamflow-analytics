"""
metrics.py — Prometheus Monitoring & PagerDuty Alerting
========================================================
Emits pipeline metrics to Prometheus for Grafana dashboards.
Triggers PagerDuty alerts on SLO breaches.

Metrics:
  - pipeline.step.duration_seconds     (gauge, by step + env)
  - pipeline.rows.written_total        (counter, by table)
  - dq.check.pass_rate                 (gauge, by table + check_type)
  - data.freshness.lag_hours           (gauge, by table)
  - api.request.latency_p99            (histogram, by endpoint)

Alerts (PagerDuty):
  - P1: data freshness > 8h (SLO breach)
  - P1: DQ pass rate < 95% on CRITICAL checks
  - P2: pipeline step runtime > 2× baseline
  - P2: row count deviation > 20%
"""

from __future__ import annotations

import json
import logging
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Generator, Optional

import requests

logger = logging.getLogger(__name__)

# ── Prometheus client (optional dependency) ───────────────────────────────────
try:
    from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, push_to_gateway
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    logger.warning("prometheus_client not installed — metrics will be logged only")


# ── Config ─────────────────────────────────────────────────────────────────────
PROMETHEUS_GATEWAY = os.getenv("PROMETHEUS_PUSHGATEWAY_URL", "http://localhost:9091")
PAGERDUTY_ROUTING_KEY = os.getenv("PAGERDUTY_ROUTING_KEY", "")
ENV = os.getenv("STREAMFLOW_ENV", "local")
JOB_NAME = "streamflow_analytics"

# ── Baseline runtimes (seconds) — alert if > 2× ───────────────────────────────
STEP_BASELINES: Dict[str, int] = {
    "step0a_raw_events":       470,    # ~7m 46s
    "step0b_ixp_assignments":  180,    # ~3m
    "step1_cancel_initiations":587,    # ~9m 47s
    "step2_ipd_engagement":    1397,   # ~23m 17s
    "step3_save_attribution":  319,    # ~5m 19s
    "step4_final_metrics":     1355,   # ~22m 35s
}


# ── Metrics registry ───────────────────────────────────────────────────────────

@dataclass
class PipelineMetrics:
    """Holds all Prometheus metrics for the pipeline."""
    registry: object = None
    step_duration:     object = None
    rows_written:      object = None
    dq_pass_rate:      object = None
    data_freshness:    object = None

    def __post_init__(self):
        if not PROMETHEUS_AVAILABLE:
            return
        self.registry = CollectorRegistry()

        self.step_duration = Gauge(
            "pipeline_step_duration_seconds",
            "Duration of each pipeline step",
            ["step", "env"],
            registry=self.registry,
        )
        self.rows_written = Counter(
            "pipeline_rows_written_total",
            "Total rows written by pipeline step",
            ["table", "env"],
            registry=self.registry,
        )
        self.dq_pass_rate = Gauge(
            "dq_check_pass_rate",
            "Data quality check pass rate (0.0–1.0)",
            ["table", "check_type", "env"],
            registry=self.registry,
        )
        self.data_freshness = Gauge(
            "data_freshness_lag_hours",
            "Hours since last successful pipeline run",
            ["table", "env"],
            registry=self.registry,
        )


_metrics: Optional[PipelineMetrics] = None


def get_metrics() -> PipelineMetrics:
    global _metrics
    if _metrics is None:
        _metrics = PipelineMetrics()
    return _metrics


# ── Context manager for step timing ───────────────────────────────────────────

@contextmanager
def timed_step(step_name: str) -> Generator[None, None, None]:
    """
    Context manager that times a pipeline step and emits metrics.

    Usage:
        with timed_step("step1_cancel_initiations"):
            build_cancel_initiations(...)
    """
    start = time.perf_counter()
    logger.info(f"[METRICS] Starting step: {step_name}")
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        emit_step_duration(step_name, elapsed)

        # Alert if runtime > 2× baseline
        baseline = STEP_BASELINES.get(step_name)
        if baseline and elapsed > baseline * 2:
            send_pagerduty_alert(
                severity="warning",
                summary=f"StreamFlow: {step_name} runtime {elapsed:.0f}s exceeds 2× baseline ({baseline * 2}s)",
                details={"step": step_name, "elapsed_seconds": elapsed, "baseline_seconds": baseline},
            )
        logger.info(f"[METRICS] Step {step_name} complete: {elapsed:.1f}s")


# ── Emission functions ─────────────────────────────────────────────────────────

def emit_step_duration(step_name: str, duration_seconds: float) -> None:
    """Emit step duration to Prometheus."""
    m = get_metrics()
    if PROMETHEUS_AVAILABLE and m.step_duration:
        m.step_duration.labels(step=step_name, env=ENV).set(duration_seconds)
        _push_metrics(m)
    logger.info(f"METRIC step_duration step={step_name} duration={duration_seconds:.1f}s env={ENV}")


def emit_rows_written(table: str, row_count: int) -> None:
    """Emit row count written metric."""
    m = get_metrics()
    if PROMETHEUS_AVAILABLE and m.rows_written:
        m.rows_written.labels(table=table, env=ENV).inc(row_count)
        _push_metrics(m)
    logger.info(f"METRIC rows_written table={table} count={row_count:,} env={ENV}")


def emit_dq_pass_rate(table: str, check_type: str, pass_rate: float) -> None:
    """
    Emit DQ pass rate metric.
    Triggers P1 PagerDuty if pass_rate < 0.95 on CRITICAL checks.
    """
    m = get_metrics()
    if PROMETHEUS_AVAILABLE and m.dq_pass_rate:
        m.dq_pass_rate.labels(table=table, check_type=check_type, env=ENV).set(pass_rate)
        _push_metrics(m)

    logger.info(f"METRIC dq_pass_rate table={table} type={check_type} rate={pass_rate:.4f}")

    if check_type.upper() == "CRITICAL" and pass_rate < 0.95 and ENV == "prod":
        send_pagerduty_alert(
            severity="critical",
            summary=f"StreamFlow P1: DQ CRITICAL pass rate {pass_rate:.1%} < 95% on {table}",
            details={"table": table, "check_type": check_type, "pass_rate": pass_rate},
        )


def emit_data_freshness(table: str, lag_hours: float) -> None:
    """
    Emit data freshness lag metric.
    Triggers P1 PagerDuty if lag > 8h (SLO breach).
    """
    m = get_metrics()
    if PROMETHEUS_AVAILABLE and m.data_freshness:
        m.data_freshness.labels(table=table, env=ENV).set(lag_hours)
        _push_metrics(m)

    logger.info(f"METRIC data_freshness table={table} lag_hours={lag_hours:.2f}")

    if lag_hours > 8.0 and ENV == "prod":
        send_pagerduty_alert(
            severity="critical",
            summary=f"StreamFlow P1: Data freshness SLO breach — {table} is {lag_hours:.1f}h stale",
            details={"table": table, "lag_hours": lag_hours, "slo_hours": 8.0},
        )


# ── Prometheus push ────────────────────────────────────────────────────────────

def _push_metrics(m: PipelineMetrics) -> None:
    if not PROMETHEUS_AVAILABLE:
        return
    try:
        push_to_gateway(PROMETHEUS_GATEWAY, job=JOB_NAME, registry=m.registry)
    except Exception as exc:
        logger.warning(f"Prometheus push failed: {exc}")


# ── PagerDuty alerting ─────────────────────────────────────────────────────────

def send_pagerduty_alert(
    severity: str,
    summary: str,
    details: Optional[dict] = None,
    dedup_key: Optional[str] = None,
) -> None:
    """
    Send a PagerDuty Events API v2 alert.

    Args:
        severity:  "critical" | "warning" | "info"
        summary:   Alert summary (shown in PagerDuty)
        details:   Additional context dict
        dedup_key: Deduplication key (prevents duplicate alerts)
    """
    if not PAGERDUTY_ROUTING_KEY:
        logger.warning(f"PagerDuty alert suppressed (no routing key): {summary}")
        return

    if ENV != "prod":
        logger.info(f"[{ENV}] PagerDuty alert (suppressed in non-prod): {summary}")
        return

    payload = {
        "routing_key": PAGERDUTY_ROUTING_KEY,
        "event_action": "trigger",
        "dedup_key": dedup_key or summary[:255],
        "payload": {
            "summary": summary,
            "severity": severity,
            "source": "streamflow-analytics",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "custom_details": details or {},
        },
        "client": "StreamFlow Analytics",
    }

    try:
        response = requests.post(
            "https://events.pagerduty.com/v2/enqueue",
            json=payload,
            timeout=10,
        )
        if response.status_code == 202:
            logger.info(f"PagerDuty alert sent: [{severity.upper()}] {summary}")
        else:
            logger.error(f"PagerDuty error {response.status_code}: {response.text}")
    except Exception as exc:
        logger.error(f"PagerDuty request failed: {exc}")
