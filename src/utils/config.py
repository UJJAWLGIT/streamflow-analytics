"""
config.py — Environment Configuration
======================================
Centralised configuration management for all pipeline environments.
Reads from environment variables + YAML config files.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional

import yaml


class Environment(str, Enum):
    LOCAL = "local"
    DEV   = "dev"
    PROD  = "prod"


@dataclass
class S3Config:
    bronze_path:   str
    silver_path:   str
    gold_path:     str
    mlflow_path:   str
    scripts_path:  str
    logs_path:     str


@dataclass
class EMRConfig:
    app_id:           str
    execution_role:   str
    region:           str = "us-east-1"
    max_executors:    int = 600


@dataclass
class PipelineConfig:
    env:           Environment
    project_name:  str
    s3:            S3Config
    emr:           Optional[EMRConfig] = None
    mlflow_uri:    str = "http://localhost:5000"
    slack_webhook: str = ""

    # Table paths (derived from S3 config)
    @property
    def stg_raw_events(self)        -> str: return f"{self.s3.bronze_path}/raw_events"
    @property
    def stg_ixp_assignments(self)   -> str: return f"{self.s3.bronze_path}/ixp_assignments"
    @property
    def stg_cancel_initiations(self)-> str: return f"{self.s3.silver_path}/stg_cancel_initiations"
    @property
    def rpt_ipd_engagement(self)    -> str: return f"{self.s3.gold_path}/rpt_ipd_detailed_engagement"
    @property
    def stg_save_attribution(self)  -> str: return f"{self.s3.silver_path}/stg_save_attribution"
    @property
    def rpt_final_metrics(self)     -> str: return f"{self.s3.gold_path}/rpt_cancel_flow_final_metrics"


# ── Environment-specific configs ───────────────────────────────────────────────

_CONFIGS: dict[Environment, PipelineConfig] = {
    Environment.LOCAL: PipelineConfig(
        env=Environment.LOCAL,
        project_name="streamflow-analytics",
        s3=S3Config(
            bronze_path  ="./data/output/bronze",
            silver_path  ="./data/output/silver",
            gold_path    ="./data/output/gold",
            mlflow_path  ="./data/mlflow",
            scripts_path ="./scripts",
            logs_path    ="./logs",
        ),
        mlflow_uri="http://localhost:5000",
    ),
    Environment.DEV: PipelineConfig(
        env=Environment.DEV,
        project_name="streamflow-analytics",
        s3=S3Config(
            bronze_path  ="s3://streamflow-analytics-dev-bronze",
            silver_path  ="s3://streamflow-analytics-dev-silver",
            gold_path    ="s3://streamflow-analytics-dev-gold",
            mlflow_path  ="s3://streamflow-analytics-dev-mlflow",
            scripts_path ="s3://streamflow-analytics-dev-scripts",
            logs_path    ="s3://streamflow-analytics-dev-logs",
        ),
        mlflow_uri="http://mlflow-dev.internal:5000",
    ),
    Environment.PROD: PipelineConfig(
        env=Environment.PROD,
        project_name="streamflow-analytics",
        s3=S3Config(
            bronze_path  ="s3://streamflow-analytics-prod-bronze",
            silver_path  ="s3://streamflow-analytics-prod-silver",
            gold_path    ="s3://streamflow-analytics-prod-gold",
            mlflow_path  ="s3://streamflow-analytics-prod-mlflow",
            scripts_path ="s3://streamflow-analytics-prod-scripts",
            logs_path    ="s3://streamflow-analytics-prod-logs",
        ),
        emr=EMRConfig(
            app_id        =os.getenv("EMR_APP_ID", ""),
            execution_role=os.getenv("EMR_EXECUTION_ROLE", ""),
        ),
        mlflow_uri =os.getenv("MLFLOW_TRACKING_URI", "http://mlflow.internal:5000"),
        slack_webhook=os.getenv("SLACK_WEBHOOK_URL", ""),
    ),
}


def get_config(env: Optional[str] = None) -> PipelineConfig:
    """
    Get configuration for the given environment.

    Args:
        env: Environment name. Falls back to STREAMFLOW_ENV env var, then "local".

    Returns:
        PipelineConfig for the requested environment.
    """
    env_str = env or os.getenv("STREAMFLOW_ENV", "local")
    try:
        environment = Environment(env_str.lower())
    except ValueError:
        raise ValueError(f"Unknown environment: {env_str}. Valid: {[e.value for e in Environment]}")
    return _CONFIGS[environment]
