"""
logger.py — Structured JSON Logger
=====================================
Production-grade structured logging for all pipeline components.
Emits JSON log lines compatible with CloudWatch, Datadog, and ELK.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict, Optional


class JSONFormatter(logging.Formatter):
    """Formats log records as single-line JSON for log aggregation systems."""

    def format(self, record: logging.LogRecord) -> str:
        log_obj: Dict[str, Any] = {
            "timestamp":  datetime.utcnow().isoformat() + "Z",
            "level":      record.levelname,
            "logger":     record.name,
            "message":    record.getMessage(),
            "module":     record.module,
            "function":   record.funcName,
            "line":       record.lineno,
            "env":        os.getenv("STREAMFLOW_ENV", "local"),
            "pipeline":   os.getenv("PIPELINE_NAME", "unknown"),
        }
        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_obj, default=str)


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Get a structured JSON logger.

    Args:
        name:  Logger name (use __name__).
        level: Log level (default: INFO).

    Returns:
        Configured logger.
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JSONFormatter())
        logger.addHandler(handler)
        logger.setLevel(level)
        logger.propagate = False

    return logger
