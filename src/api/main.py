"""
main.py — StreamFlow Analytics Data API
=========================================
FastAPI-based serving layer exposing:
  - Retention metrics endpoints
  - IPD effectiveness reports
  - Real-time churn propensity scoring (MLflow inference)
  - Pipeline health + data freshness monitoring

Run locally:
    uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000
    # Swagger: http://localhost:8000/docs
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime
from typing import Dict, List, Optional

import mlflow.pyfunc
import numpy as np
import pandas as pd
from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator

logger = logging.getLogger(__name__)

# ── App setup ──────────────────────────────────────────────────────────────────

app = FastAPI(
    title="StreamFlow Analytics API",
    description=(
        "Production data API for the SaaS subscription cancel-flow analytics platform. "
        "Exposes retention metrics, IPD effectiveness, and real-time churn scoring."
    ),
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    contact={"name": "Ujjawl Kumar", "email": "info.ujjawlkr094@gmail.com"},
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# ── Request timing middleware ──────────────────────────────────────────────────

@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    elapsed = (time.perf_counter() - start) * 1000
    response.headers["X-Process-Time-Ms"] = f"{elapsed:.2f}"
    return response


# ── Pydantic models ────────────────────────────────────────────────────────────

class RetentionMetrics(BaseModel):
    product: str
    initiation_year: str
    initiation_month: str
    total_initiations: int
    saves: int
    save_rate_pct: float
    retention_31d_pct: Optional[float]
    retention_92d_pct: Optional[float]


class IpdEffectivenessRow(BaseModel):
    ipd_type: str
    companies_shown: int
    views: int
    clicks: int
    ctr_pct: float
    saves: int
    save_rate_pct: float


class CompanyCancelHistory(BaseModel):
    company_id: int
    n_initiations: int
    last_initiation_date: Optional[str]
    last_save_attribution: Optional[str]
    total_saves: int
    retention_rate_31d: Optional[float]
    risk_tier: str


class ChurnScoreRequest(BaseModel):
    company_id: int = Field(..., description="Company identifier", example=123456789)
    as_of_date: Optional[date] = Field(None, description="Score as of date (default: today)")

    @validator("company_id")
    def must_be_positive(cls, v):
        if v <= 0:
            raise ValueError("company_id must be positive")
        return v


class ChurnScoreResponse(BaseModel):
    company_id: int
    score: float = Field(..., description="Churn probability [0.0, 1.0]")
    risk_tier: str = Field(..., description="LOW | MEDIUM | HIGH | CRITICAL")
    top_features: List[Dict[str, float]] = Field(..., description="Top 5 SHAP features driving the score")
    as_of_date: str
    model_version: str


class PipelineHealthResponse(BaseModel):
    status: str
    tables: List[Dict]
    last_checked: str


# ── Risk tier helper ───────────────────────────────────────────────────────────

def score_to_risk_tier(score: float) -> str:
    if score >= 0.75:
        return "CRITICAL"
    elif score >= 0.55:
        return "HIGH"
    elif score >= 0.35:
        return "MEDIUM"
    return "LOW"


# ── Mock data layer (replace with Delta Lake / Trino queries in production) ────

def query_retention_metrics(
    product: Optional[str],
    year: Optional[str],
    month: Optional[str],
) -> List[RetentionMetrics]:
    """
    In production: query gold.rpt_cancel_flow_final_metrics via Trino/Spark.
    Here we return sample data for demonstration.
    """
    sample = [
        RetentionMetrics(
            product="SAAS_CORE", initiation_year="2024", initiation_month="06",
            total_initiations=15234, saves=9140, save_rate_pct=59.99,
            retention_31d_pct=72.3, retention_92d_pct=61.8,
        ),
        RetentionMetrics(
            product="SAAS_PLUS", initiation_year="2024", initiation_month="06",
            total_initiations=8921, saves=5352, save_rate_pct=59.99,
            retention_31d_pct=68.9, retention_92d_pct=57.4,
        ),
    ]
    if product:
        sample = [r for r in sample if r.product == product]
    if year:
        sample = [r for r in sample if r.initiation_year == year]
    if month:
        sample = [r for r in sample if r.initiation_month == month]
    return sample


# ── Endpoints ──────────────────────────────────────────────────────────────────

@app.get("/", tags=["Health"])
async def root():
    return {
        "service": "StreamFlow Analytics API",
        "version": "2.0.0",
        "status": "healthy",
        "docs": "/docs",
    }


@app.get("/v1/health", tags=["Health"])
async def health_check():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}


@app.get(
    "/v1/retention/metrics",
    response_model=List[RetentionMetrics],
    tags=["Retention Analytics"],
    summary="Get retention metrics by product and month",
    description=(
        "Returns save rate and 31d/92d retention outcomes. "
        "Filter by product, year, and/or month. "
        "Based on `gold.rpt_cancel_flow_final_metrics` (3★ governed)."
    ),
)
async def get_retention_metrics(
    product: Optional[str] = Query(None, example="SAAS_CORE"),
    year: Optional[str]    = Query(None, example="2024"),
    month: Optional[str]   = Query(None, example="06"),
) -> List[RetentionMetrics]:
    results = query_retention_metrics(product, year, month)
    if not results:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No retention data found for product={product}, year={year}, month={month}",
        )
    return results


@app.get(
    "/v1/ipd/effectiveness",
    response_model=List[IpdEffectivenessRow],
    tags=["IPD Analytics"],
    summary="IPD view-to-click-to-save funnel by dialog type",
)
async def get_ipd_effectiveness(
    product: Optional[str] = Query(None),
    month: Optional[str]   = Query(None, example="2024-06"),
) -> List[IpdEffectivenessRow]:
    return [
        IpdEffectivenessRow(ipd_type="CS IPD",          companies_shown=45231, views=43100, clicks=12400, ctr_pct=28.8, saves=8900,  save_rate_pct=19.7),
        IpdEffectivenessRow(ipd_type="Discount IPD",    companies_shown=38912, views=37800, clicks=28900, ctr_pct=76.5, saves=24100, save_rate_pct=61.9),
        IpdEffectivenessRow(ipd_type="Upgrade IPD",     companies_shown=12045, views=11900, clicks=2800,  ctr_pct=23.5, saves=1900,  save_rate_pct=15.8),
        IpdEffectivenessRow(ipd_type="Keep my Plan IPD",companies_shown=9823,  views=9700,  clicks=6100,  ctr_pct=62.9, saves=4800,  save_rate_pct=48.9),
        IpdEffectivenessRow(ipd_type="Downgrade IPD",   companies_shown=6234,  views=6100,  clicks=1900,  ctr_pct=31.1, saves=1400,  save_rate_pct=22.5),
    ]


@app.get(
    "/v1/company/{company_id}/cancel-history",
    response_model=CompanyCancelHistory,
    tags=["Company"],
    summary="Cancel flow history and risk profile for a company",
)
async def get_company_cancel_history(company_id: int) -> CompanyCancelHistory:
    if company_id <= 0:
        raise HTTPException(status_code=400, detail="company_id must be positive")
    # In production: query Delta table
    return CompanyCancelHistory(
        company_id=company_id,
        n_initiations=3,
        last_initiation_date="2024-06-15",
        last_save_attribution="Discount Save",
        total_saves=2,
        retention_rate_31d=0.83,
        risk_tier="MEDIUM",
    )


@app.post(
    "/v1/churn-score",
    response_model=ChurnScoreResponse,
    tags=["ML — Churn Propensity"],
    summary="Real-time churn propensity score from MLflow model",
    description=(
        "Loads the production churn model from MLflow registry and returns "
        "a probability score + SHAP-based top feature drivers. "
        "Latency target: < 50ms P99."
    ),
)
async def churn_score(request: ChurnScoreRequest) -> ChurnScoreResponse:
    as_of = request.as_of_date or date.today()

    # In production: load features from feature store + run MLflow model inference
    # Here we simulate with a realistic response
    score = float(np.clip(np.random.beta(2, 5), 0.05, 0.95))

    return ChurnScoreResponse(
        company_id=request.company_id,
        score=round(score, 4),
        risk_tier=score_to_risk_tier(score),
        top_features=[
            {"billing_frequency_annual_flag": -0.312},
            {"tenure_days": -0.198},
            {"cancel_frequency_90d": 0.287},
            {"viewed_discount_ipd": -0.145},
            {"days_since_last_cancel": 0.112},
        ],
        as_of_date=str(as_of),
        model_version="cancel_flow_churn_propensity/Production/v3",
    )


@app.get(
    "/v1/pipeline/health",
    response_model=PipelineHealthResponse,
    tags=["Monitoring"],
    summary="Data freshness and pipeline health",
)
async def pipeline_health() -> PipelineHealthResponse:
    # In production: query Delta table metadata + Airflow API
    tables = [
        {"table": "gold.rpt_cancel_flow_final_metrics", "max_date": "2024-07-14", "row_count": 2_107_994, "freshness_lag_hours": 4.2, "status": "✅ HEALTHY"},
        {"table": "gold.rpt_ipd_detailed_engagement",   "max_date": "2024-07-14", "row_count": 987_412,   "freshness_lag_hours": 4.3, "status": "✅ HEALTHY"},
        {"table": "silver.stg_cancel_initiations",      "max_date": "2024-07-14", "row_count": 2_107_994, "freshness_lag_hours": 3.8, "status": "✅ HEALTHY"},
    ]
    return PipelineHealthResponse(
        status="HEALTHY",
        tables=tables,
        last_checked=datetime.utcnow().isoformat(),
    )
