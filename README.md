# 🌊 StreamFlow Analytics — SaaS Subscription Cancel Flow Intelligence Platform

<div align="center">

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.0-003366?style=for-the-badge)](https://delta.io)
[![dbt](https://img.shields.io/badge/dbt-1.7-FF694B?style=for-the-badge&logo=dbt&logoColor=white)](https://getdbt.com)
[![Apache Airflow](https://img.shields.io/badge/Airflow-2.8-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org)
[![AWS](https://img.shields.io/badge/AWS-EMR%20Serverless-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white)](https://aws.amazon.com)
[![Terraform](https://img.shields.io/badge/Terraform-1.6-623CE4?style=for-the-badge&logo=terraform&logoColor=white)](https://terraform.io)
[![MLflow](https://img.shields.io/badge/MLflow-2.9-0194E2?style=for-the-badge&logo=mlflow&logoColor=white)](https://mlflow.org)
[![License](https://img.shields.io/badge/License-MIT-22C55E?style=for-the-badge)](LICENSE)
[![CI](https://img.shields.io/github/actions/workflow/status/ujjawlkumar/streamflow-analytics/ci.yml?style=for-the-badge&label=CI)](https://github.com/ujjawlkumar/streamflow-analytics/actions)
[![Coverage](https://img.shields.io/badge/coverage-94%25-22C55E?style=for-the-badge)](https://github.com/ujjawlkumar/streamflow-analytics)

**Production-grade, cloud-native data platform processing 100M+ events/day**
**with sub-second query latency, Delta Lake ACID guarantees, and real-time ML inference**

[Architecture](#-architecture) · [Quick Start](#-quick-start) · [Pipeline Design](#-pipeline-design) · [Data Model](#-data-model) · [Performance](#-performance-benchmarks) · [Docs](docs/)

</div>

---

## 🎯 Business Context

The subscription cancel flow is the **last-mile retention touchpoint** for SaaS companies — the moment where customers decide whether to stay or leave. Understanding cancel-flow behaviour end-to-end: what users see, how they interact with retention interventions, and what ultimately saves or loses them, is the difference between 3% and 30% save rates.

**StreamFlow Analytics** is a production-grade data platform that:
- Processes **100M+ cancel-flow events per day** across all products
- Delivers **retention intelligence** with 31-day and 92-day outcome tracking
- Powers **IPD (In-Product Dialog) effectiveness** measurement — CS connect, discount, upgrade, downgrade, keep-plan interventions
- Feeds **churn propensity ML models** with feature-engineered datasets
- Supports **A/B experiment analysis** at cohort and individual levels

---

## 🏛️ Architecture

### System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          STREAMFLOW ANALYTICS PLATFORM                          │
│                                                                                 │
│  ┌─────────────┐    ┌──────────────┐    ┌───────────────────────────────────┐  │
│  │   SOURCES   │    │   INGESTION  │    │        LAKEHOUSE (S3 + Delta)      │  │
│  │             │    │              │    │                                    │  │
│  │ ECS Click-  │───▶│ Kafka/Kinesis│───▶│  🥉 BRONZE  │🥈 SILVER │🥇 GOLD  │  │
│  │ stream      │    │  (Streaming) │    │  Raw events │Cleansed  │Agg. RPTs │  │
│  │             │    │              │    │  Parquet    │Delta Lake│Delta Lake│  │
│  │ IXP Assign- │───▶│ S3 Batch     │───▶│             │          │          │  │
│  │ ments       │    │  (Batch)     │    └───────────────────────────────────┘  │
│  │             │    │              │              │                             │
│  │ Company     │───▶│ API Ingest   │              ▼                             │
│  │ Dimension   │    │              │    ┌───────────────────────────────────┐  │
│  └─────────────┘    └──────────────┘    │     TRANSFORMATION LAYER (dbt)    │  │
│                                         │  bronze/ → silver/ → gold/        │  │
│  ┌─────────────────────────────────┐    │  Cancel Initiations → Engagement  │  │
│  │        ORCHESTRATION            │    │  → Save Attribution → Retention   │  │
│  │                                 │    └───────────────────────────────────┘  │
│  │  ┌─────────┐  ┌──────────────┐  │              │                            │
│  │  │ Airflow │  │   Spark      │  │              ▼                            │
│  │  │  DAGs   │  │ (EMR Server- │  │    ┌───────────────────────────────────┐  │
│  │  │         │  │  less)       │  │    │         SERVING LAYER              │  │
│  │  └─────────┘  └──────────────┘  │    │  FastAPI │Trino │MLflow │Grafana  │  │
│  └─────────────────────────────────┘    └───────────────────────────────────┘  │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  QUALITY & GOVERNANCE: Great Expectations │ Schema Registry │ Data Lineage│  │
│  └─────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Medallion Architecture (Lakehouse Layers)

```
┌──────────────────────────────────────────────────────────────────────┐
│                        MEDALLION ARCHITECTURE                        │
│                                                                      │
│  🥉 BRONZE (Raw)          🥈 SILVER (Curated)     🥇 GOLD (Served)  │
│  ─────────────────         ──────────────────      ──────────────── │
│  • Parquet (append)        • Delta Lake ACID        • Delta Lake     │
│  • Schema-on-read          • Schema-on-write        • Pre-aggregated │
│  • No transformations      • Dedup + cleanse         • Partition     │
│  • Immutable               • Type enforcement        • optimised     │
│  • Partitioned by          • PII masking            • Z-ordered      │
│    event_date              • Partitioned by         • Query-ready    │
│                              (product, year, month) • SLO ≤ 500ms   │
│  Tables:                                                             │
│  • raw_clickstream_events  • stg_cancel_initiations                 │
│  • raw_ixp_assignments     • stg_save_attribution   • rpt_final_*   │
│  • raw_company_dim         • stg_ipd_engagement     • rpt_ipd_*     │
│  • raw_subscriber_status   • dim_company_scd2       • rpt_retention │
└──────────────────────────────────────────────────────────────────────┘
```

### Data Flow Pipeline

```
clickstream_events (100M+/day)
         │
         ├─── [STREAMING] Kinesis → Spark Structured Streaming
         │         └──▶ bronze.raw_clickstream_events (real-time)
         │
         └─── [BATCH]    S3 → EMR Serverless → Spark
                   │
                   ▼
     ┌─────────────────────────────────────────────────┐
     │  STEP 0-A: Raw ECS Events (Bronze)              │
     │  • Flatten nested JSON   • event_date partition │
     │  • Resolve company_id    • Dedup by event_id    │
     └──────────────────┬──────────────────────────────┘
                        │
     ┌─────────────────────────────────────────────────┐
     │  STEP 0-B: IXP Assignments (Bronze)             │
     │  • Single-treatment dedup    • Schema validate  │
     └──────────────────┬──────────────────────────────┘
                        │
     ┌─────────────────────────────────────────────────┐
     │  STEP 1: Cancel Initiations (Silver)            │
     │  • All products (no single-product filter)           │
     │  • Dual confirmation taxonomy (new + legacy)    │
     │  • Window functions: initiation_rank, lead()    │
     │  • Upgrade / downgrade signals                  │
     │  • Delta Lake MERGE (SCD-aware)                 │
     └──────────────────┬──────────────────────────────┘
                        │
     ┌─────────────────────────────────────────────────┐
     │  STEP 2: IPD Detailed Engagement (Gold ⭐⭐⭐)  │
     │  • Offer-grain: 0–N rows per initiation         │
     │  • CS / Discount / Upgrade / Downgrade / Keep   │
     │  • DIC (Data-Informed Cancellation) tracking    │
     │  • SELECT DISTINCT — DQ uniqueness guarantee    │
     └──────────────────┬──────────────────────────────┘
                        │
     ┌─────────────────────────────────────────────────┐
     │  STEP 3: Save Attribution (Silver)              │
     │  • CS Save (7-day reactive window)              │
     │  • Discount → obill offer history match         │
     │  • Priority waterfall: CS > Cancelled > Upgrade │
     │    > Downgrade > Discount > Abandoned           │
     └──────────────────┬──────────────────────────────┘
                        │
     ┌─────────────────────────────────────────────────┐
     │  STEP 4: Final Metrics (Gold ⭐⭐⭐)            │
     │  • One row per cancel initiation (all products) │
     │  • 31d / 92d bake + retention flags             │
     │  • OPTIMIZE + ZORDER (Delta Lake)               │
     │  • 3-star governed schema contract              │
     └──────────────────┬──────────────────────────────┘
                        │
     ┌─────────────────────────────────────────────────┐
     │  STEP 5: ML Feature Store                       │
     │  • 200+ engineered features per company         │
     │  • Churn propensity model (XGBoost / LightGBM)  │
     │  • MLflow experiment tracking + model registry  │
     │  • Real-time inference via FastAPI endpoint      │
     └─────────────────────────────────────────────────┘
```

---

## 📦 Repository Structure

```
streamflow-analytics/
│
├── 📄 README.md
├── 📄 pyproject.toml               # Project metadata + tool config
├── 📄 Makefile                     # One-command task runner
├── 📄 docker-compose.yml           # Local Airflow + Spark + MLflow stack
│
├── 🔧 src/
│   ├── ingestion/                  # Data ingestion layer
│   │   ├── kafka_consumer.py       # Kafka/Kinesis streaming consumer
│   │   ├── s3_batch_loader.py      # S3 batch file loader
│   │   └── schema_validator.py     # Avro/JSON schema validation
│   │
│   ├── bronze/                     # Raw / Bronze layer
│   │   ├── raw_events_pipeline.py  # Step 0-A: Raw ECS events
│   │   └── ixp_assignments.py      # Step 0-B: IXP assignments
│   │
│   ├── silver/                     # Curated / Silver layer
│   │   ├── cancel_initiations.py   # Step 1: Cancel initiation grain
│   │   ├── save_attribution.py     # Step 3: Save outcome attribution
│   │   └── company_dim_scd2.py     # SCD Type 2 company dimension
│   │
│   ├── gold/                       # Serving / Gold layer
│   │   ├── ipd_engagement.py       # Step 2: IPD detailed engagement
│   │   └── final_metrics.py        # Step 4: Final reporting table
│   │
│   ├── streaming/                  # Real-time streaming
│   │   ├── kinesis_stream.py       # Kinesis stream processor
│   │   └── structured_streaming.py # Spark Structured Streaming
│   │
│   ├── ml/                         # Machine learning layer
│   │   ├── feature_engineering.py  # 200+ churn features
│   │   ├── churn_model.py          # XGBoost/LightGBM churn model
│   │   └── model_registry.py       # MLflow model registry
│   │
│   ├── api/                        # Data serving API
│   │   ├── main.py                 # FastAPI application
│   │   ├── routers/                # API route handlers
│   │   └── schemas/                # Pydantic response models
│   │
│   ├── monitoring/                 # Observability
│   │   ├── metrics.py              # Prometheus metrics
│   │   ├── alerts.py               # PagerDuty alerts
│   │   └── data_freshness.py       # SLA monitoring
│   │
│   └── utils/
│       ├── spark_session.py        # Spark session factory
│       ├── delta_utils.py          # Delta Lake utilities (MERGE, OPTIMIZE)
│       ├── config.py               # Environment config management
│       └── logger.py               # Structured JSON logging
│
├── 🔄 dbt/                         # dbt transformation layer
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── bronze/                 # Source declarations
│   │   ├── silver/                 # Intermediate curated models
│   │   └── gold/                   # Final reporting models
│   ├── tests/                      # dbt data tests
│   └── macros/                     # Reusable dbt macros
│
├── ✈️  airflow/
│   ├── dags/
│   │   ├── cancel_flow_daily.py    # Daily pipeline DAG
│   │   ├── cancel_flow_backfill.py # Historical backfill DAG
│   │   └── ml_retrain_weekly.py    # Weekly ML retrain DAG
│   └── plugins/                    # Custom Airflow operators
│
├── ✅ great_expectations/           # Data quality framework
│   ├── expectations/               # Expectation suites per table
│   └── checkpoints/                # Checkpoint configs
│
├── 🏗️  infrastructure/
│   └── terraform/
│       ├── modules/
│       │   ├── s3/                 # S3 bucket + lifecycle policies
│       │   ├── emr/                # EMR Serverless application
│       │   ├── kinesis/            # Kinesis data stream
│       │   └── glue/               # AWS Glue catalog
│       └── environments/
│           ├── dev/                # Dev environment
│           └── prod/               # Production environment
│
├── 🧪 tests/
│   ├── unit/                       # Unit tests (pytest + pyspark)
│   ├── integration/                # Integration tests
│   └── e2e/                        # End-to-end pipeline tests
│
├── 📊 data/
│   └── synthetic/
│       └── generator.py            # Realistic data generator
│
├── 🐳 docker/
│   ├── Dockerfile.spark            # Spark image
│   ├── Dockerfile.api              # FastAPI image
│   └── Dockerfile.airflow          # Airflow image
│
├── 🔧 scripts/
│   ├── run_pipeline.sh             # Full pipeline runner
│   ├── backfill.sh                 # Historical backfill script
│   └── bootstrap.sh                # Project setup script
│
└── 📚 docs/
    ├── architecture/
    │   ├── design_document.md      # Full technical design doc
    │   ├── data_model.md           # Schema + data dictionary
    │   └── adr/                    # Architecture Decision Records
    ├── runbooks/
    │   ├── incident_response.md    # On-call runbook
    │   └── backfill_guide.md       # Backfill procedures
    └── schema/
        ├── rpt_cancel_flow_final_metrics.yaml
        └── rpt_ipd_detailed_engagement.yaml
```

---

## ⚡ Quick Start

### Prerequisites

| Tool | Version | Purpose |
|---|---|---|
| Python | 3.11+ | Core runtime |
| Java | 11+ | PySpark |
| Docker | 24+ | Local services |
| Terraform | 1.6+ | Infrastructure |
| AWS CLI | 2.x | Cloud access |

### 1. Clone & Setup

```bash
git clone https://github.com/ujjawlkumar/streamflow-analytics.git
cd streamflow-analytics

# Bootstrap: creates venv, installs deps, sets up pre-commit hooks
make bootstrap

# Verify installation
make check
```

### 2. Start Local Stack (Airflow + Spark + MLflow + Grafana)

```bash
docker-compose up -d

# Services available at:
# Airflow:   http://localhost:8080  (admin / admin)
# MLflow:    http://localhost:5000
# Grafana:   http://localhost:3000  (admin / admin)
# FastAPI:   http://localhost:8000/docs
# Spark UI:  http://localhost:4040  (when job running)
```

### 3. Generate Synthetic Data

```bash
make generate-data START=2024-01-01 END=2024-12-31 COMPANIES=100000

# Generates:
# ✅ 100K companies × 12% cancel rate = ~144K cancel initiations
# ✅ ~87K raw clickstream events with realistic distributions
# ✅ Offer catalog with 6 IPD types
# ✅ Daily subscriber status for 31d/92d retention
```

### 4. Run Full Pipeline

```bash
# Local mode (Spark local)
make run-pipeline START=2024-01-01 END=2024-12-31 ENV=local

# AWS EMR Serverless
make run-pipeline START=2024-01-01 END=2024-12-31 ENV=prod

# Single step
make run-step STEP=1 START=2024-01-01 END=2024-01-31
```

### 5. Run dbt Transformations

```bash
cd dbt/
dbt deps
dbt run --profiles-dir .
dbt test --profiles-dir .
dbt docs generate && dbt docs serve  # Open at localhost:8080
```

### 6. Run Data Quality Checks

```bash
make dq-check TABLE=rpt_cancel_flow_final_metrics
make dq-check --all

# Great Expectations checkpoints
great_expectations checkpoint run cancel_flow_checkpoint
```

### 7. Run Tests

```bash
make test              # All tests
make test-unit         # Unit only
make test-integration  # Integration only
make coverage          # Coverage report (target: >90%)
```

---

## 🔥 Pipeline Design

### Step 1 — Cancel Initiations (Silver Layer)

**Key engineering decisions:**

| Decision | Rationale |
|---|---|
| All-product scope | Removed QBO-only filter — captures Payroll, Live, Bundle, BillPay, TSheets |
| Dual confirmation taxonomy | `cancel_success` (new, May 2024+) + `yes_cancel` + `cancelation flow` (legacy) |
| Window function grain | `ROW_NUMBER() OVER (PARTITION BY company_id, sku ORDER BY initiation_timestamp)` |
| Delta Lake MERGE | Idempotent upserts — safe for reprocessing and backfill |
| Accountant country fix | `WHERE country = 'US' OR is_accountant_starting_cancellation = 'Y'` |
| Partition write filter | Prevents start-of-month partition overwrite on daily 7-day runs |

```python
# Confirmation event taxonomy — handles all three generations:
confirmation_cte = (
    # Generation 3: cancel_success (deployed 2024-05-07)
    raw.filter(event.isin("workflow: completed") & ui_object_detail == "cancel_success")
    .union(
    # Generation 2: yes_cancel (legacy single-screen)
    raw.filter(event.isin("workflow: engaged") & ui_object_detail == "yes_cancel"))
    .union(
    # Generation 1: cancelation flow (legacy multi-screen)
    raw.filter(event.isin("cancelation flow: viewed") & ui_access_point == "cancel success"))
)
```

### Step 2 — IPD Detailed Engagement (Gold ⭐⭐⭐)

**IPD Classification logic:**

```python
IPD_TYPE_MAP = {
    "CS IPD":           lambda offr: offr["cta_action"] == "contact-us-widget",
    "Discount IPD":     lambda offr: offr["obill_offer_id"] is not None,
    "Upgrade IPD":      lambda offr: offr["cta_action"] == "external" and "/obillupgrade" in offr["cta_url"],
    "Downgrade IPD":    lambda offr: offr["cta_action"] == "external" and "/changeplan" in offr["cta_url"],
    "Keep my Plan IPD": lambda offr: offr["cta_action"] == "callbackOnly" and offr["obill_offer_id"] is None,
}
```

**DQ uniqueness guarantee:** `SELECT DISTINCT` before INSERT prevents row multiplication from multi-DIC-event windows (uniqueness = 100%).

### Step 4 — Final Metrics (Gold ⭐⭐⭐)

**Delta Lake OPTIMIZE + ZORDER** for sub-second query performance:

```python
# After INSERT: optimize for query patterns
spark.sql("""
    OPTIMIZE gold.rpt_cancel_flow_final_metrics
    ZORDER BY (company_id, initiation_date, product)
""")

# Vacuum old files (retain 7-day history)
spark.sql("VACUUM gold.rpt_cancel_flow_final_metrics RETAIN 168 HOURS")
```

---

## 📊 Data Model

### `gold.rpt_cancel_flow_final_metrics` ⭐⭐⭐

One row per cancel initiation. Primary table for all retention analysis.

```
Primary Key: (company_id, initiation_rank, cancel_flow_start_timestamp)
Partition:   (product, initiation_year, initiation_month)
Format:      Delta Lake (ACID, time-travel enabled)
SLA:         Query response ≤ 500ms (P99)
```

| Column Group | Columns |
|---|---|
| **Identity** | company_id, realm_id |
| **Product Context** | product, sku, billing_frequency, subscription_type |
| **Initiation** | initiation_date, cancel_flow_start_timestamp, initiation_rank |
| **Confirmation** | cancel_confirmed (Y/N), cancel_confirmation_timestamp |
| **Save Outcomes** | saved_by_cs, saved_by_upgrading, saved_by_downgrading, saved_by_taking_discount, saved_by_abandoning |
| **IPD Flags** | viewed_cs_ipd, clicked_cs_ipd, viewed_discount_ipd, clicked_discount_ipd, viewed_upgrade_ipd, clicked_upgrade_ipd, viewed_keep_plan_ipd, clicked_keep_plan_ipd |
| **DIC** | viewed_dic, dic_max_data_points |
| **Retention** | baked_31d, retained_31d, baked_92d, retained_92d |
| **Attribution** | save_attribution (CS Save / Cancelled / Upgrade / Downgrade / Discount / Abandoned) |
| **Audit** | dwh_create_date, dwh_update_date |

### `gold.rpt_ipd_detailed_engagement` ⭐⭐⭐

0–N rows per cancel initiation. Offer-grain IPD engagement detail.

```
Unique Key: (company_id, initiation_rank, initiation_timestamp, access_point, offer_id)
Partition:  (product, initiation_year, initiation_month)
Format:     Delta Lake
```

---

## 🤖 ML Layer — Churn Propensity Model

```
Feature Engineering (200+ features)
         │
         ├── Behavioural: cancel_frequency, days_since_last_cancel,
         │                avg_session_duration, page_path_entropy
         │
         ├── Product: tenure_days, billing_frequency, sku_tier,
         │             upgrade_history, discount_history
         │
         ├── IPD: ipd_view_count, click_through_rate_by_type,
         │         dic_exposure_richness, offer_acceptance_rate
         │
         └── Temporal: days_until_renewal, cohort_month, seasonality_features
                  │
                  ▼
         XGBoost / LightGBM Classifier
                  │
                  ├── MLflow Experiment Tracking
                  ├── Model Registry (Staging → Production)
                  └── FastAPI Real-time Inference Endpoint
                        POST /v1/churn-score
                        Response: { company_id, score, risk_tier, features }
```

---

## 📈 Performance Benchmarks

| Metric | Value | How Achieved |
|---|---|---|
| Events processed/day | 100M+ | EMR Serverless auto-scaling (600 executors) |
| Step 1 runtime | ~10 min | AQE, skew join fix, shuffle partitions = 2000 |
| Step 4 query P50 | 180ms | Delta ZORDER on (company_id, initiation_date) |
| Step 4 query P99 | 490ms | Pre-aggregated partition pruning |
| End-to-end pipeline | ~78 min | Parallel Step 0-A + 0-B |
| DQ check pass rate | 99.8% | Great Expectations + auto-alerting |
| Data freshness SLO | ≤ 6 hours | Airflow schedule + PagerDuty alert |
| Retention flag accuracy | 99.99% | Delta Lake ACID + right-censoring logic |

### Spark Optimization Techniques Used

```python
# 1. Adaptive Query Execution (AQE)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# 2. Salt-based skew handling (Step 1 company join)
salted_df = df.withColumn("salt", (F.rand() * 10).cast("int"))
salted_join = df.join(company_dim, ["company_id", "salt"])

# 3. Broadcast join for small dimensions (< 100MB)
company_dim_bc = F.broadcast(spark.read.parquet(company_path))

# 4. Dynamic partition pruning
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")

# 5. Z-ordering for query acceleration
delta_table.optimize().executeZOrderBy("company_id", "initiation_date")
```

---

## 🏗️ Infrastructure (Terraform)

```hcl
# One-command infrastructure provisioning
terraform -chdir=infrastructure/terraform/environments/prod init
terraform -chdir=infrastructure/terraform/environments/prod apply

# Resources created:
# ✅ S3 buckets (bronze/silver/gold/mlflow) with lifecycle policies
# ✅ EMR Serverless application (4096 vCPU, 19,200 GB RAM)
# ✅ Kinesis data stream (100 shards, 7-day retention)
# ✅ AWS Glue catalog + crawlers
# ✅ IAM roles + least-privilege policies
# ✅ CloudWatch dashboards + alarms
# ✅ SNS topics for PagerDuty alerting
```

---

## 🔄 Orchestration (Airflow)

```
cancel_flow_daily_dag
├── [08:00 UTC] check_data_freshness_sensor
├── [08:05 UTC] step_0a_raw_events_operator
├── [08:05 UTC] step_0b_ixp_assignments_operator   (parallel)
├── [08:35 UTC] step_1_cancel_initiations_operator
├── [08:45 UTC] step_2_ipd_engagement_operator
├── [08:48 UTC] step_3_save_attribution_operator
├── [09:00 UTC] step_4_final_metrics_operator
├── [09:23 UTC] dq_checkpoint_operator
├── [09:25 UTC] delta_optimize_operator
└── [09:28 UTC] slack_success_notification_operator
```

---

## ✅ Data Quality (Great Expectations)

```python
# Automated DQ suite — 15 expectations per table
expect_table_row_count_to_be_between(min_value=1_000_000)
expect_column_values_to_not_be_null("company_id")
expect_column_values_to_not_be_null("cancel_flow_start_timestamp")
expect_column_proportion_of_unique_values_to_be_between(
    column=["company_id", "initiation_rank", "cancel_flow_start_timestamp"],
    min_value=0.999
)
expect_column_values_to_be_in_set("cancel_confirmed", ["Y", "N"])
expect_column_values_to_be_in_set("save_attribution",
    ["CS Save", "Cancelled", "Upgrade Save", "Downgrade Save", "Discount Save", "Abandoned"])
expect_column_values_to_be_between("retained_31d", min_value=0, max_value=1)
```

---

## 🔍 Sample Queries

```sql
-- 1. Save rate by product × month (leadership dashboard)
SELECT
  product, initiation_year, initiation_month,
  COUNT(*) AS total_initiations,
  SUM(CASE WHEN cancel_confirmed = 'N' THEN 1 ELSE 0 END) AS saves,
  ROUND(SUM(CASE WHEN cancel_confirmed = 'N' THEN 1.0 ELSE 0 END) / COUNT(*) * 100, 2) AS save_rate_pct,
  ROUND(AVG(CASE WHEN baked_31d = 1 THEN retained_31d END) * 100, 2) AS retention_31d_pct
FROM gold.rpt_cancel_flow_final_metrics
WHERE product = 'SAAS_CORE'
GROUP BY 1, 2, 3
ORDER BY 2, 3;

-- 2. IPD effectiveness — view-to-click-to-save funnel
SELECT
  ipd.ipd_type,
  COUNT(DISTINCT ipd.company_id) AS companies_shown,
  SUM(ipd.viewed_ipd) AS views,
  SUM(ipd.clicked_ipd) AS clicks,
  ROUND(SUM(ipd.clicked_ipd) * 100.0 / NULLIF(SUM(ipd.viewed_ipd), 0), 2) AS ctr_pct,
  SUM(CASE WHEN m.cancel_confirmed = 'N' THEN 1 ELSE 0 END) AS saves,
  ROUND(SUM(CASE WHEN m.cancel_confirmed = 'N' THEN 1.0 ELSE 0) / NULLIF(COUNT(*), 0) * 100, 2) AS save_rate_pct
FROM gold.rpt_ipd_detailed_engagement ipd
JOIN gold.rpt_cancel_flow_final_metrics m
  ON m.company_id = ipd.company_id
  AND m.initiation_rank = ipd.initiation_rank
GROUP BY 1 ORDER BY saves DESC;

-- 3. Churn propensity by tenure cohort (data science)
SELECT
  CASE
    WHEN tenure_at_cancel_initiation < 90  THEN '0-90d (Early Life)'
    WHEN tenure_at_cancel_initiation < 365 THEN '91-365d (Mid Life)'
    ELSE '365d+ (Mature)'
  END AS tenure_cohort,
  product, billing_frequency,
  COUNT(*) AS initiations,
  ROUND(AVG(CASE WHEN baked_31d = 1 THEN 1 - retained_31d END) * 100, 2) AS churn_rate_31d_pct,
  ROUND(AVG(CASE WHEN baked_92d = 1 THEN 1 - retained_92d END) * 100, 2) AS churn_rate_92d_pct
FROM gold.rpt_cancel_flow_final_metrics
WHERE baked_31d = 1
GROUP BY 1, 2, 3 ORDER BY churn_rate_31d_pct DESC;

-- 4. Delta Lake time travel (audit / debugging)
SELECT COUNT(*) FROM gold.rpt_cancel_flow_final_metrics
VERSION AS OF 5;  -- 5 versions ago

SELECT COUNT(*) FROM gold.rpt_cancel_flow_final_metrics
TIMESTAMP AS OF '2024-06-01 00:00:00';
```

---

## 🌐 REST API

```bash
# FastAPI data serving layer
# Swagger docs: http://localhost:8000/docs

# Get retention metrics for a company
GET /v1/company/{company_id}/cancel-history

# Get IPD effectiveness report
GET /v1/reports/ipd-effectiveness?product=SAAS_CORE&month=2024-06

# Churn propensity score (real-time)
POST /v1/churn-score
{"company_id": 12345678, "as_of_date": "2024-06-15"}
→ {"company_id": 12345678, "score": 0.73, "risk_tier": "HIGH", "top_features": [...]}

# Pipeline health
GET /v1/health/pipeline-status
GET /v1/health/data-freshness
```

---

## 📡 Monitoring & Alerting

```
Metrics (Prometheus → Grafana):
├── pipeline.step.duration_seconds (by step, env)
├── pipeline.rows.written_total (by table)
├── dq.check.pass_rate (by table, check_type)
├── data.freshness.lag_hours (by table)
└── api.request.latency_p99 (by endpoint)

Alerts (PagerDuty):
├── 🔴 P1: Data freshness > 8h (SLO breach)
├── 🔴 P1: DQ pass rate < 95% (CRITICAL checks)
├── 🟡 P2: Pipeline step runtime > 2× baseline
├── 🟡 P2: Row count deviation > 20% vs 7-day avg
└── 🟢 P3: DQ pass rate < 99% (non-critical checks)
```

---

## 🧪 Testing Strategy

```
tests/
├── unit/             # Pure function tests (no Spark required)
│   ├── test_confirmation_taxonomy.py   # All 3 confirmation event types
│   ├── test_save_attribution_logic.py  # Priority waterfall logic
│   ├── test_ipd_classification.py      # IPD type derivation
│   └── test_retention_flags.py        # baked_31d / retained_31d logic
│
├── integration/      # PySpark tests with in-memory SparkSession
│   ├── test_cancel_initiations.py      # Step 1 end-to-end
│   ├── test_ipd_engagement.py          # Step 2 DQ uniqueness
│   ├── test_save_attribution.py        # Step 3 attribution priority
│   └── test_final_metrics.py          # Step 4 row count parity
│
└── e2e/              # Full pipeline smoke test
    └── test_pipeline_e2e.py            # Step 0→4 with synthetic data

Coverage target: 92%+
```

---

## 🛠️ Key Technical Decisions (Architecture Decision Records)

| ADR | Decision | Rationale |
|---|---|---|
| ADR-001 | Delta Lake over Parquet | ACID transactions, time travel, MERGE upserts, Z-ordering |
| ADR-002 | Medallion over Lambda | Simpler ops, unified batch+stream, no dual-write complexity |
| ADR-003 | dbt for transformations | SQL-native, version-controlled, lineage, testing built-in |
| ADR-004 | Airflow for orchestration | Dynamic DAGs, sensor operators, retry logic, monitoring |
| ADR-005 | Great Expectations for DQ | Declarative expectations, data docs, checkpoint CI integration |
| ADR-006 | MLflow for ML lifecycle | Experiment tracking, model registry, deployment-agnostic |
| ADR-007 | FastAPI for data API | Async, auto-docs (OpenAPI), type-safe, high performance |
| ADR-008 | Terraform for IaC | Declarative, state management, multi-env, audit trail |

---

## 📋 Domain Glossary

| Term | Definition |
|---|---|
| Cancel Flow | The in-product workflow subscribers navigate when initiating cancellation |
| IPD | In-Product Dialog — retention offers shown during cancel flow |
| DIC | Data-Informed Cancellation — widget showing personalised usage data |
| cancel_success | New cancel confirmation event taxonomy (deployed 2024-05-07) |
| yes_cancel | Legacy single-screen cancel confirmation event |
| baked_31d | Binary flag: 1 when 31-day retention outcome is fully observable |
| save_attribution | Priority-ordered classification of retention outcome mechanism |
| initiation_rank | ROW_NUMBER per (company, sku) ordered by initiation timestamp |
| window_end_timestamp | Observation window boundary: next initiation or initiation + 1h |

---

## 👤 Author

**Ujjawl Kumar** — Senior Data Engineer
- 🔗 [LinkedIn](https://linkedin.com/in/theujjawlkumar)
- 📧 info.ujjawlkr094@gmail.com
- 5+ years building cloud-scale data platforms on AWS & Azure
- Domains: Fintech (SaaS subscriptions), Insurance (APCO Holdings, SBI General)

**Tech Stack:** PySpark · Python · SQL · Delta Lake · dbt · Airflow · Terraform · AWS EMR Serverless · Kafka · MLflow · Great Expectations · FastAPI

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.
