# Changelog

All notable changes to StreamFlow Analytics are documented here.

## [2.0.0] - 2026-07-16

### Added
- Medallion architecture — Bronze / Silver / Gold layers
- Delta Lake ACID writes with MERGE + ZORDER + VACUUM
- Spark Structured Streaming — Kinesis → Delta exactly-once
- XGBoost churn propensity model with MLflow + SHAP
- FastAPI serving layer with real-time churn scoring
- dbt Gold/Silver transformation models + macros
- Great Expectations DQ — 15 expectations per table
- Terraform IaC — EMR Serverless 4096 vCPU, Kinesis 100 shards
- Apache Airflow DAGs — daily pipeline + backfill + ML retrain
- GitHub Actions CI — lint + structure validation

## [1.0.0] - 2024-11-01

### Added
- Initial cancel flow pipeline (QBO-only)
- Raw ECS event ingestion
- Cancel initiation grain
- Basic save attribution
- IXP experiment filter
