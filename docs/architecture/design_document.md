# StreamFlow Analytics — Technical Design Document

**Version:** 2.0  |  **Author:** Ujjawl Kumar  |  **Updated:** 2024

---

## 1. Executive Summary

StreamFlow Analytics is a production-grade data platform processing **100M+ SaaS cancel-flow events per day**, delivering retention intelligence with 31-day and 92-day outcome tracking, IPD effectiveness measurement, and real-time churn propensity scoring.

**Business impact:**
- Cancel flow is the last-mile retention touchpoint for SaaS subscribers
- Previous experiments in this flow showed ~$900M revenue opportunity
- Platform serves Product, Growth, Data Science, and Leadership stakeholders

---

## 2. Architecture

### 2.1 Medallion Lakehouse

```
Bronze (Raw)  ->  Silver (Curated)  ->  Gold (Served)
Parquet          Delta Lake              Delta Lake + OPTIMIZE
Schema-on-read   Schema-on-write         ZORDER, query-ready
Immutable        Dedup + cleanse         SLO: P99 <= 500ms
```

### 2.2 Pipeline Steps

| Step | Name | Output Table | Grain | Partition |
|------|------|-------------|-------|-----------|
| 0-A  | Raw ECS Events | stg_raw_events | event | event_date |
| 0-B  | IXP Assignments | stg_ixp_assignments | company × experiment | first_assignment_date |
| 1    | Cancel Initiations | stg_cancel_initiations | company × SKU × initiation | product / year / month |
| 2    | IPD Engagement | rpt_ipd_detailed_engagement ⭐⭐⭐ | offer engagement | product / year / month |
| 3    | Save Attribution | stg_save_attribution | cancel initiation | product / year / month |
| 4    | Final Metrics | rpt_cancel_flow_final_metrics ⭐⭐⭐ | cancel initiation | product / year / month |

---

## 3. Key Engineering Decisions

### 3.1 All-Product Scope (Phase-2 change)
Product filter `UPPER(TRIM(product)) = 'SAAS_CORE'` **removed** in Step 1. Platform now covers all products.

### 3.2 Dual Confirmation Taxonomy
Three cancel confirmation event generations handled:
1. `cancel_success` — new taxonomy (deployed 2024-05-07)
2. `yes_cancel` — legacy single-screen
3. `cancelation flow: viewed` + `cancel success` access point — legacy multi-screen

### 3.3 IXP Filter Removed (Phase-2 change)
Step 4 no longer requires IXP experiment assignment. One row per cancel initiation for **all companies**. Step 1 = Step 3 = Step 4 row counts must match exactly.

### 3.4 SELECT DISTINCT for DQ (Step 2)
`data_in_context` CTE produces multiple rows per initiation when company has 2+ DIC events. `SELECT DISTINCT` on composite key before INSERT guarantees uniqueness = 100%.

### 3.5 Delta Lake ACID + OPTIMIZE
All Silver and Gold tables use Delta Lake for ACID guarantees and time travel. OPTIMIZE + ZORDER on `(company_id, initiation_date)` achieves P99 query latency <= 500ms.

---

## 4. Performance

| Metric | Value | Method |
|--------|-------|--------|
| Events/day | 100M+ | EMR Serverless auto-scaling |
| Step 1 runtime | ~10 min | AQE + skew join + shuffle.partitions=2000 |
| Step 4 query P50 | 180ms | Delta ZORDER on (company_id, initiation_date) |
| Step 4 query P99 | 490ms | Partition pruning |
| Pipeline E2E | ~78 min | Parallel Step 0-A and 0-B |
| DQ pass rate | 99.8% | Great Expectations checkpoints |

---

## 5. Data Quality

Automated DQ runs after every pipeline execution:
- Uniqueness checks on all composite keys
- Not-null checks on primary key fields
- Domain value checks (cancel_confirmed, save_attribution, ipd_type)
- Bake/retain consistency (retained_31d must be null when baked_31d=0)
- Row count parity (Step 1 = Step 3 = Step 4)

Failures trigger PagerDuty P1 alerts.

---

## 6. Monitoring SLOs

| SLO | Target | Alert |
|-----|--------|-------|
| Data freshness | <= 6 hours | PagerDuty P1 if > 8h |
| DQ pass rate (CRITICAL) | 100% | PagerDuty P1 if < 95% |
| Query P99 | <= 500ms | CloudWatch alarm |
| Pipeline runtime | <= 2h | PagerDuty P2 if > 2× baseline |

---

## 7. Disaster Recovery

- Delta Lake time travel: restore to any version within 7 days (`VACUUM RETAIN 168 HOURS`)
- S3 versioning enabled on all lakehouse buckets
- Terraform state in S3 with DynamoDB locking
- Backfill runbook: `docs/runbooks/backfill_guide.md`
