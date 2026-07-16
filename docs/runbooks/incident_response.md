# StreamFlow Analytics — Incident Response Runbook

## P1 Alerts

### Data Freshness > 8 Hours

**Symptoms:** PagerDuty alert `streamflow-analytics-prod-data-freshness-breach`

**Diagnosis:**
```bash
# Check last pipeline run
curl http://airflow:8080/api/v1/dags/streamflow_cancel_flow_daily/dagRuns?limit=1

# Check Step 4 max date
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.sql('SELECT MAX(initiation_date), COUNT(*) FROM gold.rpt_cancel_flow_final_metrics').show()
"
```

**Resolution:**
1. Trigger manual DAG run: `airflow dags trigger streamflow_cancel_flow_daily`
2. If EMR failure: check EMR Serverless logs in S3 at `s3://streamflow-analytics-prod-logs/emr-logs/`
3. If data source delay: wait and re-trigger

---

### DQ Pass Rate < 95%

**Symptoms:** PagerDuty alert `streamflow-analytics-prod-dq-pass-rate-low`

**Diagnosis:**
```bash
# Run DQ checks and see which expectations failed
python src/dq/dq_checks.py --all --data-path s3://streamflow-analytics-gold/
```

**Common root causes:**
- **Uniqueness < 1.0 on rpt_ipd_detailed_engagement**: DIC multi-event window multiply. Fix: `SELECT DISTINCT` in Step 2 INSERT.
- **cancel_confirmed has unexpected values**: Check raw ECS for new event schemas.
- **Row count deviation > 20%**: Check Step 0 data freshness first.

---

## Backfill Procedure

```bash
# 1. Identify missing date range
SELECT MIN(initiation_date), MAX(initiation_date) FROM gold.rpt_cancel_flow_final_metrics;

# 2. Run backfill
./scripts/backfill.sh --start-date 2024-06-01 --end-date 2024-06-30 --env prod

# 3. Validate
python src/dq/dq_checks.py --all --data-path s3://streamflow-analytics-gold/
```
