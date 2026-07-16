"""
cancel_flow_daily.py — Airflow DAG
====================================
Daily orchestration DAG for the StreamFlow Analytics pipeline.

Schedule: 08:00 UTC daily (data through previous day)
SLO:      Complete by 10:00 UTC (2-hour window)
Owner:    Data Engineering — Ujjawl Kumar
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobRunOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.task_group import TaskGroup

# ── DAG defaults ───────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner": "ujjawl.kumar",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=3),
}

EMR_APP_ID = "{{ var.value.emr_serverless_app_id }}"
EMR_ROLE_ARN = "{{ var.value.emr_execution_role_arn }}"
S3_LOGS = "{{ var.value.s3_logs_bucket }}/emr-logs/"
SLACK_CONN = "slack_webhook_data_eng"
PIPELINE_NAME = "cancel_flow"

# ── Spark job factory ──────────────────────────────────────────────────────────

def make_emr_job(
    task_id: str,
    script_path: str,
    arguments: list[str],
    shuffle_partitions: int = 800,
    driver_memory: str = "14g",
    executor_memory: str = "16g",
    max_executors: int = 200,
) -> EmrServerlessStartJobRunOperator:
    return EmrServerlessStartJobRunOperator(
        task_id=task_id,
        application_id=EMR_APP_ID,
        execution_role_arn=EMR_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": script_path,
                "entryPointArguments": arguments,
                "sparkSubmitParameters": (
                    f"--conf spark.driver.memory={driver_memory} "
                    f"--conf spark.executor.memory={executor_memory} "
                    f"--conf spark.sql.shuffle.partitions={shuffle_partitions} "
                    f"--conf spark.dynamicAllocation.maxExecutors={max_executors} "
                    "--conf spark.sql.adaptive.enabled=true "
                    "--conf spark.sql.adaptive.skewJoin.enabled=true "
                    "--conf spark.sql.sources.partitionOverwriteMode=dynamic "
                    "--packages io.delta:delta-core_2.12:2.4.0"
                ),
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": S3_LOGS}
            }
        },
        waiter_max_attempts=120,
        waiter_delay=60,
    )


# ── Notification helpers ───────────────────────────────────────────────────────

def build_slack_success_message(context: dict) -> str:
    return (
        f"✅ *StreamFlow Analytics — Daily Pipeline Complete*\n"
        f"> Date: `{context['ds']}`\n"
        f"> DAG: `{context['dag'].dag_id}`\n"
        f"> Duration: `{(datetime.utcnow() - context['dag_run'].start_date).seconds // 60}` min\n"
        f"> Status: *SUCCESS* 🎉"
    )


def build_slack_failure_message(context: dict) -> str:
    return (
        f"❌ *StreamFlow Analytics — Pipeline FAILED*\n"
        f"> Date: `{context['ds']}`\n"
        f"> Task: `{context['task_instance'].task_id}`\n"
        f"> Log: {context['task_instance'].log_url}"
    )


# ── DAG definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="streamflow_cancel_flow_daily",
    description="Daily cancel-flow analytics pipeline: Bronze → Silver → Gold → DQ → OPTIMIZE",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 8 * * *",       # 08:00 UTC daily
    catchup=False,
    max_active_runs=1,
    tags=["cancel_flow", "data_engineering", "production", "sla:2h"],
) as dag:

    start = EmptyOperator(task_id="start")

    # ── [0] Data freshness sensor ──────────────────────────────────────────────
    wait_for_upstream = TimeDeltaSensor(
        task_id="wait_for_upstream_data",
        delta=timedelta(hours=2),   # Wait for ECS events to land in S3
    )

    # ── [Step 0] Parallel ingestion ────────────────────────────────────────────
    with TaskGroup("step_0_ingestion", tooltip="Parallel: Raw events + IXP assignments") as step_0:

        step_0a_raw_events = make_emr_job(
            task_id="step_0a_raw_ecs_events",
            script_path="s3://streamflow-scripts/bronze/raw_events_pipeline.py",
            arguments=[
                "--start-date", "{{ ds }}",
                "--end-date", "{{ ds }}",
                "--env", "prod",
            ],
            shuffle_partitions=400,
            max_executors=100,
        )

        step_0b_ixp = make_emr_job(
            task_id="step_0b_ixp_assignments",
            script_path="s3://streamflow-scripts/bronze/ixp_assignments.py",
            arguments=[
                "--start-date", "{{ macros.ds_add(ds, -7) }}",
                "--end-date", "{{ ds }}",
                "--env", "prod",
            ],
            shuffle_partitions=200,
            max_executors=50,
        )

        # Run 0-A and 0-B in parallel
        [step_0a_raw_events, step_0b_ixp]

    # ── [Step 1] Cancel initiations (Silver) ───────────────────────────────────
    step_1_cancel_initiations = make_emr_job(
        task_id="step_1_cancel_initiations",
        script_path="s3://streamflow-scripts/silver/cancel_initiations.py",
        arguments=["--start-date", "{{ ds }}", "--end-date", "{{ ds }}", "--env", "prod"],
        shuffle_partitions=2000,
        driver_memory="32g",
        max_executors=300,
    )

    # ── [Step 2] IPD engagement (Gold ⭐⭐⭐) ───────────────────────────────────
    step_2_ipd_engagement = make_emr_job(
        task_id="step_2_ipd_engagement",
        script_path="s3://streamflow-scripts/gold/ipd_engagement.py",
        arguments=["--start-date", "{{ ds }}", "--end-date", "{{ ds }}", "--env", "prod"],
        shuffle_partitions=800,
        max_executors=200,
    )

    # ── [Step 3] Save attribution (Silver) ────────────────────────────────────
    step_3_save_attribution = make_emr_job(
        task_id="step_3_save_attribution",
        script_path="s3://streamflow-scripts/silver/save_attribution.py",
        arguments=["--start-date", "{{ ds }}", "--end-date", "{{ ds }}", "--env", "prod"],
        shuffle_partitions=800,
        max_executors=200,
    )

    # ── [Step 4] Final metrics (Gold ⭐⭐⭐) ───────────────────────────────────
    step_4_final_metrics = make_emr_job(
        task_id="step_4_final_metrics",
        script_path="s3://streamflow-scripts/gold/final_metrics.py",
        arguments=["--env", "prod"],
        shuffle_partitions=800,
        max_executors=200,
    )

    # ── [DQ] Great Expectations checkpoint ─────────────────────────────────────
    dq_checkpoint = make_emr_job(
        task_id="dq_great_expectations_checkpoint",
        script_path="s3://streamflow-scripts/dq/run_checkpoint.py",
        arguments=["--checkpoint", "cancel_flow_daily", "--date", "{{ ds }}"],
        shuffle_partitions=200,
        max_executors=50,
    )

    # ── [Optimize] Delta OPTIMIZE + ZORDER ────────────────────────────────────
    with TaskGroup("delta_optimize", tooltip="OPTIMIZE + ZORDER Gold tables") as optimize:

        optimize_final_metrics = make_emr_job(
            task_id="optimize_rpt_cancel_flow_final_metrics",
            script_path="s3://streamflow-scripts/maintenance/optimize.py",
            arguments=[
                "--table", "gold.rpt_cancel_flow_final_metrics",
                "--zorder-cols", "company_id,initiation_date",
                "--partition-filter", "initiation_year = '{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}'",
            ],
            shuffle_partitions=200,
            max_executors=100,
        )

        optimize_ipd = make_emr_job(
            task_id="optimize_rpt_ipd_detailed_engagement",
            script_path="s3://streamflow-scripts/maintenance/optimize.py",
            arguments=[
                "--table", "gold.rpt_ipd_detailed_engagement",
                "--zorder-cols", "company_id,initiation_timestamp",
            ],
            shuffle_partitions=200,
            max_executors=100,
        )

    # ── [Notify] Slack success ─────────────────────────────────────────────────
    notify_success = SlackWebhookOperator(
        task_id="notify_success",
        http_conn_id=SLACK_CONN,
        message="{{ ti.xcom_pull(task_ids='build_success_message') }}",
        trigger_rule="all_success",
    )

    notify_failure = SlackWebhookOperator(
        task_id="notify_failure",
        http_conn_id=SLACK_CONN,
        message=build_slack_failure_message,
        trigger_rule="one_failed",
    )

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    # ── DAG dependency chain ───────────────────────────────────────────────────
    (
        start
        >> wait_for_upstream
        >> step_0
        >> step_1_cancel_initiations
        >> step_2_ipd_engagement
        >> step_3_save_attribution
        >> step_4_final_metrics
        >> dq_checkpoint
        >> optimize
        >> [notify_success, notify_failure]
        >> end
    )
