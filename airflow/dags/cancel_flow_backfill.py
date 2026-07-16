"""
cancel_flow_backfill.py -- Airflow DAG: Historical Backfill
============================================================
On-demand historical backfill DAG. Triggered manually with conf:
  {"start_date": "2024-01-01", "end_date": "2024-12-31"}
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

DEFAULT_ARGS = {
    "owner": "ujjawl.kumar", "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="streamflow_cancel_flow_backfill",
    description="On-demand historical backfill — triggered manually with start/end dates",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,     # manual trigger only
    catchup=False,
    tags=["cancel_flow","backfill","on-demand"],
    params={"start_date": "2024-01-01", "end_date": "2024-12-31"},
) as dag:

    start = EmptyOperator(task_id="start")

    backfill = BashOperator(
        task_id="run_backfill",
        bash_command=(
            "./scripts/backfill.sh "
            "--start-date {{ params.start_date }} "
            "--end-date {{ params.end_date }} "
            "--env prod"
        ),
        execution_timeout=timedelta(hours=12),
    )

    dq_check = BashOperator(
        task_id="dq_check_post_backfill",
        bash_command="python src/dq/dq_checks.py --all --data-path ./data/output",
    )

    end = EmptyOperator(task_id="end")
    start >> backfill >> dq_check >> end
