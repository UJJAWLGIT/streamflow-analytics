"""
ml_retrain_weekly.py -- Airflow DAG: Weekly ML Model Retrain
=============================================================
Retrains the churn propensity model weekly on latest feature store data.
Schedule: Every Sunday 02:00 UTC
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobRunOperator
from airflow.operators.empty import EmptyOperator

DEFAULT_ARGS = {
    "owner": "ujjawl.kumar", "retries": 1,
    "retry_delay": timedelta(minutes=30),
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="streamflow_ml_retrain_weekly",
    description="Weekly churn propensity model retrain + MLflow registration",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * 0",  # Sunday 02:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=["ml","churn","weekly"],
) as dag:

    start = EmptyOperator(task_id="start")

    feature_engineering = EmrServerlessStartJobRunOperator(
        task_id="build_ml_features",
        application_id="{{ var.value.emr_serverless_app_id }}",
        execution_role_arn="{{ var.value.emr_execution_role_arn }}",
        job_driver={"sparkSubmit": {
            "entryPoint": "s3://streamflow-scripts/ml/feature_engineering.py",
            "entryPointArguments": ["--env","prod","--lookback-days","90"],
            "sparkSubmitParameters": "--conf spark.sql.shuffle.partitions=400 --packages io.delta:delta-core_2.12:2.4.0"
        }},
    )

    train_model = EmrServerlessStartJobRunOperator(
        task_id="train_churn_model",
        application_id="{{ var.value.emr_serverless_app_id }}",
        execution_role_arn="{{ var.value.emr_execution_role_arn }}",
        job_driver={"sparkSubmit": {
            "entryPoint": "s3://streamflow-scripts/ml/churn_model.py",
            "entryPointArguments": [
                "--features-path", "s3://streamflow-analytics-gold/ml_features",
                "--mlflow-uri",    "{{ var.value.mlflow_tracking_uri }}",
                "--experiment-name", "cancel-flow-churn-propensity",
                "--n-estimators",  "500",
            ],
        }},
    )

    notify = EmptyOperator(task_id="notify_slack_on_retrain_complete")
    end    = EmptyOperator(task_id="end")

    start >> feature_engineering >> train_model >> notify >> end
