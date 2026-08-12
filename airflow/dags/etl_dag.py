"""
Distributed ML Data Pipeline & Model Serving
=============================================
Airflow DAG: daily ETL orchestration for ML training dataset generation.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default DAG args
# ---------------------------------------------------------------------------
default_args = {
    "owner":            "ridwan-umar",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def validate_source_data(**context):
    """Check source data availability and schema before ETL begins."""
    execution_date = context["execution_date"]
    logger.info(f"Validating source data for date: {execution_date.date()}")
    # TODO: implement schema check on raw source partitions
    logger.info("Source data validation passed.")


def run_spark_etl(**context):
    """Trigger PySpark ETL job via spark-submit."""
    execution_date = context["execution_date"]
    date_str = execution_date.strftime("%Y-%m-%d")
    logger.info(f"Triggering Spark ETL for partition: {date_str}")
    # TODO: call spark-submit or SparkSubmitOperator
    logger.info(f"Spark ETL completed for {date_str}")


def run_data_quality_checks(**context):
    """Run post-ETL data quality checks on output partitions."""
    logger.info("Running data quality checks on ETL output...")
    # TODO: implement null rate, row count, schema validation
    logger.info("Data quality checks passed.")


def update_feature_store(**context):
    """Register new features in the feature store metadata table."""
    logger.info("Updating feature store registry...")
    # TODO: implement feature store update
    logger.info("Feature store updated.")


def trigger_model_training(**context):
    """Kick off model training DAG once fresh features are ready."""
    logger.info("Triggering model training pipeline...")
    # TODO: trigger training_dag via Airflow API or TriggerDagRunOperator
    logger.info("Model training triggered.")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="ml_data_etl_daily",
    default_args=default_args,
    description="Daily ETL pipeline: raw → processed features for ML training",
    schedule_interval="0 2 * * *",    # 02:00 UTC daily
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["ml", "etl", "data-engineering"],
) as dag:

    # Task 1: Validate source availability
    t_validate = PythonOperator(
        task_id="validate_source_data",
        python_callable=validate_source_data,
        provide_context=True,
    )

    # Task 2: Run Spark ETL pipeline
    t_spark_etl = PythonOperator(
        task_id="run_spark_etl",
        python_callable=run_spark_etl,
        provide_context=True,
    )

    # Task 3: Post-ETL data quality checks
    t_dq_checks = PythonOperator(
        task_id="run_data_quality_checks",
        python_callable=run_data_quality_checks,
        provide_context=True,
    )

    # Task 4: Update feature store
    t_feature_store = PythonOperator(
        task_id="update_feature_store",
        python_callable=update_feature_store,
        provide_context=True,
    )

    # Task 5: Trigger model training (conditional on DQ pass)
    t_trigger_training = PythonOperator(
        task_id="trigger_model_training",
        python_callable=trigger_model_training,
        provide_context=True,
    )

    # Task 6: Success notification (Slack / email)
    t_notify = BashOperator(
        task_id="notify_success",
        bash_command='echo "ETL pipeline completed successfully for {{ ds }}"',
    )

    # ---------------------------------------------------------------------------
    # Task dependencies
    # ---------------------------------------------------------------------------
    t_validate >> t_spark_etl >> t_dq_checks >> t_feature_store >> t_trigger_training >> t_notify
