"""DAG: dbt_transform

This DAG runs DBT models in three logical layers: bronze -> silver -> gold.
Each layer runs `dbt run` for that layer then `dbt test` for data quality.
The DAG includes retries, failure callbacks (alerts), and an hourly schedule.

Notes:
- The DAG executes DBT by running `docker exec` against the local `dbt` container
  (adjust container name if it differs in your environment).
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

# import alerting helper from dags utils
try:
    from utils.alerting import AlertManager
    from utils.logging_utils import setup_logger
except Exception:
    # When Airflow parses the DAG outside the container context, imports may fail.
    AlertManager = None
    setup_logger = None


def task_failure_callback(context):
    """Called when any task fails. Sends an alert via AlertManager (if available).

    `context` is the Airflow context dict provided by the scheduler.
    """
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id if context.get("dag") else context.get("dag_id")
    task_id = task_instance.task_id if task_instance else context.get("task_id")
    exception = context.get("exception")
    error_message = str(exception)

    # Log locally
    try:
        if setup_logger:
            logger = setup_logger("dbt_dag")
            logger.error(f"Task failed: {dag_id}.{task_id} - {error_message}")
    except Exception:
        print(f"Failed to log failure for {dag_id}.{task_id}: {error_message}")

    # Send alert via AlertManager if available (e.g., Slack webhook configured)
    try:
        if AlertManager:
            am = AlertManager()
            am.alert_pipeline_failure(dag_id, task_id, error_message)
        else:
            print("AlertManager not available; skipping external alert.")
    except Exception as e:
        print(f"Failed to send alert: {e}")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_failure_callback,
}


dag = DAG(
    "dbt_transform",
    default_args=default_args,
    description="Orchestrate DBT models: bronze -> silver -> gold",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "sqlserver"],
)


# Helper: base docker exec prefix. Adjust container name if needed.
# Using docker exec to run dbt inside the dbt container created by docker-compose.
DBT_CONTAINER = "dbt_airflow_project-dbt-1"
DOCKER_EXEC = f"docker exec {DBT_CONTAINER}"


# --- Bronze layer ---
dbt_run_bronze = BashOperator(
    task_id="dbt_run_bronze",
    bash_command=f"{DOCKER_EXEC} dbt run --models bronze",
    retries=2,
    retry_delay=timedelta(minutes=5),
    dag=dag,
)

dbt_test_bronze = BashOperator(
    task_id="dbt_test_bronze",
    bash_command=f"{DOCKER_EXEC} dbt test --models bronze",
    retries=1,
    retry_delay=timedelta(minutes=5),
    dag=dag,
)


# --- Silver layer ---
dbt_run_silver = BashOperator(
    task_id="dbt_run_silver",
    bash_command=f"{DOCKER_EXEC} dbt run --models silver",
    retries=2,
    retry_delay=timedelta(minutes=5),
    dag=dag,
)

dbt_test_silver = BashOperator(
    task_id="dbt_test_silver",
    bash_command=f"{DOCKER_EXEC} dbt test --models silver",
    retries=1,
    retry_delay=timedelta(minutes=5),
    dag=dag,
)


# --- Gold layer ---
dbt_run_gold = BashOperator(
    task_id="dbt_run_gold",
    bash_command=f"{DOCKER_EXEC} dbt run --models gold",
    retries=2,
    retry_delay=timedelta(minutes=5),
    dag=dag,
)

dbt_test_gold = BashOperator(
    task_id="dbt_test_gold",
    bash_command=f"{DOCKER_EXEC} dbt test --models gold",
    retries=1,
    retry_delay=timedelta(minutes=5),
    dag=dag,
)


# Final full test (data quality sweep) - runs regardless of upstream test outcome only if previous runs succeeded.
full_project_tests = BashOperator(
    task_id="dbt_test_project",
    bash_command=f"{DOCKER_EXEC} dbt test",
    retries=0,
    retry_delay=timedelta(minutes=5),
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)


# Optional: generate docs (runs if all tests pass)
dbt_docs = BashOperator(
    task_id="dbt_docs_generate",
    bash_command=f"{DOCKER_EXEC} dbt docs generate",
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)


# Set dependencies: bronze -> silver -> gold -> full tests -> docs
(
    dbt_run_bronze
    >> dbt_test_bronze
    >> dbt_run_silver
    >> dbt_test_silver
    >> dbt_run_gold
    >> dbt_test_gold
    >> full_project_tests
    >> dbt_docs
)
