from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.operators.bash import BashOperator

AIRBYTE_CONN_ID = "airbyte_conn"  # defined in Airflow Connections UI
AIRBYTE_CONNECTION_ID = "your-airbyte-connection-uuid"  # from Airbyte UI

default_args = {
    "owner": "opiyo",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="hdss_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    description='ETL job to process trigger the process',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # 1. Sync data with Airbyte
    airbyte_sync = AirbyteTriggerSyncOperator(
        task_id="airbyte_sync",
        airbyte_conn_id=AIRBYTE_CONN_ID,
        connection_id=AIRBYTE_CONNECTION_ID,
        asynchronous=False,
        timeout=3600,
        wait_seconds=10,
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command="cd /opt/airflow/hdss_dbt && dbt seed --profiles-dir .",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/hdss_dbt && dbt run --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/hdss_dbt && dbt test --profiles-dir .",
    )

    # Orchestration order
    airbyte_sync >> dbt_seed >> dbt_test >> dbt_run
