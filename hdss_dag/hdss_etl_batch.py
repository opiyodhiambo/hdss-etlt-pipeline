import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

AIRBYTE_CONN_IDS = {
    "average_survival_by_sex": "e7436be9-5de8-44de-8929-05d73bac60d4",
    "mbita_core": "6a7f12ba-db96-477f-90c4-c6db7efa6438",
    "seasonality": "239e6ecf-dfff-4e7a-9d06-e148c00b6360",
}

AIRBYTE_CONN_ID = os.getenv("AIRBYTE_CONN_ID")
DBT_CLOUD_CONN_ID = os.getenv("DBT_CLOUD_CONN_ID")
DBT_CLOUD_JOB_ID = os.getenv("DBT_CLOUD_JOB_ID")\

default_args = {
    "owner": "opiyo",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="hdss_multi_airbyte_dbt_cloud_operator",
    default_args=default_args,
    schedule_interval="@daily",
    description="Sync multiple Airbyte connections then trigger dbt Cloud job",
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Airbyte sync tasks
    airbyte_tasks = {}
    for name, conn_uuid in AIRBYTE_CONN_IDS.items():
        task = AirbyteTriggerSyncOperator(
            task_id=f"airbyte_sync_{name}",
            airbyte_conn_id=AIRBYTE_CONN_ID,
            connection_id=conn_uuid,
            asynchronous=False,
            timeout=3600,
            wait_seconds=10,
        )
        airbyte_tasks[name] = task

    # dbt Cloud job
    dbt_cloud_task = DbtCloudRunJobOperator(
        task_id="run_dbt_cloud_job",
        job_id=DBT_CLOUD_JOB_ID,
        dbt_cloud_conn_id=DBT_CLOUD_CONN_ID,
        wait_for_termination=True,
        timeout=3600,
    )

    # All Airbyte syncs must finish before running dbt
    list(airbyte_tasks.values()) >> dbt_cloud_task
