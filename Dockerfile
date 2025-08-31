FROM apache/airflow:2.10.5-python3.10

# Install dbt + adapter (choose postgres, bigquery, etc.)
RUN pip install --no-cache-dir \
    apache-airflow-providers-airbyte \
    apache-airflow-providers-postgres \
    apache-airflow-providers-redis \
    apache-airflow-providers-celery \
    apache-airflow-providers-dbt-cloud
