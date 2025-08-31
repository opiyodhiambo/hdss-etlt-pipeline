FROM apache/airflow:2.10.5-python3.10

# Install dbt + adapter (choose postgres, bigquery, etc.)
RUN pip install --no-cache-dir dbt-core dbt-postgres
