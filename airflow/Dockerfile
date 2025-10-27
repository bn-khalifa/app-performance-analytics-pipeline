FROM apache/airflow:2.9.0
USER root
USER airflow
RUN pip install pymongo mysql-connector-python duckdb dbt-duckdb
COPY dags/ /opt/airflow/dags/