FROM apache/airflow:2.8.1-python3.10

USER root
RUN apt-get update && apt-get install -y git && apt-get clean

USER airflow
RUN pip install --no-cache-dir --prefer-binary \
    "dbt-postgres==1.7.4" \
    "apache-airflow-providers-postgres==5.10.0"