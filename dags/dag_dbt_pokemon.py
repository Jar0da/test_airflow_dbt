from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
import os

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Chemin ABSOLU du projet dbt sur la machine hôte (Ubuntu)
HOST_DBT_PATH = '/home/jaroda/Dev/test_airflow_dbt/dbt_project'

with DAG(
    dag_id='dbt_pokemon_docker',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    schedule=None,
    tags=['dbt', 'docker'],
) as dag:

    dbt_run = DockerOperator(
        task_id='dbt_run_ods',
        image='ghcr.io/dbt-labs/dbt-postgres:1.7.0',
        command='run --select ods --profiles-dir .',
        api_version='auto',
        auto_remove=True,       # Desactiver temporairement pour conserver le conteneur en cas de crash
        tty=True,               # Active l'emulation TTY pour diffuser le stdout
        docker_url='unix://var/run/docker.sock',
        network_mode='test_airflow_dbt_default',
        environment={
            'PYTHONUNBUFFERED': '1', # Empêche le buffering des sorties de dbt
            'HOST_PC_IP': os.getenv('HOST_PC_IP'),
            'DB_DEMO_USER': os.getenv('DB_DEMO_USER'),
            'DB_DEMO_PASSWORD': os.getenv('DB_DEMO_PASSWORD'),
        },
        working_dir='/usr/app',
        mounts=[
            Mount(source=HOST_DBT_PATH, target='/usr/app', type='bind')
        ],
    )

    dbt_test = DockerOperator(
        task_id='dbt_test',
        image='ghcr.io/dbt-labs/dbt-postgres:1.7.0',
        command='test --profiles-dir .',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='test_airflow_dbt_default',
        environment={
            'HOST_PC_IP': os.getenv('HOST_PC_IP'),
            'DB_DEMO_USER': os.getenv('DB_DEMO_USER'),
            'DB_DEMO_PASSWORD': os.getenv('DB_DEMO_PASSWORD'),
        },
        working_dir='/usr/app',
        mounts=[
            Mount(source=HOST_DBT_PATH, target='/usr/app', type='bind')
        ],
    )

    dbt_run >> dbt_test