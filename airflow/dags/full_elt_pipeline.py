from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from cosmos import DbtTaskGroup
from cosmos.config import ProjectConfig, ProfileConfig
import sys
from pathlib import Path

# Agregamos la raíz del proyecto para que Python encuentre 'src'
sys.path.append("/opt/airflow")

# Importamos las funciones principales de tus scripts
from src.extraction.extract_data import run_extraction_pipeline
from src.load.validate_and_load_gcs import run_validation_and_load
from src.load.load_data import run_pipeline as run_load_to_sql_pipeline

with DAG(
    dag_id="full_elt_pipeline",
    schedule="0 9 * * *",
    start_date=pendulum.datetime(2025, 9, 1, tz="UTC"),
    catchup=False,
    tags=["elt", "dbt", "mysql", "gcs"],
) as dag:

    # 1. Tarea de Extracción (E)
    extract_data_task = PythonOperator(
        task_id="extract_data_from_sources",
        python_callable=run_extraction_pipeline,
    )

    # 3. Tarea de Carga a MySQL (L)
    load_to_mysql_task = PythonOperator(
        task_id="load_raw_data_to_mysql",
        python_callable=run_load_to_sql_pipeline,
    )

    # 4. Tareas de Transformación (T) con dbt usando Cosmos
    dbt_task_group = DbtTaskGroup(
        group_id="dbt_project",
        project_config=ProjectConfig(
            Path("/opt/airflow/src/transformation/nyc_data_warehouse")
        ),
        profile_config=ProfileConfig(
            profiles_yml_filepath=Path("/opt/airflow/dbt_profiles/profiles.yml"),
            profile_name="nyc_data_warehouse",  # el nombre del profile en profiles.yml
            target_name="dev"  # el target que definiste en profiles.yml
        ),
        operator_args={"dbt_executable_path": "/usr/bin/dbt"},
    )

    # Orquestación del Pipeline
    extract_data_task >> load_to_mysql_task >> dbt_task_group