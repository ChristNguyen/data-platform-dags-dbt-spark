from datetime import datetime

from airflow.decorators import dag
from airflow.operators.python import PythonOperator


from cosmos import DbtTaskGroup, ProjectConfig
from cosmos import ProfileConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping, SparkThriftProfileMapping

from pathlib import Path
from cosmos import ExecutionConfig

shop_analytics_path = Path("/opt/bitnami/airflow/plugins/dbt/shop_analytics")
dbt_executable = Path("/opt/bitnami/airflow/venv/bin/dbt")

venv_execution_config = ExecutionConfig(
    dbt_executable_path=str(dbt_executable),
)


warehouse_db = ProfileConfig(
    profile_name="warehouse_db",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="warehouse_db",
        profile_args={"schema": "dbt"},
    )
)


@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["simple"],
)
def simple_task_group() -> None:
    def print_hello():
        return "Hello World!"

    # Define the PythonOperators
    pre_dbt = PythonOperator(
        task_id="print_hello",
        python_callable=print_hello
    )

    shop_analytics = DbtTaskGroup(
        group_id="my_shop_project",
        task_group="shop_analytics_group",
        project_config=ProjectConfig(shop_analytics_path),
        profile_config=warehouse_db,
        execution_config=venv_execution_config
    )

    def print_goodbye():
        return "Goodbye World!"

    post_dbt = PythonOperator(
        task_id="print_goodbye",
        python_callable=print_goodbye
    )

    pre_dbt >> shop_analytics >> post_dbt


simple_task_group()
