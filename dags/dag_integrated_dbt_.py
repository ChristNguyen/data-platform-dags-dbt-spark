from datetime import datetime

from airflow.decorators import dag
from airflow.operators.python import PythonOperator


from cosmos import DbtTaskGroup, ProjectConfig

from include.profiles import warehouse_db
from include.constants import shop_analytics_path, venv_execution_config


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
