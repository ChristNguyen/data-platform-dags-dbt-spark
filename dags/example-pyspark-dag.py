from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from plugins.spark_config import Config


DAG_NAME = "test_local_pi"
default_args = {
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    DAG_NAME,
    default_args=default_args,
    description="An example DAG with SparkOperator",
    schedule=None,
    tags=["test"],
    catchup=False,
    max_active_runs=1,
)


# Define Python functions to be executed
def print_hello():
    return "Hello World!"


# Define the PythonOperators
task1 = PythonOperator(
    task_id="print_hello",
    python_callable=print_hello,
    dag=dag,
)

spark_submit_task = SparkSubmitOperator(
    task_id="test-pi-local-spark",
    conn_id="spark",
    deploy_mode="cluster",
    application="local:///opt/spark/examples/src/main/python/pi.py",
    name="airflow-pyspark-job",
    verbose=True,
    conf=Config().config_spark().build(),
    dag=dag,
)

# Set the task dependencies
task1 >> spark_submit_task
