
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# from plugins.spark_config import Config

class Config:
    def __init__(self) -> None:
        self.dict= {}
        pass
    def build(self):
        return self.dict
    
    def config_spark(self):
        self.dict.update({
            'spark.kubernetes.container.image': 'docker.io/asc686f61/dataplatform_pyspark:v1.1',
            'spark.executorEnv.LD_PRELOAD':'/opt/bitnami/common/lib/libnss_wrapper.so',
            'spark.kubernetes.namespace':'infra',
            'spark.kubernetes.file.upload.path': 's3a://dataplatform/spark',
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.s3a.endpoint': 'https://myminio-hl.data-platform-tenant.svc.cluster.local:9000',
            'spark.hadoop.fs.s3a.fast.upload': 'true',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            'spark.hadoop.fs.s3a.access.key': 'minio',
            'spark.hadoop.fs.s3a.secret.key': 'minio123',
            'spark.jars.ivy': '/tmp',
            'conf spark.kubernetes.driver.secrets.kubeconfig':'/secret', \
            'conf spark.kubernetes.executor.secrets.kubeconfig':'/secret', \
            'conf spark.kubernetes.authenticate.driver.serviceAccountName':'my-release-spark', \
            'conf spark.kubernetes.authenticate.executor.serviceAccountName':'my-release-spark', \
            'conf spark.driver.extraJavaOptions=-Dcom.sun.net.ssl.checkRevocation':'false', \
            'conf spark.executor.extraJavaOptions=-Dcom.sun.net.ssl.checkRevocation':'false', \
            'conf spark.ssl.noCertVerification':'true'
        })
        return self
        



DAG_NAME = "dag_spark_operator"
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
    catchup=False,
    max_active_runs=1
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
    task_id='example_py_spark_pi',
    conn_id='spark_default',
    deploy_mode="cluster",
    application='local:///opt/spark/examples/src/main/python/pi.py',
    name='airflow-pyspark-job',
    verbose=True,
    conf=Config().config_spark().build(),
    dag=dag
)

# Set the task dependencies
task1 >> spark_submit_task
