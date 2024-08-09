
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
            'spark.kubernetes.authenticate.submission.caCertFile': '/opt/spark/ca.crt',
            'spark.kubernetes.authenticate.submission.oauthToken': 'eyJhbGciOiJSUzI1NiIsImtpZCI6InVOWDR1aEJIOGdLQ1RxcXp5LUxCNGhRT2tPU1RvWGtMUTJJaDh5RlBIZ3MifQ.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJrdWJlLXN5c3RlbSIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VjcmV0Lm5hbWUiOiJtaWNyb2s4cy1kYXNoYm9hcmQtdG9rZW4iLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC5uYW1lIjoiZGVmYXVsdCIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VydmljZS1hY2NvdW50LnVpZCI6IjQ1MDU2MGJlLTUyZjQtNDk5Ny04NDhmLWRiMWUwMDI3MDEwOSIsInN1YiI6InN5c3RlbTpzZXJ2aWNlYWNjb3VudDprdWJlLXN5c3RlbTpkZWZhdWx0In0.OxUIOzPDuDeWF8nqCvyt9Bihkp_E_Z9nT-2zs_KTf8_pTpbB_DPQgYUW7o59pLr3kn3a5P8MnJ1hK3UZbemxStAuBsNUJ2BMDYOkk4eLaoOS-GxdeHLt31FLZEDAERUHhLE3NqXAii27z3i6Q_26w4mcW4yWa_MAuZuDaqc9twJw9_sf1nmo6UH59iL1uoh7n3onZgsW9ctoAITRq0XD4QIvBgFWytMpvVy_CQ4wUJB-4AlwtgF1Cxn2YMwI5W8QsvGRuCyY4KmF46gAfUM-YKl-EacHCJDssnE5L5eXYC2r5sJMnCkauPfPfkRFtXCRnY6D1seV9SNKxRhoiH5DYA',
            'spark.kubernetes.container.image': 'docker.io/asc686f61/pyspark:v1',
            'spark.kubernetes.file.upload.path': 's3a://dataplatform/spark',
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.s3a.endpoint': 'myminio-hl.data-platform-tenant.svc.cluster.local:9000',
            'spark.hadoop.fs.s3a.fast.upload': 'true',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            'spark.hadoop.fs.s3a.access.key': 'minio',
            'spark.hadoop.fs.s3a.secret.key': 'minio123',
            'spark.jars.ivy': '/tmp'
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
    application='s3a://dataplatform/spark-job/pi.py',
    name='airflow-pyspark-job',
    verbose=True,
    conf=Config().config_spark().build(),
    dag=dag
)

# Set the task dependencies
task1 >> spark_submit_task
