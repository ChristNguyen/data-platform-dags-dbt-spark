from airflow.hooks.base import BaseHook
import json

class Config:
    def __init__(self) -> None:
        self.dict = {}

    def build(self):
        return self.dict

    def config_s3(self):
        # Lấy kết nối từ Airflow
        s3_conn = BaseHook.get_connection('minio_s3')

        # Giải mã trường extra từ kết nối
        extra = json.loads(s3_conn.extra)

        # Cập nhật cấu hình S3 (MinIO)
        self.dict.update({
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.s3a.endpoint': extra['endpoint_url'],  
            'spark.hadoop.fs.s3a.fast.upload': 'true',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            'spark.hadoop.fs.s3a.access.key': extra['aws_access_key_id'],  
            'spark.hadoop.fs.s3a.secret.key': extra['aws_secret_access_key']
        })

        return self  # Trả về self để cho phép chain phương thức

    def config_spark(self):
        # Lấy kết nối từ Airflow
        s3_conn = BaseHook.get_connection('minio_s3')

        # Giải mã trường extra từ kết nối
        extra = json.loads(s3_conn.extra)

        # Cập nhật cấu hình Spark
        self.dict.update({
            'spark.kubernetes.container.image': 'docker-dso-cloud.abbank.vn/data-platform/dev/dataplatform_pyspark:v1.2.2',
            'spark.kubernetes.container.image.pullSecrets': 'regcred',
            'spark.kubernetes.namespace': 'airflow',
            'spark.kubernetes.file.upload.path': 's3a://dataplatform/spark-jobs',
            'spark.jars.ivy': '/tmp',
            'spark.kubernetes.authenticate.driver.serviceAccountName': 'airflow',
            'spark.kubernetes.authenticate.executor.serviceAccountName': 'airflow',

            # Java options
            'spark.driver.extraJavaOptions': '-Dcom.sun.net.ssl.checkRevocation=false',
            'spark.executor.extraJavaOptions': '-Dcom.sun.net.ssl.checkRevocation=false',
            'spark.ssl.noCertVerification': 'true',

            # S3 configurations (MinIO) - sử dụng extra trực tiếp từ kết nối
            'spark.hadoop.fs.s3a.access.key': extra['aws_access_key_id'],
            'spark.hadoop.fs.s3a.secret.key': extra['aws_secret_access_key'],
            'spark.hadoop.fs.s3a.endpoint': extra['endpoint_url'],
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',

            # Event logging and history configurations
            'spark.eventLog.enabled': 'true',
            'spark.eventLog.dir': 's3a://dataplatform/spark-history/',
            'spark.history.fs.logDirectory': 's3a://dataplatform/spark-history/'
        })
        return self
