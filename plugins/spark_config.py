from airflow.hooks.base import BaseHook
import json

class Config:
    def __init__(self) -> None:
        self.dict = {}
        self.extra = {}  # Khởi tạo extra để sử dụng trong các phương thức khác

    def build(self):
        return self.dict

    def config_s3(self):
        # Lấy kết nối từ Airflow
        s3_conn = BaseHook.get_connection('minio_s3')
        print(f"Extra config: {s3_conn.extra}")  # Debugging output

        self.extra = json.loads(s3_conn.extra)  # Lưu giá trị extra vào thuộc tính self.extra

        self.dict.update({
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.s3a.endpoint': self.extra['endpoint_url'],  
            'spark.hadoop.fs.s3a.fast.upload': 'true',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            'spark.hadoop.fs.s3a.access.key': self.extra['aws_access_key_id'],  
            'spark.hadoop.fs.s3a.secret.key': self.extra['aws_secret_access_key']
        })

        return self  # Trả về self để cho phép chain phương thức

    def config_spark(self):
        # Đảm bảo rằng config_s3 đã được gọi trước để lấy thông tin từ extra
        self.dict.update({
            # Kubernetes-specific configurations
            'spark.kubernetes.container.image': 'docker.io/asc686f61/dataplatform_pyspark:v1.2',
            'spark.kubernetes.namespace': 'airflow',
            'spark.kubernetes.file.upload.path': 's3a://dataplatform/spark',
            'spark.jars.ivy': '/tmp',
            'spark.kubernetes.authenticate.driver.serviceAccountName': 'airflow',
            'spark.kubernetes.authenticate.executor.serviceAccountName': 'airflow',

            # Java options
            'spark.driver.extraJavaOptions': '-Dcom.sun.net.ssl.checkRevocation=false',
            'spark.executor.extraJavaOptions': '-Dcom.sun.net.ssl.checkRevocation=false',
            'spark.ssl.noCertVerification': 'true',

            # S3 (MinIO) configurations
            'spark.hadoop.fs.s3a.access.key': self.extra['aws_access_key_id'],  # Sử dụng self.extra
            'spark.hadoop.fs.s3a.secret.key': self.extra['aws_secret_access_key'],  # Sử dụng self.extra
            'spark.hadoop.fs.s3a.endpoint': self.extra['endpoint_url'],  # Sử dụng self.extra
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',

            # Spark event logging
            'spark.eventLog.enabled': 'true',
            'spark.eventLog.dir': 's3a://dataplatform/spark-history/',
            'spark.history.fs.logDirectory': 's3a://dataplatform/spark-history/'
        })
        return self
