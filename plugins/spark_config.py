
class Config:
    def __init__(self) -> None:
        self.dict= {}
        pass
    def build(self):
        return self.dict
    
    def config_s3(self):
        self.dict.update({
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.s3a.endpoint': 'https://myminio-hl.data-platform-tenant.svc.cluster.local:9000',
            'spark.hadoop.fs.s3a.fast.upload': 'true',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            'spark.hadoop.fs.s3a.access.key': 'minio',
            'spark.hadoop.fs.s3a.secret.key': 'minio123'
        })
    
    def config_spark(self):
        self.dict.update({
            'spark.kubernetes.container.image': 'docker.io/asc686f61/dataplatform_pyspark:v1.2',
            'spark.executorEnv.LD_PRELOAD':'/opt/bitnami/common/lib/libnss_wrapper.so',
            'spark.kubernetes.namespace':'spark-operator',
            'spark.kubernetes.file.upload.path': 's3a://dataplatform/spark',
            'spark.jars.ivy': '/tmp',
            'spark.kubernetes.driver.secrets.kubeconfig':'/secret', \
            'spark.kubernetes.executor.secrets.kubeconfig':'/secret', \
            'spark.kubernetes.authenticate.driver.serviceAccountName':'my-release-spark', \
            'spark.kubernetes.authenticate.executor.serviceAccountName':'my-release-spark', \
            'spark.driver.extraJavaOptions=-Dcom.sun.net.ssl.checkRevocation':'false', \
            'spark.executor.extraJavaOptions=-Dcom.sun.net.ssl.checkRevocation':'false', \
            'spark.ssl.noCertVerification':'true'
        })
        return self
