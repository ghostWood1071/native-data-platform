from pyspark import SparkContext
from minio import Minio
from minio.error import S3Error
# "spark.hadoop.fs.s3a.endpoint": "http://minio.storage.svc.cluster.local:9000",
# "spark.hadoop.fs.s3a.access.key": "minioadmin",
# "spark.hadoop.fs.s3a.secret.key": "minio@demo!",
def download_file_from_minio(minio_endpoint, access_key, secret_key, bucket, file_name):
    client = Minio(
        endpoint=minio_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False
    )
    download_path = file_name.split("/")[-1]
    try:
        client.fget_object(bucket, file_name, download_path)
        return download_path
    except S3Error as e:
        raise e

lib_path = download_file_from_minio(
    "minio.storage.svc.cluster.local:9000",
    "minioadmin",
    "minio@demo!",
    "asset",
    "spark-jobs/src.zip"
)

sc = SparkContext.getOrCreate()
sc.addPyFile(lib_path)
from src.core import runner
runner.run()
