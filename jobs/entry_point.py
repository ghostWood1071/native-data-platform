from pyspark import SparkContext
from pyspark.sql import SparkSession
from neutronx import PlatformContext, PlatformConfig
import os

spark = SparkSession.builder.appName("entry-point").getOrCreate()

os.putenv("ONPREM_MINIO_ENDPOINT", spark.conf.get("spark.hadoop.fs.s3a.endpoint"))
os.putenv("ONPREM_MINIO_ACCESS_KEY", spark.conf.get("spark.hadoop.fs.s3a.access.key"))
os.putenv("ONPREM_MINIO_SECRET_KEY", spark.conf.get("spark.hadoop.fs.s3a.secret.key"))

print("minio endpoint: ", os.getenv("ONPREM_MINIO_ENDPOINT"))

ctx = PlatformContext(
    spark=spark,
    config=PlatformConfig.from_env(),
)

bucket = "asset"
file_name = "src.zip"
download_path = "spark-jobs"

ctx.download_minio_file(
    bucket=bucket,
    object_name=file_name,
    file_path=download_path,
).unwrap()

sc = spark.sparkContext
sc.addPyFile(file_name)

from src.core import runner

runner.run()
