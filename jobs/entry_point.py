from pyspark import SparkContext
from pyspark.sql import SparkSession
from neutronx import PlatformContext, PlatformConfig

spark = SparkSession.builder.appName("entry-point").getOrCreate()

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
