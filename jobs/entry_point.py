from pyspark import SparkContext
from pyspark.sql import SparkSession
from neutronx import PlatformContext, PlatformConfig
import os
import argparse

spark = SparkSession.builder.appName("entry-point").getOrCreate()

def parse_args():
    parser = argparse.ArgumentParser(
        description="Run Spark data processing job"
    )

    parser.add_argument(
        "--job_asset_bucket",
        type=str,
        required=True,
        help="MinIO bucket chứa job assets",
    )

    parser.add_argument(
        "--job_input_path",
        type=str,
        required=True,
        help="Đường dẫn file JSON cấu hình job",
    )

    parser.add_argument(
        "--minio_endpoint",
        type=str,
        default="dev"
    )

    parser.add_argument(
        "--minio_user",
        type=str,
        default=""
    )

    parser.add_argument(
        "--minio_pwd",
        type=str,
        default=""
    )

    return parser.parse_args()

args = parse_args()
os.environ["ONPREM_MINIO_ENDPOINT"] = str.strip(args.minio_endpoint)
os.environ["ONPREM_MINIO_ACCESS_KEY"] = str.strip(args.minio_user)
os.environ["ONPREM_MINIO_SECRET_KEY"] = str.strip(args.minio_pwd)

print("minio endpoint: ", args.minio_endpoint)

ctx = PlatformContext(
    spark=spark,
    config=PlatformConfig.from_env(),
)

bucket = "asset"
file_name = "src.zip"
download_path = "spark-jobs"

ctx.download_minio_file(
    bucket=bucket,
    object_name=download_path + "/" +file_name,
    file_path=download_path,
).unwrap()

sc = spark.sparkContext
sc.addPyFile(file_name)

from src.core import runner

runner.run()
