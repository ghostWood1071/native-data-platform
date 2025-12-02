from datetime import datetime, timedelta
from src.core.factory import Factory
from src.core.engine import ETL
from src.core import util

def run():
    run_data_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    spark = Factory.create_spark()
    config_path = util.download_file_from_minio(
        spark.conf.get("spark.hadoop.fs.s3a.endpoint").replace("http://", ""),
        spark.conf.get("spark.hadoop.fs.s3a.access.key"),
        spark.conf.get("spark.hadoop.fs.s3a.secret.key"),
        # spark.conf.get("app.job_asset_bucket"),
        util.get_external_config("job_asset_bucket"),
        util.get_external_config("job_input_path")
        # spark.conf.get("app.job_input_path")
    )
    job_conf = Factory.read_config(config_path)
    run_mode = job_conf.get("run_mode")
    if run_mode.get("name") == "default":
        ETL.execute(spark, job_conf, run_data_date)
    if run_mode.get("name") == "back_fill":
        start_date = datetime.strptime(run_mode.get("start"), "%Y-%m-%d")
        end_date = datetime.strptime(run_mode.get("end"), "%-%m-%d")
        step = run_mode.get("step")
        dates_run = util.gen_date_range(start_date, end_date, step)
        for date_run in dates_run:
            ETL.execute(spark, job_conf, date_run)
