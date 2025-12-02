from datetime import datetime, timedelta
from minio import Minio
from minio.error import S3Error


def gen_date_range(start: datetime, end: datetime, step: int=None):
    if not step:
        step = 1
    cur_date = start.date()
    end_date = end.date()
    result = []
    delta = timedelta(days=step)
    while cur_date <= end_date:
        result.append(cur_date.strftime("%Y-%m-%d"))
        cur_date += delta
    return result

def read_file(file_path, mode = 'r'):
    with open(file_path, mode = mode) as f:
       content = f.read()
    return content


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


def get_external_config(config_name):
    import sys
    for i in range(len(sys.argv)):
        if sys.argv[i] == "--" + config_name:
            value = sys.argv[i + 1]
            return value
    return None