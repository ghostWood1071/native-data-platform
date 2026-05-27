# neutronx

`neutronx` là thư viện tiện ích cho PySpark job chạy trên Kubernetes on-premise.

Mục tiêu của thư viện này là cung cấp một API gần giống ý tưởng `GlueContext`, nhưng dành cho data platform tự dựng gồm:

- MinIO / S3-compatible object storage
- Airflow REST API
- Kafka
- MongoDB
- Kubernetes CRD / Operator
- External HTTP API
- OpenMetadata
- HiveServer2 / Hive Metastore access qua Hive SQL
- OpenLineage event emission
- Spark S3A configuration helper

> Thư viện này không thay SparkSession. Nó nằm cạnh SparkSession để Spark job tương tác an toàn với môi trường ngoài pod.

## Cài đặt local để build wheel

```bash
python -m pip install -U build
python -m build
```

Kết quả:

```text
dist/neutronx-0.1.0-py3-none-any.whl
```

## Cài vào Docker image Spark

```dockerfile
FROM ghostwood/mbs-spark:1.0.7-protobuf

USER root
COPY dist/neutronx-0.1.0-py3-none-any.whl /tmp/
RUN pip install --no-cache-dir /tmp/neutronx-0.1.0-py3-none-any.whl

USER 185
```

## Ví dụ dùng trong Spark job

```python
from pyspark.sql import SparkSession
from neutronx import PlatformContext, PlatformConfig

spark = SparkSession.builder.appName("demo-neutronx").getOrCreate()

ctx = PlatformContext(
    spark=spark,
    config=PlatformConfig.from_env(),
)

ctx.configure_s3a().unwrap()

df = ctx.read_minio_parquet("warehouse", "bronze/customer").unwrap()

ctx.download_minio_file(
    bucket="configs",
    object_name="apps/settings.yaml",
    file_path="/tmp/settings.yaml",
).unwrap()

ctx.put_minio_json(
    bucket="logs",
    object_name="spark/demo-job/status.json",
    payload={"status": "STARTED"},
).unwrap()

ctx.emit_kafka_json(
    topic="data-platform-events",
    key="demo-job",
    value={"job": "demo-job", "status": "STARTED"},
).unwrap()
```

## Env vars hỗ trợ

```bash
# MinIO
ONPREM_MINIO_ENDPOINT=minio-svc-private.storage.svc.cluster.local:9000
ONPREM_MINIO_ACCESS_KEY=...
ONPREM_MINIO_SECRET_KEY=...
ONPREM_MINIO_SECURE=false

# Airflow
ONPREM_AIRFLOW_BASE_URL=http://airflow-webserver.orchestration.svc.cluster.local:8080
ONPREM_AIRFLOW_USERNAME=admin
ONPREM_AIRFLOW_PASSWORD=admin
ONPREM_AIRFLOW_API_VERSION=v1

# Kafka
ONPREM_KAFKA_BOOTSTRAP_SERVERS=kafka.streaming.svc.cluster.local:9092

# MongoDB
ONPREM_MONGO_URI=mongodb://user:password@mongodb.default.svc.cluster.local:27017
ONPREM_MONGO_DATABASE=platform

# Kubernetes
ONPREM_K8S_NAMESPACE=compute
ONPREM_K8S_IN_CLUSTER=true

# Hive
ONPREM_HIVE_HOST=hive-server.metastore.svc.cluster.local
ONPREM_HIVE_PORT=10000
ONPREM_HIVE_USERNAME=hive
ONPREM_HIVE_DATABASE=default

# OpenMetadata
ONPREM_OPENMETADATA_BASE_URL=http://openmetadata.openmetadata.svc.cluster.local:8585
ONPREM_OPENMETADATA_TOKEN=...

# OpenLineage
ONPREM_OPENLINEAGE_ENDPOINT=http://marquez.default.svc.cluster.local:5000
ONPREM_OPENLINEAGE_NAMESPACE=onprem-data-platform
```
