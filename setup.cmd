@echo off
setlocal enabledelayedexpansion

if "%1"=="" (
    exit /b 1
)

set COMPOSE_FILE=config\environment\%1

if not exist %COMPOSE_FILE% (
    echo File %COMPOSE_FILE% not exists
    exit /b 1
)

if not exist infra\jars mkdir infra\jars
curl -L -o infra\jars\aws-java-sdk-bundle-1.12.367.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar
curl -L -o infra\jars\hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
curl -L -o infra\jars\postgresql-42.6.2.jar https://jdbc.postgresql.org/download/postgresql-42.6.2.jar
curl -L -o infra\jars\iceberg-spark-runtime-3.5_2.12-1.5.2.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar

if not exist infra\minio-storage mkdir infra\minio-storage
if not exist infra\pg-metastore-data mkdir infra\pg-metastore-data
docker compose -f %COMPOSE_FILE% build
docker compose -f %COMPOSE_FILE% up -d
