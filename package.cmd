@echo off
setlocal enabledelayedexpansion

if exist infra\minio-storage rmdir /s /q infra\minio-storage
if exist infra\jars rmdir /s /q infra\jars
if exist infra\pg-metastore-data rmdir /s /q infra\pg-metastore-data

echo >> Docker build...
docker compose down