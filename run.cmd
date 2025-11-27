@echo off
if "%1"=="" (
    exit /b 1
)
set CONTAINER_NAME=spark-master
set FILE_NAME=%1

docker exec -u root -it %CONTAINER_NAME% /bin/bash -c "/bin/bash /opt/spark/bin/spark-submit --py-files /opt/spark/job-lib/src.zip /opt/spark/jobs/%FILE_NAME%"

echo Done!
pause
