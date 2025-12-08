--kubectl get pod -n orchestration
--kubectl exec -it <postgres-db> -n orchestration -- psql -U postgres -d airflow -W

        TRUNCATE TABLE etl_dependency RESTART IDENTITY;
        TRUNCATE TABLE etl_task RESTART IDENTITY;
        TRUNCATE TABLE etl_dag RESTART IDENTITY;

       INSERT INTO etl_dag (dag_id, description, schedule_interval, start_date, is_active, tags, default_args)
       VALUES (
                  'source2silver_T_BACK_ADVANCE_WITHDRAW_workflow',
                  'source -> bronze -> silver',
                  '0 18 * * *',
                  '2025-01-01 00:00:00',
                  true,
                  ARRAY['source2bronze', 'bronze2silver'],
                  '{"owner": "cmc_team", "email": ["dqthinh1@cmcglobal.vn"], "retries": 2, "retry_delay": "300"}'::jsonb
              );
    
        INSERT INTO etl_task (dag_id, task_id, conn_id ,task_type, task_params)
        VALUES (
                   'source2silver_T_BACK_ADVANCE_WITHDRAW_workflow',
                   'source2bronze_T_BACK_ADVANCE_WITHDRAW',
                   'spark_k8s',
                   'spark',
                   '{"application": "s3a://asset/spark-jobs/entry_point.py", "name": "source2bronze_T_BACK_ADVANCE_WITHDRAW", "application_args": ["--job_asset_bucket", "asset", "--job_input_path", "job-input/source2bronze_T_BACK_ADVANCE_WITHDRAW.json"], "conf": {"spark.kubernetes.namespace": "compute", "spark.kubernetes.container.image": "ghostwood/mbs-spark:1.0.7-protobuf", "spark.kubernetes.authenticate.driver.serviceAccountName": "spark", "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension", "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog", "spark.sql.catalogImplementation": "hive", "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore.metastore.svc.cluster.local:9083", "spark.sql.warehouse.dir": "s3a://warehouse/", "spark.hadoop.fs.s3a.endpoint": "http://minio.storage.svc.cluster.local:9000", "spark.hadoop.fs.s3a.access.key": "minioadmin", "spark.hadoop.fs.s3a.secret.key": "minio@demo!", "spark.hadoop.fs.s3a.path.style.access": "true", "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem", "spark.sql.sources.partitionOverwriteMode": "dynamic", "spark.eventLog.enabled": true, "spark.eventLog.dir": "s3a://spark-logs/events", "spark.driver.extraJavaOptions": "-Divy.cache.dir=/tmp -Divy.home=/tmp", "app.job_asset_bucket": "asset", "app.job_input_path": "job-input/source2bronze_T_BACK_ADVANCE_WITHDRAW.json"}}'::jsonb
               );
        
        INSERT INTO etl_task (dag_id, task_id, conn_id ,task_type, task_params)
        VALUES (
                   'source2silver_T_BACK_ADVANCE_WITHDRAW_workflow',
                   'bronze2silver_T_BACK_ADVANCE_WITHDRAW',
                   'spark_k8s',
                   'spark',
                   '{"application": "s3a://asset/spark-jobs/entry_point.py", "name": "bronze2silver_T_BACK_ADVANCE_WITHDRAW", "application_args": ["--job_asset_bucket", "asset", "--job_input_path", "job-input/bronze2silver_T_BACK_ADVANCE_WITHDRAW.json"], "conf": {"spark.kubernetes.namespace": "compute", "spark.kubernetes.container.image": "ghostwood/mbs-spark:1.0.7-protobuf", "spark.kubernetes.authenticate.driver.serviceAccountName": "spark", "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension", "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog", "spark.sql.catalogImplementation": "hive", "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore.metastore.svc.cluster.local:9083", "spark.sql.warehouse.dir": "s3a://warehouse/", "spark.hadoop.fs.s3a.endpoint": "http://minio.storage.svc.cluster.local:9000", "spark.hadoop.fs.s3a.access.key": "minioadmin", "spark.hadoop.fs.s3a.secret.key": "minio@demo!", "spark.hadoop.fs.s3a.path.style.access": "true", "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem", "spark.sql.sources.partitionOverwriteMode": "dynamic", "spark.eventLog.enabled": true, "spark.eventLog.dir": "s3a://spark-logs/events", "spark.driver.extraJavaOptions": "-Divy.cache.dir=/tmp -Divy.home=/tmp", "app.job_asset_bucket": "asset", "app.job_input_path": "job-input/bronze2silver_T_BACK_ADVANCE_WITHDRAW.json"}}'::jsonb
               );
        
        INSERT INTO etl_dependency (dag_id, upstream_task_id, downstream_task_id)
        VALUES
            ('source2silver_T_BACK_ADVANCE_WITHDRAW_workflow', 'source2bronze_T_BACK_ADVANCE_WITHDRAW', 'bronze2silver_T_BACK_ADVANCE_WITHDRAW');
    
       INSERT INTO etl_dag (dag_id, description, schedule_interval, start_date, is_active, tags, default_args)
       VALUES (
                  'source2silver_T_BACK_DEAL_HISTORY_workflow',
                  'source -> bronze -> silver',
                  '0 18 * * *',
                  '2025-01-01 00:00:00',
                  true,
                  ARRAY['source2bronze', 'bronze2silver'],
                  '{"owner": "cmc_team", "email": ["dqthinh1@cmcglobal.vn"], "retries": 2, "retry_delay": "300"}'::jsonb
              );
    
        INSERT INTO etl_task (dag_id, task_id, conn_id ,task_type, task_params)
        VALUES (
                   'source2silver_T_BACK_DEAL_HISTORY_workflow',
                   'source2bronze_T_BACK_DEAL_HISTORY',
                   'spark_k8s',
                   'spark',
                   '{"application": "s3a://asset/spark-jobs/entry_point.py", "name": "source2bronze_T_BACK_DEAL_HISTORY", "application_args": ["--job_asset_bucket", "asset", "--job_input_path", "job-input/source2bronze_T_BACK_DEAL_HISTORY.json"], "conf": {"spark.kubernetes.namespace": "compute", "spark.kubernetes.container.image": "ghostwood/mbs-spark:1.0.7-protobuf", "spark.kubernetes.authenticate.driver.serviceAccountName": "spark", "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension", "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog", "spark.sql.catalogImplementation": "hive", "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore.metastore.svc.cluster.local:9083", "spark.sql.warehouse.dir": "s3a://warehouse/", "spark.hadoop.fs.s3a.endpoint": "http://minio.storage.svc.cluster.local:9000", "spark.hadoop.fs.s3a.access.key": "minioadmin", "spark.hadoop.fs.s3a.secret.key": "minio@demo!", "spark.hadoop.fs.s3a.path.style.access": "true", "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem", "spark.sql.sources.partitionOverwriteMode": "dynamic", "spark.eventLog.enabled": true, "spark.eventLog.dir": "s3a://spark-logs/events", "spark.driver.extraJavaOptions": "-Divy.cache.dir=/tmp -Divy.home=/tmp", "app.job_asset_bucket": "asset", "app.job_input_path": "job-input/source2bronze_T_BACK_DEAL_HISTORY.json", "spark.executor.instances": "2", "spark.executor.cores": "8", "spark.executor.memory": "8g"}}'::jsonb
               );
        
        INSERT INTO etl_task (dag_id, task_id, conn_id ,task_type, task_params)
        VALUES (
                   'source2silver_T_BACK_DEAL_HISTORY_workflow',
                   'bronze2silver_T_BACK_DEAL_HISTORY',
                   'spark_k8s',
                   'spark',
                   '{"application": "s3a://asset/spark-jobs/entry_point.py", "name": "bronze2silver_T_BACK_DEAL_HISTORY", "application_args": ["--job_asset_bucket", "asset", "--job_input_path", "job-input/bronze2silver_T_BACK_DEAL_HISTORY.json"], "conf": {"spark.kubernetes.namespace": "compute", "spark.kubernetes.container.image": "ghostwood/mbs-spark:1.0.7-protobuf", "spark.kubernetes.authenticate.driver.serviceAccountName": "spark", "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension", "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog", "spark.sql.catalogImplementation": "hive", "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore.metastore.svc.cluster.local:9083", "spark.sql.warehouse.dir": "s3a://warehouse/", "spark.hadoop.fs.s3a.endpoint": "http://minio.storage.svc.cluster.local:9000", "spark.hadoop.fs.s3a.access.key": "minioadmin", "spark.hadoop.fs.s3a.secret.key": "minio@demo!", "spark.hadoop.fs.s3a.path.style.access": "true", "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem", "spark.sql.sources.partitionOverwriteMode": "dynamic", "spark.eventLog.enabled": true, "spark.eventLog.dir": "s3a://spark-logs/events", "spark.driver.extraJavaOptions": "-Divy.cache.dir=/tmp -Divy.home=/tmp", "app.job_asset_bucket": "asset", "app.job_input_path": "job-input/bronze2silver_T_BACK_DEAL_HISTORY.json"}}'::jsonb
               );
        
        INSERT INTO etl_dependency (dag_id, upstream_task_id, downstream_task_id)
        VALUES
            ('source2silver_T_BACK_DEAL_HISTORY_workflow', 'source2bronze_T_BACK_DEAL_HISTORY', 'bronze2silver_T_BACK_DEAL_HISTORY');
    
       INSERT INTO etl_dag (dag_id, description, schedule_interval, start_date, is_active, tags, default_args)
       VALUES (
                  'source2silver_T_LIST_BRANCH_BANK_ADV_WDR_workflow',
                  'source -> bronze -> silver',
                  NULL,
                  '2025-01-01 00:00:00',
                  true,
                  ARRAY['source2bronze', 'bronze2silver'],
                  '{"owner": "cmc_team", "email": ["dqthinh1@cmcglobal.vn"], "retries": 2, "retry_delay": "300"}'::jsonb
              );
    
        INSERT INTO etl_task (dag_id, task_id, conn_id ,task_type, task_params)
        VALUES (
                   'source2silver_T_LIST_BRANCH_BANK_ADV_WDR_workflow',
                   'source2bronze_T_LIST_BRANCH_BANK_ADV_WDR',
                   'spark_k8s',
                   'spark',
                   '{"application": "s3a://asset/spark-jobs/entry_point.py", "name": "source2bronze_T_LIST_BRANCH_BANK_ADV_WDR", "application_args": ["--job_asset_bucket", "asset", "--job_input_path", "job-input/source2bronze_T_LIST_BRANCH_BANK_ADV_WDR.json"], "conf": {"spark.kubernetes.namespace": "compute", "spark.kubernetes.container.image": "ghostwood/mbs-spark:1.0.7-protobuf", "spark.kubernetes.authenticate.driver.serviceAccountName": "spark", "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension", "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog", "spark.sql.catalogImplementation": "hive", "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore.metastore.svc.cluster.local:9083", "spark.sql.warehouse.dir": "s3a://warehouse/", "spark.hadoop.fs.s3a.endpoint": "http://minio.storage.svc.cluster.local:9000", "spark.hadoop.fs.s3a.access.key": "minioadmin", "spark.hadoop.fs.s3a.secret.key": "minio@demo!", "spark.hadoop.fs.s3a.path.style.access": "true", "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem", "spark.sql.sources.partitionOverwriteMode": "dynamic", "spark.eventLog.enabled": true, "spark.eventLog.dir": "s3a://spark-logs/events", "spark.driver.extraJavaOptions": "-Divy.cache.dir=/tmp -Divy.home=/tmp", "app.job_asset_bucket": "asset", "app.job_input_path": "job-input/source2bronze_T_LIST_BRANCH_BANK_ADV_WDR.json"}}'::jsonb
               );
        
        INSERT INTO etl_task (dag_id, task_id, conn_id ,task_type, task_params)
        VALUES (
                   'source2silver_T_LIST_BRANCH_BANK_ADV_WDR_workflow',
                   'bronze2silver_T_LIST_BRANCH_BANK_ADV_WDR',
                   'spark_k8s',
                   'spark',
                   '{"application": "s3a://asset/spark-jobs/entry_point.py", "name": "bronze2silver_T_LIST_BRANCH_BANK_ADV_WDR", "application_args": ["--job_asset_bucket", "asset", "--job_input_path", "job-input/bronze2silver_T_LIST_BRANCH_BANK_ADV_WDR.json"], "conf": {"spark.kubernetes.namespace": "compute", "spark.kubernetes.container.image": "ghostwood/mbs-spark:1.0.7-protobuf", "spark.kubernetes.authenticate.driver.serviceAccountName": "spark", "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension", "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog", "spark.sql.catalogImplementation": "hive", "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore.metastore.svc.cluster.local:9083", "spark.sql.warehouse.dir": "s3a://warehouse/", "spark.hadoop.fs.s3a.endpoint": "http://minio.storage.svc.cluster.local:9000", "spark.hadoop.fs.s3a.access.key": "minioadmin", "spark.hadoop.fs.s3a.secret.key": "minio@demo!", "spark.hadoop.fs.s3a.path.style.access": "true", "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem", "spark.sql.sources.partitionOverwriteMode": "dynamic", "spark.eventLog.enabled": true, "spark.eventLog.dir": "s3a://spark-logs/events", "spark.driver.extraJavaOptions": "-Divy.cache.dir=/tmp -Divy.home=/tmp", "app.job_asset_bucket": "asset", "app.job_input_path": "job-input/bronze2silver_T_LIST_BRANCH_BANK_ADV_WDR.json"}}'::jsonb
               );
        
        INSERT INTO etl_dependency (dag_id, upstream_task_id, downstream_task_id)
        VALUES
            ('source2silver_T_LIST_BRANCH_BANK_ADV_WDR_workflow', 'source2bronze_T_LIST_BRANCH_BANK_ADV_WDR', 'bronze2silver_T_LIST_BRANCH_BANK_ADV_WDR');
    
       INSERT INTO etl_dag (dag_id, description, schedule_interval, start_date, is_active, tags, default_args)
       VALUES (
                  'source2silver_T_MARGIN_EXTRA_BALANCE_HIS_workflow',
                  'source -> bronze -> silver',
                  '0 18 * * *',
                  '2025-01-01 00:00:00',
                  true,
                  ARRAY['source2bronze', 'bronze2silver'],
                  '{"owner": "cmc_team", "email": ["dqthinh1@cmcglobal.vn"], "retries": 2, "retry_delay": "300"}'::jsonb
              );
    
        INSERT INTO etl_task (dag_id, task_id, conn_id ,task_type, task_params)
        VALUES (
                   'source2silver_T_MARGIN_EXTRA_BALANCE_HIS_workflow',
                   'source2bronze_T_MARGIN_EXTRA_BALANCE_HIS',
                   'spark_k8s',
                   'spark',
                   '{"application": "s3a://asset/spark-jobs/entry_point.py", "name": "source2bronze_T_MARGIN_EXTRA_BALANCE_HIS", "application_args": ["--job_asset_bucket", "asset", "--job_input_path", "job-input/source2bronze_T_MARGIN_EXTRA_BALANCE_HIS.json"], "conf": {"spark.kubernetes.namespace": "compute", "spark.kubernetes.container.image": "ghostwood/mbs-spark:1.0.7-protobuf", "spark.kubernetes.authenticate.driver.serviceAccountName": "spark", "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension", "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog", "spark.sql.catalogImplementation": "hive", "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore.metastore.svc.cluster.local:9083", "spark.sql.warehouse.dir": "s3a://warehouse/", "spark.hadoop.fs.s3a.endpoint": "http://minio.storage.svc.cluster.local:9000", "spark.hadoop.fs.s3a.access.key": "minioadmin", "spark.hadoop.fs.s3a.secret.key": "minio@demo!", "spark.hadoop.fs.s3a.path.style.access": "true", "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem", "spark.sql.sources.partitionOverwriteMode": "dynamic", "spark.eventLog.enabled": true, "spark.eventLog.dir": "s3a://spark-logs/events", "spark.driver.extraJavaOptions": "-Divy.cache.dir=/tmp -Divy.home=/tmp", "app.job_asset_bucket": "asset", "app.job_input_path": "job-input/source2bronze_T_MARGIN_EXTRA_BALANCE_HIS.json"}}'::jsonb
               );
        
        INSERT INTO etl_task (dag_id, task_id, conn_id ,task_type, task_params)
        VALUES (
                   'source2silver_T_MARGIN_EXTRA_BALANCE_HIS_workflow',
                   'bronze2silver_T_MARGIN_EXTRA_BALANCE_HIS',
                   'spark_k8s',
                   'spark',
                   '{"application": "s3a://asset/spark-jobs/entry_point.py", "name": "bronze2silver_T_MARGIN_EXTRA_BALANCE_HIS", "application_args": ["--job_asset_bucket", "asset", "--job_input_path", "job-input/bronze2silver_T_MARGIN_EXTRA_BALANCE_HIS.json"], "conf": {"spark.kubernetes.namespace": "compute", "spark.kubernetes.container.image": "ghostwood/mbs-spark:1.0.7-protobuf", "spark.kubernetes.authenticate.driver.serviceAccountName": "spark", "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension", "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog", "spark.sql.catalogImplementation": "hive", "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore.metastore.svc.cluster.local:9083", "spark.sql.warehouse.dir": "s3a://warehouse/", "spark.hadoop.fs.s3a.endpoint": "http://minio.storage.svc.cluster.local:9000", "spark.hadoop.fs.s3a.access.key": "minioadmin", "spark.hadoop.fs.s3a.secret.key": "minio@demo!", "spark.hadoop.fs.s3a.path.style.access": "true", "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem", "spark.sql.sources.partitionOverwriteMode": "dynamic", "spark.eventLog.enabled": true, "spark.eventLog.dir": "s3a://spark-logs/events", "spark.driver.extraJavaOptions": "-Divy.cache.dir=/tmp -Divy.home=/tmp", "app.job_asset_bucket": "asset", "app.job_input_path": "job-input/bronze2silver_T_MARGIN_EXTRA_BALANCE_HIS.json"}}'::jsonb
               );
        
        INSERT INTO etl_dependency (dag_id, upstream_task_id, downstream_task_id)
        VALUES
            ('source2silver_T_MARGIN_EXTRA_BALANCE_HIS_workflow', 'source2bronze_T_MARGIN_EXTRA_BALANCE_HIS', 'bronze2silver_T_MARGIN_EXTRA_BALANCE_HIS');
    
       INSERT INTO etl_dag (dag_id, description, schedule_interval, start_date, is_active, tags, default_args)
       VALUES (
                  'source2silver_T_TLO_DEBIT_BALANCE_HISTORY_workflow',
                  'source -> bronze -> silver',
                  '0 18 * * *',
                  '2025-01-01 00:00:00',
                  true,
                  ARRAY['source2bronze', 'bronze2silver'],
                  '{"owner": "cmc_team", "email": ["dqthinh1@cmcglobal.vn"], "retries": 2, "retry_delay": "300"}'::jsonb
              );
    
        INSERT INTO etl_task (dag_id, task_id, conn_id ,task_type, task_params)
        VALUES (
                   'source2silver_T_TLO_DEBIT_BALANCE_HISTORY_workflow',
                   'source2bronze_T_TLO_DEBIT_BALANCE_HISTORY',
                   'spark_k8s',
                   'spark',
                   '{"application": "s3a://asset/spark-jobs/entry_point.py", "name": "source2bronze_T_TLO_DEBIT_BALANCE_HISTORY", "application_args": ["--job_asset_bucket", "asset", "--job_input_path", "job-input/source2bronze_T_TLO_DEBIT_BALANCE_HISTORY.json"], "conf": {"spark.kubernetes.namespace": "compute", "spark.kubernetes.container.image": "ghostwood/mbs-spark:1.0.7-protobuf", "spark.kubernetes.authenticate.driver.serviceAccountName": "spark", "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension", "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog", "spark.sql.catalogImplementation": "hive", "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore.metastore.svc.cluster.local:9083", "spark.sql.warehouse.dir": "s3a://warehouse/", "spark.hadoop.fs.s3a.endpoint": "http://minio.storage.svc.cluster.local:9000", "spark.hadoop.fs.s3a.access.key": "minioadmin", "spark.hadoop.fs.s3a.secret.key": "minio@demo!", "spark.hadoop.fs.s3a.path.style.access": "true", "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem", "spark.sql.sources.partitionOverwriteMode": "dynamic", "spark.eventLog.enabled": true, "spark.eventLog.dir": "s3a://spark-logs/events", "spark.driver.extraJavaOptions": "-Divy.cache.dir=/tmp -Divy.home=/tmp", "app.job_asset_bucket": "asset", "app.job_input_path": "job-input/source2bronze_T_TLO_DEBIT_BALANCE_HISTORY.json"}}'::jsonb
               );
        
        INSERT INTO etl_task (dag_id, task_id, conn_id ,task_type, task_params)
        VALUES (
                   'source2silver_T_TLO_DEBIT_BALANCE_HISTORY_workflow',
                   'bronze2silver_T_TLO_DEBIT_BALANCE_HISTORY',
                   'spark_k8s',
                   'spark',
                   '{"application": "s3a://asset/spark-jobs/entry_point.py", "name": "bronze2silver_T_TLO_DEBIT_BALANCE_HISTORY", "application_args": ["--job_asset_bucket", "asset", "--job_input_path", "job-input/bronze2silver_T_TLO_DEBIT_BALANCE_HISTORY.json"], "conf": {"spark.kubernetes.namespace": "compute", "spark.kubernetes.container.image": "ghostwood/mbs-spark:1.0.7-protobuf", "spark.kubernetes.authenticate.driver.serviceAccountName": "spark", "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension", "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog", "spark.sql.catalogImplementation": "hive", "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore.metastore.svc.cluster.local:9083", "spark.sql.warehouse.dir": "s3a://warehouse/", "spark.hadoop.fs.s3a.endpoint": "http://minio.storage.svc.cluster.local:9000", "spark.hadoop.fs.s3a.access.key": "minioadmin", "spark.hadoop.fs.s3a.secret.key": "minio@demo!", "spark.hadoop.fs.s3a.path.style.access": "true", "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem", "spark.sql.sources.partitionOverwriteMode": "dynamic", "spark.eventLog.enabled": true, "spark.eventLog.dir": "s3a://spark-logs/events", "spark.driver.extraJavaOptions": "-Divy.cache.dir=/tmp -Divy.home=/tmp", "app.job_asset_bucket": "asset", "app.job_input_path": "job-input/bronze2silver_T_TLO_DEBIT_BALANCE_HISTORY.json"}}'::jsonb
               );
        
        INSERT INTO etl_dependency (dag_id, upstream_task_id, downstream_task_id)
        VALUES
            ('source2silver_T_TLO_DEBIT_BALANCE_HISTORY_workflow', 'source2bronze_T_TLO_DEBIT_BALANCE_HISTORY', 'bronze2silver_T_TLO_DEBIT_BALANCE_HISTORY');
    
       INSERT INTO etl_dag (dag_id, description, schedule_interval, start_date, is_active, tags, default_args)
       VALUES (
                  'source2silver_V_T_BACK_ACCOUNT_workflow',
                  'source -> bronze -> silver',
                  '0 18 * * *',
                  '2025-01-01 00:00:00',
                  true,
                  ARRAY['source2bronze', 'bronze2silver'],
                  '{"owner": "cmc_team", "email": ["dqthinh1@cmcglobal.vn"], "retries": 2, "retry_delay": "300"}'::jsonb
              );
    
        INSERT INTO etl_task (dag_id, task_id, conn_id ,task_type, task_params)
        VALUES (
                   'source2silver_V_T_BACK_ACCOUNT_workflow',
                   'source2bronze_V_T_BACK_ACCOUNT',
                   'spark_k8s',
                   'spark',
                   '{"application": "s3a://asset/spark-jobs/entry_point.py", "name": "source2bronze_V_T_BACK_ACCOUNT", "application_args": ["--job_asset_bucket", "asset", "--job_input_path", "job-input/source2bronze_V_T_BACK_ACCOUNT.json"], "conf": {"spark.kubernetes.namespace": "compute", "spark.kubernetes.container.image": "ghostwood/mbs-spark:1.0.7-protobuf", "spark.kubernetes.authenticate.driver.serviceAccountName": "spark", "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension", "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog", "spark.sql.catalogImplementation": "hive", "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore.metastore.svc.cluster.local:9083", "spark.sql.warehouse.dir": "s3a://warehouse/", "spark.hadoop.fs.s3a.endpoint": "http://minio.storage.svc.cluster.local:9000", "spark.hadoop.fs.s3a.access.key": "minioadmin", "spark.hadoop.fs.s3a.secret.key": "minio@demo!", "spark.hadoop.fs.s3a.path.style.access": "true", "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem", "spark.sql.sources.partitionOverwriteMode": "dynamic", "spark.eventLog.enabled": true, "spark.eventLog.dir": "s3a://spark-logs/events", "spark.driver.extraJavaOptions": "-Divy.cache.dir=/tmp -Divy.home=/tmp", "app.job_asset_bucket": "asset", "app.job_input_path": "job-input/source2bronze_V_T_BACK_ACCOUNT.json"}}'::jsonb
               );
        
        INSERT INTO etl_task (dag_id, task_id, conn_id ,task_type, task_params)
        VALUES (
                   'source2silver_V_T_BACK_ACCOUNT_workflow',
                   'bronze2silver_V_T_BACK_ACCOUNT',
                   'spark_k8s',
                   'spark',
                   '{"application": "s3a://asset/spark-jobs/entry_point.py", "name": "bronze2silver_V_T_BACK_ACCOUNT", "application_args": ["--job_asset_bucket", "asset", "--job_input_path", "job-input/bronze2silver_V_T_BACK_ACCOUNT.json"], "conf": {"spark.kubernetes.namespace": "compute", "spark.kubernetes.container.image": "ghostwood/mbs-spark:1.0.7-protobuf", "spark.kubernetes.authenticate.driver.serviceAccountName": "spark", "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension", "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog", "spark.sql.catalogImplementation": "hive", "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore.metastore.svc.cluster.local:9083", "spark.sql.warehouse.dir": "s3a://warehouse/", "spark.hadoop.fs.s3a.endpoint": "http://minio.storage.svc.cluster.local:9000", "spark.hadoop.fs.s3a.access.key": "minioadmin", "spark.hadoop.fs.s3a.secret.key": "minio@demo!", "spark.hadoop.fs.s3a.path.style.access": "true", "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem", "spark.sql.sources.partitionOverwriteMode": "dynamic", "spark.eventLog.enabled": true, "spark.eventLog.dir": "s3a://spark-logs/events", "spark.driver.extraJavaOptions": "-Divy.cache.dir=/tmp -Divy.home=/tmp", "app.job_asset_bucket": "asset", "app.job_input_path": "job-input/bronze2silver_V_T_BACK_ACCOUNT.json"}}'::jsonb
               );
        
        INSERT INTO etl_dependency (dag_id, upstream_task_id, downstream_task_id)
        VALUES
            ('source2silver_V_T_BACK_ACCOUNT_workflow', 'source2bronze_V_T_BACK_ACCOUNT', 'bronze2silver_V_T_BACK_ACCOUNT');
    
       INSERT INTO etl_dag (dag_id, description, schedule_interval, start_date, is_active, tags, default_args)
       VALUES (
                  'source2silver_V_T_ERC_MONTHLY_DETAIL_workflow',
                  'source -> bronze -> silver',
                  '0 18 L * *',
                  '2025-01-01 00:00:00',
                  true,
                  ARRAY['source2bronze', 'bronze2silver'],
                  '{"owner": "cmc_team", "email": ["dqthinh1@cmcglobal.vn"], "retries": 2, "retry_delay": "300"}'::jsonb
              );
    
        INSERT INTO etl_task (dag_id, task_id, conn_id ,task_type, task_params)
        VALUES (
                   'source2silver_V_T_ERC_MONTHLY_DETAIL_workflow',
                   'source2bronze_V_T_ERC_MONTHLY_DETAIL',
                   'spark_k8s',
                   'spark',
                   '{"application": "s3a://asset/spark-jobs/entry_point.py", "name": "source2bronze_V_T_ERC_MONTHLY_DETAIL", "application_args": ["--job_asset_bucket", "asset", "--job_input_path", "job-input/source2bronze_V_T_ERC_MONTHLY_DETAIL.json"], "conf": {"spark.kubernetes.namespace": "compute", "spark.kubernetes.container.image": "ghostwood/mbs-spark:1.0.7-protobuf", "spark.kubernetes.authenticate.driver.serviceAccountName": "spark", "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension", "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog", "spark.sql.catalogImplementation": "hive", "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore.metastore.svc.cluster.local:9083", "spark.sql.warehouse.dir": "s3a://warehouse/", "spark.hadoop.fs.s3a.endpoint": "http://minio.storage.svc.cluster.local:9000", "spark.hadoop.fs.s3a.access.key": "minioadmin", "spark.hadoop.fs.s3a.secret.key": "minio@demo!", "spark.hadoop.fs.s3a.path.style.access": "true", "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem", "spark.sql.sources.partitionOverwriteMode": "dynamic", "spark.eventLog.enabled": true, "spark.eventLog.dir": "s3a://spark-logs/events", "spark.driver.extraJavaOptions": "-Divy.cache.dir=/tmp -Divy.home=/tmp", "app.job_asset_bucket": "asset", "app.job_input_path": "job-input/source2bronze_V_T_ERC_MONTHLY_DETAIL.json"}}'::jsonb
               );
        
        INSERT INTO etl_task (dag_id, task_id, conn_id ,task_type, task_params)
        VALUES (
                   'source2silver_V_T_ERC_MONTHLY_DETAIL_workflow',
                   'bronze2silver_V_T_ERC_MONTHLY_DETAIL',
                   'spark_k8s',
                   'spark',
                   '{"application": "s3a://asset/spark-jobs/entry_point.py", "name": "bronze2silver_V_T_ERC_MONTHLY_DETAIL", "application_args": ["--job_asset_bucket", "asset", "--job_input_path", "job-input/bronze2silver_V_T_ERC_MONTHLY_DETAIL.json"], "conf": {"spark.kubernetes.namespace": "compute", "spark.kubernetes.container.image": "ghostwood/mbs-spark:1.0.7-protobuf", "spark.kubernetes.authenticate.driver.serviceAccountName": "spark", "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension", "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog", "spark.sql.catalogImplementation": "hive", "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore.metastore.svc.cluster.local:9083", "spark.sql.warehouse.dir": "s3a://warehouse/", "spark.hadoop.fs.s3a.endpoint": "http://minio.storage.svc.cluster.local:9000", "spark.hadoop.fs.s3a.access.key": "minioadmin", "spark.hadoop.fs.s3a.secret.key": "minio@demo!", "spark.hadoop.fs.s3a.path.style.access": "true", "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem", "spark.sql.sources.partitionOverwriteMode": "dynamic", "spark.eventLog.enabled": true, "spark.eventLog.dir": "s3a://spark-logs/events", "spark.driver.extraJavaOptions": "-Divy.cache.dir=/tmp -Divy.home=/tmp", "app.job_asset_bucket": "asset", "app.job_input_path": "job-input/bronze2silver_V_T_ERC_MONTHLY_DETAIL.json"}}'::jsonb
               );
        
        INSERT INTO etl_dependency (dag_id, upstream_task_id, downstream_task_id)
        VALUES
            ('source2silver_V_T_ERC_MONTHLY_DETAIL_workflow', 'source2bronze_V_T_ERC_MONTHLY_DETAIL', 'bronze2silver_V_T_ERC_MONTHLY_DETAIL');
    
       INSERT INTO etl_dag (dag_id, description, schedule_interval, start_date, is_active, tags, default_args)
       VALUES (
                  'source2silver_V_T_LIST_FRONT_USER_workflow',
                  'source -> bronze -> silver',
                  NULL,
                  '2025-01-01 00:00:00',
                  true,
                  ARRAY['source2bronze', 'bronze2silver'],
                  '{"owner": "cmc_team", "email": ["dqthinh1@cmcglobal.vn"], "retries": 2, "retry_delay": "300"}'::jsonb
              );
    
        INSERT INTO etl_task (dag_id, task_id, conn_id ,task_type, task_params)
        VALUES (
                   'source2silver_V_T_LIST_FRONT_USER_workflow',
                   'source2bronze_V_T_LIST_FRONT_USER',
                   'spark_k8s',
                   'spark',
                   '{"application": "s3a://asset/spark-jobs/entry_point.py", "name": "source2bronze_V_T_LIST_FRONT_USER", "application_args": ["--job_asset_bucket", "asset", "--job_input_path", "job-input/source2bronze_V_T_LIST_FRONT_USER.json"], "conf": {"spark.kubernetes.namespace": "compute", "spark.kubernetes.container.image": "ghostwood/mbs-spark:1.0.7-protobuf", "spark.kubernetes.authenticate.driver.serviceAccountName": "spark", "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension", "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog", "spark.sql.catalogImplementation": "hive", "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore.metastore.svc.cluster.local:9083", "spark.sql.warehouse.dir": "s3a://warehouse/", "spark.hadoop.fs.s3a.endpoint": "http://minio.storage.svc.cluster.local:9000", "spark.hadoop.fs.s3a.access.key": "minioadmin", "spark.hadoop.fs.s3a.secret.key": "minio@demo!", "spark.hadoop.fs.s3a.path.style.access": "true", "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem", "spark.sql.sources.partitionOverwriteMode": "dynamic", "spark.eventLog.enabled": true, "spark.eventLog.dir": "s3a://spark-logs/events", "spark.driver.extraJavaOptions": "-Divy.cache.dir=/tmp -Divy.home=/tmp", "app.job_asset_bucket": "asset", "app.job_input_path": "job-input/source2bronze_V_T_LIST_FRONT_USER.json"}}'::jsonb
               );
        
        INSERT INTO etl_task (dag_id, task_id, conn_id ,task_type, task_params)
        VALUES (
                   'source2silver_V_T_LIST_FRONT_USER_workflow',
                   'bronze2silver_V_T_LIST_FRONT_USER',
                   'spark_k8s',
                   'spark',
                   '{"application": "s3a://asset/spark-jobs/entry_point.py", "name": "bronze2silver_V_T_LIST_FRONT_USER", "application_args": ["--job_asset_bucket", "asset", "--job_input_path", "job-input/bronze2silver_V_T_LIST_FRONT_USER.json"], "conf": {"spark.kubernetes.namespace": "compute", "spark.kubernetes.container.image": "ghostwood/mbs-spark:1.0.7-protobuf", "spark.kubernetes.authenticate.driver.serviceAccountName": "spark", "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension", "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog", "spark.sql.catalogImplementation": "hive", "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore.metastore.svc.cluster.local:9083", "spark.sql.warehouse.dir": "s3a://warehouse/", "spark.hadoop.fs.s3a.endpoint": "http://minio.storage.svc.cluster.local:9000", "spark.hadoop.fs.s3a.access.key": "minioadmin", "spark.hadoop.fs.s3a.secret.key": "minio@demo!", "spark.hadoop.fs.s3a.path.style.access": "true", "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem", "spark.sql.sources.partitionOverwriteMode": "dynamic", "spark.eventLog.enabled": true, "spark.eventLog.dir": "s3a://spark-logs/events", "spark.driver.extraJavaOptions": "-Divy.cache.dir=/tmp -Divy.home=/tmp", "app.job_asset_bucket": "asset", "app.job_input_path": "job-input/bronze2silver_V_T_LIST_FRONT_USER.json"}}'::jsonb
               );
        
        INSERT INTO etl_dependency (dag_id, upstream_task_id, downstream_task_id)
        VALUES
            ('source2silver_V_T_LIST_FRONT_USER_workflow', 'source2bronze_V_T_LIST_FRONT_USER', 'bronze2silver_V_T_LIST_FRONT_USER');
    
       INSERT INTO etl_dag (dag_id, description, schedule_interval, start_date, is_active, tags, default_args)
       VALUES (
                  'source2silver_t_margin_extra_balance_his_workflow',
                  'source -> bronze -> silver',
                  '0 2 * * *',
                  '2025-01-01 00:00:00',
                  true,
                  ARRAY['source2bronze', 'source2silver'],
                  '{"owner": "cmc_team", "email": ["dqthinh1@cmcglobal.vn"], "retries": 0, "retry_delay": "0"}'::jsonb
              );
    
        INSERT INTO etl_task (dag_id, task_id, conn_id ,task_type, task_params)
        VALUES (
                   'source2silver_t_margin_extra_balance_his_workflow',
                   'source2bronze_t_margin_extra_balance_his',
                   'None',
                   'spark',
                   '{"application": "/opt/airflow/jobs/entry_point.py", "py_files": "/opt/airflow/jobs/src.zip", "conn_id": "spark_conf", "name": "transform_customer", "application_args": ["--job_asset_bucket", "asset", "--job_input_path", "job-input/source2bronze_t_margin_extra_balance_his.json"], "conf": {"spark.databricks.delta.schema.autoMerge.enabled": "true", "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension", "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog", "spark.sql.catalogImplementation": "hive", "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore:9083", "spark.sql.warehouse.dir": "s3a://warehouse/delta/", "spark.hadoop.fs.s3a.endpoint": "http://minio:9000", "spark.hadoop.fs.s3a.access.key": "minio", "spark.hadoop.fs.s3a.secret.key": "minio@123", "spark.hadoop.fs.s3a.path.style.access": "true", "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem", "spark.sql.sources.partitionOverwriteMode": "dynamic", "spark.eventLog.enabled": true, "spark.eventLog.dir": "s3a://spark-logs/events", "spark.driver.extraJavaOptions": "-Divy.cache.dir=/tmp -Divy.home=/tmp"}}'::jsonb
               );
        
        INSERT INTO etl_task (dag_id, task_id, conn_id ,task_type, task_params)
        VALUES (
                   'source2silver_t_margin_extra_balance_his_workflow',
                   'bronze2silver_t_margin_extra_balance_his',
                   'spark_conf',
                   'spark',
                   '{"application": "/opt/airflow/jobs/entry_point.py", "name": "transform_customer", "py_files": "/opt/airflow/jobs/src.zip", "application_args": ["--job_asset_bucket", "asset", "--job_input_path", "job-input/bronze2silver_t_margin_extra_balance_his.json"], "conf": {"spark.databricks.delta.schema.autoMerge.enabled": "true", "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension", "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog", "spark.sql.catalogImplementation": "hive", "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore:9083", "spark.sql.warehouse.dir": "s3a://warehouse/delta/", "spark.hadoop.fs.s3a.endpoint": "http://minio:9000", "spark.hadoop.fs.s3a.access.key": "minio", "spark.hadoop.fs.s3a.secret.key": "minio@123", "spark.hadoop.fs.s3a.path.style.access": "true", "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem", "spark.sql.sources.partitionOverwriteMode": "dynamic", "spark.eventLog.enabled": true, "spark.eventLog.dir": "s3a://spark-logs/events", "spark.driver.extraJavaOptions": "-Divy.cache.dir=/tmp -Divy.home=/tmp"}}'::jsonb
               );
        
        INSERT INTO etl_dependency (dag_id, upstream_task_id, downstream_task_id)
        VALUES
            ('source2silver_t_margin_extra_balance_his_workflow', 'source2bronze_t_margin_extra_balance_his', 'bronze2silver_t_margin_extra_balance_his');
    