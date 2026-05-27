pipeline {
    agent any

    environment {
        MINIO_ENDPOINT = 'minio-svc-private.storage.svc.cluster.local:9000'
        MINIO_BUCKET = 'asset'
        DB_HOST = 'postgres-airflow.orchestration.svc.cluster.local'
        DB_NAME = 'airflow'
    }

    stages {
        stage('Zipping source') {
            steps {
                echo 'Zipping src folder...'
                sh 'zip -r src.zip src'
            }
        }

        stage('Upload to MinIO') {
            steps {
                echo 'Uploading files to MinIO...'
                withCredentials([
                    usernamePassword(
                        credentialsId: 'minio-creds',
                        usernameVariable: 'MINIO_ACCESS_KEY',
                        passwordVariable: 'MINIO_SECRET_KEY'
                    )
                ]) {
                    // Upload src.zip to spark-jobs/
                    sh 'mc alias set myminio http://${MINIO_ENDPOINT} ${MINIO_ACCESS_KEY} ${MINIO_SECRET_KEY}'
                    sh "mc cp src.zip myminio/${MINIO_BUCKET}/spark-jobs/"

                    // Upload jobs/entry_point.py to spark-jobs/
                    sh "mc cp jobs/entry_point.py myminio/${MINIO_BUCKET}/spark-jobs/"

                    // Upload recursive job/ to job-input/
                    sh "mc cp --recursive config/job/ myminio/${MINIO_BUCKET}/job-input/"

                    // Upload recursive workflow/ to workflow/
                    sh "mc cp --recursive config/workflow/ myminio/${MINIO_BUCKET}/workflow/"
                }
            }
        }

        stage('Update Metadata') {
            steps {
                echo 'Running update_metadata.py...'
                withCredentials([
                    usernamePassword(
                        credentialsId: 'airflow-postgres-creds',
                        usernameVariable: 'DB_USER',
                        passwordVariable: 'DB_PASSWORD'
                    )
                ]) {
                    sh 'python orchestration/update_metadata.py'
                }
            }
        }
    }
}
