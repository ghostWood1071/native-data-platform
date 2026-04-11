pipeline {
    agent any

    environment {
        COMPOSE_FILE = 'config/environment/docker-compose-dev.yaml'
    }

    stages {
        stage('Checkout') {
            steps {
                echo 'Checkout code'
                checkout scm
            }
        }

        stage('Debug') {
            steps {
                sh 'pwd'
                sh 'ls -la'
            }
        }

        stage('Check Docker') {
            steps {
                sh 'docker --version'
                sh 'docker compose version'
            }
        }

        stage('Check file Compose') {
            steps {
                sh 'docker compose -f $COMPOSE_FILE config'
            }
        }
        
        stage('Deploy environment') {
            when {
                branch 'tunglt'
            }
            steps {
                sh 'docker compose -f $COMPOSE_FILE up -d'
            }
            post {
                always {
                    echo 'Pipeline completed'
                }
                success {
                    echo 'Pipeline success'
                }
                failure {
                    echo 'Pipeline failed'
                }
            }
        }
    }
}