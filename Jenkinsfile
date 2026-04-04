pipeline {
    agent any

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

        stage('Validate Compose') {
            steps {
                sh 'docker compose -f config/environment/docker-compose-dev.yaml config'
            }
        }
    }
}