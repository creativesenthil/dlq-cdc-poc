pipeline {
    agent any

    environment {
        AWS_ENDPOINT = "http://localhost:4566"
        AWS_ACCESS_KEY_ID = "test"
        AWS_SECRET_ACCESS_KEY = "test"
        AWS_REGION = "us-east-1"
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Build Stack') {
            steps {
                sh 'docker compose up --build -d'
                sh 'sleep 20'
            }
        }

        stage('Setup AWS Resources (S3 + DLQ)') {
            steps {
                sh 'bash scripts/setup_aws.sh'
            }
        }

        stage('Setup Kafka + Debezium') {
            steps {
                sh 'bash scripts/setup_kafka.sh'
                sh 'docker restart dlq-processor'
                sh 'sleep 10'
            }
        }

        stage('Seed MongoDB') {
            steps {
                sh 'bash scripts/seed_mongo.sh'
                sh 'sleep 15'
            }
        }

        stage('Test: Valid Record Reaches S3') {
            steps {
                sh '''
                    S3_COUNT=$(aws --endpoint-url=$AWS_ENDPOINT s3 ls s3://cdc-landing --recursive | wc -l)
                    echo "S3 objects: $S3_COUNT"
                    test "$S3_COUNT" -ge 1
                '''
            }
        }

        stage('Test: Poison Pill Caught by DLQ') {
            steps {
                sh '''
                    DLQ_URL=$(aws --endpoint-url=$AWS_ENDPOINT sqs get-queue-url --queue-name cdc-dlq --query QueueUrl --output text)
                    DLQ_COUNT=$(aws --endpoint-url=$AWS_ENDPOINT sqs get-queue-attributes --queue-url $DLQ_URL --attribute-names ApproximateNumberOfMessages --query Attributes.ApproximateNumberOfMessages --output text)
                    echo "DLQ messages: $DLQ_COUNT"
                    test "$DLQ_COUNT" -ge 1
                '''
            }
        }

        stage('Run Remediation (Recovery Proof)') {
            steps {
                sh 'docker exec -i dlq-processor python remediation.py'
            }
        }

        stage('Show Processor Logs') {
            steps {
                sh 'docker logs --tail 50 dlq-processor'
            }
        }
    }

    post {
        always {
            echo 'Pipeline finished. Stack left running for inspection — tear down manually with: docker compose down -v'
        }
        success {
            echo 'DLQ POC verified: valid record in S3, poison pill caught in DLQ, remediation ran.'
        }
        failure {
            echo 'Pipeline failed — check the stage logs above for which step broke.'
        }
    }
}
