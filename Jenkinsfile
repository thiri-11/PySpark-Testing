pipeline {
    agent any
    stages {
        stage('Install Dependencies') {
            steps {
                bat 'pip install pyspark pandas'
            }
        }
        stage('Run PySpark Script') {
            steps {
                bat 'python main.py'
            }
        }
    }
}