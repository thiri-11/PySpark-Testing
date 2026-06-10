pipeline {
    agent any
    stages {
        stage('Install Dependencies') {
            steps {
               
                bat '"C:\\Users\\User\\AppData\\Local\\Programs\\Python\\Python311\\python.exe" -m pip install --upgrade pip pyspark pandas'
            }
        }
        stage('Run PySpark Script') {
            steps {
                
                bat '"C:\\Users\\User\\AppData\\Local\\Programs\\Python\\Python311\\python.exe" main.py'
            }
        }
    }
}