import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, pow, sqrt, when, sum, round as spark_round

# Environment variables
os.environ["HADOOP_HOME"] = r"D:\spark\winutils"
os.environ["PYSPARK_PYTHON"] = r"C:\Users\User\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\User\AppData\Local\Programs\Python\Python311\python.exe"


# Transformation Functions
def compute_stats(df):
    return df.groupBy("sensor_id").agg(
        spark_round(avg("temperature"), 2).alias("avg_temp"),
        max("temperature").alias("max_temp"),
        min("temperature").alias("min_temp"),
        spark_round(avg("vibration"), 2).alias("avg_vib"),
        max("vibration").alias("max_vib"),
        min("vibration").alias("min_vib")
    )

def compute_rms(df):
    return df.groupBy("sensor_id").agg(
        spark_round(sqrt(avg(pow(col("vibration"), 2))), 2).alias("rms_vibration")
    )

def detect_anomalies(df, temp_threshold=70, vib_threshold=1.5):
    return df.withColumn(
        "anomaly",
        when((col("temperature") > temp_threshold) | (col("vibration") > vib_threshold), 1).otherwise(0)
    )

def count_anomalies(df):
    return df.groupBy("sensor_id").agg(sum("anomaly").alias("num_anomalies"))


# Main Execution
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("SensorDataCapstone") \
        .master("local[*]") \
        .getOrCreate()

    # Load CSV
    df = spark.read.csv("csv_data/sensor_data.csv", header=True, inferSchema=True)
    df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))
    df.show(5)

    # RDD Example
    rdd = df.rdd.map(lambda row: (row.sensor_id, row.temperature, row.vibration))
    print("Sample RDD row:", rdd.take(1))

    # Stats
    stats_df = compute_stats(df)
    stats_df.show()

    # RMS
    rms_df = compute_rms(df)
    rms_df.show()

    # Anomalies
    anomaly_df = detect_anomalies(df)
    anomaly_df.show(10)

    # Count anomalies
    anomaly_count_df = count_anomalies(anomaly_df)
    anomaly_count_df.show()

    spark.stop()