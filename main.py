import os
os.environ["HADOOP_HOME"] = r"D:\spark\winutils" 
# Tell Spark which Python executable to use
os.environ["PYSPARK_PYTHON"] = r"C:\Users\User\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\User\AppData\Local\Programs\Python\Python311\python.exe"


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, pow, sqrt, when, sum, round as spark_round

# 1. Spark session
spark = SparkSession.builder \
    .appName("SensorDataCapstone") \
    .master("local[*]") \
    .getOrCreate()

# 2. Load sensor CSV
df = spark.read.csv("test_data/sensor_data.csv", header=True, inferSchema=True)
df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))
df.show(5)

# 3. RDD Example
rdd = df.rdd.map(lambda row: (row.sensor_id, row.temperature, row.vibration))
print("Sample RDD row:", rdd.take(1))

# 4. Aggregations with 2 decimal places
stats_df = df.groupBy("sensor_id").agg(
    spark_round(avg("temperature"), 2).alias("avg_temp"),
    max("temperature").alias("max_temp"),
    min("temperature").alias("min_temp"),
    spark_round(avg("vibration"), 2).alias("avg_vib"),
    max("vibration").alias("max_vib"),
    min("vibration").alias("min_vib")
)
stats_df.show()

# 5. RMS vibration with 2 decimal places
rms_df = df.groupBy("sensor_id").agg(
    spark_round(sqrt(avg(pow(col("vibration"), 2))), 2).alias("rms_vibration")
)
rms_df.show()

# 6. Detect anomalies
threshold_temp = 70
threshold_vib = 1.5
anomaly_df = df.withColumn(
    "anomaly",
    when((col("temperature") > threshold_temp) | (col("vibration") > threshold_vib), 1).otherwise(0)
)
anomaly_df.show(10)

# 7. Count anomalies per sensor
anomaly_count_df = anomaly_df.groupBy("sensor_id").agg(sum("anomaly").alias("num_anomalies"))
anomaly_count_df.show()

spark.stop()