import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from main import compute_stats, compute_rms, detect_anomalies, count_anomalies

class TestPySparkSensor(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("PySparkUnitTest") \
            .master("local[*]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_compute_stats(self):
        data = [
            Row(sensor_id="S1", temperature=20.0, vibration=0.5),
            Row(sensor_id="S1", temperature=30.0, vibration=1.0),
            Row(sensor_id="S2", temperature=25.0, vibration=0.7)
        ]
        df = self.spark.createDataFrame(data)
        stats_df = compute_stats(df)
        result = {row['sensor_id']: row['avg_temp'] for row in stats_df.collect()}
        self.assertEqual(result["S1"], 25.0)
        self.assertEqual(result["S2"], 25.0)

    def test_compute_rms(self):
        data = [
            Row(sensor_id="S1", vibration=0.5),
            Row(sensor_id="S1", vibration=1.0),
        ]
        df = self.spark.createDataFrame(data)
        rms_df = compute_rms(df)
        rms_value = rms_df.collect()[0]['rms_vibration']
        self.assertAlmostEqual(rms_value, 0.79, places=2)

    def test_detect_anomalies(self):
        data = [
            Row(sensor_id="S1", temperature=20.0, vibration=0.5),
            Row(sensor_id="S2", temperature=75.0, vibration=1.6)
        ]
        df = self.spark.createDataFrame(data)
        anomaly_df = detect_anomalies(df)
        anomalies = {row['sensor_id']: row['anomaly'] for row in anomaly_df.collect()}
        self.assertEqual(anomalies["S1"], 0)
        self.assertEqual(anomalies["S2"], 1)

    def test_count_anomalies(self):
        data = [
            Row(sensor_id="S1", anomaly=0),
            Row(sensor_id="S1", anomaly=1),
            Row(sensor_id="S2", anomaly=1)
        ]
        df = self.spark.createDataFrame(data)
        count_df = count_anomalies(df)
        result = {row['sensor_id']: row['num_anomalies'] for row in count_df.collect()}
        self.assertEqual(result["S1"], 1)
        self.assertEqual(result["S2"], 1)

if __name__ == "__main__":
    unittest.main()