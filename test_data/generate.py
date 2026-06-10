import csv, random, datetime, os

os.makedirs("test_data", exist_ok=True)

with open('test_data/sensor_data.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['sensor_id', 'timestamp', 'temperature', 'vibration'])
    start_time = datetime.datetime(2026, 6, 10, 9, 0)
    sensors = ['S1','S2','S3']
    for i in range(1000):
        sensor = random.choice(sensors)
        timestamp = start_time + datetime.timedelta(seconds=i*30)
        temp = round(random.uniform(20, 80), 1)       # some high values are anomalies
        vib = round(random.uniform(0.1, 2.0), 2)      # >1.5 is anomaly
        writer.writerow([sensor, timestamp.strftime("%Y-%m-%d %H:%M:%S"), temp, vib])
print("Sample sensor CSV generated successfully!")