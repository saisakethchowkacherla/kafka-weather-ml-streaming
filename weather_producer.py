from kafka import KafkaProducer
import pandas as pd
import json
import time

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Load dataset
df = pd.read_csv("weatherHistory.csv")

# Keep only needed columns
df = df[[
    'Humidity',
    'Wind Speed (km/h)',
    'Pressure (millibars)',
    'Temperature (C)'
]]

# Remove missing values
df = df.dropna()

print("Starting weather data streaming...\n")

# Stream rows one by one
for index, row in df.iterrows():

    message = {
        "Humidity": row['Humidity'],
        "WindSpeed": row['Wind Speed (km/h)'],
        "Pressure": row['Pressure (millibars)'],
        "ActualTemperature": row['Temperature (C)']
    }

    # Send to Kafka topic
    producer.send('weather-data', value=message)

    print(f"Sent: {message}")

    # Simulate real-time streaming
    time.sleep(1)

producer.flush()

print("\nAll weather data sent successfully!")