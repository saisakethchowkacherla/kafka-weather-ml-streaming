from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'predictions',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='prediction-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Waiting for prediction results...\n")

for message in consumer:
    print("\nPrediction Result:")
    print(message.value)