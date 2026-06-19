from kafka import KafkaConsumer
import json

# Create Kafka consumer
consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='test-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Waiting for messages...\n")

# Read messages continuously
for message in consumer:
    print(f"Received: {message.value}")