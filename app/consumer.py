# app/consumer.py
from kafka import KafkaConsumer
import json

import uuid

consumer = KafkaConsumer(
    'output_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    # Use a unique group ID to ensure we always read from the beginning (earliest)
    # ensuring that we see messages even if we restart the consumer.
    group_id=f'my-group-{uuid.uuid4()}',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

print("Listening for messages on topic 'output_topic'...")
for message in consumer:
    # message.value is already a dictionary due to the value_deserializer
    print(f"Received Result: {message.value}")
