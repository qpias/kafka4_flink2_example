# app/consumer.py
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'output_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

print("Listening for messages on topic 'output_topic'...")
for message in consumer:
    # message.value is already a dictionary due to the value_deserializer
    print(f"Received Window Result: {message.value}")
