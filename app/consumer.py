# app/consumer.py
from kafka import KafkaConsumer
import json

# Use a stable group ID so that the consumer can resume where it left off.
# If you want to force reading from the beginning, change this ID or reset the offsets via CLI.
group_id = 'flink-example-consumer-group'

consumer = KafkaConsumer(
    'output_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=group_id,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

print(f"Listening for messages on topic 'output_topic' using group '{group_id}'...")
try:
    for message in consumer:
        # message.value is already a dictionary due to the value_deserializer
        print(f"Received Result: {message.value}")
except KeyboardInterrupt:
    print("\nStopping consumer...")
finally:
    consumer.close()
    print("Consumer closed.")
