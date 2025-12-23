# app/producer.py
import time
import json # Added json import
# import random # Not strictly needed if alternating keys

from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic = 'input_topic'

print(f"Sending messages to topic: {topic}")
for i in range(20): # Send 20 messages for windowing
    key = "key_A" if i % 2 == 0 else "key_B" # Alternate keys deterministically
    message_data = {"key": key, "message": f'message number {i}'} # Formatted as JSON with a key
    producer.send(topic, json.dumps(message_data).encode('utf-8'))
    print(f"Sent: {message_data}")
    time.sleep(0.5) # Send faster for windowing


producer.flush()
print("All messages sent.")
