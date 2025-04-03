from kafka import KafkaProducer
import json
import time
import random
import os  # new import

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

cities = ['Amsterdam', 'Rotterdam', 'Utrecht', 'Eindhoven', 'Enschede']
drivers = [f'driver_{i}' for i in range(1, 101)]

topic = os.getenv('KAFKA_TOPIC', 'driver-events')  # new topic variable

while True:
    event = {
        "driver_id": random.choice(drivers),
        "city": random.choice(cities),
        "timestamp": int(time.time())
    }
    producer.send(topic, value=event)
    print(f"Produced: {event}")
    time.sleep(1)
