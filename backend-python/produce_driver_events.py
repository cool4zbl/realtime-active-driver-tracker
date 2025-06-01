from kafka import KafkaProducer
import json
import time
import random
import os

topic = os.getenv('KAFKA_TOPIC', 'driver-logs')

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

cities = ['Amsterdam', 'Rotterdam', 'Utrecht', 'Eindhoven', 'Enschede']
drivers = [f'driver_{i}' for i in range(1, 101)]

def random_lat_lon():
    return round(random.uniform(51.0, 54.0), 6), round(random.uniform(4.0, 7.0), 6)


while True:
    lat, lon = random_lat_lon()
    event = {
        "driver_id": f'driver-{random.randint(1, 1000)}',
        "event_type": random.choice(['location_update']),
        "lat": lat,
        "lon": lon,
        "timestamp": int(time.time()) * 1000
    }
    producer.send(topic, event)
    print(f"Produced: {event}")
    time.sleep(1)
