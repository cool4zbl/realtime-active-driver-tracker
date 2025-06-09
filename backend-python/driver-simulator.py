from kafka import KafkaProducer
import json, time, random, os
from geopy.distance import distance

HOTSPOTS = [
    # Example hotspots with coordinates and radius in km
    ("AmsterdamCS", 52.378, 4.900, 1.0),  # Amsterdam Central Station
    ("Leidseplein", 52.364, 4.883, 0.6),  # Leidseplein
    ("Zuid", 52.338, 4.920, 0.5),  # Zuid
    ("RotterdamCS", 51.922, 4.479, 1.0),  # Rotterdam Central Station
]

def random_lat_lon(lat, lon, radius_km):
    brg = random.uniform(0, 360)  # Bearing in degrees
    dst = random.uniform(0, radius_km)  # Distance in km
    dest = distance(kilometers=dst).destination((lat, lon), brg)
    return dest.latitude, dest.longitude

def peak_off_hours_hotspot_ratio():
    # Simulate peak hours (8-9 AM, 5-6 PM) and off-peak hours (rest of the day)
    current_hour = time.localtime().tm_hour
    if 7 <= current_hour < 10 or 16 <= current_hour < 19:
        return 0.9
    elif 0 <= current_hour < 6:
        return 0.4
    else:
        return 0.7

def driver_event(driver_id):
    if random.random() < peak_off_hours_hotspot_ratio():
        hotspot = random.choice(HOTSPOTS)
        name, lat, lon, radius = hotspot
        lat, lon = random_lat_lon(lat, lon, radius)
    else:
        lat = random.uniform(50.75, 53.5)
        lon = random.uniform(3.5, 7.22)
    return {
        "driver_id": driver_id,
        "event_type": "location_update",
        "lat": round(lat, 6),
        "lon": round(lon, 6),
        "timestamp": int(time.time()) * 1000
    }

topic = os.getenv('KAFKA_TOPIC', 'driver-logs')
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    for i in range(500):
        event = driver_event(f"driver-{i}")
        producer.send(topic, event)
        print(f"Produced: {event}")
    time.sleep(1) # 500 msg/s = 30,000 msg/min
