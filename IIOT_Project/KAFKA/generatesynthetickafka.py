import json
import time
import random
from datetime import datetime, timezone
from confluent_kafka import Producer

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092'  # Change to your Kafka server address
}

producer = Producer(conf)
topic = 'pothole-events'

def generate_event():
    lat = 12.981 + random.uniform(-0.002, 0.002)
    lon = 77.587 + random.uniform(-0.002, 0.002)

    event = {
        "event_type": "pothole_detected",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "device_id": "raspi-2",
        "location": {
            "lat": lat,
            "lon": lon
        },
        "accelerometer": {
            "x": round(random.uniform(-2, 2), 2),
            "y": round(random.uniform(-2, 2), 2),
            "z": round(random.uniform(9, 20), 2)  # assuming z axis is gravity + bumps
        },
        "severity" : round(random.uniform(0.0, 0.4), 2),
        "location_id": f"{round(lat, 3)}_{round(lon, 3)}"
    }
    return event

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for message: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


# # --- TEMPORARY TEST CODE for Deduplication ---
# if __name__ == "__main__":
#     print("--- Starting Controlled Deduplication Test ---")

#     # Define a specific location we will test repeatedly
#     test_location_id_A = "10.100_20.200"
#     # Define a second, different location
#     test_location_id_B = "30.300_40.400"

#     try:
#         # Event 1: The FIRST time we see location A
#         event1 = {
#             "event_type": "pothole_detected",
#             "timestamp": datetime.now(timezone.utc).isoformat(),
#             "device_id": "test-device-A1",
#             "location": {"lat": 10.1001, "lon": 20.2001},
#             "severity": 0.5, # Should be severity 3
#             "location_id": test_location_id_A
#         }
#         producer.produce(topic, json.dumps(event1).encode('utf-8'), callback=delivery_report)
#         print(f"\nSENT [1] NEW: {test_location_id_A} -> Should create a new record.")
#         producer.flush()
#         time.sleep(2)

#         # Event 2: A DUPLICATE of location A
#         event2 = {
#             "event_type": "pothole_detected",
#             "timestamp": datetime.now(timezone.utc).isoformat(),
#             "device_id": "test-device-A2",
#             "location": {"lat": 10.1002, "lon": 20.2002},
#             "severity": 0.7, # Higher severity
#             "location_id": test_location_id_A # SAME location_id
#         }
#         producer.produce(topic, json.dumps(event2).encode('utf-8'), callback=delivery_report)
#         print(f"\nSENT [2] DUPLICATE: {test_location_id_A} -> Should be an UPDATE, NOT a new record.")
#         producer.flush()
#         time.sleep(2)

#         # Event 3: A NEW event from a different location B
#         event3 = {
#             "event_type": "pothole_detected",
#             "timestamp": datetime.now(timezone.utc).isoformat(),
#             "device_id": "test-device-B1",
#             "location": {"lat": 30.3001, "lon": 40.4001},
#             "severity": 0.2, # Should be severity 1
#             "location_id": test_location_id_B
#         }
#         producer.produce(topic, json.dumps(event3).encode('utf-8'), callback=delivery_report)
#         print(f"\nSENT [3] NEW: {test_location_id_B} -> Should create a new record.")
#         producer.flush()

#     except KeyboardInterrupt:
#         print("Simulation stopped")
#     finally:
#         # Wait a moment to ensure messages are sent before exiting
#         producer.flush()
#         print("\n--- Test Finished ---")
# # --- END OF TEMPORARY TEST CODE ---



if __name__ == "__main__":
    try:
        while True:
            event = generate_event()
            event_json = json.dumps(event)
            producer.produce(topic, event_json.encode('utf-8'), callback=delivery_report)
            producer.poll(0)
            time.sleep(1)  # Send a message every second
    except KeyboardInterrupt:
        print("Simulation stopped")
    finally:
        producer.flush()
