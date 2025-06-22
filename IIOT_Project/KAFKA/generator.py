import json
import time
import random
from datetime import datetime, timezone
from confluent_kafka import Producer
import string # For generating random initials

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092'  # Change to your Kafka server address
}

producer = Producer(conf)
topic = 'pothole-events'

# Constants for Emden, Germany
EMDEN_LAT_CENTER = 53.3678
EMDEN_LON_CENTER = 7.2060
LAT_LON_VARIATION = 0.03 # For coordinate spread

# Accelerometer normal and spike ranges (adapted from your Bangalore script)
NORMAL_Z_MIN = 0.8  # Normal gravity-influenced Z (e.g., 0.8g to 1.2g)
NORMAL_Z_MAX = 1.2
NORMAL_XY_MIN = -0.5 # Normal XY jitter
NORMAL_XY_MAX = 0.5

# Z-Spike thresholds for pothole detection (illustrative)
# A pothole event occurs if Z is OUTSIDE normal_z_stable +- Z_SPIKE_THRESHOLD_LOW/MED/HIGH
# Let's define stable Z around 1.0g
Z_STABLE_AVG = 1.0

# For positive spikes (bumps)
Z_SPIKE_LOW_BUMP_MIN = 1.8 # e.g., > 1.0g + 0.8g
Z_SPIKE_LOW_BUMP_MAX = 2.5
Z_SPIKE_MEDIUM_BUMP_MIN = 2.51
Z_SPIKE_MEDIUM_BUMP_MAX = 3.5
Z_SPIKE_HIGH_BUMP_MIN = 3.51
Z_SPIKE_HIGH_BUMP_MAX = 5.0 # Max expected g-force for a severe pothole

# For negative spikes (dips, Z becomes much less than 1g or even negative if sensor inverts)
Z_SPIKE_LOW_DIP_MAX = 0.2  # e.g., < 1.0g - 0.8g
Z_SPIKE_LOW_DIP_MIN = -0.5
Z_SPIKE_MEDIUM_DIP_MAX = -0.51
Z_SPIKE_MEDIUM_DIP_MIN = -1.5
Z_SPIKE_HIGH_DIP_MAX = -1.51
Z_SPIKE_HIGH_DIP_MIN = -3.0

# Severity float ranges (0.0 to 1.0)
SEVERITY_FLOAT_LOW_MIN = 0.01
SEVERITY_FLOAT_LOW_MAX = 0.33
SEVERITY_FLOAT_MEDIUM_MIN = 0.34
SEVERITY_FLOAT_MEDIUM_MAX = 0.66
SEVERITY_FLOAT_HIGH_MIN = 0.67
SEVERITY_FLOAT_HIGH_MAX = 1.00

def generate_location_id(lat, lon, precision=5):
    return f"{round(lat, precision)}_{round(lon, precision)}"

def get_severity_type_from_float(severity_float):
    if severity_float <= SEVERITY_FLOAT_LOW_MAX: # Matches 0.01-0.33
        return "LOW"
    elif severity_float <= SEVERITY_FLOAT_MEDIUM_MAX: # Matches 0.34-0.66
        return "MEDIUM"
    else: # Matches 0.67-1.00
        return "HIGH"

def generate_device_id():
    city_code = "EMD"
    model_version = "V1"
    owner_initials = "".join(random.choices(string.ascii_uppercase, k=2))
    unit_number = random.randint(1, 999)
    return f"{city_code}-{model_version}-{owner_initials}-{unit_number:03d}"

def generate_sensor_reading():
    """Generates a sensor reading. Returns data if a pothole spike is detected, else None."""
    current_lat = EMDEN_LAT_CENTER + random.uniform(-LAT_LON_VARIATION, LAT_LON_VARIATION)
    current_lon = EMDEN_LON_CENTER + random.uniform(-LAT_LON_VARIATION, LAT_LON_VARIATION)
    device_id_str = generate_device_id()

    # Base accelerometer readings (normal state)
    accel_x = round(random.uniform(NORMAL_XY_MIN, NORMAL_XY_MAX), 2)
    accel_y = round(random.uniform(NORMAL_XY_MIN, NORMAL_XY_MAX), 2)
    accel_z = round(random.uniform(NORMAL_Z_MIN, NORMAL_Z_MAX), 2)

    # Probabilistic pothole detection
    # Let's say there's a 20% chance to simulate hitting a pothole in this reading cycle
    if random.random() > 0.20: # 80% chance of NO pothole
        # print(f"Sensor reading cycle: No significant Z-spike (Normal Z: {accel_z:.2f})")
        return None # No pothole event generated this time

    # If we are here, a pothole is being simulated (20% chance)
    # Decide if it's a bump or a dip, and what severity category
    spike_type = random.choice(["bump", "dip"])
    severity_category_choice = random.choice(["LOW", "MEDIUM", "HIGH"]) # Pre-decide category

    pothole_severity_float = 0.0
    detected_z_spike = accel_z # Start with normal Z, will be overridden

    if spike_type == "bump":
        if severity_category_choice == "LOW":
            detected_z_spike = round(random.uniform(Z_SPIKE_LOW_BUMP_MIN, Z_SPIKE_LOW_BUMP_MAX), 2)
            pothole_severity_float = round(random.uniform(SEVERITY_FLOAT_LOW_MIN, SEVERITY_FLOAT_LOW_MAX), 2)
        elif severity_category_choice == "MEDIUM":
            detected_z_spike = round(random.uniform(Z_SPIKE_MEDIUM_BUMP_MIN, Z_SPIKE_MEDIUM_BUMP_MAX), 2)
            pothole_severity_float = round(random.uniform(SEVERITY_FLOAT_MEDIUM_MIN, SEVERITY_FLOAT_MEDIUM_MAX), 2)
        elif severity_category_choice == "HIGH":
            detected_z_spike = round(random.uniform(Z_SPIKE_HIGH_BUMP_MIN, Z_SPIKE_HIGH_BUMP_MAX), 2)
            pothole_severity_float = round(random.uniform(SEVERITY_FLOAT_HIGH_MIN, SEVERITY_FLOAT_HIGH_MAX), 2)
    else: # spike_type == "dip"
        if severity_category_choice == "LOW":
            detected_z_spike = round(random.uniform(Z_SPIKE_LOW_DIP_MIN, Z_SPIKE_LOW_DIP_MAX), 2)
            pothole_severity_float = round(random.uniform(SEVERITY_FLOAT_LOW_MIN, SEVERITY_FLOAT_LOW_MAX), 2)
        elif severity_category_choice == "MEDIUM":
            detected_z_spike = round(random.uniform(Z_SPIKE_MEDIUM_DIP_MIN, Z_SPIKE_MEDIUM_DIP_MAX), 2)
            pothole_severity_float = round(random.uniform(SEVERITY_FLOAT_MEDIUM_MIN, SEVERITY_FLOAT_MEDIUM_MAX), 2)
        elif severity_category_choice == "HIGH":
            detected_z_spike = round(random.uniform(Z_SPIKE_HIGH_DIP_MIN, Z_SPIKE_HIGH_DIP_MAX), 2)
            pothole_severity_float = round(random.uniform(SEVERITY_FLOAT_HIGH_MIN, SEVERITY_FLOAT_HIGH_MAX), 2)

    # The actual string type should be derived from the final pothole_severity_float
    severity_type_str = get_severity_type_from_float(pothole_severity_float)
    if pothole_severity_float == 0.0: # Should not happen if a spike was generated
         print("Warning: Pothole detected but severity float is 0.0. Defaulting type.")
         severity_type_str = "LOW" # Or some error state

    event = {
        "event_type": "pothole_detected",
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "device_id": device_id_str,
        "location_latitude": round(current_lat, 7),
        "location_longitude": round(current_lon, 7),
        "accelerometer_x": accel_x, # XY can remain somewhat normal during the Z event
        "accelerometer_y": accel_y,
        "accelerometer_z": detected_z_spike, # The Z-value that indicates the pothole
        "severity": pothole_severity_float,
        "severity_type": severity_type_str,
        "location_id": generate_location_id(current_lat, current_lon, 5)
    }
    print(f"POTHOLE DETECTED! Type: {spike_type}, Chosen Severity: {severity_category_choice}, Z: {detected_z_spike:.2f}, SevFloat: {pothole_severity_float:.2f}, SevType: {severity_type_str}")
    return event

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for message record: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

if __name__ == "__main__":
    print(f"Starting pothole event simulation for Emden. Producing to topic: '{topic}' if Z-spike occurs.")
    print(f"Kafka brokers: {conf['bootstrap.servers']}")
    print("Press Ctrl+C to stop.")
    
    msg_counter = 0
    pothole_counter = 0
    try:
        while True:
            event_data = generate_sensor_reading() # This now might return None
            
            if event_data: # Only produce if a pothole was detected
                pothole_counter +=1
                event_json = json.dumps(event_data)
                producer.produce(topic, event_json.encode('utf-8'), callback=delivery_report)
            
            producer.poll(0) 
            msg_counter+=1
            if msg_counter % 20 == 0: # Print a status update every 20 cycles
                print(f"Sim cycle {msg_counter}, Potholes detected so far: {pothole_counter}")

            time.sleep(0.2) # Read sensor/check for potholes every 0.2 seconds

    except KeyboardInterrupt:
        print("\nSimulation stopped by user.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print(f"\n--- Simulation Summary ---")
        print(f"Total simulation cycles: {msg_counter}")
        print(f"Total pothole events generated: {pothole_counter}")
        print("Flushing final messages...")
        producer.flush(timeout=10)
        print("Producer flushed. Exiting.")