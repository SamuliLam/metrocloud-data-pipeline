import json
import time
import uuid
import random
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from faker import Faker

from src.utils.logger import log
from src.config.config import settings

# Initialize Faker for generating random data
fake = Faker()

class IoTDevice:
    # Simulates an IoT device that generates sensor data.
    
    def __init__(self, device_id: Optional[str] = None, device_type: Optional[str] = None):
        # Initialize an IoT device with a unique ID and type.
        # If not provided, generates random UUID and selects a random sensor type.
    
        self.device_id = device_id or str(uuid.uuid4())
        self.device_type = device_type or random.choice(["temperature", "humidity", "pressure", "motion", "light"])
        self.location = {
            "latitude": float(f"{fake.latitude():2.6f}"),
            "longitude": float(f"{fake.longitude():2.6f}"),
            "building": fake.building_number(),
            "floor": random.randint(1, 10)
        }
        log.info(f"Initialized IoT device: {self.device_id} of type {self.device_type}")
    
    def generate_reading(self) -> Dict[str, Any]:
        # Generate a simulated sensor reading with value, unit, location, battery level, and timestamp.
        # Occassionally includes anomalous values for realism

        timestamp = datetime.now(timezone.utc).isoformat() + "Z"
        
        # Generate sensor reading based on device type
        if self.device_type == "temperature":
            value = round(random.uniform(15.0, 35.0), 2)  # Temperature in Celsius
            unit = "°C"
        elif self.device_type == "humidity":
            value = round(random.uniform(30.0, 80.0), 2)  # Humidity percentage
            unit = "%"
        elif self.device_type == "pressure":
            value = round(random.uniform(980.0, 1050.0), 2)  # Pressure in hPa
            unit = "hPa"
        elif self.device_type == "motion":
            value = random.choice([0, 1])  # Motion detected (1) or not (0)
            unit = "boolean"
        elif self.device_type == "light":
            value = round(random.uniform(0.0, 1000.0), 2)  # Light level in lux
            unit = "lux"
        else:
            value = random.random()
            unit = "unknown"
        
        # Add some occasional anomalies
        if random.random() < 0.05:  # 5% chance of anomaly
            if self.device_type in ["temperature", "humidity", "pressure", "light"]:
                # Extreme value
                value = value * 2 if random.random() < 0.5 else value * 0.1
        
        # Final reading package
        reading = {
            "device_id": self.device_id,
            "device_type": self.device_type,
            "timestamp": timestamp,
            "value": value,
            "unit": unit,
            "location": self.location,
            "battery_level": round(random.uniform(0.0, 100.0), 2)
        }
        
        return reading


class IoTSimulator:
    """Simulates multiple IoT devices generating continuous or batch sensor data."""
    
    def __init__(self, num_devices: int = None):
        # Initialize the simulator with a specified number of IoT devices."""
        # Defaults to value in settings if not provided.

        self.num_devices = num_devices or settings.iot_simulator.num_devices
        self.devices: List[IoTDevice] = []
        
        # Create a variety of device types
        device_types = ["temperature", "humidity", "pressure", "motion", "light"]
        
        for _ in range(self.num_devices):
            device_type = random.choice(device_types)
            self.devices.append(IoTDevice(device_type=device_type))
        
        log.info(f"IoT simulator initialized with {self.num_devices} devices")
    
    def generate_batch(self) -> List[Dict[str, Any]]:
        # Generate a batch of readings from all devices.
        # Returns: List of sensor readings
        readings = []
        for device in self.devices:
            readings.append(device.generate_reading())
        
        return readings
    
    def generate_continuous(self, callback_fn, interval_sec: float = None):
        """
        Continuously generate data at specified intervals and call the callback function.
        
        Args:
            callback_fn: Function to call with each batch of readings
            interval_sec: Interval between data generation in seconds
        """
        interval = interval_sec or settings.iot_simulator.data_generation_interval_sec
        
        log.info(f"Starting continuous data generation every {interval} seconds")
        try:
            while True:
                readings = self.generate_batch()
                callback_fn(readings)
                time.sleep(interval)
        except KeyboardInterrupt:
            log.info("Data generation stopped by user")
        except Exception as e:
            log.error(f"Error in data generation: {str(e)}")


# For testing
if __name__ == "__main__":
    # Create a simulator with 3 devices
    simulator = IoTSimulator(3)
    
    # Function to print readings
    def print_readings(readings):
        for reading in readings:
            print(json.dumps(reading, indent=2))
    
    # Generate data for 5 cycles
    for _ in range(5):
        readings = simulator.generate_batch()
        print_readings(readings)
        time.sleep(1)