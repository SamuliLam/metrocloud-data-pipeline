import json
import time
import uuid
import random
# Import math module for day/night cycle simulation
import math
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from faker import Faker

from src.utils.logger import log
from src.config.config import settings

# Initialize Faker for generating random data
fake = Faker()

class IoTDevice:
    """
    Simulates an IoT device that generates sensor data.
    
    This class represents a single IoT device that can generate
    readings of a specific type (temperature, humidity, etc.).
    """
    
    def __init__(self, device_id: Optional[str] = None, device_type: Optional[str] = None):
        """
        Initialize an IoT device with a unique ID and type.
        
        Args:
            device_id: Optional device ID (generated if not provided)
            device_type: Optional device type (randomly selected if not provided)
        """
    
        self.device_id = device_id or str(uuid.uuid4())
        # Use device types from settings or fallback to default list
        available_types = settings.iot_simulator.device_types
        self.device_type = device_type or random.choice(available_types)
        
        # Generate a realistic location for the device
        self.location = {
            "latitude": float(f"{fake.latitude():2.6f}"),
            "longitude": float(f"{fake.longitude():2.6f}"),
            "building": fake.building_number(),
            "floor": random.randint(1, 10),
            "zone": random.choice(["north", "south", "east", "west", "central"])
        }

        # Set device-specific parameters for more realistic simulation
        self._set_device_parameters()

        log.info(f"Initialized IoT device: {self.device_id} of type {self.device_type}")

    def _set_device_parameters(self):
        """
        Set device-specific parameters based on device type.
        
        These parameters help generate more realistic data with consistent
        trends and baselines for each device.
        """
        # Base value ranges for different device types
        if self.device_type == "temperature":
            self.base_value = random.uniform(20.0, 25.0)  # Starting temperature in Celsius
            self.variation = random.uniform(0.1, 2.0)     # How much it can vary
            self.trend = random.uniform(-0.1, 0.1)        # General trend direction
        elif self.device_type == "humidity":
            self.base_value = random.uniform(40.0, 60.0)  # Starting humidity percentage
            self.variation = random.uniform(1.0, 5.0)     # How much it can vary
            self.trend = random.uniform(-0.2, 0.2)        # General trend direction
        elif self.device_type == "pressure":
            self.base_value = random.uniform(1000.0, 1020.0)  # Starting pressure in hPa
            self.variation = random.uniform(0.5, 3.0)         # How much it can vary
            self.trend = random.uniform(-0.5, 0.5)            # General trend direction
        elif self.device_type == "motion":
            self.activity_level = random.uniform(0.0, 1.0)    # Probability of motion detection
            self.last_motion = 0                              # Time since last motion
        elif self.device_type == "light":
            self.base_value = random.uniform(300.0, 700.0)    # Starting light level in lux
            self.variation = random.uniform(10.0, 100.0)      # How much it can vary
            self.day_night_cycle = random.random() * 2 * 3.14159  # Phase in day/night cycle
        else:
            # Generic parameters for other device types
            self.base_value = random.uniform(0.0, 100.0)
            self.variation = random.uniform(1.0, 10.0)
            self.trend = random.uniform(-0.5, 0.5)
    
    def generate_reading(self) -> Dict[str, Any]:
        """
        Generate a simulated sensor reading based on the device type.
        
        Returns:
            Dictionary containing the simulated reading data
        """

        timestamp = datetime.now(timezone.utc).isoformat() + "Z"
        
        # Generate sensor reading based on device type
        if self.device_type == "temperature":
            # Simulate temperature with some noise, trend, and occasional spikes
            noise = random.uniform(-self.variation, self.variation)
            self.base_value += self.trend + (noise * 0.1) # Slow drift
            value = round(self.base_value + noise, 2)
            unit = "°C"

        elif self.device_type == "humidity":
            # Simulate humidity with some noise and trend
            noise = random.uniform(-self.variation, self.variation)
            self.base_value += self.trend + (noise * 0.1)  # Slow drift
            # Keep humidity in realistic bounds
            self.base_value = max(10.0, min(95.0, self.base_value))
            value = round(self.base_value + noise, 2)
            unit = "%"
        
        elif self.device_type == "pressure":
            # Simulate atmospheric pressure with some noise and trend
            noise = random.uniform(-self.variation, self.variation)
            self.base_value += self.trend + (noise * 0.1)  # Slow drift
            # Keep pressure in realistic bounds
            self.base_value = max(980.0, min(1050.0, self.base_value))
            value = round(self.base_value + noise, 2)
            unit = "hPa"
        
        elif self.device_type == "motion":
            # Simulate motion detector with occasional activation
            # More likely to detect motion if there was recent motion (simulates continuous activity)
            if self.last_motion > 0:
                self.last_motion -= 1
                motion_probability = min(0.8, self.activity_level + 0.3)
            else:
                motion_probability = self.activity_level * 0.3
                
            value = 1 if random.random() < motion_probability else 0
            
            if value == 1:
                self.last_motion = random.randint(1, 3)  # Motion lasts for a few cycles
            
            unit = "boolean"
        
        elif self.device_type == "light":
            # Simulate light levels with day/night cycle and noise
            # Advance the day/night cycle
            self.day_night_cycle += 0.02
            day_night_factor = (math.sin(self.day_night_cycle) + 1) / 2  # 0 to 1
            noise = random.uniform(-self.variation, self.variation)
            value = round(self.base_value * day_night_factor + noise, 2)
            # Keep light levels non-negative
            value = max(0.0, value)
            unit = "lux"
        
        else:
            # Generic sensor reading
            value = round(random.uniform(0.0, 100.0), 2)
            unit = "unknown"
        
        # Add anomalies based on configurable probability
        anomaly_probability = settings.iot_simulator.anomaly_probability
        is_anomaly = random.random() < anomaly_probability
        
        if is_anomaly:
            if self.device_type in ["temperature", "humidity", "pressure", "light"]:
                # Generate extreme values for anomalies (higher or lower)
                if random.random() < 0.5:
                    value = value * random.uniform(1.5, 3.0)  # Higher value
                else:
                    value = value * random.uniform(0.1, 0.5)  # Lower value
                
                # Round to maintain precision
                value = round(value, 2)
                
                # Log the anomaly for monitoring
                log.warning(f"Generated anomaly for device {self.device_id}: {value} {unit}")
            
            elif self.device_type == "motion" and value == 0:
                # Force a motion detection as anomaly
                value = 1
                log.warning(f"Generated anomaly: unexpected motion for device {self.device_id}")
        
        # Create the reading object with all metadata
        reading = {
            "device_id": self.device_id,
            "device_type": self.device_type,
            "timestamp": timestamp,
            "value": value,
            "unit": unit,
            "location": self.location,
            "battery_level": round(random.uniform(0.0, 100.0), 2),
            "signal_strenth": round(random.uniform(-100.0, -30.0), 2), # in dBm
            "is_anomaly": is_anomaly
        }
        
        return reading


class IoTSimulator:
    """
    Simulates multiple IoT devices generating data.
    
    This class manages a collection of IoT devices and provides methods
    to generate readings from all devices simultaneously.
    """

    def __init__(self, num_devices: int = None):
        """
        Initialize a set of IoT devices.
        
        Args:
            num_devices: Number of devices to simulate (default from settings)
        """

        self.num_devices = num_devices or settings.iot_simulator.num_devices
        self.devices: List[IoTDevice] = []
        
        # Create a variety of device types
        device_types = settings.iot_simulator.device_types 
        
        # Create a variety of device types with distribution
        for _ in range(self.num_devices):
            # Create devices with different types according to a distribution
            # More temperature and humidity sensors, fewer motion sensors
            weights = {
                "temperature": 0.3,
                "humidity": 0.3,
                "pressure": 0.15,
                "motion": 0.1,
                "light": 0.15
            }

            # Default to random choice if device type doesn't have a weight
            device_type = random.choices(
                population=device_types,
                weights=[weights.get(t, 1.0/len(device_types)) for t in device_types],
                k=1
            )[0]
            self.devices.append(IoTDevice(device_type=device_type))
        
        log.info(f"IoT simulator initialized with {self.num_devices} devices")

        # Log the distribution of device types
        type_counts = {}
        for device in self.devices:
            type_counts[device.device_type] = type_counts.get(device.device_type, 0) + 1
            
        for device_type, count in type_counts.items():
            log.info(f"  - {device_type}: {count} devices")
    
    def generate_batch(self) -> List[Dict[str, Any]]:
        """
        Generate a batch of readings from all devices.
        
        Returns:
            List of readings from all devices
        """
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
