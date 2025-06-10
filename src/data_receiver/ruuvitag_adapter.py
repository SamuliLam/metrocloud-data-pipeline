#!/usr/bin/env python3

"""
RuuviTag Data Apapter - Receives data from ESP32 and forwards to Kafka.
"""

import os
import json
import time
import traceback
import paho.mqtt.client as mqtt
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

from src.utils.logger import log
from src.config.config import settings
from src.data_ingestion.producer import KafkaProducer


class RuuviTagAdapter:
    """
    Adapter for RuuviTag data that connects ESP32 gateway with Kafka pipeline.
    
    This class subscribes to MQTT topics where ESP32 publishes data, processes
    and validates the data, then forwards it to Kafka using the existing pipeline
    infrastructure.
    """

    def __init__(
            self,
            mqtt_broker: str = None,
            mqtt_port: int = None,
            mqtt_topic: str = None,
            client_id: str = None
    ):
        """
        Initialize the RuuviTag Adapter

        Args:
            mqtt_broker: MQTT broker hostname or IP
            mqtt_port: MQTT broker port
            mqtt_topic: MQTT topic to subscribe to
            client_id: MQTT client ID
        """

        # Get configuration from environment or settings
        self.mqtt_broker = mqtt_broker or settings.mqtt.broker_host
        self.mqtt_port = mqtt_port or settings.mqtt.broker_port
        self.mqtt_topic = mqtt_topic or settings.mqtt.topic
        self.client_id = client_id or settings.mqtt.client_id
        self.qos = settings.mqtt.qos
        self.keep_alive = settings.mqtt.keep_alive

        # Setup MQTT client
        self.mqtt_client = mqtt.Client(client_id=self.client_id)
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message

        # Setup authentication if configured
        if settings.mqtt.username and settings.mqtt.password:
            self.mqtt_client.username_pw_set(settings.mqtt.username, settings.mqtt.password)

        # Create Kafka producer for sending data with exception handling
        try:
            self.kafka_producer = KafkaProducer()
            log.info("Kafka producer initialized successfully")
        except Exception as e:
            log.error(f"Failed to initialize Kafka producer: str{e}")
            raise

        # Store device data for periodic monitoring and debugging
        self.devices = {}
        self.message_count = 0

        log.info(f"RuuviTag adapter initialized with MQTT broker: {self.mqtt_broker}:{self.mqtt_port}")

    def on_connect(self, client, userdata, flags, rc):
        """
        Callback when connected to MQTT broker
        
        Args:
            client: MQTT client instance
            userdata: User data
            flags: Connection flags
            rc: Connection result code
        """

        if rc == 0:
            log.info(f"Connected to MQTT broker: {self.mqtt_broker}")
            # Subscribe to RuuviTag data topic
            client.subscribe(self.mqtt_topic, qos=self.qos)
            log.info(f"Subscribed to topic: {self.mqtt_topic} with QoS {self.qos}")
        else:
            log.error(f"Failed to connect to MQTT broker, return code: {rc}")

    def on_message(self, client, userdata, msg):
        """
        Callback when message is received from MQTT broker

        Args:
            client: MQTT client instance
            userdata: User data
            msg: MQTT messag
        """
        try:
            # Log raw message for debugging
            log.debug(f"Raw MQTT message received on {msg.topic}: {msg.payload}")

            # Decode and parse JSON
            payload = msg.payload.decode('utf-8')
            data = json.loads(payload)

            self.message_count += 1
            if self.message_count % 10 == 0:  # Log every 10th message to avoid log flooding
                log.info(f"Received message #{self.message_count}: {payload[:100]}...")
            
            # Process and adapt data for Kafka
            kafka_messages = self.adapt_ruuvitag_data(data)

            # Sending each message to Kafka
            if kafka_messages:
                log.info(f"Sending {len(kafka_messages)} separate sensor readings to Kafka")
                for kafka_message in kafka_messages:
                    self.kafka_producer.send_message(kafka_message)

                # Store device data for monitoring (using the parent device ID)
                parent_device_id = kafka_message.get('device_metadata', {}).get('parent_device') 
                device_id = kafka_message.get('device_id')
                sensor_type = kafka_message.get('device_metadata', {}).get('sensor_type', 'unknown')

                log.debug(f"Processed and sent {sensor_type} reading from device: {parent_device_id}, value: {kafka_message.get('value')}{kafka_message.get('unit')}")

                # Store in devices dictionary using the actual device_id
                if device_id:
                    self.devices[device_id] = {
                        'last_seen': datetime.now(),
                        'data': kafka_message
                    }                
            else:
                log.warning(f"Failed to adapt message: {payload[:100]}...")

        except json.JSONDecodeError as e:
            log.error(f"Failed to parse JSON from MQTT message: {e}")
            log.error(f"Raw message: {msg.payload}")
        except Exception as e:
            log.error(f"Error processing MQTT message: {e}")
            log.error(traceback.format_exc())


    def adapt_ruuvitag_data(self, ruuvitag_data: Dict[str, Any]) -> List[Dict[str, any]]:
        """
        Adapt RuuviTag data to match the Kafka schema

        Args:
            ruuvitag_data: Raw data from RuuviTag via ESP32

        Returns:
            List of adapted data matching kafka schema, one entry per sensor type
        """

        try:
            # Log the input for debugging
            log.debug(f"Adapting data: {ruuvitag_data}")

            # Validate required fields
            required_fields = ['device_id']
            for field in required_fields:
                if field not in ruuvitag_data:
                    log.warning(f"Missing required field in RuuviTag data: {field}")
                    log.warning(f"Available fields: {list(ruuvitag_data.keys())}")
                    return None
                
            # Get device ID (MAC address)
            device_id = ruuvitag_data['device_id']

            # Handle timestamp
            iso_timestamp = self._get_timestamp(ruuvitag_data)

            # List to hold all messages
            kafka_messages = []

            # Common properties
            common_properties = {
                "location": settings.ruuvitag.default_location,
                "battery_level": self._calculate_battery_level(self._safe_float(ruuvitag_data.get('battery_voltage', 0))),
                "signal_strength": settings.ruuvitag.signal_strength,
                "firmware_version": settings.ruuvitag.firmware_version,
                "timestamp": iso_timestamp,
                "status": "ACTIVE",
                "maintenance_date": None
            }

            # Define sensor mapping - each mapping defines how to extract a sensor reading
            # This makes it easy to add new sensors in the future
            sensor_mapping = [
                {
                    "field": "temperature",
                    "device_type": "temperature_sensor",
                    "unit": "°C",
                    "tags": ["ruuvitag", "ble", "temperature"],
                },
                {
                    "field": "humidity",
                    "device_type": "humidity_sensor",
                    "unit": "%",
                    "tags": ["ruuvitag", "ble", "humidity"],
                },
                {
                    "field": "pressure",
                    "device_type": "pressure_sensor",
                    "unit": "Pa",
                    "tags": ["ruuvitag", "ble", "pressure"],
                },
                {
                    "field": "accel_x",
                    "device_type": "acceleration_sensor",
                    "unit": "g",
                    "tags": ["ruuvitag", "ble", "acceleration", "x-axis"],
                    "metadata_extra": {"axis": "x", "sensor_type": "acceleration"}
                },
                {
                    "field": "accel_y",
                    "device_type": "acceleration_sensor",
                    "unit": "g",
                    "tags": ["ruuvitag", "ble", "acceleration", "y-axis"],
                    "metadata_extra": {"axis": "y", "sensor_type": "acceleration"}
                },
                {
                    "field": "accel_z",
                    "device_type": "acceleration_sensor",
                    "unit": "g",
                    "tags": ["ruuvitag", "ble", "acceleration", "z-axis"],
                    "metadata_extra": {"axis": "z", "sensor_type": "acceleration"}
                },
                {
                    "field": "battery_voltage",
                    "device_type": "battery_sensor",
                    "unit": "V",
                    "tags": ["ruuvitag", "ble", "battery"],
                },
                {
                    "field": "tx_power",
                    "device_type": "transmit_power_sensor",
                    "unit": "dBm",
                    "tags": ["ruuvitag", "ble", "tx_power"],
                },
                {
                    "field": "movement_counter",
                    "device_type": "movement_sensor",
                    "unit": "count",
                    "tags": ["ruuvitag", "ble", "movement"],
                }
                # new sensors can be added here in the future
            ]

            # Process each sensor mapping
            for mapping in sensor_mapping:
                field = mapping["field"]
                if field in ruuvitag_data:
                    value = self._safe_float(ruuvitag_data[field])

                    # Create device ID for this specific sensor
                    sensor_device_id = f"{device_id}_{field}"

                    # Build device metadata
                    device_metadata = {
                        "parent_device": device_id,
                        "sensor_type": field
                    }

                    # Add any extra metadata specified in the mapping
                    if "metadata_exra" in mapping:
                        device_metadata.update(mapping["metadata_extra"])

                    # Create message for this sensor
                    sensor_message = {
                        "device_id": sensor_device_id,
                        "device_type": mapping["device_type"],
                        "value": value,
                        "unit": mapping["unit"],
                        "is_anomaly": self._detect_anomaly({field: value}),
                        "device_metadata": device_metadata,
                        "tags": mapping["tags"],
                    }

                    # Add common properties
                    sensor_message.update(common_properties)

                    # Add to list of messages
                    kafka_messages.append(sensor_message)

            return kafka_messages
        
        except Exception as e:
            log.error(f"Error adapting RuuviTag data: {e}")
            log.error(traceback.format_exc())
            return []

    def _get_timestamp(self, ruuvitag_data: Dict[str, Any]) -> str:
        """
        Extract or generate ISO-8601 timestamp from RuuviTag data

        Args:
            ruuvitag_data: Raw data from RuuviTag
        
        Returns:
            ISO-8601 formatted timestamp strings
        """
        if 'timestamp' in ruuvitag_data:
            try:
                # Try parsing as integer (seconds since epoch)
                ts_value = ruuvitag_data['timestamp']
                if isinstance(ts_value, str) and ts_value.isdigit():
                    ts_value = int(ts_value)
                
                if isinstance(ts_value, int) or isinstance(ts_value, float):
                    # If it's a small number (< 10000000), it might be relative time, not epoch
                    if ts_value < 10000000: # Arbitrary threshold
                        # Use current time instead
                        return datetime.now(timezone.utc).isoformat()
                    else:
                        timestamp = datetime.fromtimestamp(ts_value, tz=timezone.utc)
                        return timestamp.isoformat()
                elif isinstance(ts_value, str) and "T" in ts_value:
                    # Already looks like ISO format
                    return ts_value
                else:
                    # Unknown format, use current time
                    return datetime.now(timezone.utc).isoformat()
            except Exception as e:
                log.warning(f"Failed to parse timestamp '{ruuvitag_data['timestamp']}': {e}")
                return datetime.now(timezone.utc).isoformat()
        else:
            return datetime.now(timezone.utc).isoformat()


    def _safe_float(self, value: Any) -> float:
        """
        Safely convert a value to float with fallback
        """
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0

    def _calculate_battery_level(self, voltage: float) -> float:
        """
        Calculate battery level percentage from voltage

        Args:
            voltage: Battery voltage

        Returns:
            Batter level as percentage(1-100)
        """
        if not voltage or voltage < 1.8:
            return 0.0
        
        # Get battery parameters from settings
        min_voltage = settings.ruuvitag.battery["min_voltage"]
        max_voltage = settings.ruuvitag.battery["max_voltage"]

        if voltage >= max_voltage:
            return 100.0
        elif voltage <= min_voltage:
            return 0.0
        else:
            # Linear mapping
            percentage = (voltage - min_voltage) / (max_voltage - min_voltage) * 100.0
            return round(percentage, 2)
        
    def _detect_anomaly(self, data: Dict[str, Any]) -> bool:
        """
        Detect anomalies in RuuviTag data.

        Args:
            data: RuuviTag data for a single sensor or multiple sensors

        Returns:
            True if anomaly detected, False otherwise
        """

        # Get anomaly thresholds from settings
        thresholds = settings.ruuvitag.anomaly_thresholds

        try:
            if 'temperature' in data:
                temp = self._safe_float(data['temperature'])
                if temp > thresholds["temperature_max"] or temp < thresholds["temperature_min"]:
                    log.warning(f"Anomaly detected: extreme temperature: {temp}°C")
                    return True

            # Check for humidity anomaly
            if 'humidity' in data:
                humidity = self._safe_float(data['humidity'])
                if humidity > thresholds["humidity_max"] or humidity < thresholds["humidity_min"]:
                    log.warning(f"Anomaly detected: invalid humidity: {humidity}%")
                    return True

            # Check for pressure anomaly
            if 'pressure' in data:
                pressure = self._safe_float(data['pressure'])
                if pressure > thresholds["pressure_max"] or pressure < thresholds["pressure_min"]:
                    log.warning(f"Anomaly detected: extreme pressure: {pressure} Pa")
                    return True
            
             # Check for battery anomaly
            if 'battery_voltage' in data:
                battery = self._safe_float(data['battery_voltage'])
                if battery < thresholds["battery_low"] and battery > 0:
                    log.warning(f"Anomaly detected: low battery: {battery}V")
                    return True
            
            return False
        except (ValueError, TypeError) as e:
            log.error(f"Error detecting anomalies: {e}")
            return False
    
    def connect(self):
        """Connect to MQTT broker with retry."""
        max_retries = 5
        retry = 0
        
        while retry < max_retries:
            try:
                self.mqtt_client.connect(self.mqtt_broker, self.mqtt_port, keepalive=60)
                log.info(f"Connecting to MQTT broker {self.mqtt_broker}:{self.mqtt_port}")
                return
            except Exception as e:
                retry += 1
                log.warning(f"Failed to connect to MQTT broker (attempt {retry}/{max_retries}): {e}")
                if retry >= max_retries:
                    log.error(f"Failed to connect to MQTT broker after {max_retries} attempts")
                    raise
                time.sleep(5)  # Wait before retry

    def start(self):
        """Start the adapter"""
        # Connect to MQTT broker
        self.connect()

        # Start MQTT loop in background
        self.mqtt_client.loop_start()

        log.info("RuuviTag adapter started")

    def stop(self):
        """Stop the adapter."""
        # Stop MQTT loop
        self.mqtt_client.loop_stop()
        
        # Disconnect from MQTT broker
        try:
            self.mqtt_client.disconnect()
        except Exception as e:
            log.warning(f"Error disconnecting from MQTT: {e}")
        
        # Close Kafka producer
        try:
            self.kafka_producer.close()
        except Exception as e:
            log.warning(f"Error closing Kafka producer: {e}")
        
        log.info("Improved RuuviTag adapter stopped")

    def run(self):
        """Run the adapter in foreground"""
        try:
            # Start adapter
            self.start()

            # Run until interrupted
            while True:
                # Log periodic status
                active_devices = len(self.devices)
                log.info(f"RuuviTag adapter running with {active_devices} active devices")

                # Sleep for a while
                time.sleep(settings.mqtt.reconnect_interval)
        
        except KeyboardInterrupt:
            log.info("RuuviTag adapter interrupted by user")
        except Exception as e:
            log.error(f"Error in RuuviTag adapter: {e}")
        finally:
            # Stop adapter
            self.stop()


if __name__ == "__main__":
    # Create and run RuuviTag adapter
    adapter = RuuviTagAdapter()
    adapter.run()