#!/usr/bin/env python3

"""
RuuviTag Data Apapter - Receives data from ESP32 and forwards to Kafka.
"""

import os
import json
import time
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

        # Create Kafka producer for sending data
        self.kafka_producer = KafkaProducer()

        # Store device data for periodic monitoring and debugging
        self.devices = {}

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
            # Decode and parse JSON
            payload = msg.payload.decode('utf-8')
            data = json.loads(payload)
            
            # Process and adapt data for Kafka
            kafka_message = self.adapt_ruuvitag_data(data)

            # Send to Kafka
            if kafka_message:
                self.kafka_producer.send_message(kafka_message)

                # Store device data for monitoring
                device_id = kafka_message.get('device_id')
                if device_id:
                    self.devices[device_id] = {
                        'last_seen': datetime.now(),
                        'data': kafka_message
                    }
                
                log.debug(f"Processed RuuviTag data from device: {kafka_message.get('device_id')}")

        except json.JSONDecodeError as e:
            log.error(f"Failed to parse JSON from MQTT message: {e}")
        except Exception as e:
            log.error(f"Error processing MQTT message: {e}")

    def adapt_ruuvitag_data(self, ruuvitag_data: Dict[str, Any]) -> Dict[str, any]:
        """
        Adapt RuuviTag data to match the Kafka schema

        Args:
            ruuvitag_data: Raw data from RuuviTag via ESP32

        Returns:
            Adapted data matching kafka schema
        """
        # Validate required fields
        required_fields = ['device_id', 'temperature', 'humidity', 'pressure']
        for field in required_fields:
            if field not in ruuvitag_data:
                log.warning(f"Missing required field in RuuviTag data: {field}")
                return None
            
        # Get device ID (MAC address)
        device_id = ruuvitag_data['device_id']

        # Create ISO-8601 timestamp with timezone
        if 'timestamp' in ruuvitag_data:
            try:
                # If timestamp is a unix timestamp (seconds since epoch)
                timestamp = datetime.fromtimestamp(int(ruuvitag_data['timestamp']), tz=timezone.utc)
                iso_timestamp = timestamp.isoformat()
            except (ValueError, TypeError):
                # If already formatted or invalid, use current time
                iso_timestamp = datetime.now(timezone.utc).isoformat()
        else:
            iso_timestamp = datetime.now(timezone.utc).isoformat()

        # Map RuuviTag data to Kafka schema
        kafka_data = {
            "device_id": device_id,
            "device_type": settings.ruuvitag.device_type,
            "timestamp": iso_timestamp,
            "value": float(ruuvitag_data.get('temperature')),
            "unit": "°C",
            "location": settings.ruuvitag.default_location,
            "battery_level": self._calculate_battery_level(float(ruuvitag_data.get('battery_voltage', 0))),
            "signal_strength": settings.ruuvitag.signal_strength,
            "is_anomaly": self._detect_anomaly(ruuvitag_data),
            "firware_version": settings.ruuvitag.firmware_version,
            "metadata": {
                "humidity": str(ruuvitag_data.get('humidity', 0)),
                "pressure": str(ruuvitag_data.get('pressure', 0)),
                "accel_x": str(ruuvitag_data.get('accel_x', 0)),
                "accel_y": str(ruuvitag_data.get('accel_y', 0)),
                "accel_z": str(ruuvitag_data.get('accel_z', 0)),
                "tx_power": str(ruuvitag_data.get('tx_power', 0)),
                "movement_counter": str(ruuvitag_data.get('movement_counter', 0)),
                "measurement_sequence": str(ruuvitag_data.get('measurement_sequence', 0))
                # Note: ESP32 sends 'seq_number' instead of 'measurement_sequence'
                # Adding both for compatibility
                if 'measurement_sequence' in ruuvitag_data
                else str(ruuvitag_data.get('seq_number', 0))
            },
            "status": "ACTIVE",
            "tags": ["ruuvitag", "ble", "temperature", "humidity", "pressure"],
            "maintenance_date": None
        }

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
            data: RuuviTag data

        Returns:
            True if anomaly detected, False otherwise
        """

        # Get anomaly thresholds from settings
        thresholds = settings.ruuvitag.anomaly_thresholds

        try:
            temp = float(data.get('temperature', 0))
            humidity = float(data.get('humidity', 0))
            pressure = float(data.get('pressure', 0))
            battery = float(data.get('battery_voltage', 0))

            # Check for anomalies
            if temp > thresholds["temperature_max"] or temp < thresholds["temperature_min"]:
                log.warning(f"Anomaly detected: extreme temperature: {temp}°C")
                return True
            if humidity > thresholds["humidity_max"] or humidity < thresholds["humidity_min"]:
                log.warning(f"Anomaly detected: invalid humidity: {humidity}%")
                return True
            if pressure > thresholds["pressure_max"] or pressure < thresholds["pressure_min"]:
                log.warning(f"Anomaly detected: extreme pressure: {pressure} Pa")
                return True
            if battery < thresholds["battery_low"] and battery > 0:
                log.warning(f"Anomaly detected: low battery: {battery}V")
                return True
            
            return False
        except (ValueError, TypeError) as e:
            log.error(f"Error detecting anomalies: {e}")
            return False
        
    
    def connect(self):
        """Connect to MQTT broker"""
        try:
            self.mqtt_client.connect(self.mqtt_broker, self.mqtt_port, self.keep_alive)
            log.info(f"Connecting to MQTT broker {self.mqtt_broker}:{self.mqtt_port}")
        except Exception as e:
            log.error(f"Failed to connect to MQTT broker: {e}")
            raise

    def start(self):
        """Start the adapter"""
        # Connect to MQTT broker
        self.connect()

        # Start MQTT loop in background
        self.mqtt_client.loop_start()

        log.info("RuuviTag adapter started")

    def stop(self):
        """Stop the adapter"""
        # Stop MQTT loop
        self.mqtt_client.loop_stop()

        # Disconnect from MQTT broker
        self.mqtt_client.disconnect()

        # Close Kafka producer
        self.kafka_producer.close()

        log.info("RuuviTag adapter stopped")

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