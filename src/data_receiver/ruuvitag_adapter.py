#!/usr/bin/env python3

"""
RuuviTag Data Adapter with Prometheus Metrics - Real-time IoT data producer.
This is the actual production service that handles real IoT sensor data.
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
from src.utils.metrics import get_metrics_instance, MetricsServer, timed_operation


class MetricsAwareRuuviTagAdapter:
    """
    Enhanced RuuviTag adapter with comprehensive Prometheus metrics integration.
    
    This class bridges ESP32 gateway with Kafka pipeline while providing
    detailed observability into the real-time IoT data flow.
    """

    def __init__(
            self,
            mqtt_broker: str = None,
            mqtt_port: int = None,
            mqtt_topic: str = None,
            client_id: str = None
    ):
        """
        Initialize the RuuviTag Adapter with metrics support.

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

        # Initialize metrics
        self.metrics = get_metrics_instance("adapter")
        
        # Start metrics server
        metrics_port = int(os.getenv("METRICS_PORT", "8002"))
        self.metrics_server = MetricsServer(port=metrics_port)
        self.metrics_server.start()
        log.info(f"Metrics server started on port {metrics_port}")

        # Setup MQTT client
        self.mqtt_client = mqtt.Client(client_id=self.client_id)
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.on_disconnect = self.on_disconnect

        # Setup authentication if configured
        if settings.mqtt.username and settings.mqtt.password:
            self.mqtt_client.username_pw_set(settings.mqtt.username, settings.mqtt.password)

        # Create Kafka producer for sending data with exception handling
        try:
            self.kafka_producer = KafkaProducer()
            log.info("Kafka producer initialized successfully")
            self.metrics.set_connection_status(True, "kafka")
        except Exception as e:
            log.error(f"Failed to initialize Kafka producer: {str(e)}")
            self.metrics.set_connection_status(False, "kafka")
            self.metrics.record_message_failed(error_type=type(e).__name__)
            raise

        # Store device data for periodic monitoring and debugging
        self.devices = {}
        self.message_count = 0
        self.last_metrics_log = time.time()

        # Initialize connection status
        self.metrics.set_connection_status(False, "mqtt")  # Will be set to True on successful connect

        log.info(f"RuuviTag adapter with metrics initialized: {self.mqtt_broker}:{self.mqtt_port}")

    def on_connect(self, client, userdata, flags, rc):
        """
        Callback when connected to MQTT broker with metrics tracking.
        
        Args:
            client: MQTT client instance
            userdata: User data
            flags: Connection flags
            rc: Connection result code
        """

        if rc == 0:
            log.info(f"Connected to MQTT broker: {self.mqtt_broker}")
            self.metrics.set_connection_status(True, "mqtt")
            
            # Subscribe to RuuviTag data topic
            client.subscribe(self.mqtt_topic, qos=self.qos)
            log.info(f"Subscribed to topic: {self.mqtt_topic} with QoS {self.qos}")
            
        else:
            log.error(f"Failed to connect to MQTT broker, return code: {rc}")
            self.metrics.set_connection_status(False, "mqtt")
            self.metrics.record_message_failed(error_type="mqtt_connection_failed")

    def on_disconnect(self, client, userdata, rc):
        """
        Callback when disconnected from MQTT broker.
        """
        log.warning(f"Disconnected from MQTT broker with code: {rc}")
        self.metrics.set_connection_status(False, "mqtt")

    @timed_operation(get_metrics_instance("adapter"), "mqtt_message_processing")
    def on_message(self, client, userdata, msg):
        """
        Callback when message is received from MQTT broker with comprehensive metrics.

        Args:
            client: MQTT client instance
            userdata: User data
            msg: MQTT message
        """
        message_start_time = time.time()
        
        try:
            # Record message received
            self.metrics.record_mqtt_message_received(topic=msg.topic)
            self.metrics.record_message_received()
            
            # Log raw message for debugging (less frequently to avoid log flooding)
            log.debug(f"Raw MQTT message received on {msg.topic}: {msg.payload}")

            # Decode and parse JSON
            payload = msg.payload.decode('utf-8')
            
            try:
                data = json.loads(payload)
            except json.JSONDecodeError as e:
                log.error(f"Failed to parse JSON from MQTT message: {e}")
                self.metrics.record_validation_failure("json_decode_error")
                self.metrics.record_message_failed(error_type="json_decode_error")
                return

            self.message_count += 1
            
            # Process and adapt data for Kafka with metrics
            kafka_messages = self.adapt_ruuvitag_data_with_metrics(data)

            # Send each message to Kafka with detailed metrics
            if kafka_messages:
                log.debug(f"Sending {len(kafka_messages)} separate sensor readings to Kafka")
                
                successful_sends = 0
                for kafka_message in kafka_messages:
                    try:
                        send_start = time.time()
                        self.kafka_producer.send_message(kafka_message)
                        send_duration = time.time() - send_start
                        
                        # Record successful send metrics
                        self.metrics.record_message_sent(topic=settings.kafka.topic_name)
                        self.metrics.record_send_duration(send_duration, topic=settings.kafka.topic_name)
                        self.metrics.record_message_processed()
                        successful_sends += 1
                        
                    except Exception as e:
                        log.error(f"Failed to send Kafka message: {e}")
                        self.metrics.record_send_failure(
                            topic=settings.kafka.topic_name,
                            error_type=type(e).__name__
                        )
                        self.metrics.record_message_failed(error_type=type(e).__name__)

                # Update device tracking and metrics
                if kafka_messages:
                    parent_device_id = kafka_messages[0].get('device_metadata', {}).get('parent_device', 'unknown')
                    device_id = kafka_messages[0].get('device_id', 'unknown')
                    
                    # Store in devices dictionary
                    if device_id:
                        self.devices[device_id] = {
                            'last_seen': datetime.now(),
                            'data': kafka_messages[0],
                            'parent_device': parent_device_id
                        }
                    
                    # Update metrics for unique devices seen
                    unique_parent_devices = len(set(
                        device_data.get('parent_device', 'unknown') 
                        for device_data in self.devices.values()
                    ))
                    self.metrics.set_ruuvitag_devices_count(unique_parent_devices)
                
                log.debug(f"Successfully processed {successful_sends}/{len(kafka_messages)} sensor readings")
                
            else:
                log.warning(f"Failed to adapt message: {payload[:100]}...")
                self.metrics.record_validation_failure("adaptation_failed")
                self.metrics.record_message_failed(error_type="adaptation_failed")

            # Record overall processing time
            total_processing_time = time.time() - message_start_time
            self.metrics.record_transformation_duration(total_processing_time)
            
            # Periodic metrics logging (every 50 messages to avoid spam)
            if self.message_count % 50 == 0:
                log.info(f"Processed message #{self.message_count} in {total_processing_time:.3f}s")
                self.log_periodic_metrics()

        except Exception as e:
            log.error(f"Error processing MQTT message: {e}")
            log.error(traceback.format_exc())
            self.metrics.record_message_failed(error_type=type(e).__name__)

    @timed_operation(get_metrics_instance("adapter"), "data_adaptation")
    def adapt_ruuvitag_data_with_metrics(self, ruuvitag_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Adapt RuuviTag data to match the Kafka schema with comprehensive metrics tracking.

        Args:
            ruuvitag_data: Raw data from RuuviTag via ESP32

        Returns:
            List of adapted data matching Kafka schema, one entry per sensor type
        """

        try:
            # Log the input for debugging
            log.debug(f"Adapting data: {ruuvitag_data}")

            # Validate required fields
            required_fields = ['device_id']
            for field in required_fields:
                if field not in ruuvitag_data:
                    log.warning(f"Missing required field in RuuviTag data: {field}")
                    self.metrics.record_validation_failure("missing_device_id")
                    return []
                
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

            # Define sensor mapping
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
                    "field": "acceleration_x",
                    "device_type": "acceleration_sensor",
                    "unit": "g",
                    "tags": ["ruuvitag", "ble", "acceleration", "x-axis"],
                    "metadata_extra": {"axis": "x", "sensor_type": "acceleration"}
                },
                {
                    "field": "acceleration_y",
                    "device_type": "acceleration_sensor",
                    "unit": "g",
                    "tags": ["ruuvitag", "ble", "acceleration", "y-axis"],
                    "metadata_extra": {"axis": "y", "sensor_type": "acceleration"}
                },
                {
                    "field": "acceleration_z",
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
            ]

            # Process each sensor mapping with validation
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
                    if "metadata_extra" in mapping:
                        device_metadata.update(mapping["metadata_extra"])

                    # Detect anomalies and update metrics
                    is_anomaly = self._detect_anomaly({field: value})
                    if is_anomaly:
                        self.metrics.record_anomaly_detected(
                            device_type=mapping["device_type"]
                        )

                    # Create message for this sensor
                    sensor_message = {
                        "device_id": sensor_device_id,
                        "device_type": mapping["device_type"],
                        "value": value,
                        "unit": mapping["unit"],
                        "is_anomaly": is_anomaly,
                        "device_metadata": device_metadata,
                        "tags": mapping["tags"],
                    }

                    # Add common properties
                    sensor_message.update(common_properties)

                    # Validate the final message
                    if self._validate_sensor_message(sensor_message):
                        kafka_messages.append(sensor_message)
                    else:
                        self.metrics.record_validation_failure("invalid_sensor_message")

            return kafka_messages
        
        except Exception as e:
            log.error(f"Error adapting RuuviTag data: {e}")
            log.error(traceback.format_exc())
            self.metrics.record_message_failed(error_type=type(e).__name__)
            return []

    def _validate_sensor_message(self, message: Dict[str, Any]) -> bool:
        """
        Validate a sensor message before sending to Kafka.
        
        Args:
            message: Sensor message to validate
            
        Returns:
            True if valid, False otherwise
        """
        required_fields = ['device_id', 'device_type', 'unit', 'timestamp']
        
        for field in required_fields:
            if field not in message or message[field] is None:
                log.warning(f"Missing required field: {field}")
                self.metrics.record_validation_failure(f"missing_{field}")
                return False
        
        return True

    def _get_timestamp(self, ruuvitag_data: Dict[str, Any]) -> str:
        """
        Extract or generate ISO-8601 timestamp from RuuviTag data.

        Args:
            ruuvitag_data: Raw data from RuuviTag
        
        Returns:
            ISO-8601 formatted timestamp string
        """
        if 'timestamp' in ruuvitag_data:
            try:
                ts_value = ruuvitag_data['timestamp']
                if isinstance(ts_value, str) and ts_value.isdigit():
                    ts_value = int(ts_value)
                
                if isinstance(ts_value, (int, float)):
                    if ts_value < 10000000:  # Relative time
                        return datetime.now(timezone.utc).isoformat()
                    else:
                        timestamp = datetime.fromtimestamp(ts_value, tz=timezone.utc)
                        return timestamp.isoformat()
                elif isinstance(ts_value, str) and "T" in ts_value:
                    return ts_value
                else:
                    return datetime.now(timezone.utc).isoformat()
            except Exception as e:
                log.warning(f"Failed to parse timestamp '{ruuvitag_data['timestamp']}': {e}")
                return datetime.now(timezone.utc).isoformat()
        else:
            return datetime.now(timezone.utc).isoformat()

    def _safe_float(self, value: Any) -> float:
        """Safely convert a value to float with fallback."""
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0

    def _calculate_battery_level(self, voltage: float) -> float:
        """
        Calculate battery level percentage from voltage.

        Args:
            voltage: Battery voltage

        Returns:
            Battery level as percentage (0-100)
        """
        if not voltage or voltage < 1.8:
            return 0.0
        
        min_voltage = settings.ruuvitag.battery["min_voltage"]
        max_voltage = settings.ruuvitag.battery["max_voltage"]

        if voltage >= max_voltage:
            return 100.0
        elif voltage <= min_voltage:
            return 0.0
        else:
            percentage = (voltage - min_voltage) / (max_voltage - min_voltage) * 100.0
            return round(percentage, 2)
        
    def _detect_anomaly(self, data: Dict[str, Any]) -> bool:
        """
        Detect anomalies in RuuviTag data with metrics tracking.

        Args:
            data: RuuviTag data for a single sensor

        Returns:
            True if anomaly detected, False otherwise
        """

        thresholds = settings.ruuvitag.anomaly_thresholds

        try:
            if 'temperature' in data:
                temp = self._safe_float(data['temperature'])
                if temp > thresholds["temperature_max"] or temp < thresholds["temperature_min"]:
                    log.warning(f"Anomaly detected: extreme temperature: {temp}°C")
                    return True

            if 'humidity' in data:
                humidity = self._safe_float(data['humidity'])
                if humidity > thresholds["humidity_max"] or humidity < thresholds["humidity_min"]:
                    log.warning(f"Anomaly detected: invalid humidity: {humidity}%")
                    return True

            if 'pressure' in data:
                pressure = self._safe_float(data['pressure'])
                if pressure > thresholds["pressure_max"] or pressure < thresholds["pressure_min"]:
                    log.warning(f"Anomaly detected: extreme pressure: {pressure} Pa")
                    return True
            
            if 'battery_voltage' in data:
                battery = self._safe_float(data['battery_voltage'])
                if battery < thresholds["battery_low"] and battery > 0:
                    log.warning(f"Anomaly detected: low battery: {battery}V")
                    return True
            
            return False
        except (ValueError, TypeError) as e:
            log.error(f"Error detecting anomalies: {e}")
            return False

    def log_periodic_metrics(self):
        """Log periodic status and metrics information."""
        current_time = time.time()
        time_since_last = current_time - self.last_metrics_log
        
        if time_since_last >= 60:  # Log every minute
            active_devices = len(self.devices)
            unique_parent_devices = len(set(
                device_data.get('parent_device', 'unknown') 
                for device_data in self.devices.values()
            ))
            
            log.info(f"Metrics Summary: {active_devices} total sensors from {unique_parent_devices} RuuviTag devices")
            log.info(f"Message processing rate: {self.message_count / time_since_last:.2f} messages/second")
            
            self.last_metrics_log = current_time

    def connect(self):
        """Connect to MQTT broker with retry and metrics."""
        max_retries = 5
        retry = 0
        
        while retry < max_retries:
            try:
                self.mqtt_client.connect(self.mqtt_broker, self.mqtt_port, keepalive=self.keep_alive)
                log.info(f"Connecting to MQTT broker {self.mqtt_broker}:{self.mqtt_port}")
                return
            except Exception as e:
                retry += 1
                log.warning(f"Failed to connect to MQTT broker (attempt {retry}/{max_retries}): {e}")
                self.metrics.record_message_failed(error_type="mqtt_connection_error")
                
                if retry >= max_retries:
                    log.error(f"Failed to connect to MQTT broker after {max_retries} attempts")
                    self.metrics.set_connection_status(False, "mqtt")
                    raise
                time.sleep(5)

    def start(self):
        """Start the adapter with metrics monitoring."""
        # Connect to MQTT broker
        self.connect()

        # Start MQTT loop in background
        self.mqtt_client.loop_start()

        log.info("RuuviTag adapter with metrics started successfully")

    def stop(self):
        """Stop the adapter and cleanup metrics."""
        # Stop MQTT loop
        self.mqtt_client.loop_stop()
        
        # Disconnect from MQTT broker
        try:
            self.mqtt_client.disconnect()
            self.metrics.set_connection_status(False, "mqtt")
        except Exception as e:
            log.warning(f"Error disconnecting from MQTT: {e}")
        
        # Close Kafka producer
        try:
            self.kafka_producer.close()
            self.metrics.set_connection_status(False, "kafka")
        except Exception as e:
            log.warning(f"Error closing Kafka producer: {e}")
        
        # Stop metrics server
        try:
            self.metrics_server.stop()
        except Exception as e:
            log.warning(f"Error stopping metrics server: {e}")
        
        log.info("RuuviTag adapter with metrics stopped")

    def run(self):
        """Run the adapter in foreground with metrics monitoring."""
        try:
            # Start adapter
            self.start()

            log.info("RuuviTag adapter running with comprehensive metrics monitoring")
            log.info(f"Metrics available at: http://localhost:{os.getenv('METRICS_PORT', '8002')}/metrics")

            # Main monitoring loop
            while True:
                # Log periodic status with enhanced metrics
                self.log_periodic_metrics()
                
                # Sleep for monitoring interval
                time.sleep(settings.mqtt.reconnect_interval)
        
        except KeyboardInterrupt:
            log.info("RuuviTag adapter interrupted by user")
        except Exception as e:
            log.error(f"Error in RuuviTag adapter: {e}")
            self.metrics.record_message_failed(error_type=type(e).__name__)
        finally:
            # Stop adapter
            self.stop()


# For backwards compatibility, create an alias
RuuviTagAdapter = MetricsAwareRuuviTagAdapter


if __name__ == "__main__":
    # Create and run RuuviTag adapter with metrics
    adapter = MetricsAwareRuuviTagAdapter()
    adapter.run()