import time
import signal
import os
from typing import Dict, Any, Callable, Optional, List
from confluent_kafka import Consumer, KafkaError, KafkaException

from src.utils.logger import log
from src.config.config import settings
from src.utils.schema_registry import schema_registry

class KafkaConsumer:
    """
    Kafka Consumer for IoT sensor data with Avro deserialization and Schema Registry.
    
    This class handles the connection to a Kafka cluster, subscribes to topics,
    and processes messages. It includes fault tolerance for broker failures
    and rebalancing in a multi-broker environment.
    """
    
    def __init__(
        self,
        bootstrap_servers: str = None,
        topic_name: str = None,
        group_id: str = None,
        auto_offset_reset: str = None
    ):
        """
        Initialize Kafka consumer with configuration for a multi-broker environment.
        
        Args:
            bootstrap_servers: Comma-separated list of broker addresses (host:port)
            topic_name: Name of the Kafka topic to consume messages from
            group_id: Consumer group ID for load balancing
            auto_offset_reset: Strategy for consuming messages ('earliest' or 'latest')
        """
        self.bootstrap_servers = bootstrap_servers or settings.kafka.bootstrap_servers
        self.topic_name = topic_name or settings.kafka.topic_name
        self.group_id = group_id or settings.kafka.consumer_group_id
        self.auto_offset_reset = auto_offset_reset or settings.kafka.auto_offset_reset
        
        # Flag to control consumption loop
        self.running = False
        
        # Get consumer configuration from settings
        self.conf = settings.consumer.get_config(
            group_id=self.group_id,
            auto_offset_reset=self.auto_offset_reset
        )
        
        # Override bootstrap servers if provided
        if bootstrap_servers:
            self.conf['bootstrap.servers'] = bootstrap_servers
        
        # Create consumer instance
        self.consumer = Consumer(self.conf)
        log.info(f"Kafka consumer initialized with bootstrap servers: {self.bootstrap_servers}")
        log.debug(f"Consumer configuration: {self.conf}")
        
        # Schema Registry
        self.schema_registry_client = schema_registry
        log.info("Schema Registry client initialized for consumer")
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, sig, frame):
        """
        Handle termination signals for graceful shutdown.
        
        Args:
            sig: Signal number
            frame: Current stack frame
        """
        log.info(f"Caught signal {sig}. Stopping consumer...")
        self.running = False
    
    def subscribe(self) -> None:
        """
        Subscribe to the Kafka topic.
        
        In a multi-broker environment, subscription triggers a rebalance
        to assign partitions among consumers in the group.
        """
        try:
            self.consumer.subscribe([self.topic_name], on_assign=self._on_assign_callback)
            log.info(f"Subscribed to topic: {self.topic_name}")
        except KafkaException as e:
            log.error(f"Error subscribing to topic {self.topic_name}: {str(e)}")
            raise
    
    def _on_assign_callback(self, consumer, partitions):
        """
        Callback executed when partitions are assigned to this consumer.
        
        Args:
            consumer: The consumer instance
            partitions: List of TopicPartition objects assigned
        """
        if partitions:
            partition_info = [f"{p.topic}[{p.partition}]" for p in partitions]
            log.info(f"Assigned partitions: {', '.join(partition_info)}")
        else:
            log.warning("No partitions assigned to this consumer")
    
    def process_message(self, message: Dict[str, Any]) -> None:
        """
        Process a single message from Kafka topic.
        
        This is a base implementation that should be overridden in subclasses
        for custom processing logic.
        
        Args:
            message: Dictionary containing the message data
        """
        # Default implementation just logs the message
        device_id = message.get('device_id', 'unknown')
        device_type = message.get('device_type', 'unknown')
        value = message.get('value', 'unknown')
        unit = message.get('unit', '')
        
        log.info(f"Received data from device {device_id} ({device_type}): {value} {unit}")
    
    def _handle_message_errors(self, msg):
        """
        Handle errors in Kafka messages.
        
        Args:
            msg: Kafka message object
            
        Returns:
            True if the message was processed, False if it had errors
        """
        if msg is None:
            return False
        
        if msg.error():
            # End of partition event - not an error
            if msg.error().code() == KafkaError._PARTITION_EOF:
                log.debug(f"Reached end of partition {msg.partition()}")
            # Broker connection error
            elif msg.error().code() == KafkaError._TRANSPORT:
                log.warning(f"Broker connection error: {msg.error()}. Will reconnect automatically.")
            # Other errors
            else:
                log.error(f"Error while consuming message: {msg.error()}")
            return False
        
        return True
    
    def consume_batch(self, batch_size: int = 100, timeout: float = 1.0) -> List[Dict[str, Any]]:
        """
        Consume a batch of messages from Kafka topic.
        
        This method polls for a batch of messages and processes them.
        
        Args:
            batch_size: Maximum number of messages to consume in one batch
            timeout: Poll timeout in seconds
            
        Returns:
            List of processed messages
        """
        try:
            # Make sure we're subscribed
            if not self.consumer.assignment():
                self.subscribe()
            
            messages = []
            start_time = time.time()
            
            # Poll for messages until we get a batch or timeout
            while len(messages) < batch_size and (time.time() - start_time) < (timeout * 5):
                msg = self.consumer.poll(timeout=timeout)
                
                # Skip message if it has errors
                if not self._handle_message_errors(msg):
                    continue
                
                # Deserialize the message value using Avro and Schema Registry
                try:
                    # Deserialization context
                    value_bytes = msg.value()
                    
                    # Deserialize with Schema Registry
                    message = self.schema_registry_client.deserialize_sensor_reading(
                        value_bytes, 
                        self.topic_name
                    )
                    
                    messages.append(message)
                except Exception as e:
                    log.error(f"Error deserializing message: {str(e)}")
                    continue
            
            # Process the batch of messages
            if messages:
                log.info(f"Consumed {len(messages)} Avro-serialized messages from topic: {self.topic_name}")
                for message in messages:
                    self.process_message(message)
            
            return messages
            
        except KafkaException as e:
            log.error(f"Kafka error during batch consumption: {str(e)}")
            raise
        except Exception as e:
            log.error(f"Unexpected error during batch consumption: {str(e)}")
            raise
    
    def consume_loop(self, process_fn: Optional[Callable[[Dict[str, Any]], None]] = None, timeout: float = 1.0) -> None:
        """
        Start a continuous consumption loop from Kafka topic.
        
        This method continuously polls for messages and processes them
        until the consumer is stopped.
        
        Args:
            process_fn: Optional function to process each message, defaults to self.process_message
            timeout: Poll timeout in seconds
        """
        try:
            # Make sure we're subscribed
            self.subscribe()
            
            # Set processing function
            processor = process_fn if process_fn is not None else self.process_message
            
            # Start consumption loop
            self.running = True
            log.info(f"Starting consumption loop from topic: {self.topic_name}")
            
            # Main consumption loop
            while self.running:
                msg = self.consumer.poll(timeout=timeout)
                
                # Skip message if it has errors
                if not self._handle_message_errors(msg):
                    continue
                
                # Parse and process the message
                try:
                    # Deserialize with Schema Registry
                    value_bytes = msg.value()
                    message = self.schema_registry_client.deserialize_sensor_reading(
                        value_bytes,
                        self.topic_name
                    )
                    
                    # Process the deserialized message
                    processor(message)
                    
                except Exception as e:
                    log.error(f"Error processing message: {str(e)}")
                    continue
                
        except KafkaException as e:
            log.error(f"Kafka error during consumption loop: {str(e)}")
            # If connection to a broker fails, others may still be available
            # Try to continue if possible
            if not self.running:
                raise
        except Exception as e:
            log.error(f"Unexpected error during consumption loop: {str(e)}")
            raise
        finally:
            self.close()
    
    def close(self) -> None:
        """
        Close the consumer.
        
        This ensures that offsets are committed and the consumer
        leaves the group cleanly.
        """
        self.running = False
        if hasattr(self, 'consumer'):
            # Final commit of offsets before closing
            try:
                # Use auto commit interval from config or default to 5 seconds for final commit timeout
                commit_timeout = self.conf.get('auto.commit.interval.ms', 5000) / 1000
                self.consumer.commit(asynchronous=False)
                log.info("Final offsets committed successfully")
            except Exception as e:
                log.warning(f"Error committing final offsets: {str(e)}")
                
            self.consumer.close()
            log.info("Kafka consumer closed")

class IoTAlertConsumer(KafkaConsumer):
    """
    Custom consumer that processes IoT data with Avro deserialization and generates alerts.
    This implementation is aware of the multi-broker setup and 
    includes logic to handle broker failovers.
    """
    
    def process_message(self, message):
        """
        Process a received IoT sensor reading and generate alerts 
        based on specified thresholds.
        
        Args:
            message: Dictionary containing the deserialized Avro IoT sensor reading
        """
        device_id = message.get('device_id', 'unknown')
        device_type = message.get('device_type', 'unknown')
        value = message.get('value', 'unknown')
        unit = message.get('unit', '')
        timestamp = message.get('timestamp', 'unknown')
        firmware_version = message.get('firmware_version', 'unknown')
        is_anomaly = message.get('is_anomaly', False)
        status = message.get('status', 'UNKNOWN')  # Get status from updated schema
        tags = message.get('tags', [])  # Get tags from updated schema

        # Get metadata
        metadata = message.get('metadata', {})

        # Extract additional sensor data from metadata for enhanced logging
        humidity = metadata.get('humidity', 'N/A')
        pressure = metadata.get('pressure', 'N/A')
        battery_level = message.get('battery_level', 'N/A')

        # Format numeric values nicely
        if isinstance(value, (int, float)):
            formatted_value = f"{value:.2f}"
        else:
            formatted_value = str(value)
        
        # Enhanced logging for Avro-deserialized messages
        if is_anomaly:
            log.warning(f"[ANOMALY] Device: {device_id} | Type: {device_type} | TEMPERATURE: {formatted_value}{unit} | "
                      f"Time: {timestamp} | Status: {status}")
        
        # Check for anomalies or specific conditions
        try:            
            # Process based on device type
            if device_type.lower() in ["ruuvitag", "temperature"]:
                #Process temperature readings
                if isinstance(value, (int, float)):
                    if value > settings.ruuvitag.anomaly_thresholds.get("temerature_max", 50):
                        log.warning(f"HIGH TEMPERATURE ALERT: Device {device_id} | Reading: {formatted_value}{unit} | Time: {timestamp}")
                    elif value < settings.ruuvitag.anomaly_thresholds.get("temperature_min", -50):
                        log.warning(f"LOW TEMPERATURE ALERT: Device {device_id} | Reading: {formatted_value}{unit} | Time: {timestamp}")
                    else:
                        # Normal temperature reading
                        log.info(f"Device: {device_id} ({device_type}) | Temperature: {formatted_value}{unit} | "
                                f"Humidity: {humidity}% | Pressure: {pressure} Pa | Battery: {battery_level}% | "
                                f"Metadata: {metadata}")
                else:
                    log.info(f"Device: {device_id} ({device_type}) | Reading: {formatted_value}{unit} | Status: {status}")
                
            # Process humidity if available
            elif device_type.lower() == "humidity" or (device_type.lower() == "ruuvitag" and humidity != 'N/A'):
                try:
                    humidity_val = float(humidity)
                    if humidity_val > settings.ruuvitag.anomaly_thresholds.get("humidity_max", 100):
                        log.warning(f"HIGH HUMIDITY ALERT: Device {device_id} | Reading: {humidity}% | Time: {timestamp}") 
                    elif humidity_val < settings.ruuvitag.anomaly_thresholds.get("humidity_min", 15):
                        log.warning(f"LOW HUMIDITY ALERT: Device {device_id} | Reading: {humidity}% | Time: {timestamp}")
                    else:
                        log.info(f"Device: {device_id} ({device_type}) | Humidity: {humidity}% | "
                                f"Temperature: {formatted_value}{unit} | Status: {status}")
                except (ValueError, TypeError):
                    pass

            # Process pressure if available
            elif device_type.lower() == "pressure" or (device_type.lower() == "ruuvitag" and pressure != 'N/A'):
                try:
                    pressure_val = float(pressure)
                    if pressure_val > settings.ruuvitag.anomaly_thresholds.get("pressure_max", 108500):
                        log.warning(f"HIGH PRESSURE ALERT: Device {device_id} | Reading: {pressure}% | Time: {timestamp}") 
                    elif pressure_val < settings.ruuvitag.anomaly_thresholds.get("pressure_min", 87000):
                        log.warning(f"LOW PRESSURE ALERT: Device {device_id} | Reading: {pressure}% | Time: {timestamp}")
                    else:
                        log.info(f"Device: {device_id} ({device_type}) | Pressure: {pressure}% | "
                                f"Temperature: {formatted_value}{unit} | Status: {status}")
                except (ValueError, TypeError):
                    pass

            # Handle motion detection
            elif device_type == "motion" and value == 1:
                log.info(f"MOTION DETECTED: Device {device_id} | Time: {timestamp}")

            # Handle light level
            elif device_type == "light" and float(value) < 10:
                log.info(f"LOW LIGHT LEVEL: Device {device_id} | Reading: {value}{unit} | Time: {timestamp}")

            # Check for error status
            elif status == "ERROR":
                log.warning(f"ERROR ALERT: Device {device_id} | Type: {device_type} | Status: {status}")
            
            # Default case for other device types or when no specific condition is met
            else:
                # Include firmware version and metadata in standard logs
                metadata_items = []
                for k, v in metadata.items():
                    if k not in ['humidity', 'pressure']:  # Skip items we already displayed
                        metadata_items.append(f"{k}: {v}")
                metadata_str = ", ".join(metadata_items) if metadata_items else "none"

                # Include tags in logs if present
                tags_str = f"[{', '.join(tags)}]" if tags else ""

                # Standard log
                log.info(f"Device {device_id} | Type: ({device_type} v{firmware_version}) | "
                        f"Reading: {formatted_value}{unit} | Status: {status} | "
                        f"Tags: {tags_str} | Metadata: {metadata_str}")
                
        except (ValueError, TypeError) as e:
            # Handle case where value conversion fails
            log.error(f"Error processing value from device {device_id}: {str(e)}")
            log.info(f"Raw message: {message}")