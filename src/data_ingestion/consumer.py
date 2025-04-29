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
        self.group_id = group_id or os.getenv("KAFKA_CONSUMER_GROUP_ID", "iot-data-consumer")
        self.auto_offset_reset = auto_offset_reset or os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
        
        # Flag to control consumption loop
        self.running = False
        
        # Consumer configuration with fault tolerance for multi-broker setup
        self.conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': self.auto_offset_reset,
            
            # Commit settings
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000,
            
            # Performance settings
            'fetch.min.bytes': 1,           # Minimum bytes to fetch
            
            # Fault tolerance settings
            'session.timeout.ms': 30000,    # Longer timeout for fault tolerance
            'heartbeat.interval.ms': 10000, # Heartbeat interval
            'max.poll.interval.ms': 300000, # Max time between polls
            
            # Load balancing
            'partition.assignment.strategy': 'cooperative-sticky'  # Better handling of rebalances
        }
        
        # Create consumer instance
        self.consumer = Consumer(self.conf)
        log.info(f"Kafka consumer initialized with bootstrap servers: {self.bootstrap_servers}")
        
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
                self.consumer.commit(asynchronous=False)
                log.info("Final offsets committed successfully")
            except Exception as e:
                log.warning(f"Error committing final offsets: {str(e)}")
                
            self.consumer.close()
            log.info("Kafka consumer closed")