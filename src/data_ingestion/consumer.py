import json
import time
import signal
import os
from typing import Dict, Any, Callable, Optional
from confluent_kafka import Consumer, KafkaError, KafkaException

from src.utils.logger import log
from src.config.config import settings

class KafkaConsumer:
    """
    Kafka Consumer for IoT sensor data with multi-broker cluster support.
    
    This class handles the connection to a Kafka cluster, subscribers to topics, and
    processes messages. It includes fault tolerance for broker failures and
    rebalancing in a multi-cultural environment.
    """
    def __init__(
        self,
        bootstrap_servers: str = None,
        topic_name: str = None,
        group_id: str = None,
        auto_offset_reset: str = None
    ):
        """
        Initialize Kafka consumer with configuration.
        
        Args:
            bootstrap_servers: Comma-separated list of broker addresses (host:port)
            topic_name: Name of the Kafka topic to consume message from
            group_id: Consumer group ID for load balancing
            auto_offset_reset: Strategy for consuming messages ('earliest' or 'latest')
        """
        self.bootstrap_servers = bootstrap_servers or settings.kafka.bootstrap_servers
        self.topic_name = topic_name or settings.kafka.topic_name
        self.group_id = group_id or settings.kafka.group_id
        self.auto_offset_reset = auto_offset_reset or settings.kafka.auto_offset_reset
        
        # Flag to control consumption loop
        self.running = False
        
        # Consumer configuration
        self.conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': self.auto_offset_reset,

            # Commit settings
            'enable.auto.commit': True, # Automatically commit offsets
            'auto.commit.interval.ms': 5000, # Commit every 5 seconds

            # Performance settings
            'fetch.min.bytes': 1, # Minimum bytest to fetch

            # Fault tolerance settings
            'session.timeout.ms': 30000, # Longer timeout for fault tolerance
            'heartbeat.interval.ms': 10000, # Heartbeat interval
            'max.poll.interval.ms': 300000, # Max time between polls

            # Load balancing
            'partition.assignment.strategy': 'cooperative-sticky' # Better handling of rebalances
        }
        
        # Create Kafka consumer instance
        self.consumer = Consumer(self.conf)
        log.info(f"Kafka consumer initialized with bootstrap servers: {self.bootstrap_servers}")
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, sig, frame):
        """
        Graceful shutdown on SIGINT or SIGTERM.
        
        Args:
            sig: Signal numebr
            frame: Current stack frame
        """
        log.info(f"Caught signal {sig}. Stopping consumer...")
        self.running = False
    
    def subscribe(self) -> None:
        """
        Subscribe to the configured Kafka topic.

        In a multi-broker environment, subscription triggers a rebalance to assign
        partitions among consumers in the group
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
    
    def consume_batch(self, batch_size: int = 100, timeout: float = 1.0) -> None:
        """Consume a batch of messages from Kafka topic.

        This method polls for a batch of messags and processes them.
        
        Args:
            batch_size: Number of messages to consume before processing
            timeout: Poll timeout per message

        Return:
            List of processes messages
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
                
                # Skip message if it has errors:
                if not self._handle_message_errors(msg):
                    continue

                # Parse the message value
                try:
                    message_str = msg.value().decode('utf-8')
                    message = json.loads(message_str)
                    messages.append(message)
                except Exception as e:
                    log.error(f"Error parsing message: {str(e)}")
                    continue
                
            # Process the batch of messages
            if messages:
                log.info(f"Consumed {len(messages)} messages from topic: {self.topic_name}")
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
        Continuously consume messages from Kafka topic.
        
        This method continuously polls for messags and processes them until the consumer is stopped.
        
        Args:
            process_fn: Optional function to process each message, defaults to self.process_message
            timeout: Poll timeout for each message in seconds
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
                    message_str = msg.value().decode('utf-8')
                    message = json.loads(message_str)
                    processor(message)

                except Exception as e:
                    log.error(f"Error processing message: {str(e)}")
                    continue
                
        except KafkaException as e:
            log.error(f"Kafka error during consumption loop: {str(e)}")
            # if connection to a broker fails, other may still be available
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
            # Final comit of offsets before closing
            try:
                self.consumer.commit(asynchronous=False)
                log.info("final offsets committed successfully")
            except Exception as e:
                log.warning(f"Error committing final offsets: {str(e)}")

            self.consumer.close()
            log.info("Kafka consumer closed")

# For testing
if __name__ == "__main__":
    # Create custom processor class by inheritance
    class CustomProcessor(KafkaConsumer):
        def process_message(self, message):
            # Override with custom processing logic
            device_id = message.get('device_id', 'unknown')
            device_type = message.get('device_type', 'unknown')
            value = message.get('value', 'unknown')
            timestamp = message.get('timestamp', 'unknown')
            
            if device_type == "temperature" and value > 30:
                log.warning(f"HIGH TEMPERATURE ALERT: Device {device_id} reported {value}°C at {timestamp}")
            elif device_type == "motion" and value == 1:
                log.info(f"MOTION DETECTED: Device {device_id} detected motion at {timestamp}")
            else:
                log.info(f"Device {device_id} ({device_type}): {value}")
    
    # Create consumer with custom processing
    consumer = CustomProcessor()
    
    try:
        # Run the consumption loop
        consumer.consume_loop(timeout=1.0)
    except KeyboardInterrupt:
        log.info("Consumer stopped by user")
    finally:
        consumer.close()
