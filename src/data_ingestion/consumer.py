import json
import time
import signal
import os
from typing import Dict, Any, Callable, Optional
from confluent_kafka import Consumer, KafkaError, KafkaException

from src.utils.logger import log
from src.config.config import settings

class KafkaConsumer:
    """Kafka Consumer for IoT sensor data."""
    
    def __init__(
        self,
        bootstrap_servers: str = None,
        topic_name: str = None,
        group_id: str = None,
        auto_offset_reset: str = None
    ):
        """Initialize Kafka consumer with configuration."""
        self.bootstrap_servers = bootstrap_servers or settings.kafka.bootstrap_servers
        self.topic_name = topic_name or settings.kafka.topic_name
        self.group_id = group_id or os.getenv("KAFKA_CONSUMER_GROUP_ID", "iot-data-consumer")
        self.auto_offset_reset = auto_offset_reset or os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
        
        # Flag to control consumption loop
        self.running = False
        
        # Consumer configuration
        self.conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': self.auto_offset_reset,
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000
        }
        
        # Create consumer instance
        self.consumer = Consumer(self.conf)
        log.info(f"Kafka consumer initialized with bootstrap servers: {self.bootstrap_servers}")
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, sig, frame):
        """Handle termination signals for graceful shutdown."""
        log.info(f"Caught signal {sig}. Stopping consumer...")
        self.running = False
    
    def subscribe(self) -> None:
        """Subscribe to the Kafka topic."""
        try:
            self.consumer.subscribe([self.topic_name])
            log.info(f"Subscribed to topic: {self.topic_name}")
        except KafkaException as e:
            log.error(f"Error subscribing to topic {self.topic_name}: {str(e)}")
            raise
    
    def process_message(self, message: Dict[str, Any]) -> None:
        """Process a single message from Kafka topic. Override this method to implement custom processing."""
        # Default implementation just logs the message
        device_id = message.get('device_id', 'unknown')
        device_type = message.get('device_type', 'unknown')
        value = message.get('value', 'unknown')
        unit = message.get('unit', '')
        
        log.info(f"Received data from device {device_id} ({device_type}): {value} {unit}")
    
    def consume_batch(self, batch_size: int = 100, timeout: float = 1.0) -> None:
        """Consume a batch of messages from Kafka topic."""
        try:
            self.subscribe()
            
            messages = []
            while len(messages) < batch_size:
                msg = self.consumer.poll(timeout=timeout)
                
                if msg is None:
                    break
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        log.debug(f"Reached end of partition {msg.partition()}")
                    else:
                        log.error(f"Error while consuming message: {msg.error()}")
                    continue
                
                # Parse the message value
                try:
                    message_str = msg.value().decode('utf-8')
                    message = json.loads(message_str)
                    messages.append(message)
                except Exception as e:
                    log.error(f"Error parsing message: {str(e)}")
                    continue
            
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
        Start a continuous consumption loop from Kafka topic.
        
        Args:
            process_fn: Optional function to process each message, defaults to self.process_message
            timeout: Poll timeout in seconds
        """
        try:
            self.subscribe()
            
            # Set processing function
            processor = process_fn if process_fn is not None else self.process_message
            
            # Start consumption loop
            self.running = True
            log.info(f"Starting consumption loop from topic: {self.topic_name}")
            
            while self.running:
                msg = self.consumer.poll(timeout=timeout)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        log.debug(f"Reached end of partition {msg.partition()}")
                    else:
                        log.error(f"Error while consuming message: {msg.error()}")
                    continue
                
                # Parse the message value
                try:
                    message_str = msg.value().decode('utf-8')
                    message = json.loads(message_str)
                    processor(message)
                except Exception as e:
                    log.error(f"Error processing message: {str(e)}")
                    continue
                
        except KafkaException as e:
            log.error(f"Kafka error during consumption loop: {str(e)}")
            raise
        except Exception as e:
            log.error(f"Unexpected error during consumption loop: {str(e)}")
            raise
        finally:
            self.close()
    
    def close(self) -> None:
        """Close the consumer."""
        self.running = False
        if hasattr(self, 'consumer'):
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