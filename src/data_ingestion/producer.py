import json
import time
from typing import Dict, Any, List
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException

from src.utils.logger import log
from src.config.config import settings

class KafkaProducer:
    """Kafka Producer for IoT sensor data."""
    
    def __init__(self, bootstrap_servers: str = None, topic_name: str = None):
        """Initialize Kafka producer with configuration."""
        self.bootstrap_servers = bootstrap_servers or settings.kafka.bootstrap_servers
        self.topic_name = topic_name or settings.kafka.topic_name
        
        # Producer configuration
        self.conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': 'iot-data-producer',
            'retries': 5,  # Number of retries if sending fails
            'retry.backoff.ms': 500,  # Backoff time between retries
            'message.timeout.ms': 10000  # Time to wait for acknowledgement
        }
        
        # Create producer instance
        self.producer = Producer(self.conf)
        log.info(f"Kafka producer initialized with bootstrap servers: {self.bootstrap_servers}")
        
        # Ensure the topic exists
        self._ensure_topic_exists()
    
    def _ensure_topic_exists(self, num_partitions: int = 3, replication_factor: int = 1) -> None:
        """Ensure that the Kafka topic exists, creating it if necessary."""
        try:
            admin_client = AdminClient({'bootstrap.servers': self.bootstrap_servers})
            
            # Check if topic already exists
            topics = admin_client.list_topics(timeout=10)
            
            if self.topic_name not in topics.topics:
                log.info(f"Topic {self.topic_name} does not exist. Creating it...")
                topic_list = [
                    NewTopic(
                        self.topic_name,
                        num_partitions=num_partitions,
                        replication_factor=replication_factor
                    )
                ]
                
                # Create the topic
                futures = admin_client.create_topics(topic_list)
                
                # Wait for topic creation to complete
                for topic, future in futures.items():
                    try:
                        future.result()
                        log.info(f"Topic {topic} created successfully")
                    except Exception as e:
                        log.error(f"Failed to create topic {topic}: {str(e)}")
            else:
                log.info(f"Topic {self.topic_name} already exists")
        except KafkaException as e:
            log.warning(f"Kafka operation failed: {str(e)}")
            log.info("Will continue and try to use the topic even if we couldn't verify its existence")
    
    def _delivery_report(self, err, msg) -> None:
        """Delivery report callback for produced messages."""
        if err is not None:
            log.error(f"Message delivery failed: {err}")
        else:
            log.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
    
    def send_message(self, message: Dict[str, Any], key: str = None) -> None:
        """Send a single message to Kafka topic."""
        try:
            # Convert the message to JSON string
            message_str = json.dumps(message)
            
            # Use device_id as the key if not specified
            if key is None and 'device_id' in message:
                key = message['device_id']
            
            # Convert key to bytes if it's a string
            key_bytes = key.encode('utf-8') if isinstance(key, str) else None
            
            # Produce the message
            self.producer.produce(
                topic=self.topic_name,
                key=key_bytes,
                value=message_str.encode('utf-8'),
                callback=self._delivery_report
            )
            
            # Serve delivery callback queue
            self.producer.poll(0)
            
        except Exception as e:
            log.error(f"Error sending message to Kafka: {str(e)}")
    
    def send_batch(self, messages: List[Dict[str, Any]]) -> None:
        """Send a batch of messages to Kafka topic."""
        for message in messages:
            self.send_message(message)
        
        # Flush to ensure all messages are sent
        self.producer.flush(timeout=10.0)  # Increased timeout to 10 seconds
        log.info(f"Batch of {len(messages)} messages sent to Kafka topic: {self.topic_name}")
    
    def close(self) -> None:
        """Flush and close the producer."""
        self.producer.flush()
        log.info("Kafka producer closed")