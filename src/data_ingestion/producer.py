import json
import time
from typing import Dict, Any, List
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException

from src.utils.logger import log
from src.config.config import settings

class KafkaProducer:
    """
    Kafka Producer for IoT sensor data with multi-broker cluster support

    This class handles the connection to a Kafka cluster, ensure the target
    topics exists with proper replication, and provides methods to publish
    IoT sensor data to the Kafka topic.
    """
    
    def __init__(self, bootstrap_servers: str = None, topic_name: str = None):
        """
        Initialize Kafka producer with configuration.
        
        Args:
            bootstrap_servers: Comma-separated list of broker addresses (host:port)
            topic_name: Name of the Kafka topic to produce message to
        """
        self.bootstrap_servers = bootstrap_servers or settings.kafka.bootstrap_servers
        self.topic_name = topic_name or settings.kafka.topic_name
        
        # Producer configuration
        self.conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': 'iot-data-producer',

            # Reliability settings
            'acks': 'all', # Wait for all replicas to acknowledge
            'retries': 5,  # Number of retries if sending fails
            'retry.backoff.ms': 500,  # Backoff time between retries
            'max.in.flight.requests.per.connection': 1, # Prevent message reordering on retries

            # Performance and timeout settings
            'queue.buffering.max.ms': 5, # Small delay to batch messages
            'batch.num.messages': 16384, # Batch size in bytes
            'queue.buffering.max.kbytes': 32768,  # 32MB producer buffer (corrected)
            'socket.timeout.ms': 30000, # Socket timeout
            'message.timeout.ms': 10000,  # Time to wait for acknowledgement

            # Compression to reduce network bandwidth
            'compression.type': 'snappy' # Enable compression for efficiency
        }
        
        # Create producer instance
        self.producer = Producer(self.conf)
        log.info(f"Kafka producer initialized with bootstrap servers: {self.bootstrap_servers}")
        
        # Ensure the topic exists
        self._ensure_topic_exists()
    
    def _ensure_topic_exists(self, num_partitions: int = 3, replication_factor: int = 1) -> None:
        """
        Ensure that the Kafka topic exists, creating it if necessary.
        
        Args:
            num_partitions: Number of partitions for the topic (higher allows more parallelism)
            replication_factor: Number of replicas for each partition (should be <= number of brokers)
        """
        try:
            admin_client = AdminClient({'bootstrap.servers': self.bootstrap_servers})
            
            # Check if topic already exists
            topics = admin_client.list_topics(timeout=10)
            
            if self.topic_name not in topics.topics:
                log.info(f"Topic {self.topic_name} does not exist. Creating it with {num_partitions} partitions and replication factor {replication_factor}...")
                topic_list = [
                    NewTopic(
                        self.topic_name,
                        num_partitions=num_partitions,
                        replication_factor=replication_factor,
                        # Additional topic configs for reliability
                        config={
                            'min.insync.replicas': '2', # At least 2 replicas must acknowledge
                            'retention.ms': '604800000', # 7 days retention
                            'cleanup.policy': 'delete' # Delete old messages
                        }
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
                # Topic exists, log its configuration
                topic_metadata = topics.topics[self.topic_name]
                partitions_count = len(topic_metadata.partitions)
                log.info(f"Topic {self.topic_name} already exists with {partitions_count} partitions")

                # Check replication
                replication_counts = [len(p.replicas) for p in topic_metadata.partitions.values()]
                avg_replication = sum(replication_counts) / len(replication_counts)
                log.info(f"Topic has an average replication factor of {avg_replication:.1f}")

        except KafkaException as e:
            log.warning(f"Kafka operation failed: {str(e)}")
            log.info("Will continue and try to use the topic even if we couldn't verify its existence")
    
    def _delivery_report(self, err, msg) -> None:
        """
        Delivery report callback for produced messages.
        Called once for each message produced to indicate delivery success or failure.

        Args:
            err: Error (or None if success)
            msg: Message object
        """
        if err is not None:
            log.error(f"Message delivery failed: {err}")
        else:
            log.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
    
    def send_message(self, message: Dict[str, Any], key: str = None) -> None:
        """
        Send a single message to Kafka topic
        
        Args:
            message: Dictionary containing the message data
            key: Optional key for partitioning (defaults to device_id from message)
        """
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
            
        except BufferError:
            # Handle case where producer queue is full
            log.warning("Producer queue is full. Waiting for queue to drain...")
            self.producer.flush(5) # Wait up to 5 seconds for messages to be sent

            # Try again after flushing
            self.send_message(message, key)

        except Exception as e:
            log.error(f"Error sending message to Kafka: {str(e)}")
    
    def send_batch(self, messages: List[Dict[str, Any]]) -> None:
        """
        Send a batch of messages to Kafka topic.
        
        This message efficiently batches multiple messages for better throughput.
        Messages from the same device will be sent to the same partition, preserving
        order for each device.

        Args:
            messages: List of dictionaries containing the message data
        """
        device_count = {}
        for message in messages:
            self.send_message(message)

            # Count messages per device for logging
            device_id = message.get('device_id', 'unknown')
            device_count[device_id] = device_count.get(device_id, 0) + 1
        
        # Flush to ensure all messages are sent
        # Longer timeout to account for multiple brokers
        self.producer.flush(timeout=10.0)  # Increased timeout to 10 seconds

        # Log summary of sent messages
        unique_devices = len(device_count)
        total_messages = sum(device_count.values())
        log.info(f"Batch of {total_messages} messages from {unique_devices} devices sent to Kafka topic: {self.topic_name}")
    
    def close(self) -> None:
        """
        Flush and close the producer
        Always call this method when done with the producer to ensure all messages are sent.
        """
        log.info("Closing Kafka prodcuer, flushing any pending messages...")
        self.producer.flush(timeout=30.0) # Give plenty of time for pending messages to be sent
        log.info("Kafka producer closed")