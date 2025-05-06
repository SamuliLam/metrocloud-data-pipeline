from typing import Dict, Any, List
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException

from src.utils.logger import log
from src.config.config import settings
from src.utils.schema_registry import schema_registry

class KafkaProducer:
    """
    Kafka Producer for IoT sensor data with Avro serialization and Schema Registry.
    
    This class handles the connection to a Kafka cluster, ensures the target
    topic exists with proper replication, and provides methods to publish
    IoT sensor data to the Kafka topic using Avro serialization.
    """
    
    def __init__(self, bootstrap_servers: str = None, topic_name: str = None):
        """
        Initialize Kafka producer with configuration for a multi-broker environment.
        
        Args:
            bootstrap_servers: Comma-separated list of broker addresses (host:port)
            topic_name: Name of the Kafka topic to produce messages to
        """
        self.bootstrap_servers = bootstrap_servers or settings.kafka.bootstrap_servers
        self.topic_name = topic_name or settings.kafka.topic_name
        
        # Get producer configuration from settings
        self.conf = settings.producer.get_config()
        
        # Override bootstrap servers if provided
        if bootstrap_servers:
            self.conf['bootstrap.servers'] = bootstrap_servers
        
        # Create producer instance
        self.producer = Producer(self.conf)
        log.info(f"Kafka producer initialized with bootstrap servers: {self.bootstrap_servers}")
        log.debug(f"Producer configuration: {self.conf}")
        
        # Schema Registry
        self.schema_registry_client = schema_registry
        log.info("Schema Registry client initialized for producer")
        
        # Register schema for the topic
        self._register_schema()
        
        # Ensure the topic exists with proper replication
        self._ensure_topic_exists()
    
    def _register_schema(self):
        """
        Register Avro schema with Schema Registry.
        """
        try:
            subject = f"{self.topic_name}-value"
            schema_id = self.schema_registry_client.register_schema(
                subject, 
                self.schema_registry_client.sensor_schema_str
            )
            log.info(f"Registered schema for subject {subject} with ID: {schema_id}")
        except Exception as e:
            log.error(f"Error registering schema: {str(e)}")
            raise
    
    def _ensure_topic_exists(self) -> None:
        """
        Ensure that the Kafka topic exists with proper settings for a multi-broker environment.
        Creates the topic if it doesn't exist yet.
        """
        try:
            # Get replication factor and partitions from settings
            num_partitions = settings.kafka.partitions
            replication_factor = settings.kafka.replication_factor
            
            # Get topic configuration from settings
            topic_config = settings.kafka.topic_config
            
            admin_client = AdminClient({'bootstrap.servers': self.bootstrap_servers})
            
            # Check if topic already exists
            topics = admin_client.list_topics(timeout=10)
            
            if self.topic_name not in topics.topics:
                log.info(f"Topic {self.topic_name} does not exist. Creating it with {num_partitions} " +
                         f"partitions and replication factor {replication_factor}...")
                
                # Convert topic config dict to the format expected by NewTopic
                config = {}
                for k, v in topic_config.items():
                    # Convert min_insync_replicas to min.insync.replicas format
                    key = k.replace('_', '.')
                    config[key] = str(v)

                log.debug(f"Topic config: {config}")
                
                topic_list = [
                    NewTopic(
                        self.topic_name,
                        num_partitions=num_partitions,
                        replication_factor=replication_factor,
                        config=config
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
                avg_replication = sum(replication_counts) / len(replication_counts) if replication_counts else 0
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
            # For verbose logging, uncomment this line
            # log.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
            pass
    
    def send_message(self, message: Dict[str, Any], key: str = None) -> None:
        """
        Send a single message to Kafka topic with Avro serialization.
        
        Args:
            message: Dictionary containing the message data
            key: Optional key for partitioning (defaults to device_id from message)
        """
        try:
            # Use device_id as the key if not specified
            if key is None and 'device_id' in message:
                key = message['device_id']
            
            # Convert key to bytes if it's a string
            key_bytes = key.encode('utf-8') if isinstance(key, str) else None
            
            # Serialize the message using Avro and Schema Registry
            value_bytes = self.schema_registry_client.serialize_sensor_reading(message, self.topic_name)
            
            # Produce the message
            self.producer.produce(
                topic=self.topic_name,
                key=key_bytes,
                value=value_bytes,
                callback=self._delivery_report
            )
            
            # Serve delivery callback queue (non-blocking)
            self.producer.poll(0)
            
        except BufferError:
            # Handle case where producer queue is full
            log.warning("Producer queue is full. Waiting for queue to drain...")
            self.producer.flush(5)  # Wait up to 5 seconds for messages to be sent
            
            # Try again after flushing
            self.send_message(message, key)
        except Exception as e:
            log.error(f"Error sending message to Kafka: {str(e)}")
    
    def send_batch(self, messages: List[Dict[str, Any]]) -> None:
        """
        Send a batch of messages to Kafka topic with Avro serialization.
        
        This method efficiently batches multiple messages for better throughput.
        Messages from the same device will be sent to the same partition,
        preserving order for each device.
        
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
        flush_timeout = settings.producer.message_timeout_ms / 1000 * 3  # Convert ms to seconds and triple it
        self.producer.flush(timeout=flush_timeout)
        
        # Log summary of sent messages
        unique_devices = len(device_count)
        total_messages = sum(device_count.values())
        log.info(f"Batch of {total_messages} Avro-serialized messages from {unique_devices} devices sent to Kafka topic: {self.topic_name}")
    
    def close(self) -> None:
        """
        Flush and close the producer.
        Always call this method when done with the producer to ensure all messages are sent.
        """
        log.info("Closing Kafka producer, flushing any pending messages...")
        self.producer.flush(timeout=30.0)  # Give plenty of time for pending messages to be sent
        log.info("Kafka producer closed")