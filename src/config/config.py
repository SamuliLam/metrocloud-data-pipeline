import os
import yaml
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings
from dotenv import load_dotenv
from typing import Dict, List, Any, Optional

# Load environment variables from .env file if it exists
load_dotenv()

def load_yaml_config():
    try:
        config_path = os.getenv('CONFIG_FILE_PATH', 'config.yaml')
        if os.path.isdir(config_path):
            print(f"Expected config file but found directory at {config_path}")
            return {}
            
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    except Exception as e:
        print(f"Error loading configuration from {config_path}: {str(e)}")
        print("Falling back to environment variables and default values")
        return {}

# Load configuration from YAML file
yaml_config = load_yaml_config()


class SchemaRegistrySettings(BaseSettings):
    """
    Schema Registry configuration settings.
    
    Attributes:
        url: URL of the Schema Registry service
        auto_register_schemas: Whether to automatically register schemas
        compatibility_level: Schema compatibility level (e.g., BACKWARD, FORWARD, FULL)
        subject_name_strategy: Strategy for subject naming
    """
    url: str = Field(
        default_factory=lambda: yaml_config.get('schema_registry', {}).get('url') or 
                       os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    )
    
    auto_register_schemas: bool = Field(
        default_factory=lambda: yaml_config.get('schema_registry', {}).get('auto_register_schemas') or 
                       os.getenv("SCHEMA_AUTO_REGISTER", "True").lower() in ("true", "1", "yes")
    )
    
    compatibility_level: str = Field(
        default_factory=lambda: yaml_config.get('schema_registry', {}).get('compatibility_level') or 
                       os.getenv("SCHEMA_COMPATIBILITY_LEVEL", "BACKWARD")
    )
    
    subject_name_strategy: str = Field(
        default_factory=lambda: yaml_config.get('schema_registry', {}).get('subject_name_strategy') or 
                       os.getenv("SCHEMA_SUBJECT_STRATEGY", "TopicNameStrategy")
    )
    
    # Schema paths
    schema_dir: str = Field(
        default_factory=lambda: yaml_config.get('schema_registry', {}).get('schema_dir') or 
                       os.getenv("SCHEMA_DIR", "src/schemas")
    )
    
    sensor_schema_file: str = Field(
        default_factory=lambda: yaml_config.get('schema_registry', {}).get('sensor_schema_file') or 
                       os.getenv("SENSOR_SCHEMA_FILE", "iot_sensor_reading.avsc")
    )
    
    # Serialization settings
    serialize_format: str = Field(
        default_factory=lambda: yaml_config.get('schema_registry', {}).get('serialize_format') or 
                       os.getenv("SERIALIZE_FORMAT", "avro")
    )
    
    @property
    def sensor_schema_path(self) -> str:
        """
        Get the full path to the sensor schema file.
        
        Returns:
            Full path to the sensor schema file
        """
        return os.path.join(self.schema_dir, self.sensor_schema_file)


class KafkaSettings(BaseSettings):
    """
    Kafka configuration settings for a multi-broker environment.
    
    Attributes:
        bootstrap_servers: Comma-separated list of Kafka broker addresses
        topic_name: Name of the Kafka topic for IoT data
        consumer_group_id: ID of the consumer group for load balancing
        auto_offset_reset: Strategy for consuming messages ('earliest' or 'latest')
        replication_factor: Number of replicas for topic partitions
        partitions: Number of partitions for the topic
    """
    # Support comma-delimited string of brokers or use default if not provided
    bootstrap_servers: str = Field(
        default_factory=lambda: yaml_config.get('kafka', {}).get('bootstrap_servers') or 
                       os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092,kafka3:9092")
    )
    
    # Topic configuration
    topic_name: str = Field(
        default_factory=lambda: yaml_config.get('kafka', {}).get('topic_name') or 
                       os.getenv("KAFKA_TOPIC_NAME", "iot-sensor-data")
    )
    
    consumer_group_id: str = Field(
        default_factory=lambda: yaml_config.get('kafka', {}).get('consumer_group_id') or 
                       os.getenv("KAFKA_CONSUMER_GROUP_ID", "iot-data-consumer")
    )
    
    auto_offset_reset: str = Field(
        default_factory=lambda: yaml_config.get('kafka', {}).get('auto_offset_reset') or 
                       os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
    )
    
    # Multi-broker specific settings for fault tolerance
    replication_factor: int = Field(
        default_factory=lambda: yaml_config.get('kafka', {}).get('replication_factor') or 
                       int(os.getenv("KAFKA_REPLICATION_FACTOR", "3"))
    )
    
    partitions: int = Field(
        default_factory=lambda: yaml_config.get('kafka', {}).get('partitions') or 
                       int(os.getenv("KAFKA_PARTITIONS", "6"))
    )

    # Topic config
    topic_config: Dict[str, Any] = Field(
        default_factory=lambda: yaml_config.get('kafka', {}).get('topic_config') or {
            'min_insync_replicas': 2,
            'retention_ms': 604800000,  # 7 days
            'cleanup_policy': 'delete'
        }
    )
    
    # Get list of brokers for programmatic access
    @property
    def broker_list(self) -> List[str]:
        """
        Get list of broker addresses from the bootstrap_servers string.
        
        Returns:
            List of broker addresses
        """
        return self.bootstrap_servers.split(',')
    
    # Avro topic name with .value suffix for Schema Registry
    @property
    def avro_value_subject(self) -> str:
        """
        Get the Schema Registry subject name for the topic value schema.
        
        Returns:
            Subject name for the value schema
        """
        return f"{self.topic_name}-value"
    

class ProducerSettings(BaseSettings):
    """
    Kafka Producer specific settings.
    
    Attributes:
        client_id: Client ID for the producer
        acks: Acknowledgement level
        retries: Number of retries
        retry_backoff_ms: Backoff time between retries
    """
    client_id: str = Field(
        default_factory=lambda: yaml_config.get('producer', {}).get('client_id') or 
                       os.getenv("PRODUCER_CLIENT_ID", "iot-data-producer")
    )
    
    acks: str = Field(
        default_factory=lambda: yaml_config.get('producer', {}).get('acks') or 
                       os.getenv("PRODUCER_ACKS", "all")
    )
    
    retries: int = Field(
        default_factory=lambda: yaml_config.get('producer', {}).get('retries') or 
                       int(os.getenv("PRODUCER_RETRIES", "5"))
    )
    
    retry_backoff_ms: int = Field(
        default_factory=lambda: yaml_config.get('producer', {}).get('retry_backoff_ms') or 
                       int(os.getenv("PRODUCER_RETRY_BACKOFF_MS", "500"))
    )
    
    max_in_flight_requests_per_connection: int = Field(
        default_factory=lambda: yaml_config.get('producer', {}).get('max_in_flight_requests_per_connection') or 
                       int(os.getenv("PRODUCER_MAX_IN_FLIGHT", "1"))
    )
    
    # Performance tuning
    queue_buffering_max_ms: int = Field(
        default_factory=lambda: yaml_config.get('producer', {}).get('queue_buffering_max_ms') or 
                       int(os.getenv("PRODUCER_QUEUE_BUFFERING_MAX_MS", "5"))
    )
    
    queue_buffering_max_kbytes: int = Field(
        default_factory=lambda: yaml_config.get('producer', {}).get('queue_buffering_max_kbytes') or 
                       int(os.getenv("PRODUCER_QUEUE_BUFFERING_MAX_KBYTES", "32768"))
    )
    
    batch_num_messages: int = Field(
        default_factory=lambda: yaml_config.get('producer', {}).get('batch_num_messages') or 
                       int(os.getenv("PRODUCER_BATCH_NUM_MESSAGES", "1000"))
    )
    
    socket_timeout_ms: int = Field(
        default_factory=lambda: yaml_config.get('producer', {}).get('socket_timeout_ms') or 
                       int(os.getenv("PRODUCER_SOCKET_TIMEOUT_MS", "30000"))
    )
    
    message_timeout_ms: int = Field(
        default_factory=lambda: yaml_config.get('producer', {}).get('message_timeout_ms') or 
                       int(os.getenv("PRODUCER_MESSAGE_TIMEOUT_MS", "10000"))
    )
    
    # Compression
    compression_type: str = Field(
        default_factory=lambda: yaml_config.get('producer', {}).get('compression_type') or 
                       os.getenv("PRODUCER_COMPRESSION_TYPE", "snappy")
    )
    
    def get_config(self) -> Dict[str, Any]:
        """
        Get producer configuration as a dictionary for Confluent Kafka.
        
        Returns:
            Dictionary with producer configuration
        """
        return {
            'bootstrap.servers': settings.kafka.bootstrap_servers,
            'client.id': self.client_id,
            'acks': self.acks,
            'retries': self.retries,
            'retry.backoff.ms': self.retry_backoff_ms,
            'max.in.flight.requests.per.connection': self.max_in_flight_requests_per_connection,
            'queue.buffering.max.ms': self.queue_buffering_max_ms,
            'queue.buffering.max.kbytes': self.queue_buffering_max_kbytes,
            'batch.num.messages': self.batch_num_messages,
            'socket.timeout.ms': self.socket_timeout_ms,
            'message.timeout.ms': self.message_timeout_ms,
            'compression.type': self.compression_type
        }


class ConsumerSettings(BaseSettings):
    """
    Kafka Consumer specific settings.
    
    Attributes:
        enable_auto_commit: Whether to automatically commit offsets
        auto_commit_interval_ms: Interval between auto commits
        fetch_min_bytes: Minimum bytes to fetch
    """
    enable_auto_commit: bool = Field(
        default_factory=lambda: yaml_config.get('consumer', {}).get('enable_auto_commit') or 
                       os.getenv("CONSUMER_ENABLE_AUTO_COMMIT", "True").lower() in ("true", "1", "yes")
    )
    
    auto_commit_interval_ms: int = Field(
        default_factory=lambda: yaml_config.get('consumer', {}).get('auto_commit_interval_ms') or 
                       int(os.getenv("CONSUMER_AUTO_COMMIT_INTERVAL_MS", "5000"))
    )
    
    fetch_min_bytes: int = Field(
        default_factory=lambda: yaml_config.get('consumer', {}).get('fetch_min_bytes') or 
                       int(os.getenv("CONSUMER_FETCH_MIN_BYTES", "1"))
    )
    
    # Fault tolerance settings
    session_timeout_ms: int = Field(
        default_factory=lambda: yaml_config.get('consumer', {}).get('session_timeout_ms') or 
                       int(os.getenv("CONSUMER_SESSION_TIMEOUT_MS", "30000"))
    )
    
    heartbeat_interval_ms: int = Field(
        default_factory=lambda: yaml_config.get('consumer', {}).get('heartbeat_interval_ms') or 
                       int(os.getenv("CONSUMER_HEARTBEAT_INTERVAL_MS", "10000"))
    )
    
    max_poll_interval_ms: int = Field(
        default_factory=lambda: yaml_config.get('consumer', {}).get('max_poll_interval_ms') or 
                       int(os.getenv("CONSUMER_MAX_POLL_INTERVAL_MS", "300000"))
    )
    
    # Load balancing
    partition_assignment_strategy: str = Field(
        default_factory=lambda: yaml_config.get('consumer', {}).get('partition_assignment_strategy') or 
                       os.getenv("CONSUMER_PARTITION_ASSIGNMENT_STRATEGY", "cooperative-sticky")
    )
    
    def get_config(self, group_id: str = None, auto_offset_reset: str = None) -> Dict[str, Any]:
        """
        Get consumer configuration as a dictionary for Confluent Kafka.
        
        Args:
            group_id: Optional consumer group ID (overrides default)
            auto_offset_reset: Optional auto offset reset strategy (overrides default)
            
        Returns:
            Dictionary with consumer configuration
        """
        return {
            'bootstrap.servers': settings.kafka.bootstrap_servers,
            'group.id': group_id or settings.kafka.consumer_group_id,
            'auto.offset.reset': auto_offset_reset or settings.kafka.auto_offset_reset,
            'enable.auto.commit': self.enable_auto_commit,
            'auto.commit.interval.ms': self.auto_commit_interval_ms,
            'fetch.min.bytes': self.fetch_min_bytes,
            'session.timeout.ms': self.session_timeout_ms,
            'heartbeat.interval.ms': self.heartbeat_interval_ms,
            'max.poll.interval.ms': self.max_poll_interval_ms,
            'partition.assignment.strategy': self.partition_assignment_strategy
        }


class IoTSimulatorSettings(BaseSettings):
    """
    IoT simulator configuration settings.
    
    Attributes:
        num_devices: Number of simulated IoT devices
        data_generation_interval_sec: Interval between data generation in seconds
        device_types: List of device types to simulate
        anomaly_probability: Probability of generating an anomalous reading
    """
    num_devices: int = Field(
        default_factory=lambda: yaml_config.get('iot_simulator', {}).get('num_devices') or 
                       int(os.getenv("IOT_NUM_DEVICES", "8"))
    )
    
    data_generation_interval_sec: float = Field(
        default_factory=lambda: yaml_config.get('iot_simulator', {}).get('data_generation_interval_sec') or 
                       float(os.getenv("IOT_DATA_INTERVAL_SEC", "1.0"))
    )
    
    # Additional simulation parameters
    device_types: List[str] = Field(
        default_factory=lambda: yaml_config.get('iot_simulator', {}).get('device_types') or 
                       os.getenv("IOT_DEVICE_TYPES", "temperature,humidity,pressure,motion,light").split(',')
    )
    
    anomaly_probability: float = Field(
        default_factory=lambda: yaml_config.get('iot_simulator', {}).get('anomaly_probability') or 
                       float(os.getenv("IOT_ANOMALY_PROBABILITY", "0.05"))
    )

    @field_validator('device_types')
    def validate_device_types(cls, v):
        """Validate device types and convert to list if it's a string"""
        if isinstance(v, str):
            return v.split(',')
        return v
    
class LoggingSettings(BaseSettings):
    """
    Logging configuration settings.
    
    Attributes:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format: Log format string
    """
    level: str = Field(
        default_factory=lambda: yaml_config.get('logging', {}).get('level') or 
                       os.getenv("LOG_LEVEL", "INFO")
    )
    
    format: str = Field(
        default_factory=lambda: yaml_config.get('logging', {}).get('format') or 
                       os.getenv("LOG_FORMAT", "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>")
    )

    # File logging settings
    file_logging_enabled: bool = Field(
        default_factory=lambda: yaml_config.get('logging', {}).get('file_logging', {}).get('enabled') or 
                       os.getenv("LOG_FILE_ENABLED", "False").lower() in ("true", "1", "yes")
    )
    
    log_dir: str = Field(
        default_factory=lambda: yaml_config.get('logging', {}).get('file_logging', {}).get('log_dir') or 
                       os.getenv("LOG_DIR", "logs")
    )
    
    max_size: str = Field(
        default_factory=lambda: yaml_config.get('logging', {}).get('file_logging', {}).get('max_size') or 
                       os.getenv("LOG_MAX_SIZE", "10MB")
    )
    
    retention: str = Field(
        default_factory=lambda: yaml_config.get('logging', {}).get('file_logging', {}).get('retention') or 
                       os.getenv("LOG_RETENTION", "1 week")
    )
    
    compression: str = Field(
        default_factory=lambda: yaml_config.get('logging', {}).get('file_logging', {}).get('compression') or 
                       os.getenv("LOG_COMPRESSION", "zip")
    )
    
    # Error log specific settings
    error_log_enabled: bool = Field(
        default_factory=lambda: yaml_config.get('logging', {}).get('file_logging', {}).get('error_log', {}).get('enabled') or 
                       os.getenv("ERROR_LOG_ENABLED", "False").lower() in ("true", "1", "yes")
    )
    
    error_log_retention: str = Field(
        default_factory=lambda: yaml_config.get('logging', {}).get('file_logging', {}).get('error_log', {}).get('retention') or 
                       os.getenv("ERROR_LOG_RETENTION", "1 month")
    )

class Settings(BaseSettings):
    """
    Global application settings.
    
    Attributes:
        app_name: Name of the application
        environment: Deployment environment (development, staging, production)
        kafka: Kafka configuration settings
        schema_registry: Schema Registry configuration settings
        iot_simulator: IoT simulator configuration settings
        logging: Logging configuration settings
        producer: Producer configuration settings
        Consumer: Consumer configuration settings
    """
    app_name: str = Field(
        default_factory=lambda: yaml_config.get('app', {}).get('name') or 
                       os.getenv("APP_NAME", "IoT Data Pipeline (Multi-Broker with Schema Registry)")
    )
    
    environment: str = Field(
        default_factory=lambda: yaml_config.get('app', {}).get('environment') or 
                       os.getenv("ENVIRONMENT", "development")
    )

    kafka: KafkaSettings = KafkaSettings()
    schema_registry: SchemaRegistrySettings = SchemaRegistrySettings()
    iot_simulator: IoTSimulatorSettings = IoTSimulatorSettings()
    logging: LoggingSettings = LoggingSettings()
    producer: ProducerSettings = ProducerSettings()
    consumer: ConsumerSettings = ConsumerSettings()
    
    class Config:
        """Configuration for the Settings class."""
        env_file = ".env"
        case_sensitive = False

# Create a singleton instance of the Settings class
settings = Settings()