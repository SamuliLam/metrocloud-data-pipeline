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

class TimescaleDBSettings(BaseSettings):
    """
    TimescaleDB database configuration settings.
    
    Attributes:
        host: Database host
        port: Database port
        database: Database name
        username: Database username
        password: Database password
        pool_size: Connection pool size
        max_overflow: Maximum overflow connections
        pool_timeout: Pool timeout in seconds
        pool_recycle: Pool recycle time in seconds
        batch_size: Batch size for bulk operations
        commit_interval: Commit interval in seconds
        main_table: Main table name for sensor readings
        archive_table: Archive table name
        retention_days: Data retention period in days
        archive_after_days: Archive data after days
        chunk_time_interval: TimescaleDB chunk time interval
        compression_after: Compress data after this interval
        retention_policy: Data retention policy
    """
    host: str = Field(
        default_factory=lambda: yaml_config.get('timescaledb', {}).get('host') or
                        os.getenv("TIMESCALEDB_HOST", "timescaledb")
    )
    
    port: int = Field(
        default_factory=lambda: yaml_config.get('timescaledb', {}).get('port') or 
                        int(os.getenv("TIMESCALEDB_PORT", "5432"))
    )
    
    database: str = Field(
        default_factory=lambda: yaml_config.get('timescaledb', {}).get('database') or 
                        os.getenv("TIMESCALEDB_DB", "iot_data")
    )
    
    username: str = Field(
        default_factory=lambda: yaml_config.get('timescaledb', {}).get('username') or 
                        os.getenv("TIMESCALEDB_USER", "iot_user")
    )
    
    password: str = Field(
        default_factory=lambda: yaml_config.get('timescaledb', {}).get('password') or 
                        os.getenv("TIMESCALEDB_PASSWORD", "iot_password")
    )
    
    # Connection pool settings
    pool_size: int = Field(
        default_factory=lambda: yaml_config.get('timescaledb', {}).get('pool_size') or 
                        int(os.getenv("TIMESCALEDB_POOL_SIZE", "5"))
    )
    
    max_overflow: int = Field(
        default_factory=lambda: yaml_config.get('timescaledb', {}).get('max_overflow') or 
                        int(os.getenv("TIMESCALEDB_MAX_OVERFLOW", "10"))
    )
    
    pool_timeout: int = Field(
        default_factory=lambda: yaml_config.get('timescaledb', {}).get('pool_timeout') or 
                        int(os.getenv("TIMESCALEDB_POOL_TIMEOUT", "30"))
    )
    
    pool_recycle: int = Field(
        default_factory=lambda: yaml_config.get('timescaledb', {}).get('pool_recycle') or 
                        int(os.getenv("TIMESCALEDB_POOL_RECYCLE", "3600"))
    )
    
    # Performance settings
    batch_size: int = Field(
        default_factory=lambda: yaml_config.get('timescaledb', {}).get('batch_size') or 
                        int(os.getenv("TIMESCALEDB_BATCH_SIZE", "100"))
    )
    
    commit_interval: float = Field(
        default_factory=lambda: yaml_config.get('timescaledb', {}).get('commit_interval') or 
                        float(os.getenv("TIMESCALEDB_COMMIT_INTERVAL", "5.0"))
    )
    
    # Table configuration
    main_table: str = Field(
        default_factory=lambda: yaml_config.get('timescaledb', {}).get('main_table') or 
                        os.getenv("TIMESCALEDB_MAIN_TABLE", "sensor_readings")
    )
    
    archive_table: str = Field(
        default_factory=lambda: yaml_config.get('timescaledb', {}).get('archive_table') or 
                        os.getenv("TIMESCALEDB_ARCHIVE_TABLE", "sensor_readings_archive")
    )

    # Data retention
    retention_days: int = Field(
        default_factory=lambda: yaml_config.get('postgresql', {}).get('retention_days') or 
                        int(os.getenv("POSTGRES_RETENTION_DAYS", "90"))
    )
    
    archive_after_days: int = Field(
        default_factory=lambda: yaml_config.get('postgresql', {}).get('archive_after_days') or 
                        int(os.getenv("POSTGRES_ARCHIVE_AFTER_DAYS", "30"))
    )
    
    # TimescaleDB specific settings
    chunk_time_interval: str = Field(
        default_factory=lambda: yaml_config.get('timescaledb', {}).get('chunk_time_interval') or 
                        os.getenv("TIMESCALEDB_CHUNK_TIME_INTERVAL", "1 day")
    )
    
    compression_after: str = Field(
        default_factory=lambda: yaml_config.get('timescaledb', {}).get('compression_after') or 
                        os.getenv("TIMESCALEDB_COMPRESSION_AFTER", "7 days")
    )
    
    drop_after: str = Field(
        default_factory=lambda: yaml_config.get('timescaledb', {}).get('drop_after') or 
                        os.getenv("TIMESCALEDB_DROP_AFTER", "90 days")
    )
    
    # Continuous aggregates settings
    enable_continuous_aggregates: bool = Field(
        default_factory=lambda: yaml_config.get('timescaledb', {}).get('enable_continuous_aggregates') or 
                        os.getenv("TIMESCALEDB_ENABLE_CONTINUOUS_AGGREGATES", "True").lower() in ("true", "1", "yes")
    )
    
    @property
    def database_url(self) -> str:
        """
        Get the database URL for SQLAlchemy.
        
        Returns:
            Database URL string
        """
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

class DataSinkSettings(BaseSettings):
    """
    Data Sink configuration settings.
    
    Attributes:
        consumer_group_id: Consumer group ID for the data sink
        batch_size: Batch size for database operations
        commit_interval: Interval between commits in seconds
        max_retries: Maximum number of retries for failed operations
        retry_backoff: Backoff time between retries in seconds
    """
    consumer_group_id: str = Field(
        default_factory=lambda: yaml_config.get('data_sink', {}).get('consumer_group_id') or
                        os.getenv("DATA_SINK_CONSUMER_GROUP_ID", "iot-data-sink")
    )
    
    batch_size: int = Field(
        default_factory=lambda: yaml_config.get('data_sink', {}).get('batch_size') or 
                        int(os.getenv("DATA_SINK_BATCH_SIZE", "50"))
    )
    
    commit_interval: float = Field(
        default_factory=lambda: yaml_config.get('data_sink', {}).get('commit_interval') or 
                        float(os.getenv("DATA_SINK_COMMIT_INTERVAL", "5.0"))
    )
    
    max_retries: int = Field(
        default_factory=lambda: yaml_config.get('data_sink', {}).get('max_retries') or 
                        int(os.getenv("DATA_SINK_MAX_RETRIES", "3"))
    )
    
    retry_backoff: float = Field(
        default_factory=lambda: yaml_config.get('data_sink', {}).get('retry_backoff') or 
                        float(os.getenv("DATA_SINK_RETRY_BACKOFF", "2.0"))
    )

class MQTTSettings(BaseSettings):
    """
    MQTT configuration settings for RuuviTag adapter

    Attributes
        broker_host: MQTT broker hostname or IP
        broker_port: MQTT broker port
        topic: MQTT topic to subscribe to
        client_id: MQTT client ID
        qos: Quality of Service level
        keep_alive: Keep alive interval in seconds
        reconnect_interval: Reconnect interval in seconds
        username: MQTT username (optional)
        password: MQTT password (optional)
    """

    broker_host: str = Field(
        default_factory=lambda: yaml_config.get('mqtt', {}).get('broker_host') or
                        os.getenv("MQTT_BROKER", "192.168.50.240")
    )

    broker_port: int = Field(
        default_factory=lambda: yaml_config.get('mqtt', {}).get('broker_port') or 
                        int(os.getenv("MQTT_PORT", "1883"))
    )
    
    topic: str = Field(
        default_factory=lambda: yaml_config.get('mqtt', {}).get('topic') or 
                        os.getenv("MQTT_TOPIC", "ruuvitag/data")
    )
    
    client_id: str = Field(
        default_factory=lambda: yaml_config.get('mqtt', {}).get('client_id') or 
                        os.getenv("MQTT_CLIENT_ID", "ruuvitag_adapter")
    )
    
    qos: int = Field(
        default_factory=lambda: yaml_config.get('mqtt', {}).get('qos') or 
                        int(os.getenv("MQTT_QOS", "1"))
    )
    
    keep_alive: int = Field(
        default_factory=lambda: yaml_config.get('mqtt', {}).get('keep_alive') or 
                        int(os.getenv("MQTT_KEEP_ALIVE", "60"))
    )
    
    reconnect_interval: int = Field(
        default_factory=lambda: yaml_config.get('mqtt', {}).get('reconnect_interval') or 
                        int(os.getenv("MQTT_RECONNECT_INTERVAL", "10"))
    )
    
    username: Optional[str] = Field(
        default_factory=lambda: yaml_config.get('mqtt', {}).get('username') or 
                        os.getenv("MQTT_USERNAME", None)
    )
    
    password: Optional[str] = Field(
        default_factory=lambda: yaml_config.get('mqtt', {}).get('password') or 
                        os.getenv("MQTT_PASSWORD", None)
    )

class RuuviTagSettings(BaseSettings):
    """
    RuuviTag configuration settings.
    
    Attributes:
        device_type: Device name
        default_location: Default location for RuuviTags
        battery: Battery parameters
        anomaly_thresholds: Thresholds for anomaly detection
        signal_strength: Signal strength
        firmware_version: Firmware's version
    """
    device_type: str = Field(
        default_factory=lambda:yaml_config.get('device_type', {}).get('device_type') or
                        os.getenv("DEVICE_TYPE", "RuuviTag")
    )

    default_location: Dict[str, Any] = Field(
        default_factory=lambda: yaml_config.get('ruuvitag', {}).get('default_location') or {
            "latitude": 60.1699,
            "longitude": 24.9384,
            "building": "building-1",
            "floor": 1,
            "zone": "main",
            "room": "room-101"
        }
    )

    battery: Dict[str, float] = Field(
        default_factory=lambda: yaml_config.get('ruuvitag', {}).get('battery') or {
            "min_voltage": 2.0,
            "max_voltage": 3.0
        }
    )

    anomaly_thresholds: Dict[str, Any] = Field(
        default_factory=lambda: yaml_config.get('ruuvitag', {}).get('anomaly_thresholds') or {
            "temperature_min": -40,
            "temperature_max": 85,
            "humidity_min": 0,
            "humidity_max": 100,
            "pressure_min": 85000,
            "pressure_max": 115000,
            "battery_low": 2.0
        }
    )

    signal_strength: Optional[int] = Field(
        default_factory=lambda: yaml_config.get('ruuvitag', {}).get('signal_strength') or
                        os.getenv("SIGNAL_STRENGTH", None)
    )

    firmware_version: Optional[int] = Field(
        default_factory=lambda: yaml_config.get('ruuvitag', {}).get('firmware_version') or
                        os.getenv("FIRMWARE_VERSION", None)
    )

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
        timescaledb: TimescaleDB configuration settings
        postgresql: PostgreSQL configuration settings
        schema_registry: Schema Registry configuration settings
        iot_simulator: IoT simulator configuration settings
        logging: Logging configuration settings
        producer: Producer configuration settings
        Consumer: Consumer configuration settings
        data_sink: Data sink configuration settings
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
    timescaledb: TimescaleDBSettings = TimescaleDBSettings()
    mqtt: MQTTSettings = MQTTSettings()
    ruuvitag: RuuviTagSettings = RuuviTagSettings()
    schema_registry: SchemaRegistrySettings = SchemaRegistrySettings()
    iot_simulator: IoTSimulatorSettings = IoTSimulatorSettings()
    logging: LoggingSettings = LoggingSettings()
    producer: ProducerSettings = ProducerSettings()
    consumer: ConsumerSettings = ConsumerSettings()
    data_sink: DataSinkSettings = DataSinkSettings()
    

    class Config:
        """Configuration for the Settings class."""
        env_file = ".env"
        case_sensitive = False

# Create a singleton instance of the Settings class
settings = Settings()