import os
from pydantic import Field
from pydantic_settings import BaseSettings
from dotenv import load_dotenv
from typing import List, Optional, Dict, Any

# Load environment variables from .env file if it exists
load_dotenv()

class SchemaRegistrySettings(BaseSettings):
    """
    Schema Registry configuration settings.
    
    Attributes:
        url: URL of the Schema Registry service
        auto_register_schemas: Whether to automatically register schemas
        compatibility_level: Schema compatibility level (e.g., BACKWARD, FORWARD, FULL)
        subject_name_strategy: Strategy for subject naming
    """
    url: str = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    auto_register_schemas: bool = os.getenv("SCHEMA_AUTO_REGISTER", "True").lower() in ("true", "1", "yes")
    compatibility_level: str = os.getenv("SCHEMA_COMPATIBILITY_LEVEL", "BACKWARD")
    subject_name_strategy: str = os.getenv("SCHEMA_SUBJECT_STRATEGY", "TopicNameStrategy")
    
    # Schema paths
    schema_dir: str = os.getenv("SCHEMA_DIR", "src/schemas")
    sensor_schema_file: str = os.getenv("SENSOR_SCHEMA_FILE", "iot_sensor_reading.avsc")
    
    @property
    def sensor_schema_path(self) -> str:
        """
        Get the full path to the sensor schema file.
        
        Returns:
            Full path to the sensor schema file
        """
        return os.path.join(self.schema_dir, self.sensor_schema_file)
    
    # Serialization settings
    serialize_format: str = os.getenv("SERIALIZE_FORMAT", "avro")
    use_specific_avro_reader: bool = os.getenv("USE_SPECIFIC_AVRO_READER", "True").lower() in ("true", "1", "yes")

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
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092,kafka3:9092")
    
    # Topic configuration
    topic_name: str = os.getenv("KAFKA_TOPIC_NAME", "iot-sensor-data")
    consumer_group_id: str = os.getenv("KAFKA_CONSUMER_GROUP_ID", "iot-data-consumer")
    auto_offset_reset: str = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
    
    # Multi-broker specific settings
    replication_factor: int = int(os.getenv("KAFKA_REPLICATION_FACTOR", "3"))
    partitions: int = int(os.getenv("KAFKA_PARTITIONS", "6"))
    
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
    
class IoTSimulatorSettings(BaseSettings):
    """
    IoT simulator configuration settings.
    
    Attributes:
        num_devices: Number of simulated IoT devices
        data_generation_interval_sec: Interval between data generation in seconds
        device_types: List of device types to simulate
        anomaly_probability: Probability of generating an anomalous reading
    """
    num_devices: int = int(os.getenv("IOT_NUM_DEVICES", "8"))
    data_generation_interval_sec: float = float(os.getenv("IOT_DATA_INTERVAL_SEC", "1.0"))
    
    # Additional simulation parameters
    device_types: List[str] = os.getenv("IOT_DEVICE_TYPES", "temperature,humidity,pressure,motion,light").split(',')
    anomaly_probability: float = float(os.getenv("IOT_ANOMALY_PROBABILITY", "0.05"))
    
class LoggingSettings(BaseSettings):
    """
    Logging configuration settings.
    
    Attributes:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format: Log format string
    """
    level: str = os.getenv("LOG_LEVEL", "INFO")
    format: str = os.getenv("LOG_FORMAT", "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>")

class Settings(BaseSettings):
    """
    Global application settings.
    
    Attributes:
        app_name: Name of the application
        kafka: Kafka configuration settings
        schema_registry: Schema Registry configuration settings
        iot_simulator: IoT simulator configuration settings
        logging: Logging configuration settings
    """
    app_name: str = "IoT Data Pipeline (Multi-Broker with Schema Registry)"
    kafka: KafkaSettings = KafkaSettings()
    schema_registry: SchemaRegistrySettings = SchemaRegistrySettings()
    iot_simulator: IoTSimulatorSettings = IoTSimulatorSettings()
    logging: LoggingSettings = LoggingSettings()
    
    class Config:
        """Configuration for the Settings class."""
        env_file = ".env"
        case_sensitive = False

# Create a singleton instance of the Settings class
settings = Settings()