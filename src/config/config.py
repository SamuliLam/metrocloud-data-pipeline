import os
from pydantic import Field
from pydantic_settings import BaseSettings
from dotenv import load_dotenv
from typing import List

# Load environment variables from .env file if it exists
load_dotenv()

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
    # Support comma-separated list of Kafka brokers or use default if not provided (e.g., kafka:9092)
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092,kafka3:9092")
    
    # Topic configuration
    # Kafka topic name to which sensor data will be published
    topic_name: str = os.getenv("KAFKA_TOPIC_NAME", "iot-sensor-data")
    # Kafka consumer group ID (used for consumer coordination)
    consumer_group_id: str = os.getenv("KAFKA_CONSUMER_GROUP_ID", "iot-data-consumer")
    # Determines where to start reading messages if no offsets are stored
    auto_offset_reset: str = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")

    # Multi-broker specific settings
    replication_factor: int = int(os.getenv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "3"))
    partitions: int = int(os.getenv("KAFKA_PARTITIONS", "6"))

    # Get list of brokers for programmatic access
    @property
    def broker_list(self) -> List[str]:
        """
        Get list of broker addresses from the bootstrap_servers string

        Returns:
            List of broker addresses
        """
        return self.bootstrap_servers.split(',')
    
class IoTSimulatorSettings(BaseSettings):
    """
    IoT simulator configuration settings.
    
    Attributes:
        num_devices: Number of simulated IoT devices
        data_generation_interval_sec: Interval between data generation in seconds
        device_types: List of device types to simulate
        anomaly_probability: Probability of generating an anomalous reading
    """
    
    # Number of IoT devices to simulate
    num_devices: int = int(os.getenv("IOT_NUM_DEVICES", "8"))

    # Time interval between each data generation batch (in seconds)
    data_generation_interval_sec: float = float(os.getenv("IOT_DATA_INTERVAL_SEC", "1.0"))

    # Addtional simulation parameters
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
        iot_simulator: IoT simulator configuration settings
        logging: Logging configuration settings
    """
    
    # Application name (can be used in logs or dashboards)
    app_name: str = "IoT Data Pipeline (Multi-Broker)"

    # kafka-related settings (loaded from env or defaults)
    kafka: KafkaSettings = KafkaSettings()

    # IoT simulator settings (loaded from env or defaults)
    iot_simulator: IoTSimulatorSettings = IoTSimulatorSettings()

    # Logger settings (loaded from env or defaults)
    logging: LoggingSettings = LoggingSettings()

    class Config:
        """Configuration for the Settings class."""
        env_file = ".env"
        case_sensitive = False

# Create a singleton instance of the Settings class to be used throughout the app
settings = Settings()