import os
from pydantic import Field
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

class KafkaSettings(BaseSettings):
    # Kafka configuration settings.
    # Comma-separated list of Kafka brokers (e.g., kafka:9092)
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    
    # Kafka topic name to which sensor data will be published
    topic_name: str = os.getenv("KAFKA_TOPIC_NAME", "iot-sensor-data")

    # Kafka consumer group ID (used for consumer coordination)
    group_id: str = os.getenv("KAFKA_CONSUMER_GROUP_ID", "iot-data-consumer")

    # Determines where to start reading messages if no offsets are stored
    auto_offset_reset: str = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")

    
class IoTSimulatorSettings(BaseSettings):
    # IoT simulator configuration settings.

    # Number of IoT devices to simulate
    num_devices: int = int(os.getenv("IOT_NUM_DEVICES", "5"))

    # Time interval between each data generation batch (in seconds)
    data_generation_interval_sec: float = float(os.getenv("IOT_DATA_INTERVAL_SEC", "1.0"))
    
class Settings(BaseSettings):
    # Global application settings, including Kafka and IoT simulator configurations
    
    # Application name (can be used in logs or dashboards)
    app_name: str = "IoT Data Pipeline"

    # kafka-related settings (loaded from env or defaults)
    kafka: KafkaSettings = KafkaSettings()

    # IoT simulator settings (loaded from env or defaults)
    iot_simulator: IoTSimulatorSettings = IoTSimulatorSettings()

# Create a singleton instance of the Settings class to be used throughout the app
settings = Settings()