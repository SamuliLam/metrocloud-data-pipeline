import os
from pydantic import Field
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

class KafkaSettings(BaseSettings):
    """Kafka configuration settings."""
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    topic_name: str = os.getenv("KAFKA_TOPIC_NAME", "iot-sensor-data")
    auto_offset_reset: str = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
    
class IoTSimulatorSettings(BaseSettings):
    """IoT simulator configuration settings."""
    num_devices: int = int(os.getenv("IOT_NUM_DEVICES", "5"))
    data_generation_interval_sec: float = float(os.getenv("IOT_DATA_INTERVAL_SEC", "1.0"))
    
class Settings(BaseSettings):
    """Global application settings."""
    app_name: str = "IoT Data Pipeline"
    kafka: KafkaSettings = KafkaSettings()
    iot_simulator: IoTSimulatorSettings = IoTSimulatorSettings()

# Create a singleton instance of the Settings class
settings = Settings()