#!/usr/bin/env python3
"""
Script to run the IoT data consumer.
This is intended to be run in a Docker container.
"""

import time
import signal
import sys
import random
import os

from src.utils.logger import log
from src.config.config import settings
from src.data_ingestion.consumer import KafkaConsumer
from confluent_kafka import Consumer


def wait_for_kafka(max_retries=30, initial_backoff=1):
    # Wait for Kafka to become available with exponential backoff.

    backoff = initial_backoff
    
    for attempt in range(1, max_retries + 1):
        try:
            log.info(f"Attempt {attempt}/{max_retries} to connect to Kafka at {settings.kafka.bootstrap_servers}")
            
            # Try to create a consumer to test connection
            consumer = Consumer({
                'bootstrap.servers': settings.kafka.bootstrap_servers,
                'group.id': 'test-group',
                'auto.offset.reset': 'earliest'
            })
            consumer.close()
            
            log.info("Successfully connected to Kafka!")
            return True
            
        except Exception as e:
            log.warning(f"Failed to connect to Kafka: {str(e)}")
            
            if attempt < max_retries:
                jitter = random.uniform(0, 0.1 * backoff)
                sleep_time = backoff + jitter
                
                log.info(f"Retrying in {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
                
                backoff = min(backoff * 2, 30)  # Cap at 30 seconds
            else:
                log.error(f"Failed to connect to Kafka after {max_retries} attempts")
                return False

class IoTAlertConsumer(KafkaConsumer):
    # Custom consumer that processes IoT data and generates alerts.
    
    def process_message(self, message):
        """Process a received IoT sensor reading."""
        device_id = message.get('device_id', 'unknown')
        device_type = message.get('device_type', 'unknown')
        value = message.get('value', 'unknown')
        unit = message.get('unit', '')
        timestamp = message.get('timestamp', 'unknown')
        
        # Check for anomalies or specific conditions
        if device_type == "temperature" and float(value) > 30:
            log.warning(f"HIGH TEMPERATURE ALERT: Device {device_id} reported {value}{unit} at {timestamp}")
        elif device_type == "humidity" and float(value) > 70:
            log.warning(f"HIGH HUMIDITY ALERT: Device {device_id} reported {value}{unit} at {timestamp}")
        elif device_type == "pressure" and float(value) < 990:
            log.warning(f"LOW PRESSURE ALERT: Device {device_id} reported {value}{unit} at {timestamp}")
        elif device_type == "motion" and value == 1:
            log.info(f"MOTION DETECTED: Device {device_id} at {timestamp}")
        elif device_type == "light" and float(value) < 10:
            log.info(f"LOW LIGHT LEVEL: Device {device_id} reported {value}{unit} at {timestamp}")
        else:
            log.info(f"Device {device_id} ({device_type}): {value}{unit}")

def main():
    # Run the IoT data consumer.
    log.info("Starting IoT Data Consumer Service")
    log.info(f"Kafka bootstrap servers: {settings.kafka.bootstrap_servers}")
    log.info(f"Kafka topic: {settings.kafka.topic_name}")

    # Get consumer group ID from environment or use default
    consumer_groupd_id = os.getenv("KAFKA_CONSUMER_GROUP_ID", "iod-data-consumer")
    log.info(f"Consumer group ID: {consumer_groupd_id}")
    
    # Wait for Kafka to be ready
    if not wait_for_kafka():
        log.error("Kafka is not available. Exiting.")
        sys.exit(1)
    
    try: 
        # Create and start the consumer
        consumer = IoTAlertConsumer(group_id=consumer_groupd_id)
        
        # Start consuming messages
        log.info("Starting continuous IoT data consumption...")
        consumer.consume_loop(timeout=1.0)        
    except KeyboardInterrupt:
        log.info("Consumer interrupted by user")
    except Exception as e:
        log.error(f"Unexpected error in consumer: {str(e)}")
        raise
    finally:
        log.info("Consumer shutdown complete")

if __name__ == "__main__":
    main()