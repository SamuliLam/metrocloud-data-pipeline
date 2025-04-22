#!/usr/bin/env python3
"""
Script to run the IoT data consumer.
This is intended to be run in a Docker container with a multi-broker cluster.
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
    """
    Wait for Kafka to become available with exponential backoff.
    With multiple brokers, connection attemps are more complex and may require more time and retries.

    Args:
        max_retries: Maximum number of retry attempts
        initial_backoff: Initial backoff time in seconds
    
    Returns:
        True if Kafka connection was successful, False otherwise
    """

    backoff = initial_backoff
    
    for attempt in range(1, max_retries + 1):
        try:
            # Get the bootstrap severs from environment or settings
            bootstrap_servers = settings.kafka.bootstrap_servers
            log.info(f"Attempt {attempt}/{max_retries} to connect to Kafka at {bootstrap_servers}")
            
            # Try to create a consumer to test connection
            consumer = Consumer({
                'bootstrap.servers': bootstrap_servers,
                'group.id': 'test-connection-group',
                'auto.offset.reset': 'earliest',
                'session.timeout.ms': 10000, # Longer timeout for multi-broker setup
                'heartbeat.interval.ms': 3000
            })
            
            # List topics as a connectivity test
            metadata = consumer.list_topics(timeout=10)
            if metadata:
                log.info(f"Successfully listed {len(metadata.topics)} topics from Kafka")
            
            # Close the consumer properly
            consumer.close()
            
            log.info("Successfully connected to a Kafka cluster!")
            return True
            
        except Exception as e:
            log.warning(f"Failed to connect to Kafka: {str(e)}")
            
            if attempt < max_retries:
                # Add some jitter to the backoff
                jitter = random.uniform(0, 0.1 * backoff)
                sleep_time = backoff + jitter
                
                log.info(f"Retrying in {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
                
                # Increase backoff for next attempt (exponential backoff)
                backoff = min(backoff * 2, 30)  # Cap at 30 seconds
            else:
                log.error(f"Failed to connect to Kafka after {max_retries} attempts")
                return False

class IoTAlertConsumer(KafkaConsumer):
    """
    Custom consumer that processes IoT data and generates alerts.
    This implementation is aware of the multi-broker setup and 
    includes logic to handle broker failovers.
    """ 
    
    def process_message(self, message):
        """
        Process a received IoT sensor reading and generate alerts based on specified thresholds
        
        Args:
            message: Dictionary containing the IoT sensor reading
        """
        device_id = message.get('device_id', 'unknown')
        device_type = message.get('device_type', 'unknown')
        value = message.get('value', 'unknown')
        unit = message.get('unit', '')
        timestamp = message.get('timestamp', 'unknown')
        
        # Check for anomalies or specific conditions
        try:
            # Convert value to float for numeric comparisons
            # but handle the case when value might be non-numeric (like boolean for motion)
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
        except (ValueError, TypeError) as e:
            # Handle case where value conversion fails
            log.error(f"Error processing value from device {device_id}: {str(e)}")
            log.info(f"Raw message: {message}")

def main():
    """
    Main functions to run the IoT data consumer.
    Handles connection to the multi-broker Kafka cluster.
    Sets up the consumer, and processes incoming IoT data.
    """
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
        consumer = IoTAlertConsumer(
            group_id=consumer_groupd_id,
            # Fault tolerance settings for multi-broker environment
            auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
        )
        
        # Start consuming messages in a continuous loop
        log.info("Starting continuous IoT data consumption...")
        consumer.consume_loop(timeout=1.0)        
    except KeyboardInterrupt:
        log.info("Consumer interrupted by user")
    except Exception as e:
        log.error(f"Unexpected error in consumer: {str(e)}")
        raise
    finally:
        # Ensure any cleanup happens
        log.info("Consumer shutdown complete")


if __name__ == "__main__":
    main()

