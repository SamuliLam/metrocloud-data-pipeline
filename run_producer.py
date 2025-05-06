#!/usr/bin/env python3
"""
Script to run the IoT data generator and producer with Avro serialization.
This is intended to be run in a Docker container with a multi-broker Kafka cluster.
"""

import time
import signal
import sys
import random
import os
from confluent_kafka import KafkaException

from src.utils.logger import log
from src.config.config import settings
from src.data_generator.iot_simulator import IoTSimulator
from src.data_ingestion.producer import KafkaProducer
from src.utils.schema_registry import schema_registry

# Global flag for controlling shutdown
running = True

def signal_handler(sig, frame):
    """
    Handle termination signals for graceful shutdown.
    This ensures the producer stops cleanly when the container is stopped.
    
    Args:
        sig: Signal number
        frame: Current stack frame
    """
    global running
    log.info(f"Caught signal {sig}. Stopping producer...")
    running = False

def wait_for_kafka_and_schema_registry(max_retries=60, initial_backoff=1):
    """
    Wait for Kafka and Schema Registry to become available with exponential backoff.
    With multiple brokers, we need more patience and resilience.
    
    Args:
        max_retries: Maximum number of retry attempts
        initial_backoff: Initial backoff time in seconds
    
    Returns:
        True if connection was successful, False otherwise
    """
    backoff = initial_backoff
    
    # First, wait for Kafka to be available
    for attempt in range(1, max_retries + 1):
        try:
            # Get the bootstrap servers from environment or settings
            bootstrap_servers = settings.kafka.bootstrap_servers
            log.info(f"Attempt {attempt}/{max_retries} to connect to Kafka cluster at {bootstrap_servers}")
            
            # Try to create a producer to test connection
            from confluent_kafka import Producer
            producer_conf = settings.producer.get_config()
            producer = Producer(producer_conf)
            producer.flush(timeout=10)  # Increased timeout for multi-broker environment
            
            log.info("Successfully connected to Kafka cluster!")
            
            # Now check Schema Registry
            sr_url = settings.schema_registry.url
            log.info(f"Attempting to connect to Schema Registry at {sr_url}")
            
            # Try to get subjects from Schema Registry
            subjects = schema_registry.get_subjects()
            log.info(f"Successfully connected to Schema Registry. Available subjects: {subjects}")
            
            return True
            
        except Exception as e:
            log.warning(f"Failed to connect to Kafka or Schema Registry: {str(e)}")
            
            if attempt < max_retries:
                # Add some jitter to the backoff to avoid thundering herd problem
                jitter = random.uniform(0, 0.1 * backoff)
                sleep_time = backoff + jitter
                
                log.info(f"Retrying in {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
                
                # Increase backoff for next attempt (exponential backoff)
                backoff = min(backoff * 2, 30)  # Cap at 30 seconds
            else:
                log.error(f"Failed to connect after {max_retries} attempts")
                return False
            
def log_configuration():
    """
    Log the current configuration settings to help with troubleshooting.
    """
    log.info("Current configuration:")
    log.info(f"App name: {settings.app_name}")
    log.info(f"Environment: {settings.environment}")
    log.info(f"Kafka bootstrap servers: {settings.kafka.bootstrap_servers}")
    log.info(f"Kafka topic: {settings.kafka.topic_name}")
    log.info(f"Schema Registry URL: {settings.schema_registry.url}")
    log.info(f"IoT devices: {settings.iot_simulator.num_devices}")
    log.info(f"Device types: {settings.iot_simulator.device_types}")
    log.info(f"Data generation interval: {settings.iot_simulator.data_generation_interval_sec} seconds")
    log.info(f"Anomaly probability: {settings.iot_simulator.anomaly_probability}")

def main():
    """
    Main function to run the IoT data generator and producer.
    Handles connection to the multi-broker Kafka cluster and
    continuously generates and sends IoT sensor data using Avro serialization.
    """
    # Setup signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Log application start and configuration
    log.info("Starting IoT Data Producer Service with Avro Serialization")
    log_configuration()
    
    # Wait for Kafka and Schema Registry to be ready
    if not wait_for_kafka_and_schema_registry():
        log.error("Kafka cluster or Schema Registry is not available. Exiting.")
        sys.exit(1)
    
    try:
        # Create Kafka producer with Avro serialization
        producer = KafkaProducer()
        
        # Create IoT simulator with Avro-compatible data
        simulator = IoTSimulator()
        
        # Start continuous data generation
        log.info("Starting continuous IoT data generation with Avro serialization...")
        
        # Get the data generation interval from settings
        interval = settings.iot_simulator.data_generation_interval_sec
        
        while running:
            # Generate batch of readings from simulated devices
            readings = simulator.generate_batch()
            
            # Send the Avro-serialized readings to Kafka
            producer.send_batch(readings)
            
            # Wait before generating next batch
            time.sleep(interval)
            
    except KeyboardInterrupt:
        log.info("Producer interrupted by user")
    except Exception as e:
        log.error(f"Unexpected error in producer: {str(e)}")
        raise
    finally:
        # Ensure producer is properly closed
        if 'producer' in locals():
            producer.close()
        log.info("Producer shutdown complete")

if __name__ == "__main__":
    main()