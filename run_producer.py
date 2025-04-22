#!/usr/bin/env python3
"""
Script to run the IoT data generator and producer.
This is intended to be run in a Docker container with a multi-broker kafka cluster.
"""

import time
import signal
import sys
import random
from confluent_kafka import KafkaException

from src.utils.logger import log
from src.config.config import settings
from src.data_generator.iot_simulator import IoTSimulator
from src.data_ingestion.producer import KafkaProducer
from confluent_kafka import Producer


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

def wait_for_kafka(max_retries=30, initial_backoff=1):
    """
    Wait for Kafka to become available with exponential backoff.
    With multiple brokers, we need more patience and resilience.
    
    Args:
        max_retries: Maximum number of retry attempts
        initial_backoff: Initial backoff time in seconds
    
    Returns:
        True if Kafka connection was successful, False otherwise
    """
    backoff = initial_backoff
    
    for attempt in range(1, max_retries + 1):
        try:
            # Get the bootstrap servers from environment or settings
            bootstrap_servers = settings.kafka.bootstrap_servers
            log.info(f"Attempt {attempt}/{max_retries} to connect to Kafka Cluster at {bootstrap_servers}")
            
            # Try to create a producer to test connection
            producer = Producer({
                'bootstrap.servers': bootstrap_servers,
                'socket.timeout.ms': 10000, # Longer timeout for multi-broker setup
                'message.timeout.ms': 1000
            })
            producer.flush(timeout=10) # Increased timeout for multi-broker setup
            
            log.info("Successfully connected to Kafka cluster!")
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

def main():
    """
    Main function to run the IoT data generator and producer.
    Handles connection to the multi-broker Kafka cluster and continuously generates and sends IoT sensor data.
    """ 
    # Setup signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    log.info("Starting IoT Data Producer Service")
    log.info(f"Kafka bootstrap servers: {settings.kafka.bootstrap_servers}")
    log.info(f"Kafka topic: {settings.kafka.topic_name}")
    log.info(f"IoT simulator configured with {settings.iot_simulator.num_devices} devices")
    log.info(f"Data generation interval: {settings.iot_simulator.data_generation_interval_sec} seconds")
    
    # Wait for Kafka to be ready - important in a multi-broker environment
    # where brokers may take longer to elect a controller and become ready
    if not wait_for_kafka():
        log.error("Kafka is not available. Exiting.")
        sys.exit(1)
    
    try:
        # Create Kafka producer
        producer = KafkaProducer()
        
        # Create IoT simulator
        simulator = IoTSimulator()
        
        # Start continuous data generation
        log.info("Starting continuous IoT data generation...")
        
        while running:
            # Generate batch or readings from simulated devices
            readings = simulator.generate_batch()

            # Send the readings to Kafka - will be distributed across brokers
            producer.send_batch(readings)
            
            # Wait before generating next batch
            time.sleep(settings.iot_simulator.data_generation_interval_sec)
            
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

