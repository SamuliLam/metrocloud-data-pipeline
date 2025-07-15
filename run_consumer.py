#!/usr/bin/env python3
"""
Script to run the IoT data consumer with Avro deserialization and Prometheus metrics.
This is intended to be run in a Docker container with a multi-broker Kafka cluster.
"""

import time
import signal
import sys
import random
import os

from src.utils.logger import log
from src.config.config import settings
from src.data_ingestion.consumer import KafkaConsumer, IoTAlertConsumer
from src.utils.schema_registry import schema_registry

def wait_for_kafka_and_schema_registry(max_retries=60, initial_backoff=1):
    """
    Wait for Kafka and Schema Registry to become available with exponential backoff.
    With multiple brokers and Schema Registry, connection attempts are more complex 
    and may require more time and retries.
    
    Args:
        max_retries: Maximum number of retry attempts
        initial_backoff: Initial backoff time in seconds
    
    Returns:
        True if connection was successful, False otherwise
    """
    backoff = initial_backoff
    
    for attempt in range(1, max_retries + 1):
        try:
            # Get the bootstrap servers from environment or settings
            bootstrap_servers = settings.kafka.bootstrap_servers
            log.info(f"Attempt {attempt}/{max_retries} to connect to Kafka cluster at {bootstrap_servers}")
            
            # Try to create a consumer to test connection
            from confluent_kafka import Consumer
            consumer_conf = settings.consumer.get_config()
            consumer = Consumer(consumer_conf)
            
            # List topics as a connectivity test
            device_metadata = consumer.list_topics(timeout=10)
            if device_metadata:
                log.info(f"Successfully listed {len(device_metadata.topics)} topics from Kafka")
            
            # Close the consumer properly
            consumer.close()
            
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
                # Add some jitter to the backoff
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
    log.info(f"Kafka consumer group ID: {settings.kafka.consumer_group_id}")
    log.info(f"Kafka auto offset reset: {settings.kafka.auto_offset_reset}")
    log.info(f"Schema Registry URL: {settings.schema_registry.url}")
    
    # Log consumer-specific configuration
    log.info("Consumer configuration:")
    log.info(f"Auto commit enabled: {settings.consumer.enable_auto_commit}")
    log.info(f"Auto commit interval: {settings.consumer.auto_commit_interval_ms}ms")
    log.info(f"Session timeout: {settings.consumer.session_timeout_ms}ms")
    log.info(f"Heartbeat interval: {settings.consumer.heartbeat_interval_ms}ms")
    log.info(f"Partition assignment strategy: {settings.consumer.partition_assignment_strategy}")

    # Log metrics configuration
    metrics_port = os.getenv("METRICS_PORT", "8001")
    log.info(f"Metrics server port: {metrics_port}")
    log.info(f"Metrics endpoint: http://localhost:{metrics_port}/metrics")

    # Log RuuviTag configuration if available
    if hasattr(settings, 'ruuvitag'):
        log.info("RuuviTag configuration:")
        log.info(f"Device type: {settings.ruuvitag.device_type}")
        log.info(f"Temperature thresholds: {settings.ruuvitag.anomaly_thresholds.get('temperature_min', -50)}°C to {settings.ruuvitag.anomaly_thresholds.get('temperature_max', 50)}°C")
        log.info(f"Humidity thresholds: {settings.ruuvitag.anomaly_thresholds.get('humidity_min', 15)}% to {settings.ruuvitag.anomaly_thresholds.get('humidity_max', 100)}%")
        log.info(f"Pressure thresholds: {settings.ruuvitag.anomaly_thresholds.get('pressure_min', 87000)} to {settings.ruuvitag.anomaly_thresholds.get('pressure_max', 108500)} Pa")
        log.info(f"Battery threshold: {settings.ruuvitag.anomaly_thresholds.get('battery_low', 2.0)}V")


def main():
    """
    Main function to run the IoT data consumer with Avro deserialization and Prometheus metrics.
    Handles connection to the multi-broker Kafka cluster,
    sets up the consumer, and processes incoming IoT data.
    """
    log.info("Starting IoT Data Consumer Service with Avro Deserialization and Prometheus Metrics")
    log_configuration()
    
    # Wait for Kafka and Schema Registry to be ready
    if not wait_for_kafka_and_schema_registry():
        log.error("Kafka cluster or Schema Registry is not available. Exiting.")
        sys.exit(1)
    
    try:
        # Create and start the consumer with multi-broker awareness and Avro deserialization
        consumer = IoTAlertConsumer()

        log.info("Consumer will now handle RuuviTag data as separate sensor readings")
        log.info("Each RuuviTag physical device now sends multiple messages (one per sensor)")
        log.info("Prometheus metrics server started and ready for scraping")

        # Setup signal handlers
        def signal_handler(sig, frame):
            log.info(f"Caught signal {sig}. Stopping consumer...")
            consumer.running = False
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Log metrics information
        metrics_port = os.getenv("METRICS_PORT", "8001")
        log.info(f"🔍 Metrics available at: http://localhost:{metrics_port}/metrics")
        log.info("📊 Prometheus can now scrape consumer metrics successfully")
        
        # Start consuming messages in a continuous loop
        log.info("Starting continuous IoT data consumption with Avro deserialization...")
        consumer.consume_loop(timeout=1.0)
        
    except KeyboardInterrupt:
        log.info("Consumer interrupted by user")
    except Exception as e:
        log.error(f"Unexpected error in consumer: {str(e)}")
        raise
    finally:
        # Ensure any cleanup happens
        if 'consumer' in locals():
            consumer.close()
        log.info("Consumer shutdown complete")

if __name__ == "__main__":
    main()