"""
Script to run the RuuviTag adapter with ESP32 gateway.
This is intended to be run in a Docker container with a multi-broker Kafka cluster.
"""

import time
import signal
import sys
import os
import socket
import random

from src.utils.logger import log
from src.config.config import settings
from src.data_receiver.ruuvitag_adapter import RuuviTagAdapter

def wait_for_mqtt_and_kafka(max_retries=20, initial_backoff=2):
    """
    Wait for MQTT broker and Kafka to become available

    Args:
        max_retries: Maximum number of retries
        initial_backoff: Initial backoff time in seconds

    Returns:
        True if successful, False otherwise
    """
    import paho.mqtt.client as mqtt
    from confluent_kafka import Producer

    # Get broker information from config
    mqtt_broker = settings.mqtt.broker_host
    mqtt_port = settings.mqtt.broker_port
    keep_alive = settings.mqtt.keep_alive
    kafka_bootstrap_servers = settings.kafka.bootstrap_servers

    log.info(f"Waiting for services to be available: MQTT ({mqtt_broker}: {mqtt_port}) and Kafka ({kafka_bootstrap_servers})")

    backoff = initial_backoff

    for attempt in range(1, max_retries + 1):
        # Check MQTT
        log.info(f"Checking MQTT broker (Attempt {attempt}/{max_retries})")
        try:
            # Try to connect to MQTT broker
            client = mqtt.Client("mqtt_check")
            client.connect(mqtt_broker, mqtt_port, keep_alive)
            client.disconnect()
            log.info("MQTT broker is available")
            mqtt_ok = True
        except (socket.error, ConnectionRefusedError) as e:
            log.warning(f"MQTT broker not available, {e}")
            mqtt_ok = False

        # Check Kafka
        log.info(f"Checking Kafka (Attempt {attempt}/{max_retries})")
        try:
            # Try to create a producer to test connection
            producer_conf = settings.producer.get_config()
            producer = Producer(producer_conf)
            producer.flush(timeout=10)
            log.info("Kafka is available")
            kafka_ok = True
        except Exception as e:
            log.warning(f"Kafka not available {e}")
            kafka_ok = False

        if mqtt_ok and kafka_ok:
            return True
        
        if attempt < max_retries:
            # Add jitter to avoid thundering herd
            jitter = random.uniform(0, 0.1 * backoff)
            sleep_time = backoff * jitter

            log.info(f"Waiting for {sleep_time:.2f} seconds before retrying...")
            time.sleep(sleep_time)

            # Increase backoff for next attempt (exponential backoff)
            backoff = min(backoff * 1.5, 20)
        
    log.error(f"Failed to connect after {max_retries} attempts")
    if not mqtt_ok:
        log.error(f"MQTT broker at {mqtt_broker}: {mqtt_port} is not available")
    if not kafka_ok:
        log.error(f"Kafka at {kafka_bootstrap_servers} is not available")
    return False

def main():
    """
    Main function to run the RuuviTag adapter service
    """
    log.info("Starting RuuviTag Adapter Service")

    # Wait for MQTT and Kafka to be available
    if not wait_for_mqtt_and_kafka():
        log.error("MQTT broker or Kafka not available. Exiting.")
        sys.exit(1)

    try:
        # Create adapter using configuration from settings
        adapter = RuuviTagAdapter()

        log.info(f"Using MQTT broker: {adapter.mqtt_broker}: {adapter.mqtt_port}, topic: {adapter.mqtt_topic}")

        # Setup signal handlers
        def signal_handler(sig, frame):
            log.info(f"Caught signal {sig}. Stopping adapter...")
            adapter.stop()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Start adapter
        adapter.start()

        # Run until interrupted
        log.info(f"Adapter running, press CTRL+C to stop")
        while True:
            time.sleep(1)

    except Exception as e:
        log.error(f"Error in RuuviTag adapter service: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()