"""
Script to run the RuuviTag adapter with ESP32 gateway and comprehensive Prometheus metrics.
This is the REAL-TIME IoT data producer that handles actual sensor data from physical devices.
"""

import time
import signal
import sys
import os
import socket
import random
import traceback

from src.utils.logger import log
from src.config.config import settings
from src.data_receiver.ruuvitag_adapter import MetricsAwareRuuviTagAdapter
from src.utils.metrics import get_metrics_instance

def wait_for_mqtt_and_kafka(max_retries=20, initial_backoff=2):
    """
    Wait for MQTT broker and Kafka to become available with metrics tracking.

    Args:
        max_retries: Maximum number of retries
        initial_backoff: Initial backoff time in seconds

    Returns:
        True if successful, False otherwise
    """
    import paho.mqtt.client as mqtt
    from confluent_kafka import Producer

    # Get metrics instance for tracking connection attempts
    metrics = get_metrics_instance("adapter")

    # Get broker information from config
    mqtt_broker = settings.mqtt.broker_host
    mqtt_port = settings.mqtt.broker_port
    keep_alive = settings.mqtt.keep_alive
    kafka_bootstrap_servers = settings.kafka.bootstrap_servers

    log.info(f"Waiting for services to be available: MQTT ({mqtt_broker}:{mqtt_port}) and Kafka ({kafka_bootstrap_servers})")

    backoff = initial_backoff

    for attempt in range(1, max_retries + 1):
        mqtt_ok = False
        kafka_ok = False
        
        # Check MQTT
        log.info(f"Checking MQTT broker (Attempt {attempt}/{max_retries})")
        try:
            # Try to connect to MQTT broker
            client = mqtt.Client("mqtt_health_check")
            client.connect(mqtt_broker, mqtt_port, keep_alive)
            client.disconnect()
            log.info("MQTT broker is available")
            mqtt_ok = True
            metrics.set_connection_status(True, "mqtt")
        except (socket.error, ConnectionRefusedError) as e:
            log.warning(f"MQTT broker not available: {e}")
            metrics.set_connection_status(False, "mqtt")

        # Check Kafka
        log.info(f"Checking Kafka (Attempt {attempt}/{max_retries})")
        try:
            # Try to create a producer to test connection
            producer_conf = settings.producer.get_config()
            producer = Producer(producer_conf)
            producer.flush(timeout=10)
            log.info("Kafka is available")
            kafka_ok = True
            metrics.set_connection_status(True, "kafka")
        except Exception as e:
            log.warning(f"Kafka not available: {e}")
            metrics.set_connection_status(False, "kafka")

        if mqtt_ok and kafka_ok:
            log.info("All services are available!")
            return True
        
        if attempt < max_retries:
            # Add jitter to avoid thundering herd
            jitter = random.uniform(0, 0.1 * backoff)
            sleep_time = backoff + jitter

            log.info(f"Waiting for {sleep_time:.2f} seconds before retrying...")
            time.sleep(sleep_time)

            # Increase backoff for next attempt (exponential backoff)
            backoff = min(backoff * 1.5, 20)
        
    log.error(f"Failed to connect after {max_retries} attempts")
    if not mqtt_ok:
        log.error(f"MQTT broker at {mqtt_broker}:{mqtt_port} is not available")
        metrics.set_connection_status(False, "mqtt")
    if not kafka_ok:
        log.error(f"Kafka at {kafka_bootstrap_servers} is not available")
        metrics.set_connection_status(False, "kafka")
    return False

def log_configuration():
    """Log the current configuration settings for the real-time adapter."""
    log.info("RuuviTag Adapter Configuration (Real-time IoT Producer):")
    log.info(f"App name: {settings.app_name}")
    log.info(f"Environment: {settings.environment}")
    
    # MQTT Configuration
    log.info(f"MQTT broker: {settings.mqtt.broker_host}:{settings.mqtt.broker_port}")
    log.info(f"MQTT topic: {settings.mqtt.topic}")
    log.info(f"MQTT QoS: {settings.mqtt.qos}")
    log.info(f"MQTT keep alive: {settings.mqtt.keep_alive}s")
    
    # Kafka Configuration
    log.info(f"Kafka bootstrap servers: {settings.kafka.bootstrap_servers}")
    log.info(f"Kafka topic: {settings.kafka.topic_name}")
    log.info(f"Schema Registry URL: {settings.schema_registry.url}")
    
    # RuuviTag Configuration
    log.info(f"Device type: {settings.ruuvitag.device_type}")
    log.info(f"Default location: {settings.ruuvitag.default_location}")
    log.info(f"Battery range: {settings.ruuvitag.battery['min_voltage']}V - {settings.ruuvitag.battery['max_voltage']}V")
    
    # Anomaly Detection Thresholds
    thresholds = settings.ruuvitag.anomaly_thresholds
    log.info(f"Temperature thresholds: {thresholds['temperature_min']}°C to {thresholds['temperature_max']}°C")
    log.info(f"Humidity thresholds: {thresholds['humidity_min']}% to {thresholds['humidity_max']}%")
    log.info(f"Pressure thresholds: {thresholds['pressure_min']} to {thresholds['pressure_max']} Pa")
    log.info(f"Low battery threshold: {thresholds['battery_low']}V")
    
    # Metrics Configuration
    metrics_port = os.getenv("METRICS_PORT", "8002")
    log.info(f"Metrics endpoint: http://localhost:{metrics_port}/metrics")

def main():
    """
    Main function to run the real-time RuuviTag adapter service with comprehensive metrics.
    """
    log.info("=" * 80)
    log.info("Starting RuuviTag Adapter Service - Real-time IoT Data Producer")
    log.info("=" * 80)
    
    # Log configuration
    log_configuration()
    
    # Get metrics instance for main process tracking
    metrics = get_metrics_instance("adapter")

    # Wait for MQTT and Kafka to be available
    log.info("Checking service dependencies...")
    if not wait_for_mqtt_and_kafka():
        log.error("MQTT broker or Kafka not available. Exiting.")
        metrics.set_connection_status(False, "mqtt")
        metrics.set_connection_status(False, "kafka")
        sys.exit(1)

    log.info("All dependencies are ready. Starting RuuviTag adapter...")

    adapter = None
    try:
        # Create adapter with metrics support
        adapter = MetricsAwareRuuviTagAdapter()

        # Setup signal handlers for graceful shutdown
        def signal_handler(sig, frame):
            log.info(f"Caught signal {sig}. Stopping RuuviTag adapter...")
            if adapter:
                adapter.stop()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Log startup information
        log.info("RuuviTag Adapter Service Details:")
        log.info(f"- Service Type: Real-time IoT Data Producer")
        log.info(f"- Data Source: ESP32 Gateway → RuuviTag Sensors")
        log.info(f"- Data Flow: MQTT → Adapter → Kafka → TimescaleDB")
        log.info(f"- Metrics: Prometheus metrics enabled")
        log.info(f"- Health Check: Connection status monitoring")
        
        metrics_port = os.getenv("METRICS_PORT", "8002")
        log.info(f"- Metrics Endpoint: http://localhost:{metrics_port}/metrics")
        
        log.info("=" * 80)
        log.info("🚀 RuuviTag Adapter is now running!")
        log.info("📊 Monitor real-time metrics and performance")
        log.info("🔍 Check logs for data processing information")
        log.info("⚡ Processing live IoT sensor data from RuuviTags")
        log.info("=" * 80)

        # Start the adapter
        adapter.start()

        # Main monitoring loop with enhanced metrics reporting
        start_time = time.time()
        last_status_log = time.time()
        
        while True:
            current_time = time.time()
            
            # Log periodic status (every 5 minutes)
            if current_time - last_status_log >= 300:
                uptime = current_time - start_time
                hours = int(uptime // 3600)
                minutes = int((uptime % 3600) // 60)
                
                log.info("=" * 60)
                log.info(f"📈 RuuviTag Adapter Status Report")
                log.info(f"⏱️  Uptime: {hours}h {minutes}m")
                log.info(f"🔗 MQTT: {adapter.mqtt_broker}:{adapter.mqtt_port}")
                log.info(f"📦 Messages Processed: {adapter.message_count}")
                log.info(f"🏷️  Active Devices: {len(adapter.devices)}")
                
                # Calculate unique parent devices (actual RuuviTags)
                unique_ruuvitags = len(set(
                    device_data.get('parent_device', 'unknown') 
                    for device_data in adapter.devices.values()
                ))
                log.info(f"📡 RuuviTag Devices: {unique_ruuvitags}")
                
                # Log recent device activity
                recent_devices = []
                cutoff_time = current_time - 300  # Last 5 minutes
                for device_id, device_data in adapter.devices.items():
                    if device_data['last_seen'].timestamp() > cutoff_time:
                        recent_devices.append(device_data.get('parent_device', device_id))
                
                unique_recent = len(set(recent_devices))
                log.info(f"🔄 Recently Active: {unique_recent} devices")
                log.info(f"📊 Metrics: http://localhost:{metrics_port}/metrics")
                log.info("=" * 60)
                
                last_status_log = current_time
            
            # Sleep for monitoring interval
            time.sleep(settings.mqtt.reconnect_interval)

    except KeyboardInterrupt:
        log.info("RuuviTag adapter interrupted by user")
        if adapter:
            metrics.record_message_processed()  # Record clean shutdown
    except Exception as e:
        log.error(f"Critical error in RuuviTag adapter service: {e}")
        log.error(traceback.format_exc())
        if adapter:
            metrics.record_message_failed(error_type=type(e).__name__)
        sys.exit(1)
    finally:
        # Ensure cleanup happens
        if adapter:
            try:
                adapter.stop()
                log.info("RuuviTag adapter stopped gracefully")
            except Exception as e:
                log.error(f"Error during adapter shutdown: {e}")
        
        log.info("=" * 80)
        log.info("RuuviTag Adapter Service Shutdown Complete")
        log.info("=" * 80)


if __name__ == "__main__":
    main()