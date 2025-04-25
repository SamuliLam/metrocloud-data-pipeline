import threading
import time
import signal
import sys
from typing import List, Dict, Any

from src.utils.logger import log
from src.config.config import settings
from src.data_generator.iot_simulator import IoTSimulator
from src.data_ingestion.producer import KafkaProducer
from src.data_ingestion.consumer import KafkaConsumer

def signal_handler(sig, frame):
    # Handle termination signals for graceful shutdown
    global running
    log.info(f"Caught signal {sig}. Stopping application...")
    running = False

def producer_task():
    # Task function for the producer thread.
    try:
        # Create Kafka producer
        producer = KafkaProducer()
        
        # Create IoT simulator
        simulator = IoTSimulator()
        
        # Function to send batch of readings to Kafka
        def send_to_kafka(readings: List[Dict[str, Any]]):
            producer.send_batch(readings)
        
        # Start continuous data generation
        log.info("Starting IoT data generation...")
        while running:
            readings = simulator.generate_batch()
            send_to_kafka(readings)
            time.sleep(settings.iot_simulator.data_generation_interval_sec)
            
    except Exception as e:
        log.error(f"Error in producer task: {str(e)}")
    finally:
        if 'producer' in locals():
            producer.close()

def consumer_task():
    # Task function for the consumer thread.
    try:
        # Wait a bit to ensure the producer has started
        time.sleep(2)
        
        # Create custom consumer
        class AlertConsumer(KafkaConsumer):
            def process_message(self, message):
                device_id = message.get('device_id', 'unknown')
                device_type = message.get('device_type', 'unknown')
                value = message.get('value', 'unknown')
                unit = message.get('unit', '')
                
                # Check for anomalies or specific conditions
                if device_type == "temperature" and float(value) > 30:
                    log.warning(f"HIGH TEMPERATURE ALERT: Device {device_id} reported {value}{unit}")
                elif device_type == "humidity" and float(value) > 70:
                    log.warning(f"HIGH HUMIDITY ALERT: Device {device_id} reported {value}{unit}")
                elif device_type == "motion" and value == 1:
                    log.info(f"MOTION DETECTED: Device {device_id}")
                else:
                    log.info(f"Received data from device {device_id} ({device_type}): {value}{unit}")
        
        # Create consumer
        consumer = AlertConsumer()
        
        # Start consumption loop
        log.info("Starting IoT data consumption...")
        while running:
            consumer.consume_batch(batch_size=10, timeout=1.0)
            
    except Exception as e:
        log.error(f"Error in consumer task: {str(e)}")
    finally:
        if 'consumer' in locals():
            consumer.close()

def main():
    # Main application entry point.
    # Setup signal handlers and global running flag
    global running
    running = True

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Print application info
    log.info(f"Starting {settings.app_name}")
    log.info(f"Kafka bootstrap servers: {settings.kafka.bootstrap_servers}")
    log.info(f"Kafka topic: {settings.kafka.topic_name}")
    log.info(f"IoT simulator configured with {settings.iot_simulator.num_devices} devices")
    log.info(f"Data generation interval: {settings.iot_simulator.data_generation_interval_sec} seconds")
    
    try:
        # Create and start producer thread
        producer_thread = threading.Thread(target=producer_task)
        producer_thread.daemon = True # Prevents the program from exiting
        producer_thread.start()
        
        # Create and start consumer thread
        consumer_thread = threading.Thread(target=consumer_task)
        consumer_thread.daemon = True # Prevents the program from exiting
        consumer_thread.start()
        
        # Keep main thread alive and responsive (e.g., for signal handling)
        while running:
            time.sleep(0.1)
            
    except KeyboardInterrupt:
        log.info("Application interrupted by user")
    except Exception as e:
        log.error(f"Unexpected error: {str(e)}")
    finally:
        # Set running flag to False to stop threads
        running = False
        
        # Wait for threads to finish
        if 'producer_thread' in locals() and producer_thread.is_alive():
            producer_thread.join(timeout=2)
        if 'consumer_thread' in locals() and consumer_thread.is_alive():
            consumer_thread.join(timeout=2)
        
        log.info("Application shutdown complete")

if __name__ == "__main__":
    main()