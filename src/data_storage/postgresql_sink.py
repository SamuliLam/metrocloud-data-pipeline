"""
PostgreSQL Data Sink - Consumes data from Kafka and stores in PostgreSQL.
"""

import time
import signal
import threading
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from confluent_kafka import Consumer, KafkaError, KafkaException

from src.utils.logger import log
from src.config.config import settings
from src.data_ingestion.consumer import KafkaConsumer
from src.data_storage.database import db_manager
from src.data_storage.models import SensorReadingDTO
from src.utils.schema_registry import schema_registry


class PostgreSQLSink:
    """
    Data sink that consumes IoT sensor data from Kafka and stores it in PostgreSQL.
    
    This class handles:
    - Kafka consumption with Avro deserialization
    - Data validation and transformation
    - Batch insertion into PostgreSQL
    - Error handling and recovery
    - Periodic data cleanup
    """
    
    def __init__(self):
        """
        Initialize the PostgreSQL data sink.
        """
        self.running = False
        self.batch = []
        self.last_commit_time = time.time()
        self.stats = {
            'messages_processed': 0,
            'messages_stored': 0,
            'batch_count': 0,
            'errors': 0,
            'last_processed': None
        }
        
        # Initialize Kafka consumer with data sink group ID
        self.kafka_consumer = KafkaConsumer(
            group_id=settings.data_sink.consumer_group_id,
            auto_offset_reset='earliest'
        )
        
        # Verify database connection
        if not db_manager.wait_for_database():
            raise Exception("PostgreSQL database is not available")
        
        log.info("PostgreSQL data sink initialized")
        log.info(f"Batch size: {settings.data_sink.batch_size}")
        log.info(f"Commit interval: {settings.data_sink.commit_interval} seconds")
        
        # Setup cleanup thread
        self.cleanup_thread = None
        self.setup_cleanup_thread()
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, sig, frame):
        """
        Handle termination signals for graceful shutdown.
        """
        log.info(f"Caught signal {sig}. Stopping data sink...")
        self.stop()
    
    def setup_cleanup_thread(self):
        """
        Setup periodic data cleanup thread.
        """
        def cleanup_worker():
            """
            Worker function for periodic data cleanup.
            """
            while self.running:
                try:
                    # Sleep for 1 hour between cleanup attempts
                    time.sleep(3600)
                    
                    if self.running:
                        log.info("Starting periodic data cleanup...")
                        result = db_manager.cleanup_old_data()
                        if result['archived_rows'] > 0 or result['cleaned_rows'] > 0:
                            log.info(f"Cleanup completed: {result}")
                        
                except Exception as e:
                    log.error(f"Error in cleanup thread: {str(e)}")
        
        self.cleanup_thread = threading.Thread(target=cleanup_worker, daemon=True)
    
    def validate_and_transform_message(self, message: Dict[str, Any]) -> Optional[SensorReadingDTO]:
        """
        Validate and transform a Kafka message into a SensorReadingDTO.
        
        Args:
            message: Kafka message data
            
        Returns:
            SensorReadingDTO or None if validation fails
        """
        try:
            # Create DTO from Kafka message (this will validate the data)
            sensor_reading = SensorReadingDTO.from_kafka_message(message)
            
            # Additional validation
            if not sensor_reading.device_id:
                log.warning("Message missing device_id, skipping")
                return None
            
            if not sensor_reading.device_type:
                log.warning(f"Message from device {sensor_reading.device_id} missing device_type, skipping")
                return None
            
            if not sensor_reading.unit:
                log.warning(f"Message from device {sensor_reading.device_id} missing unit, skipping")
                return None
            
            return sensor_reading
            
        except Exception as e:
            log.error(f"Error validating message: {str(e)}")
            log.debug(f"Invalid message: {message}")
            return None
    
    def add_to_batch(self, sensor_reading: SensorReadingDTO):
        """
        Add a sensor reading to the current batch.
        
        Args:
            sensor_reading: Validated sensor reading DTO
        """
        self.batch.append(sensor_reading.to_dict())
        
        # Check if we should commit the batch
        should_commit = (
            len(self.batch) >= settings.data_sink.batch_size or
            (time.time() - self.last_commit_time) >= settings.data_sink.commit_interval
        )
        
        if should_commit:
            self.commit_batch()
    
    def commit_batch(self):
        """
        Commit the current batch to PostgreSQL.
        """
        if not self.batch:
            return
        
        batch_size = len(self.batch)
        retry_count = 0
        max_retries = settings.data_sink.max_retries
        
        while retry_count <= max_retries:
            try:
                # Insert batch into database
                rows_inserted = db_manager.insert_sensor_readings_batch(self.batch)
                
                # Update statistics
                self.stats['messages_stored'] += rows_inserted
                self.stats['batch_count'] += 1
                self.stats['last_processed'] = datetime.utcnow().isoformat()
                
                # Clear batch and update commit time
                self.batch.clear()
                self.last_commit_time = time.time()
                
                if rows_inserted > 0:
                    log.debug(f"Committed batch: {rows_inserted} rows inserted")
                
                break  # Success, exit retry loop
                
            except Exception as e:
                retry_count += 1
                self.stats['errors'] += 1
                
                if retry_count <= max_retries:
                    backoff_time = settings.data_sink.retry_backoff * retry_count
                    log.warning(f"Batch commit failed (attempt {retry_count}/{max_retries + 1}): {str(e)}")
                    log.info(f"Retrying in {backoff_time} seconds...")
                    time.sleep(backoff_time)
                else:
                    log.error(f"Failed to commit batch after {max_retries + 1} attempts: {str(e)}")
                    log.error(f"Dropping batch of {batch_size} readings")
                    self.batch.clear()  # Drop the failed batch
                    self.last_commit_time = time.time()
                    break
    
    def process_message(self, message: Dict[str, Any]):
        """
        Process a single message from Kafka.
        
        Args:
            message: Deserialized message from Kafka
        """
        try:
            # Update statistics
            self.stats['messages_processed'] += 1
            
            # Validate and transform the message
            sensor_reading = self.validate_and_transform_message(message)
            
            if sensor_reading:
                # Add to batch for database insertion
                self.add_to_batch(sensor_reading)
                
                # Log periodic status
                if self.stats['messages_processed'] % 100 == 0:
                    log.info(f"Processed {self.stats['messages_processed']} messages, "
                           f"stored {self.stats['messages_stored']} readings, "
                           f"current batch size: {len(self.batch)}")
            
        except Exception as e:
            log.error(f"Error processing message: {str(e)}")
            self.stats['errors'] += 1
    
    def start(self):
        """
        Start the data sink service.
        """
        log.info("Starting PostgreSQL data sink service...")
        
        # Start cleanup thread
        if self.cleanup_thread:
            self.cleanup_thread.start()
        
        # Start consuming from Kafka
        self.running = True
        
        try:
            # Use the base consumer's consume_loop with our custom processor
            self.kafka_consumer.consume_loop(
                process_fn=self.process_message,
                timeout=1.0
            )
            
        except KeyboardInterrupt:
            log.info("Data sink interrupted by user")
        except Exception as e:
            log.error(f"Error in data sink main loop: {str(e)}")
            raise
        finally:
            self.stop()
    
    def stop(self):
        """
        Stop the data sink service.
        """
        log.info("Stopping PostgreSQL data sink service...")
        
        self.running = False
        
        # Commit any remaining batch
        if self.batch:
            log.info(f"Committing final batch of {len(self.batch)} readings...")
            self.commit_batch()
        
        # Close Kafka consumer
        if hasattr(self, 'kafka_consumer'):
            self.kafka_consumer.close()
        
        # Wait for cleanup thread to finish
        if self.cleanup_thread and self.cleanup_thread.is_alive():
            log.info("Waiting for cleanup thread to finish...")
            self.cleanup_thread.join(timeout=5)
        
        # Log final statistics
        self.log_statistics()
        
        log.info("PostgreSQL data sink stopped")
    
    def log_statistics(self):
        """
        Log current statistics.
        """
        log.info("=== Data Sink Statistics ===")
        log.info(f"Messages processed: {self.stats['messages_processed']}")
        log.info(f"Messages stored: {self.stats['messages_stored']}")
        log.info(f"Batches committed: {self.stats['batch_count']}")
        log.info(f"Errors encountered: {self.stats['errors']}")
        log.info(f"Last processed: {self.stats['last_processed']}")
        log.info(f"Current batch size: {len(self.batch)}")
        
        # Calculate processing rate
        if self.stats['messages_processed'] > 0:
            success_rate = (self.stats['messages_stored'] / self.stats['messages_processed']) * 100
            log.info(f"Success rate: {success_rate:.2f}%")
    
    def get_health_status(self) -> Dict[str, Any]:
        """
        Get health status of the data sink.
        
        Returns:
            Dictionary with health status information
        """
        # Test database connection
        db_healthy = db_manager.test_connection()
        
        # Check if we're processing messages recently
        recent_activity = False
        if self.stats['last_processed']:
            try:
                last_processed = datetime.fromisoformat(self.stats['last_processed'])
                recent_activity = (datetime.utcnow() - last_processed) < timedelta(minutes=5)
            except:
                pass
        
        return {
            'running': self.running,
            'database_healthy': db_healthy,
            'recent_activity': recent_activity,
            'batch_size': len(self.batch),
            'statistics': self.stats.copy()
        }


class PostgreSQLSinkManager:
    """
    Manager class for the PostgreSQL sink with additional management features.
    """
    
    def __init__(self):
        """
        Initialize the sink manager.
        """
        self.sink = None
    
    def create_sink(self) -> PostgreSQLSink:
        """
        Create a new PostgreSQL sink instance.
        
        Returns:
            PostgreSQL sink instance
        """
        self.sink = PostgreSQLSink()
        return self.sink
    
    def start_sink(self):
        """
        Start the PostgreSQL sink service.
        """
        if not self.sink:
            self.sink = self.create_sink()
        
        self.sink.start()
    
    def stop_sink(self):
        """
        Stop the PostgreSQL sink service.
        """
        if self.sink:
            self.sink.stop()
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get current statistics from the sink.
        
        Returns:
            Statistics dictionary
        """
        if self.sink:
            return self.sink.get_health_status()
        return {}
    
    def force_commit(self):
        """
        Force commit any pending batch.
        """
        if self.sink and self.sink.batch:
            log.info("Forcing commit of current batch...")
            self.sink.commit_batch()


# Create singleton instance
sink_manager = PostgreSQLSinkManager()