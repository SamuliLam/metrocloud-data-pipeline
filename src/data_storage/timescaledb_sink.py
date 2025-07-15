"""
TimescaleDB Data Sink - Consumes data from Kafka and stores in TimescaleDB.
"""

import time
import signal
import threading
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta
from confluent_kafka import Consumer, KafkaError, KafkaException

from src.utils.logger import log
from src.config.config import settings
from src.data_ingestion.consumer import KafkaConsumer
from src.data_storage.database import db_manager
from src.data_storage.models import SensorReadingDTO
from src.utils.schema_registry import schema_registry


class TimescaleDBSink:
    """
    Data sink that consumes IoT sensor data from Kafka and stores it in TimescaleDB.
    
    This class handles:
    - Kafka consumption with Avro deserialization
    - Data validation and transformation
    - Batch insertion into TimescaleDB hypertables
    - Error handling and recovery
    - TimescaleDB-specific optimizations
    - Continuous aggregate refresh
    """
    
    def __init__(self):
        """
        Initialize the TimescaleDB data sink.
        """
        self.running = False
        self.batch = []
        self.last_commit_time = time.time()
        self.last_maintenance_time = time.time()
        self.stats = {
            'messages_processed': 0,
            'messages_stored': 0,
            'batch_count': 0,
            'errors': 0,
            'last_processed': None,
            'maintenance_runs': 0
        }
        
        # Initialize Kafka consumer with data sink group ID
        self.kafka_consumer = KafkaConsumer(
            group_id=settings.data_sink.consumer_group_id,
            auto_offset_reset='earliest'
        )
        
        # Verify TimescaleDB connection
        if not db_manager.wait_for_database():
            raise Exception("TimescaleDB is not available")
        
        log.info("TimescaleDB data sink initialized")
        log.info(f"Batch size: {settings.data_sink.batch_size}")
        log.info(f"Commit interval: {settings.data_sink.commit_interval} seconds")
        log.info(f"TimescaleDB chunk interval: {settings.timescaledb.chunk_time_interval}")
        log.info(f"Compression after: {settings.timescaledb.compression_after}")
        
        # Setup maintenance thread
        self.maintenance_thread = None
        self.setup_maintenance_thread()
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, sig, frame):
        """
        Handle termination signals for graceful shutdown.
        """
        log.info(f"Caught signal {sig}. Stopping TimescaleDB sink...")
        self.stop()
    
    def setup_maintenance_thread(self):
        """
        Setup periodic maintenance thread for TimescaleDB operations.
        """
        def maintenance_worker():
            """
            Worker function for periodic TimescaleDB maintenance.
            """
            while self.running:
                try:
                    # Sleep for 1 hour between maintenance runs
                    time.sleep(3600)
                    
                    if self.running:
                        log.info("Starting periodic TimescaleDB maintenance...")
                        
                        # Refresh continuous aggregates
                        if settings.timescaledb.enable_continuous_aggregates:
                            db_manager.refresh_continuous_aggregates()
                        
                        # Run vacuum and analyze
                        db_manager.vacuum_and_analyze()
                        
                        # Get hypertable info for monitoring
                        hypertable_info = db_manager.get_hypertable_info()
                        for table_name, info in hypertable_info.items():
                            chunks = info.get('num_chunks', 0)
                            total_mb = (info.get('total_bytes', 0) / 1024 / 1024) if info.get('total_bytes') else 0
                            log.info(f"Hypertable {table_name}: {chunks} chunks, {total_mb:.2f} MB")
                        
                        # Clean up old data using TimescaleDB retention policies
                        result = db_manager.cleanup_old_data()
                        if result['archived_rows'] > 0 or result['cleaned_rows'] > 0:
                            log.info(f"Maintenance cleanup: {result}")
                        
                        self.stats['maintenance_runs'] += 1
                        log.info("TimescaleDB maintenance completed")
                        
                except Exception as e:
                    log.error(f"Error in TimescaleDB maintenance thread: {str(e)}")
        
        self.maintenance_thread = threading.Thread(target=maintenance_worker, daemon=True)
    
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
            
            # Additional validation for TimescaleDB
            if not sensor_reading.device_id:
                log.warning("Message missing device_id, skipping")
                return None
            
            if not sensor_reading.device_type:
                log.warning(f"Message from device {sensor_reading.device_id} missing device_type, skipping")
                return None
            
            if not sensor_reading.unit:
                log.warning(f"Message from device {sensor_reading.device_id} missing unit, skipping")
                return None
            
            # Validate timestamp is reasonable (not too far in future/past)
            if sensor_reading.timestamp.tzinfo is None:
                sensor_reading.timestamp = sensor_reading.timestamp.replace(tzinfo=timezone.utc)

            now = datetime.now(timezone.utc)

            # Validate timestamp is within 24 hours
            if abs((sensor_reading.timestamp - now).total_seconds()) > 86400:  # 24 hours
                log.warning(f"Timestamp too far from current time for device {sensor_reading.device_id}, adjusting")
                sensor_reading.timestamp = now
            
            return sensor_reading
            
        except Exception as e:
            log.error(f"Error validating message: {str(e)}")
            log.debug(f"Invalid message: {message}")
            return None
    
    def add_to_batch(self, sensor_reading: SensorReadingDTO):
        """
        Add a sensor reading to the current batch with TimescaleDB optimizations.
        
        Args:
            sensor_reading: Validated sensor reading DTO
        """
        self.batch.append(sensor_reading.to_dict())
        
        # Check if we should commit the batch
        # Use larger batch sizes for TimescaleDB for better performance
        batch_size_threshold = max(settings.data_sink.batch_size, 100)
        
        should_commit = (
            len(self.batch) >= batch_size_threshold or
            (time.time() - self.last_commit_time) >= settings.data_sink.commit_interval
        )
        
        if should_commit:
            self.commit_batch()
    
    def commit_batch(self):
        """
        Commit the current batch to TimescaleDB using optimized batch insert.
        """
        if not self.batch:
            return
        
        batch_size = len(self.batch)
        retry_count = 0
        max_retries = settings.data_sink.max_retries
        
        while retry_count <= max_retries:
            try:
                # Insert batch into TimescaleDB using optimized batch insert
                start_time = time.time()
                rows_inserted = db_manager.insert_sensor_readings_batch(self.batch)
                insert_time = time.time() - start_time
                
                # Update statistics
                self.stats['messages_stored'] += rows_inserted
                self.stats['batch_count'] += 1
                self.stats['last_processed'] = datetime.utcnow().isoformat()
                
                # Clear batch and update commit time
                self.batch.clear()
                self.last_commit_time = time.time()
                
                if rows_inserted > 0:
                    rate = rows_inserted / insert_time if insert_time > 0 else 0
                    log.debug(f"TimescaleDB batch commit: {rows_inserted} rows in {insert_time:.2f}s ({rate:.0f} rows/sec)")
                
                break  # Success, exit retry loop
                
            except Exception as e:
                retry_count += 1
                self.stats['errors'] += 1
                
                if retry_count <= max_retries:
                    backoff_time = settings.data_sink.retry_backoff * retry_count
                    log.warning(f"TimescaleDB batch commit failed (attempt {retry_count}/{max_retries + 1}): {str(e)}")
                    log.info(f"Retrying in {backoff_time} seconds...")
                    time.sleep(backoff_time)
                else:
                    log.error(f"Failed to commit batch to TimescaleDB after {max_retries + 1} attempts: {str(e)}")
                    log.error(f"Dropping batch of {batch_size} readings")
                    self.batch.clear()  # Drop the failed batch
                    self.last_commit_time = time.time()
                    break
    
    def process_message(self, message: Dict[str, Any]):
        """
        Process a single message from Kafka for TimescaleDB storage.
        
        Args:
            message: Deserialized message from Kafka
        """
        try:
            # Update statistics
            self.stats['messages_processed'] += 1
            
            # Validate and transform the message
            sensor_reading = self.validate_and_transform_message(message)
            
            if sensor_reading:
                # Add to batch for TimescaleDB insertion
                self.add_to_batch(sensor_reading)
                
                # Log periodic status with TimescaleDB-specific metrics
                if self.stats['messages_processed'] % 500 == 0:  # Less frequent logging for high-throughput
                    log.info(f"Processed {self.stats['messages_processed']} messages, "
                           f"stored {self.stats['messages_stored']} readings, "
                           f"current batch size: {len(self.batch)}, "
                           f"maintenance runs: {self.stats['maintenance_runs']}")
            
        except Exception as e:
            log.error(f"Error processing message: {str(e)}")
            self.stats['errors'] += 1
    
    def start(self):
        """
        Start the TimescaleDB data sink service.
        """
        log.info("Starting TimescaleDB data sink service...")
        
        # Start maintenance thread
        if self.maintenance_thread:
            self.maintenance_thread.start()
        
        # Start consuming from Kafka
        self.running = True
        
        try:
            # Use the base consumer's consume_loop with our custom processor
            self.kafka_consumer.consume_loop(
                process_fn=self.process_message,
                timeout=1.0
            )
            
        except KeyboardInterrupt:
            log.info("TimescaleDB sink interrupted by user")
        except Exception as e:
            log.error(f"Error in TimescaleDB sink main loop: {str(e)}")
            raise
        finally:
            self.stop()
    
    def stop(self):
        """
        Stop the TimescaleDB data sink service.
        """
        log.info("Stopping TimescaleDB data sink service...")
        
        self.running = False
        
        # Commit any remaining batch
        if self.batch:
            log.info(f"Committing final batch of {len(self.batch)} readings to TimescaleDB...")
            self.commit_batch()
        
        # Close Kafka consumer
        if hasattr(self, 'kafka_consumer'):
            self.kafka_consumer.close()
        
        # Wait for maintenance thread to finish
        if self.maintenance_thread and self.maintenance_thread.is_alive():
            log.info("Waiting for maintenance thread to finish...")
            self.maintenance_thread.join(timeout=5)
        
        # Log final statistics
        self.log_statistics()
        
        log.info("TimescaleDB data sink stopped")
    
    def log_statistics(self):
        """
        Log current statistics with TimescaleDB-specific information.
        """
        log.info("=== TimescaleDB Data Sink Statistics ===")
        log.info(f"Messages processed: {self.stats['messages_processed']}")
        log.info(f"Messages stored: {self.stats['messages_stored']}")
        log.info(f"Batches committed: {self.stats['batch_count']}")
        log.info(f"Errors encountered: {self.stats['errors']}")
        log.info(f"Maintenance runs: {self.stats['maintenance_runs']}")
        log.info(f"Last processed: {self.stats['last_processed']}")
        log.info(f"Current batch size: {len(self.batch)}")
        
        # Calculate processing rate
        if self.stats['messages_processed'] > 0:
            success_rate = (self.stats['messages_stored'] / self.stats['messages_processed']) * 100
            log.info(f"Success rate: {success_rate:.2f}%")
        
        # Log TimescaleDB hypertable information
        try:
            hypertable_info = db_manager.get_hypertable_info()
            for table_name, info in hypertable_info.items():
                chunks = info.get('num_chunks', 0)
                total_mb = (info.get('total_bytes', 0) / 1024 / 1024) if info.get('total_bytes') else 0
                compression = "enabled" if info.get('compression_enabled') else "disabled"
                log.info(f"Hypertable {table_name}: {chunks} chunks, {total_mb:.2f} MB, compression {compression}")
        except Exception as e:
            log.warning(f"Could not get hypertable info: {str(e)}")
    
    def get_health_status(self) -> Dict[str, Any]:
        """
        Get health status of the TimescaleDB sink.
        
        Returns:
            Dictionary with health status information
        """
        # Test TimescaleDB connection
        db_healthy = db_manager.test_connection()
        
        # Check if we're processing messages recently
        recent_activity = False
        if self.stats['last_processed']:
            try:
                last_processed = datetime.fromisoformat(self.stats['last_processed'])
                recent_activity = (datetime.utcnow() - last_processed) < timedelta(minutes=5)
            except:
                pass
        
        # Get TimescaleDB-specific status
        timescaledb_info = {}
        try:
            hypertable_info = db_manager.get_hypertable_info()
            timescaledb_info = {
                'hypertables': hypertable_info,
                'chunk_interval': settings.timescaledb.chunk_time_interval,
                'compression_after': settings.timescaledb.compression_after,
                'retention_policy': settings.timescaledb.retention_policy
            }
        except Exception as e:
            timescaledb_info = {'error': str(e)}
        
        return {
            'running': self.running,
            'database_healthy': db_healthy,
            'recent_activity': recent_activity,
            'batch_size': len(self.batch),
            'statistics': self.stats.copy(),
            'timescaledb_info': timescaledb_info
        }
    
    def force_maintenance(self):
        """
        Force run maintenance operations.
        """
        log.info("Forcing TimescaleDB maintenance...")
        
        try:
            # Refresh continuous aggregates
            if settings.timescaledb.enable_continuous_aggregates:
                db_manager.refresh_continuous_aggregates()
            
            # Run vacuum and analyze
            db_manager.vacuum_and_analyze()
            
            # Clean up old data
            result = db_manager.cleanup_old_data()
            if result['archived_rows'] > 0 or result['cleaned_rows'] > 0:
                log.info(f"Forced maintenance cleanup: {result}")
            
            self.stats['maintenance_runs'] += 1
            log.info("Forced TimescaleDB maintenance completed")
            
        except Exception as e:
            log.error(f"Error in forced maintenance: {str(e)}")


class TimescaleDBSinkManager:
    """
    Manager class for the TimescaleDB sink with additional management features.
    """
    
    def __init__(self):
        """
        Initialize the sink manager.
        """
        self.sink = None
    
    def create_sink(self) -> TimescaleDBSink:
        """
        Create a new TimescaleDB sink instance.
        
        Returns:
            TimescaleDB sink instance
        """
        self.sink = TimescaleDBSink()
        return self.sink
    
    def start_sink(self):
        """
        Start the TimescaleDB sink service.
        """
        if not self.sink:
            self.sink = self.create_sink()
        
        self.sink.start()
    
    def stop_sink(self):
        """
        Stop the TimescaleDB sink service.
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
            log.info("Forcing commit of current batch to TimescaleDB...")
            self.sink.commit_batch()
    
    def force_maintenance(self):
        """
        Force run maintenance operations.
        """
        if self.sink:
            self.sink.force_maintenance()


# Create singleton instance
sink_manager = TimescaleDBSinkManager()