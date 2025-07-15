#!/usr/bin/env python3
"""
Script to run the TimescaleDB data sink service.
This consumes IoT data from Kafka and stores it in TimescaleDB database.
"""

import time
import signal
import sys
import os
import random

from src.utils.logger import log
from src.config.config import settings
from src.data_storage.timescaledb_sink import TimescaleDBSink
from src.data_storage.database import db_manager
from src.utils.schema_registry import schema_registry


def wait_for_dependencies(max_retries=60, initial_backoff=2):
    """
    Wait for Kafka, Schema Registry, and TimescaleDB to become available.
    
    Args:
        max_retries: Maximum number of retry attempts
        initial_backoff: Initial backoff time in seconds
    
    Returns:
        True if all dependencies are available, False otherwise
    """
    backoff = initial_backoff
    
    for attempt in range(1, max_retries + 1):
        try:
            dependencies_ready = True
            
            # Check TimescaleDB
            log.info(f"Attempt {attempt}/{max_retries}: Checking TimescaleDB connection...")
            if not db_manager.test_connection():
                log.warning("TimescaleDB is not ready")
                dependencies_ready = False
            else:
                log.info("TimescaleDB is ready")
            
            # Check Kafka
            log.info(f"Attempt {attempt}/{max_retries}: Checking Kafka cluster...")
            from confluent_kafka import Consumer
            consumer_conf = settings.consumer.get_config(
                group_id=f"health-check-{attempt}",
                auto_offset_reset='earliest'
            )
            consumer = Consumer(consumer_conf)
            
            # List topics as a connectivity test
            device_metadata = consumer.list_topics(timeout=10)
            if device_metadata:
                log.info(f"Kafka is ready with {len(device_metadata.topics)} topics")
            else:
                log.warning("Kafka device metadata retrieval failed")
                dependencies_ready = False
            
            consumer.close()
            
            # Check Schema Registry
            log.info(f"Attempt {attempt}/{max_retries}: Checking Schema Registry...")
            try:
                subjects = schema_registry.get_subjects()
                log.info(f"Schema Registry is ready with subjects: {subjects}")
            except Exception as e:
                log.warning(f"Schema Registry check failed: {str(e)}")
                dependencies_ready = False
            
            if dependencies_ready:
                log.info("All dependencies are ready!")
                return True
            
        except Exception as e:
            log.warning(f"Dependency check failed: {str(e)}")
        
        if attempt < max_retries:
            # Add jitter to avoid thundering herd
            jitter = random.uniform(0, 0.1 * backoff)
            sleep_time = backoff + jitter
            
            log.info(f"Retrying in {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)
            
            # Exponential backoff with cap
            backoff = min(backoff * 1.5, 30)
        else:
            log.error(f"Dependencies not ready after {max_retries} attempts")
            return False


def log_configuration():
    """
    Log the current configuration settings.
    """
    log.info("TimescaleDB Data Sink Configuration:")
    log.info(f"App name: {settings.app_name}")
    log.info(f"Environment: {settings.environment}")
    
    # Kafka settings
    log.info(f"Kafka bootstrap servers: {settings.kafka.bootstrap_servers}")
    log.info(f"Kafka topic: {settings.kafka.topic_name}")
    log.info(f"Consumer group ID: {settings.data_sink.consumer_group_id}")
    
    # TimescaleDB settings
    log.info(f"TimescaleDB host: {settings.timescaledb.host}:{settings.timescaledb.port}")
    log.info(f"Database name: {settings.timescaledb.database}")
    log.info(f"Username: {settings.timescaledb.username}")
    log.info(f"Main table: {settings.timescaledb.main_table}")
    log.info(f"Archive table: {settings.timescaledb.archive_table}")
    
    # TimescaleDB specific settings
    log.info(f"Chunk time interval: {settings.timescaledb.chunk_time_interval}")
    log.info(f"Compression after: {settings.timescaledb.compression_after}")
    log.info(f"Drop after: {settings.timescaledb.drop_after}")
    log.info(f"Continuous aggregates enabled: {settings.timescaledb.enable_continuous_aggregates}")
    
    # Data sink settings
    log.info(f"Batch size: {settings.data_sink.batch_size}")
    log.info(f"Commit interval: {settings.data_sink.commit_interval} seconds")
    log.info(f"Max retries: {settings.data_sink.max_retries}")
    log.info(f"Retry backoff: {settings.data_sink.retry_backoff} seconds")


def check_database_setup():
    """
    Check if TimescaleDB tables exist and are properly set up.
    """
    try:
        log.info("Checking TimescaleDB setup...")
        
        # Check if main table exists and is a hypertable
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = :table_name
            );
        """
        
        result = db_manager.execute_query(query, {'table_name': settings.timescaledb.main_table})
        main_table_exists = result[0]['exists'] if result else False
        
        if not main_table_exists:
            log.error(f"Main table '{settings.timescaledb.main_table}' does not exist!")
            log.error("Please run the database initialization script first.")
            return False
        
        # Check if it's a hypertable
        hypertable_query = """
            SELECT hypertable_name 
            FROM timescaledb_information.hypertables 
            WHERE hypertable_name = :table_name
        """
        
        hypertable_result = db_manager.execute_query(hypertable_query, {'table_name': settings.timescaledb.main_table})
        is_hypertable = len(hypertable_result) > 0
        
        if not is_hypertable:
            log.error(f"Table '{settings.timescaledb.main_table}' is not a hypertable!")
            log.error("Please ensure TimescaleDB initialization completed successfully.")
            return False
        
        log.info(f"Hypertable '{settings.timescaledb.main_table}' verified")
        
        # Check if archive table exists
        result = db_manager.execute_query(query, {'table_name': settings.timescaledb.archive_table})
        archive_table_exists = result[0]['exists'] if result else False
        
        if not archive_table_exists:
            log.warning(f"Archive table '{settings.timescaledb.archive_table}' does not exist!")
            log.warning("Archival functionality will not be available.")
        else:
            # Check if archive table is also a hypertable
            archive_hypertable_result = db_manager.execute_query(hypertable_query, {'table_name': settings.timescaledb.archive_table})
            archive_is_hypertable = len(archive_hypertable_result) > 0
            
            if archive_is_hypertable:
                log.info(f"Archive hypertable '{settings.timescaledb.archive_table}' verified")
            else:
                log.warning(f"Archive table '{settings.timescaledb.archive_table}' is not a hypertable")
        
        # Check TimescaleDB extension
        extension_query = """
            SELECT extname FROM pg_extension WHERE extname = 'timescaledb'
        """
        
        extension_result = db_manager.execute_query(extension_query)
        timescaledb_enabled = len(extension_result) > 0
        
        if not timescaledb_enabled:
            log.error("TimescaleDB extension is not installed!")
            return False
        
        log.info("TimescaleDB extension verified")
        
        # Get hypertable information
        hypertable_info = db_manager.get_hypertable_info()
        for table_name, info in hypertable_info.items():
            chunks = info.get('num_chunks', 0)
            total_mb = (info.get('total_bytes', 0) / 1024 / 1024) if info.get('total_bytes') else 0
            compression = "enabled" if info.get('compression_enabled') else "disabled"
            log.info(f"Hypertable {table_name}: {chunks} chunks, {total_mb:.2f} MB, compression {compression}")
        
        return True
        
    except Exception as e:
        log.error(f"Error checking TimescaleDB setup: {str(e)}")
        return False


def run_initial_health_checks():
    """
    Run initial health checks before starting the service.
    """
    log.info("Running initial health checks...")
    
    # Check database setup
    if not check_database_setup():
        return False
    
    # Test data insertion (dry run)
    try:
        test_data = {
            'device_id': 'health_check_device',
            'device_type': 'test_sensor',
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'value': 42.0,
            'unit': 'test_unit',
            'latitude': 60.1699,
            'longitude': 24.9384,
            'building': 'test_building',
            'floor': 1,
            'zone': 'test_zone',
            'room': 'test_room',
            'battery_level': 85.5,
            'signal_strength': -60.0,
            'firmware_version': '1.0.0',
            'is_anomaly': False,
            'status': 'ACTIVE',
            'maintenance_date': None,
            'device_metadata': {'test': True},
            'tags': ['health_check']
        }
        
        # Try inserting test data
        success = db_manager.insert_sensor_reading(test_data)
        if success:
            log.info("TimescaleDB write test successful")
            
            # Clean up test data
            cleanup_query = "DELETE FROM sensor_readings WHERE device_id = 'health_check_device'"
            db_manager.execute_non_query(cleanup_query)
            log.info("Test data cleaned up")
        else:
            log.error("TimescaleDB write test failed")
            return False
            
    except Exception as e:
        log.error(f"TimescaleDB write test failed: {str(e)}")
        return False
    
    # Check continuous aggregates if enabled
    if settings.timescaledb.enable_continuous_aggregates:
        try:
            # Check if continuous aggregates exist
            cagg_query = """
                SELECT view_name 
                FROM timescaledb_information.continuous_aggregates 
                WHERE view_name IN ('sensor_readings_hourly', 'sensor_readings_daily')
            """
            
            cagg_result = db_manager.execute_query(cagg_query)
            continuous_aggs = [row['view_name'] for row in cagg_result]
            
            if continuous_aggs:
                log.info(f"Continuous aggregates found: {', '.join(continuous_aggs)}")
            else:
                log.warning("No continuous aggregates found, but they are enabled in config")
                
        except Exception as e:
            log.warning(f"Could not check continuous aggregates: {str(e)}")
    
    log.info("All health checks passed!")
    return True


def main():
    """
    Main function to run the TimescaleDB data sink service.
    """
    log.info("Starting TimescaleDB Data Sink Service")
    log_configuration()
    
    # Wait for dependencies
    log.info("Waiting for dependencies to be ready...")
    if not wait_for_dependencies():
        log.error("Dependencies are not available. Exiting.")
        sys.exit(1)
    
    # Run health checks
    if not run_initial_health_checks():
        log.error("Initial health checks failed. Exiting.")
        sys.exit(1)
    
    # Initialize and start the data sink
    sink = None
    try:
        log.info("Initializing TimescaleDB data sink...")
        sink = TimescaleDBSink()
        
        # Setup signal handlers
        def signal_handler(sig, frame):
            log.info(f"Caught signal {sig}. Stopping data sink...")
            if sink:
                sink.stop()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Start the sink
        log.info("Starting TimescaleDB data sink service...")
        log.info("Service will consume IoT data from Kafka and store in TimescaleDB")
        log.info("TimescaleDB features: hypertables, compression, continuous aggregates")
        log.info("Press CTRL+C to stop the service")
        
        sink.start()
        
    except KeyboardInterrupt:
        log.info("Data sink interrupted by user")
    except Exception as e:
        log.error(f"Unexpected error in data sink: {str(e)}")
        raise
    finally:
        # Ensure cleanup happens
        if sink:
            sink.stop()
        
        # Close database connections
        db_manager.close()
        
        log.info("TimescaleDB data sink shutdown complete")


if __name__ == "__main__":
    main()